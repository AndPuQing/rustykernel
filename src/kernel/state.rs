use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
use sysinfo::{Pid, System};

use crate::debug::DebugBridge;
use crate::protocol::{
    IMPLEMENTATION, JUPYTER_PROTOCOL_VERSION, LANGUAGE, MessageHeader, MessageSigner,
};
use crate::worker::{ExecutionOutcome, PythonWorker, WorkerInterruptHandle, WorkerKernelInfo};

use super::comms::CommStore;
use super::execute::{KernelEventSender, PendingExecute};
use super::history::HistoryStore;
use super::{ConnectionInfo, KernelError, process_is_kernel_descendant};

#[allow(dead_code)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum WorkerLifecycleState {
    Stopped,
    Starting,
    Ready,
    Executing,
    Interrupting,
    Restarting,
    Failed,
}

pub(crate) struct WorkerLifecycle {
    worker: Arc<Mutex<Option<PythonWorker>>>,
    interrupt: Option<WorkerInterruptHandle>,
    kernel_info: WorkerKernelInfo,
    epoch: u64,
    state: WorkerLifecycleState,
    active_executes: usize,
}

impl WorkerLifecycle {
    fn start(kernel_events: KernelEventSender) -> Result<Self, KernelError> {
        let epoch = 1;
        let (worker, kernel_info, interrupt) = Self::spawn_worker(kernel_events, epoch)?;
        Ok(Self {
            worker: Arc::new(Mutex::new(Some(worker))),
            interrupt: Some(interrupt),
            kernel_info,
            epoch,
            state: WorkerLifecycleState::Ready,
            active_executes: 0,
        })
    }

    fn spawn_worker(
        kernel_events: KernelEventSender,
        epoch: u64,
    ) -> Result<(PythonWorker, WorkerKernelInfo, WorkerInterruptHandle), KernelError> {
        let mut worker = PythonWorker::start(kernel_events, epoch)?;
        let kernel_info = worker.kernel_info()?;
        let interrupt = worker.interrupt_handle();
        Ok((worker, kernel_info, interrupt))
    }

    fn replace_worker(&mut self, worker: PythonWorker) -> Result<(), KernelError> {
        self.worker
            .lock()
            .map_err(|_| KernelError::Worker("python worker mutex poisoned".to_owned()))?
            .replace(worker);
        Ok(())
    }

    fn take_worker(&mut self) -> Result<Option<PythonWorker>, KernelError> {
        self.worker
            .lock()
            .map_err(|_| KernelError::Worker("python worker mutex poisoned".to_owned()))
            .map(|mut worker| worker.take())
    }

    fn set_failed(&mut self) {
        self.interrupt = None;
        self.active_executes = 0;
        self.state = WorkerLifecycleState::Failed;
    }

    fn set_ready(&mut self, kernel_info: WorkerKernelInfo, interrupt: WorkerInterruptHandle) {
        self.kernel_info = kernel_info;
        self.interrupt = Some(interrupt);
        self.active_executes = 0;
        self.state = WorkerLifecycleState::Ready;
    }

    pub(crate) fn state(&self) -> WorkerLifecycleState {
        self.state
    }

    pub(crate) fn epoch(&self) -> u64 {
        self.epoch
    }

    pub(crate) fn kernel_info(&self) -> &WorkerKernelInfo {
        &self.kernel_info
    }

    pub(crate) fn ensure_started(
        &mut self,
        kernel_events: KernelEventSender,
    ) -> Result<(), KernelError> {
        if !matches!(
            self.state,
            WorkerLifecycleState::Stopped | WorkerLifecycleState::Failed
        ) {
            return Ok(());
        }

        self.state = WorkerLifecycleState::Starting;
        let _ = self.take_worker()?;

        match Self::spawn_worker(kernel_events, self.epoch) {
            Ok((worker, kernel_info, interrupt)) => {
                self.replace_worker(worker)?;
                self.set_ready(kernel_info, interrupt);
                Ok(())
            }
            Err(error) => {
                self.set_failed();
                Err(error)
            }
        }
    }

    pub(crate) fn with_worker<T>(
        &mut self,
        kernel_events: KernelEventSender,
        f: impl FnOnce(&mut PythonWorker) -> Result<T, KernelError>,
    ) -> Result<T, KernelError> {
        self.ensure_started(kernel_events)?;
        let mut worker = self
            .worker
            .lock()
            .map_err(|_| KernelError::Worker("python worker mutex poisoned".to_owned()))?;
        let worker = worker
            .as_mut()
            .ok_or_else(|| KernelError::Worker("python worker was not available".to_owned()))?;
        f(worker)
    }

    pub(crate) fn interrupt_handle(&self) -> Result<&WorkerInterruptHandle, KernelError> {
        self.interrupt
            .as_ref()
            .ok_or_else(|| KernelError::Worker("python worker interrupt handle missing".to_owned()))
    }

    pub(crate) fn on_execute_started(&mut self) {
        self.active_executes += 1;
        if matches!(self.state, WorkerLifecycleState::Ready) {
            self.state = WorkerLifecycleState::Executing;
        }
    }

    pub(crate) fn on_execute_finished(&mut self) -> Result<(), KernelError> {
        if self.active_executes == 0 {
            return Err(KernelError::Worker(
                "worker lifecycle lost track of active executes".to_owned(),
            ));
        }

        self.active_executes -= 1;
        if self.active_executes == 0 {
            self.state = WorkerLifecycleState::Ready;
        } else if !matches!(self.state, WorkerLifecycleState::Interrupting) {
            self.state = WorkerLifecycleState::Executing;
        }
        Ok(())
    }

    pub(crate) fn mark_interrupting(&mut self) -> WorkerLifecycleState {
        let previous = self.state;
        if self.active_executes > 0 {
            self.state = WorkerLifecycleState::Interrupting;
        }
        previous
    }

    pub(crate) fn restore_state(&mut self, state: WorkerLifecycleState) {
        self.state = state;
    }

    pub(crate) fn restart(&mut self, kernel_events: KernelEventSender) -> Result<(), KernelError> {
        self.state = WorkerLifecycleState::Restarting;
        self.active_executes = 0;
        self.epoch += 1;
        let epoch = self.epoch;

        match self.take_worker()? {
            Some(mut worker) => {
                if let Err(error) = worker.restart(kernel_events, epoch) {
                    self.set_failed();
                    return Err(error);
                }
                let kernel_info = worker.kernel_info()?;
                let interrupt = worker.interrupt_handle();
                self.replace_worker(worker)?;
                self.set_ready(kernel_info, interrupt);
                Ok(())
            }
            None => match Self::spawn_worker(kernel_events, epoch) {
                Ok((worker, kernel_info, interrupt)) => {
                    self.replace_worker(worker)?;
                    self.set_ready(kernel_info, interrupt);
                    Ok(())
                }
                Err(error) => {
                    self.set_failed();
                    Err(error)
                }
            },
        }
    }
}

pub(crate) struct MessageLoopState {
    pub(crate) connection: ConnectionInfo,
    pub(crate) signer: MessageSigner,
    pub(crate) kernel_session: String,
    pub(crate) execution_count: u32,
    pub(crate) history: HistoryStore,
    pub(crate) comms: CommStore,
    pub(crate) worker: WorkerLifecycle,
    pub(crate) kernel_events: KernelEventSender,
    pub(crate) debug: DebugBridge,
    pub(crate) pending_executes: HashMap<u64, PendingExecute>,
}

impl MessageLoopState {
    pub(crate) fn new(
        connection: &ConnectionInfo,
        kernel_events: KernelEventSender,
    ) -> Result<Self, KernelError> {
        Ok(Self {
            connection: connection.clone(),
            signer: MessageSigner::new(&connection.signature_scheme, &connection.key)?,
            kernel_session: connection
                .kernel_name
                .clone()
                .unwrap_or_else(|| IMPLEMENTATION.to_owned()),
            execution_count: 0,
            history: HistoryStore::new(),
            comms: CommStore::new(),
            worker: WorkerLifecycle::start(kernel_events.clone())?,
            kernel_events,
            debug: DebugBridge::new(),
            pending_executes: HashMap::new(),
        })
    }

    pub(crate) fn with_worker<T>(
        &mut self,
        f: impl FnOnce(&mut PythonWorker) -> Result<T, KernelError>,
    ) -> Result<T, KernelError> {
        self.worker.with_worker(self.kernel_events.clone(), f)
    }

    pub(crate) fn worker_interrupt_handle(&self) -> Result<&WorkerInterruptHandle, KernelError> {
        self.worker.interrupt_handle()
    }

    pub(crate) fn worker_epoch(&self) -> u64 {
        self.worker.epoch()
    }

    pub(crate) fn worker_lifecycle_state(&self) -> WorkerLifecycleState {
        self.worker.state()
    }

    pub(crate) fn on_execute_started(&mut self) {
        self.worker.on_execute_started();
    }

    pub(crate) fn on_execute_finished(&mut self) -> Result<(), KernelError> {
        self.worker.on_execute_finished()
    }

    pub(crate) fn next_execution_count(&mut self, content: &Value) -> u32 {
        let silent = content
            .get("silent")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let store_history = content
            .get("store_history")
            .and_then(Value::as_bool)
            .unwrap_or(!silent);

        if silent || !store_history {
            self.execution_count
        } else {
            self.execution_count += 1;
            self.execution_count
        }
    }

    pub(crate) fn new_header(&self, msg_type: &str) -> MessageHeader {
        MessageHeader::new(msg_type, &self.kernel_session)
    }

    pub(crate) fn connect_reply_content(&self) -> Value {
        json!({
            "status": "ok",
            "shell_port": self.connection.shell_port,
            "iopub_port": self.connection.iopub_port,
            "stdin_port": self.connection.stdin_port,
            "control_port": self.connection.control_port,
            "hb_port": self.connection.hb_port,
        })
    }

    pub(crate) fn usage_reply_content(&self) -> Value {
        let mut system = System::new_all();
        system.refresh_all();

        let kernel_pid = Pid::from_u32(std::process::id());
        let kernel_processes = system
            .processes()
            .keys()
            .copied()
            .filter(|pid| process_is_kernel_descendant(&system, *pid, kernel_pid));

        let kernel_cpu = kernel_processes
            .clone()
            .filter_map(|pid| system.process(pid))
            .map(|process| f64::from(process.cpu_usage()))
            .sum::<f64>();
        let kernel_memory = kernel_processes
            .filter_map(|pid| system.process(pid))
            .map(|process| process.memory())
            .sum::<u64>();

        let mut content = json!({
            "hostname": System::host_name().unwrap_or_default(),
            "pid": std::process::id(),
            "kernel_cpu": kernel_cpu,
            "kernel_memory": kernel_memory,
            "cpu_count": system.cpus().len(),
            "host_virtual_memory": {
                "total": system.total_memory(),
                "available": system.available_memory(),
                "used": system.used_memory(),
                "free": system.free_memory(),
                "total_swap": system.total_swap(),
                "used_swap": system.used_swap(),
                "free_swap": system.free_swap(),
            },
        });

        let host_cpu_percent = f64::from(system.global_cpu_usage());
        if host_cpu_percent > 0.0 {
            content["host_cpu_percent"] = json!(host_cpu_percent);
        }

        content
    }

    pub(crate) fn record_history(&mut self, line: u32, input: &str, outcome: &ExecutionOutcome) {
        self.history.record(line, input, outcome);
    }

    pub(crate) fn history_reply_content(&self, request: &Value) -> Value {
        self.history.reply_content(request)
    }

    pub(crate) fn register_comm(&mut self, content: &Value) {
        self.comms.register(content);
    }

    pub(crate) fn close_comm(&mut self, content: &Value) {
        self.comms.close(content);
    }

    pub(crate) fn comm_info_reply_content(&self, request: &Value) -> Value {
        self.comms.reply_content(request)
    }

    pub(crate) fn kernel_info_content(&self) -> Value {
        let worker_kernel_info = self.worker.kernel_info();
        let language_version = &worker_kernel_info.language_version;
        let language_version_major = worker_kernel_info.language_version_major;
        let language_version_minor = worker_kernel_info.language_version_minor;

        json!({
            "status": "ok",
            "protocol_version": JUPYTER_PROTOCOL_VERSION,
            "implementation": IMPLEMENTATION,
            "implementation_version": env!("CARGO_PKG_VERSION"),
            "banner": format!(
                "{IMPLEMENTATION} {} (Python {language_version})",
                env!("CARGO_PKG_VERSION"),
            ),
            "language": LANGUAGE,
            "language_version": language_version,
            "debugger": true,
            "help_links": [{
                "text": "Python Reference",
                "url": format!("https://docs.python.org/{language_version_major}.{language_version_minor}"),
            }],
            "supported_features": ["debugger", "kernel subshells"],
            "language_info": {
                "name": LANGUAGE,
                "version": language_version,
                "mimetype": "text/x-python",
                "file_extension": ".py",
                "pygments_lexer": format!("ipython{language_version_major}"),
                "nbconvert_exporter": "python",
                "codemirror_mode": {
                    "name": "ipython",
                    "version": language_version_major,
                },
            },
        })
    }

    pub(crate) fn create_subshell_reply_content(&mut self) -> Result<Value, KernelError> {
        let subshell_id = self.with_worker(|worker| worker.create_subshell())?;
        Ok(json!({
            "status": "ok",
            "subshell_id": subshell_id,
        }))
    }

    pub(crate) fn delete_subshell_reply_content(
        &mut self,
        request: &Value,
    ) -> Result<Value, KernelError> {
        let subshell_id = request
            .get("subshell_id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                KernelError::Worker("delete_subshell_request missing subshell_id".to_owned())
            })?;
        self.with_worker(|worker| worker.delete_subshell(subshell_id))?;
        Ok(json!({ "status": "ok" }))
    }

    pub(crate) fn list_subshell_reply_content(&mut self) -> Result<Value, KernelError> {
        let subshell_ids = self.with_worker(|worker| worker.list_subshell())?;
        Ok(json!({
            "status": "ok",
            "subshell_id": subshell_ids,
        }))
    }

    pub(crate) fn restart(&mut self) -> Result<(), KernelError> {
        if let Err(error) = self.worker.restart(self.kernel_events.clone()) {
            self.pending_executes.clear();
            return Err(error);
        }
        self.execution_count = 0;
        self.history.start_new_session();
        self.comms.clear();
        self.pending_executes.clear();
        self.debug.on_restart()?;
        Ok(())
    }

    pub(crate) fn interrupt(&mut self) -> Result<(), KernelError> {
        let has_main_execute = self
            .pending_executes
            .values()
            .any(|pending| pending.subshell_id.is_none());
        let has_subshell_execute = self
            .pending_executes
            .values()
            .any(|pending| pending.subshell_id.is_some());
        let has_live_debug_session = self.debug.is_connected()?;
        let previous_state = self.worker.mark_interrupting();

        let result = (|| -> Result<(), KernelError> {
            if has_subshell_execute {
                if has_live_debug_session {
                    self.worker_interrupt_handle()?.interrupt()?;
                    self.debug.on_subshell_interrupt()?;
                } else {
                    self.with_worker(|worker| worker.interrupt_subshells())?;
                }
            }
            if has_main_execute {
                self.worker_interrupt_handle()?.interrupt()?;
            }

            Ok(())
        })();

        if result.is_err() {
            self.worker.restore_state(previous_state);
        }

        result
    }

    pub(crate) fn is_executing(&self) -> bool {
        matches!(
            self.worker_lifecycle_state(),
            WorkerLifecycleState::Executing | WorkerLifecycleState::Interrupting
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{WorkerLifecycle, WorkerLifecycleState};
    use crate::kernel::KernelError;

    fn started_lifecycle() -> WorkerLifecycle {
        WorkerLifecycle {
            worker: std::sync::Arc::new(std::sync::Mutex::new(None)),
            interrupt: None,
            kernel_info: crate::worker::WorkerKernelInfo {
                language_version: "3.12.0".to_owned(),
                language_version_major: 3,
                language_version_minor: 12,
            },
            epoch: 1,
            state: WorkerLifecycleState::Ready,
            active_executes: 0,
        }
    }

    #[test]
    fn lifecycle_transitions_between_ready_executing_and_interrupting() -> Result<(), KernelError> {
        let mut lifecycle = started_lifecycle();

        assert_eq!(lifecycle.state(), WorkerLifecycleState::Ready);
        lifecycle.on_execute_started();
        assert_eq!(lifecycle.state(), WorkerLifecycleState::Executing);

        let previous = lifecycle.mark_interrupting();
        assert_eq!(previous, WorkerLifecycleState::Executing);
        assert_eq!(lifecycle.state(), WorkerLifecycleState::Interrupting);

        lifecycle.on_execute_finished()?;
        assert_eq!(lifecycle.state(), WorkerLifecycleState::Ready);
        assert_eq!(lifecycle.active_executes, 0);
        Ok(())
    }

    #[test]
    fn lifecycle_keeps_interrupting_until_last_execute_finishes() -> Result<(), KernelError> {
        let mut lifecycle = started_lifecycle();

        lifecycle.on_execute_started();
        lifecycle.on_execute_started();
        lifecycle.mark_interrupting();
        lifecycle.on_execute_finished()?;
        assert_eq!(lifecycle.state(), WorkerLifecycleState::Interrupting);

        lifecycle.on_execute_finished()?;
        assert_eq!(lifecycle.state(), WorkerLifecycleState::Ready);
        Ok(())
    }

    #[test]
    fn lifecycle_reports_unbalanced_execute_completion() {
        let mut lifecycle = started_lifecycle();
        let error = lifecycle
            .on_execute_finished()
            .expect_err("completion without execute should fail");
        assert!(matches!(error, KernelError::Worker(_)));
    }
}
