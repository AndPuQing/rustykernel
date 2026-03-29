use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde_json::{Value, json};
use sysinfo::{Pid, System};

use crate::debug_session::DebugSession;
use crate::protocol::{
    IMPLEMENTATION, JUPYTER_PROTOCOL_VERSION, LANGUAGE, MessageHeader, MessageSigner,
};
use crate::worker::{ExecutionOutcome, PythonWorker, WorkerInterruptHandle, WorkerKernelInfo};

use super::comms::CommStore;
use super::execute::PendingExecute;
use super::history::HistoryStore;
use super::{ConnectionInfo, KernelError, process_is_kernel_descendant};

pub(crate) struct MessageLoopState {
    pub(crate) connection: ConnectionInfo,
    pub(crate) signer: MessageSigner,
    pub(crate) kernel_session: String,
    pub(crate) execution_count: u32,
    pub(crate) history: HistoryStore,
    pub(crate) comms: CommStore,
    pub(crate) worker: Arc<Mutex<Option<PythonWorker>>>,
    pub(crate) worker_interrupt: Option<WorkerInterruptHandle>,
    pub(crate) worker_kernel_info: WorkerKernelInfo,
    pub(crate) debug_session: DebugSession,
    pub(crate) pending_executes: HashMap<u64, PendingExecute>,
}

impl MessageLoopState {
    pub(crate) fn new(connection: &ConnectionInfo) -> Result<Self, KernelError> {
        let mut worker = PythonWorker::start()?;
        let worker_kernel_info = worker.kernel_info()?;
        let worker_interrupt = worker.interrupt_handle();
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
            worker: Arc::new(Mutex::new(Some(worker))),
            worker_interrupt: Some(worker_interrupt),
            worker_kernel_info,
            debug_session: DebugSession::new(),
            pending_executes: HashMap::new(),
        })
    }

    pub(crate) fn ensure_worker_started(&mut self) -> Result<(), KernelError> {
        if self.worker_interrupt.is_some() {
            return Ok(());
        }

        let worker = PythonWorker::start()?;
        self.worker_interrupt = Some(worker.interrupt_handle());
        self.worker
            .lock()
            .map_err(|_| KernelError::Worker("python worker mutex poisoned".to_owned()))?
            .replace(worker);
        Ok(())
    }

    pub(crate) fn with_worker<T>(
        &mut self,
        f: impl FnOnce(&mut PythonWorker) -> Result<T, KernelError>,
    ) -> Result<T, KernelError> {
        self.ensure_worker_started()?;
        let mut worker = self
            .worker
            .lock()
            .map_err(|_| KernelError::Worker("python worker mutex poisoned".to_owned()))?;
        let worker = worker
            .as_mut()
            .ok_or_else(|| KernelError::Worker("python worker was not available".to_owned()))?;
        f(worker)
    }

    pub(crate) fn worker_interrupt_handle(&self) -> Result<&WorkerInterruptHandle, KernelError> {
        self.worker_interrupt
            .as_ref()
            .ok_or_else(|| KernelError::Worker("python worker interrupt handle missing".to_owned()))
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
        let language_version = &self.worker_kernel_info.language_version;
        let language_version_major = self.worker_kernel_info.language_version_major;
        let language_version_minor = self.worker_kernel_info.language_version_minor;

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
        if self.worker_interrupt.is_some() {
            let worker_kernel_info = self.with_worker(|worker| {
                worker.restart()?;
                Ok((worker.kernel_info()?, worker.interrupt_handle()))
            })?;
            self.worker_kernel_info = worker_kernel_info.0;
            self.worker_interrupt = Some(worker_kernel_info.1);
        }
        self.execution_count = 0;
        self.history.start_new_session();
        self.comms.clear();
        self.pending_executes.clear();
        self.debug_session.reset()?;
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
        let has_live_debug_session = matches!(
            self.debug_session.transport()?,
            crate::debug_session::DebugTransport::Connected(_)
        );

        if has_subshell_execute {
            if has_live_debug_session {
                self.worker_interrupt_handle()?.interrupt()?;
                self.debug_session.reset()?;
            } else {
                self.with_worker(|worker| worker.interrupt_subshells())?;
            }
        }
        if has_main_execute {
            self.worker_interrupt_handle()?.interrupt()?;
        }

        Ok(())
    }

    pub(crate) fn is_executing(&self) -> bool {
        !self.pending_executes.is_empty()
    }
}
