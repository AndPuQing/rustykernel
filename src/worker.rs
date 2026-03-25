use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::kernel::KernelError;

const PYTHON_WORKER_SCRIPT: &str = include_str!("../python/rustykernel/worker_main.py");

#[derive(Debug, Deserialize)]
pub struct ExecutionDisplay {
    #[serde(default)]
    pub data: Value,
    #[serde(default)]
    pub metadata: Value,
}

#[derive(Debug, Deserialize)]
pub struct ExecutionOutcome {
    pub status: String,
    #[serde(default)]
    pub stdout: String,
    #[serde(default)]
    pub stderr: String,
    #[serde(default)]
    pub result: Option<ExecutionDisplay>,
    #[serde(default)]
    pub ename: Option<String>,
    #[serde(default)]
    pub evalue: Option<String>,
    #[serde(default)]
    pub traceback: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct CompletionOutcome {
    pub matches: Vec<String>,
    pub cursor_start: usize,
    pub cursor_end: usize,
    #[serde(default)]
    pub metadata: Value,
    #[serde(default)]
    pub status: String,
}

#[derive(Debug, Deserialize)]
pub struct InspectionOutcome {
    pub found: bool,
    #[serde(default)]
    pub data: Value,
    #[serde(default)]
    pub metadata: Value,
    #[serde(default)]
    pub status: String,
}

#[derive(Serialize)]
#[serde(tag = "kind")]
enum WorkerRequest<'a> {
    #[serde(rename = "execute")]
    Execute { id: u64, code: &'a str },
    #[serde(rename = "complete")]
    Complete {
        id: u64,
        code: &'a str,
        cursor_pos: usize,
    },
    #[serde(rename = "inspect")]
    Inspect {
        id: u64,
        code: &'a str,
        cursor_pos: usize,
        detail_level: u8,
    },
}

#[derive(Deserialize)]
struct WorkerEnvelope<T> {
    id: u64,
    #[serde(flatten)]
    payload: T,
}

pub struct PythonWorker {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    next_id: u64,
}

impl PythonWorker {
    pub fn start() -> Result<Self, KernelError> {
        let mut child = Command::new("python3")
            .arg("-u")
            .arg("-c")
            .arg(PYTHON_WORKER_SCRIPT)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(KernelError::Io)?;

        let stdin = child.stdin.take().ok_or_else(|| {
            KernelError::Worker("python worker stdin pipe was not available".to_owned())
        })?;
        let stdout = child.stdout.take().ok_or_else(|| {
            KernelError::Worker("python worker stdout pipe was not available".to_owned())
        })?;

        Ok(Self {
            child,
            stdin,
            stdout: BufReader::new(stdout),
            next_id: 1,
        })
    }

    pub fn execute(&mut self, code: &str) -> Result<ExecutionOutcome, KernelError> {
        let request_id = self.next_id;
        self.next_id += 1;
        let request = WorkerRequest::Execute {
            id: request_id,
            code,
        };
        let response: WorkerEnvelope<ExecutionOutcome> = self.send_request(&request, request_id)?;
        Ok(response.payload)
    }

    pub fn complete(
        &mut self,
        code: &str,
        cursor_pos: usize,
    ) -> Result<CompletionOutcome, KernelError> {
        let request_id = self.next_id;
        self.next_id += 1;
        let request = WorkerRequest::Complete {
            id: request_id,
            code,
            cursor_pos,
        };
        let response: WorkerEnvelope<CompletionOutcome> =
            self.send_request(&request, request_id)?;
        Ok(response.payload)
    }

    pub fn inspect(
        &mut self,
        code: &str,
        cursor_pos: usize,
        detail_level: u8,
    ) -> Result<InspectionOutcome, KernelError> {
        let request_id = self.next_id;
        self.next_id += 1;
        let request = WorkerRequest::Inspect {
            id: request_id,
            code,
            cursor_pos,
            detail_level,
        };
        let response: WorkerEnvelope<InspectionOutcome> =
            self.send_request(&request, request_id)?;
        Ok(response.payload)
    }

    pub fn restart(&mut self) -> Result<(), KernelError> {
        self.terminate();
        let replacement = Self::start()?;
        *self = replacement;
        Ok(())
    }

    fn stderr_summary(&mut self) -> String {
        match self.child.try_wait() {
            Ok(Some(status)) => format!("python worker exited unexpectedly with status {status}"),
            Ok(None) => "python worker closed its stdout unexpectedly".to_owned(),
            Err(error) => format!("python worker became unavailable: {error}"),
        }
    }

    fn terminate(&mut self) {
        let _ = self.stdin.flush();
        match self.child.try_wait() {
            Ok(Some(_)) => {}
            Ok(None) => {
                let _ = self.child.kill();
                let _ = self.child.wait();
            }
            Err(_) => {}
        }
    }

    fn send_request<T>(
        &mut self,
        request: &WorkerRequest<'_>,
        request_id: u64,
    ) -> Result<WorkerEnvelope<T>, KernelError>
    where
        T: for<'de> Deserialize<'de>,
    {
        let payload = serde_json::to_vec(request).map_err(|error| {
            KernelError::Worker(format!("failed to encode worker request: {error}"))
        })?;
        self.stdin.write_all(&payload).map_err(KernelError::Io)?;
        self.stdin.write_all(b"\n").map_err(KernelError::Io)?;
        self.stdin.flush().map_err(KernelError::Io)?;

        let mut line = String::new();
        let bytes_read = self.stdout.read_line(&mut line).map_err(KernelError::Io)?;
        if bytes_read == 0 {
            return Err(KernelError::Worker(self.stderr_summary()));
        }

        let response: WorkerEnvelope<T> = serde_json::from_str(&line).map_err(|error| {
            KernelError::Worker(format!("failed to decode worker response: {error}"))
        })?;

        if response.id != request_id {
            return Err(KernelError::Worker(format!(
                "worker response id mismatch: expected {}, got {}",
                request_id, response.id
            )));
        }

        Ok(response)
    }
}

impl Drop for PythonWorker {
    fn drop(&mut self) {
        self.terminate();
    }
}
