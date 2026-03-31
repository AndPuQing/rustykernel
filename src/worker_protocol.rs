use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, value::RawValue};

pub const WORKER_PROTOCOL_VERSION: u32 = 2;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FrameType {
    Request,
    Response,
    Event,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerEnvelope<T> {
    pub protocol_version: u32,
    pub frame_type: FrameType,
    pub request_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub seq: Option<u64>,
    pub body: T,
}

impl<T> WorkerEnvelope<T> {
    pub fn request(request_id: u64, body: T) -> Self {
        Self {
            protocol_version: WORKER_PROTOCOL_VERSION,
            frame_type: FrameType::Request,
            request_id,
            seq: None,
            body,
        }
    }

    pub fn response(request_id: u64, body: T) -> Self {
        Self {
            protocol_version: WORKER_PROTOCOL_VERSION,
            frame_type: FrameType::Response,
            request_id,
            seq: None,
            body,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct RawWorkerEnvelope {
    pub protocol_version: u32,
    pub frame_type: FrameType,
    pub request_id: u64,
    #[serde(default)]
    pub seq: Option<u64>,
    pub body: Box<RawValue>,
}

impl RawWorkerEnvelope {
    pub fn deserialize_body<T: serde::de::DeserializeOwned>(&self) -> Result<T, serde_json::Error> {
        serde_json::from_str(self.body.get())
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "request_type", rename_all = "snake_case")]
pub enum WorkerRequest {
    Execute {
        code: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        subshell_id: Option<String>,
        user_expressions: Value,
        execution_count: u32,
        silent: bool,
        store_history: bool,
    },
    InputReply {
        value: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        error: Option<String>,
    },
    Complete {
        code: String,
        cursor_pos: usize,
    },
    IsComplete {
        code: String,
    },
    Inspect {
        code: String,
        cursor_pos: usize,
        detail_level: u8,
    },
    CommOpen {
        comm_id: String,
        target_name: String,
        data: Value,
        metadata: Value,
    },
    CommMsg {
        comm_id: String,
        data: Value,
        metadata: Value,
    },
    CommClose {
        comm_id: String,
        data: Value,
        metadata: Value,
    },
    KernelInfo,
    DebugListen,
    Debug {
        message: Value,
    },
    Interrupt,
    CreateSubshell,
    DeleteSubshell {
        subshell_id: String,
    },
    ListSubshell,
}

#[derive(Debug, Deserialize)]
pub struct ExecutionDisplay {
    #[serde(default)]
    pub data: Value,
    #[serde(default)]
    pub metadata: Value,
}

#[derive(Debug, Deserialize)]
pub struct ExecutionDisplayEvent {
    pub msg_type: String,
    #[serde(default)]
    pub content: Value,
    #[serde(default)]
    pub data: Value,
    #[serde(default)]
    pub metadata: Value,
    #[serde(default)]
    pub transient: Value,
}

#[derive(Debug, Deserialize)]
pub struct WorkerCommEvent {
    pub msg_type: String,
    #[serde(default)]
    pub content: Value,
}

#[derive(Debug, Deserialize)]
pub struct WorkerDebugEvent {
    #[serde(default = "debug_event_msg_type")]
    pub msg_type: String,
    #[serde(default)]
    pub content: Value,
}

fn debug_event_msg_type() -> String {
    "debug_event".to_owned()
}

#[derive(Debug, Deserialize)]
pub struct ExecutionOutcome {
    pub status: String,
    #[serde(default)]
    pub payload: Vec<Value>,
    #[serde(default)]
    pub result: Option<ExecutionDisplay>,
    #[serde(default)]
    pub user_expressions: Map<String, Value>,
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

#[derive(Debug, Deserialize)]
pub struct IsCompleteOutcome {
    pub status: String,
    #[serde(default)]
    pub indent: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct WorkerKernelInfo {
    pub language_version: String,
    pub language_version_major: u8,
    pub language_version_minor: u8,
}

#[derive(Debug, Deserialize)]
pub struct CommOutcome {
    #[serde(default)]
    pub stdout: String,
    #[serde(default)]
    pub stderr: String,
    #[serde(default)]
    pub events: Vec<WorkerCommEvent>,
    #[serde(default)]
    pub registered: bool,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct WorkerDebugListen {
    pub available: bool,
    #[serde(default)]
    pub host: String,
    #[serde(default)]
    pub port: u16,
}

#[derive(Debug, Deserialize)]
pub struct WorkerStatusReply {
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub evalue: String,
}

#[derive(Debug, Deserialize)]
pub struct WorkerCreateSubshellReply {
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub subshell_id: String,
    #[serde(default)]
    pub evalue: String,
}

#[derive(Debug, Deserialize)]
pub struct WorkerListSubshellReply {
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub subshell_id: Vec<String>,
    #[serde(default)]
    pub evalue: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum WorkerEvent {
    InputRequest {
        #[serde(default)]
        prompt: String,
        #[serde(default)]
        password: bool,
    },
    Stream {
        name: String,
        #[serde(default)]
        text: String,
        #[serde(default = "worker_stream_source_default")]
        source: String,
    },
    Display {
        msg_type: String,
        #[serde(default)]
        content: Value,
        #[serde(default)]
        data: Value,
        #[serde(default)]
        metadata: Value,
        #[serde(default)]
        transient: Value,
    },
    Comm {
        msg_type: String,
        #[serde(default)]
        content: Value,
    },
    Debug {
        #[serde(default = "debug_event_msg_type")]
        msg_type: String,
        #[serde(default)]
        content: Value,
    },
}

fn worker_stream_source_default() -> String {
    "python".to_owned()
}

#[derive(Debug, Deserialize)]
#[serde(tag = "response_type", rename_all = "snake_case")]
pub enum WorkerResponse {
    Execute(ExecutionOutcome),
    Complete(CompletionOutcome),
    Inspect(InspectionOutcome),
    IsComplete(IsCompleteOutcome),
    KernelInfo(WorkerKernelInfo),
    DebugListen(WorkerDebugListen),
    Debug(Value),
    Comm(CommOutcome),
    CreateSubshell(WorkerCreateSubshellReply),
    DeleteSubshell(WorkerStatusReply),
    ListSubshell(WorkerListSubshellReply),
    Interrupt(WorkerStatusReply),
}
