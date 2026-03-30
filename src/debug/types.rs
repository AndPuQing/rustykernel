use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DebugListenEndpoint {
    pub host: String,
    pub port: u16,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DebugTransport {
    Inactive,
    Listening(DebugListenEndpoint),
    Connected(DebugListenEndpoint),
}

#[derive(Clone, Debug, PartialEq)]
pub struct DebugEventEnvelope {
    pub debug_epoch: u64,
    pub event: Value,
    pub parent_header: Option<Value>,
}
