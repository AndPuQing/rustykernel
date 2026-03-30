use serde_json::Value;

use super::{KernelError, state::MessageLoopState};

impl MessageLoopState {
    pub(crate) fn debug_reply_content(&mut self, request: &Value) -> Result<Value, KernelError> {
        self.ensure_worker_started()?;
        let mut worker = self
            .worker
            .lock()
            .map_err(|_| KernelError::Worker("python worker mutex poisoned".to_owned()))?;
        let worker = worker
            .as_mut()
            .ok_or_else(|| KernelError::Worker("python worker was not available".to_owned()))?;
        self.debug.handle_request(worker, request)
    }
}
