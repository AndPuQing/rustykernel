use serde_json::Value;

use super::{KernelError, state::MessageLoopState};

impl MessageLoopState {
    pub(crate) fn debug_reply_content(&mut self, request: &Value) -> Result<Value, KernelError> {
        let kernel_events = self.kernel_events.clone();
        let debug = &mut self.debug;
        self.worker.with_worker(kernel_events, |worker| {
            debug.handle_request(worker, request)
        })
    }
}
