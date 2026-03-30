use std::collections::{HashMap, HashSet};

use serde_json::Value;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct DebugCache {
    pub initialized: bool,
    pub attached: bool,
    pub configured: bool,
    pub stopped_threads: HashSet<i64>,
    pub last_threads: Vec<Value>,
    pub last_stop_reason: Option<String>,
    pub all_threads_stopped: bool,
    pub capabilities: Option<Value>,
    pub source_parents: HashMap<String, Value>,
    pub synthetic_stack_frames: Vec<Value>,
    pub synthetic_scopes: HashMap<i64, Vec<Value>>,
    pub synthetic_variables: HashMap<i64, Vec<Value>>,
}

impl DebugCache {
    pub fn update_from_event(&mut self, event: &Value) {
        let event_name = event
            .get("event")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let body = event.get("body").and_then(Value::as_object);

        match event_name {
            "initialized" => {
                self.initialized = true;
                self.attached = true;
            }
            "stopped" => {
                let Some(body) = body else {
                    return;
                };
                let thread_id = body.get("threadId").and_then(Value::as_i64);
                let all_threads_stopped = body
                    .get("allThreadsStopped")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                if all_threads_stopped {
                    self.stopped_threads.clear();
                }
                if let Some(thread_id) = thread_id {
                    self.stopped_threads.insert(thread_id);
                }
                self.last_stop_reason = body
                    .get("reason")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                self.all_threads_stopped = all_threads_stopped;
                self.clear_synthetic_snapshot();
                self.load_synthetic_snapshot(body);
            }
            "continued" => {
                let Some(body) = body else {
                    return;
                };
                let all_threads_continued = body
                    .get("allThreadsContinued")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
                if all_threads_continued {
                    self.stopped_threads.clear();
                } else if let Some(thread_id) = body.get("threadId").and_then(Value::as_i64) {
                    self.stopped_threads.remove(&thread_id);
                }
                self.last_stop_reason = None;
                self.all_threads_stopped = false;
                self.clear_synthetic_snapshot();
            }
            "terminated" | "exited" => {
                self.attached = false;
                self.configured = false;
                self.stopped_threads.clear();
                self.last_stop_reason = None;
                self.all_threads_stopped = false;
                self.clear_synthetic_snapshot();
            }
            "thread" => {
                if let Some(body) = body {
                    self.last_threads.push(Value::Object(body.clone()));
                }
            }
            _ => {}
        }
    }

    pub fn record_capabilities(&mut self, capabilities: Value) {
        self.capabilities = Some(capabilities);
    }

    pub fn update_from_response(&mut self, command: &str, response: &Value) {
        if !response
            .get("success")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            return;
        }
        match command {
            "attach" => self.attached = true,
            "configurationDone" => self.configured = true,
            "threads" => {
                if let Some(threads) = response.pointer("/body/threads").and_then(Value::as_array) {
                    self.last_threads = threads.clone();
                }
            }
            "continue" | "next" | "stepIn" | "stepOut" => {
                self.stopped_threads.clear();
                self.last_stop_reason = None;
                self.all_threads_stopped = false;
            }
            _ => {}
        }
    }

    fn load_synthetic_snapshot(&mut self, body: &serde_json::Map<String, Value>) {
        let Some(snapshot) = body.get("rustykernel").and_then(Value::as_object) else {
            return;
        };

        self.synthetic_stack_frames = snapshot
            .get("stackFrames")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default();

        self.synthetic_scopes = snapshot
            .get("scopesByFrame")
            .and_then(Value::as_object)
            .map(|frames| {
                frames
                    .iter()
                    .filter_map(|(frame_id, scopes)| {
                        Some((
                            frame_id.parse::<i64>().ok()?,
                            scopes.as_array().cloned().unwrap_or_default(),
                        ))
                    })
                    .collect()
            })
            .unwrap_or_default();

        self.synthetic_variables = snapshot
            .get("variablesByRef")
            .and_then(Value::as_object)
            .map(|refs| {
                refs.iter()
                    .filter_map(|(reference, variables)| {
                        Some((
                            reference.parse::<i64>().ok()?,
                            variables.as_array().cloned().unwrap_or_default(),
                        ))
                    })
                    .collect()
            })
            .unwrap_or_default();
    }

    fn clear_synthetic_snapshot(&mut self) {
        self.synthetic_stack_frames.clear();
        self.synthetic_scopes.clear();
        self.synthetic_variables.clear();
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::DebugCache;

    #[test]
    fn debug_cache_updates_stop_state_from_events() {
        let mut state = DebugCache::default();
        state.update_from_event(&json!({
            "type": "event",
            "event": "stopped",
            "body": {"threadId": 7, "reason": "breakpoint", "allThreadsStopped": true}
        }));

        assert!(state.stopped_threads.contains(&7));
        assert_eq!(state.last_stop_reason.as_deref(), Some("breakpoint"));
        assert!(state.all_threads_stopped);

        state.update_from_event(&json!({
            "type": "event",
            "event": "continued",
            "body": {"threadId": 7, "allThreadsContinued": false}
        }));
        assert!(state.stopped_threads.is_empty());
        assert_eq!(state.last_stop_reason, None);
    }

    #[test]
    fn debug_cache_loads_synthetic_snapshot_from_stopped_event() {
        let mut state = DebugCache::default();
        state.update_from_event(&json!({
            "type": "event",
            "event": "stopped",
            "body": {
                "threadId": 1,
                "reason": "breakpoint",
                "allThreadsStopped": true,
                "rustykernel": {
                    "stackFrames": [
                        {"id": 1, "name": "<module>", "line": 2, "column": 1}
                    ],
                    "scopesByFrame": {
                        "1": [
                            {"name": "Locals", "variablesReference": 1000}
                        ]
                    },
                    "variablesByRef": {
                        "1000": [
                            {"name": "x", "value": "1", "variablesReference": 0}
                        ]
                    }
                }
            }
        }));

        assert_eq!(state.synthetic_stack_frames.len(), 1);
        assert_eq!(state.synthetic_scopes.get(&1).map(Vec::len), Some(1));
        assert_eq!(state.synthetic_variables.get(&1000).map(Vec::len), Some(1));
    }

    #[test]
    fn debug_cache_clears_synthetic_snapshot_when_new_stop_has_no_snapshot() {
        let mut state = DebugCache::default();
        state.update_from_event(&json!({
            "type": "event",
            "event": "stopped",
            "body": {
                "threadId": 1,
                "reason": "breakpoint",
                "allThreadsStopped": true,
                "rustykernel": {
                    "stackFrames": [
                        {"id": 1, "name": "<module>", "line": 6, "column": 1}
                    ],
                    "scopesByFrame": {
                        "1": [
                            {"name": "Locals", "variablesReference": 1000}
                        ]
                    },
                    "variablesByRef": {
                        "1000": [
                            {"name": "x", "value": "1", "variablesReference": 0}
                        ]
                    }
                }
            }
        }));

        state.update_from_event(&json!({
            "type": "event",
            "event": "stopped",
            "body": {"threadId": 1, "reason": "step", "allThreadsStopped": true}
        }));

        assert!(state.synthetic_stack_frames.is_empty());
        assert!(state.synthetic_scopes.is_empty());
        assert!(state.synthetic_variables.is_empty());
    }

    #[test]
    fn debug_cache_continue_and_step_responses_clear_stop_state() {
        let mut state = DebugCache::default();
        state.update_from_event(&json!({
            "type": "event",
            "event": "stopped",
            "body": {"threadId": 7, "reason": "breakpoint", "allThreadsStopped": true}
        }));

        state.update_from_response(
            "continue",
            &json!({
                "type": "response",
                "success": true,
                "body": {"allThreadsContinued": true}
            }),
        );
        assert!(state.stopped_threads.is_empty());

        state.update_from_event(&json!({
            "type": "event",
            "event": "stopped",
            "body": {"threadId": 8, "reason": "step", "allThreadsStopped": false}
        }));
        state.update_from_response(
            "stepIn",
            &json!({
                "type": "response",
                "success": true,
                "body": {}
            }),
        );
        assert!(state.stopped_threads.is_empty());
    }
}
