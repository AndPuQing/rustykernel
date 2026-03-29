use std::collections::HashMap;

use serde_json::{Value, json};

use crate::worker::WorkerCommEvent;

pub(crate) struct CommStore {
    entries: HashMap<String, CommEntry>,
}

struct CommEntry {
    target_name: String,
}

impl CommStore {
    pub(crate) fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    pub(crate) fn register(&mut self, content: &Value) {
        let Some(comm_id) = content.get("comm_id").and_then(Value::as_str) else {
            return;
        };
        let Some(target_name) = content.get("target_name").and_then(Value::as_str) else {
            return;
        };

        self.entries.insert(
            comm_id.to_owned(),
            CommEntry {
                target_name: target_name.to_owned(),
            },
        );
    }

    pub(crate) fn close(&mut self, content: &Value) {
        let Some(comm_id) = content.get("comm_id").and_then(Value::as_str) else {
            return;
        };
        self.entries.remove(comm_id);
    }

    pub(crate) fn reply_content(&self, request: &Value) -> Value {
        let target_name = request.get("target_name").and_then(Value::as_str);
        let comms = self
            .entries
            .iter()
            .filter(|(_, entry)| target_name.is_none_or(|target| entry.target_name == target))
            .map(|(comm_id, entry)| {
                (
                    comm_id.clone(),
                    json!({
                        "target_name": entry.target_name,
                    }),
                )
            })
            .collect::<serde_json::Map<_, _>>();

        json!({
            "status": "ok",
            "comms": comms,
        })
    }

    pub(crate) fn clear(&mut self) {
        self.entries.clear();
    }

    pub(crate) fn apply_event(&mut self, event: &WorkerCommEvent) {
        match event.msg_type.as_str() {
            "comm_open" => self.register(&event.content),
            "comm_close" => self.close(&event.content),
            _ => {}
        }
    }
}
