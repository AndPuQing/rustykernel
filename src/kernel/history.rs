use std::collections::{HashMap, VecDeque};

use serde_json::{Value, json};

use crate::worker::ExecutionOutcome;

pub(crate) struct HistoryStore {
    sessions: Vec<HistorySession>,
}

struct HistorySession {
    id: u32,
    entries: Vec<HistoryEntry>,
}

struct HistoryEntry {
    line: u32,
    input: String,
    output: Option<String>,
}

struct HistoryReplyEntry {
    session: u32,
    line: u32,
    input: String,
    output: Option<String>,
}

impl HistoryStore {
    pub(crate) fn new() -> Self {
        Self {
            sessions: vec![HistorySession::new(1)],
        }
    }

    pub(crate) fn record(&mut self, line: u32, input: &str, outcome: &ExecutionOutcome) {
        self.current_session_mut().entries.push(HistoryEntry {
            line,
            input: input.to_owned(),
            output: history_output(outcome),
        });
    }

    pub(crate) fn reply_content(&self, request: &Value) -> Value {
        let hist_access_type = request
            .get("hist_access_type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let output = request
            .get("output")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let session = request
            .get("session")
            .and_then(Value::as_i64)
            .and_then(|value| i32::try_from(value).ok())
            .unwrap_or(0);
        let start = request
            .get("start")
            .and_then(Value::as_i64)
            .and_then(|value| i32::try_from(value).ok())
            .unwrap_or(0);
        let stop = request
            .get("stop")
            .and_then(Value::as_i64)
            .and_then(|value| i32::try_from(value).ok());
        let n = request
            .get("n")
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok());
        let pattern = request
            .get("pattern")
            .and_then(Value::as_str)
            .unwrap_or("*");
        let unique = request
            .get("unique")
            .and_then(Value::as_bool)
            .unwrap_or(false);

        let history = match hist_access_type {
            "tail" => self
                .tail_entries(n.unwrap_or_else(|| self.entry_count()))
                .into_iter()
                .map(|entry| entry.as_value(output))
                .collect::<Vec<_>>(),
            "range" => self
                .range_entries(session, start, stop)
                .into_iter()
                .map(|entry| entry.as_value(output))
                .collect::<Vec<_>>(),
            "search" => self
                .search_entries(pattern, n, unique)
                .into_iter()
                .map(|entry| entry.as_value(output))
                .collect::<Vec<_>>(),
            _ => Vec::new(),
        };

        json!({
            "status": "ok",
            "history": history,
        })
    }

    pub(crate) fn start_new_session(&mut self) {
        let next_id = self.current_session_id().saturating_add(1);
        self.sessions.push(HistorySession::new(next_id));
    }

    fn entry_count(&self) -> usize {
        self.sessions
            .iter()
            .map(|session| session.entries.len())
            .sum()
    }

    fn current_session(&self) -> &HistorySession {
        self.sessions
            .last()
            .expect("history store must keep a current session")
    }

    fn current_session_mut(&mut self) -> &mut HistorySession {
        self.sessions
            .last_mut()
            .expect("history store must keep a current session")
    }

    fn current_session_id(&self) -> u32 {
        self.current_session().id
    }

    fn entry_refs(&self) -> impl DoubleEndedIterator<Item = (u32, &HistoryEntry)> + '_ {
        self.sessions
            .iter()
            .flat_map(|session| session.entries.iter().map(move |entry| (session.id, entry)))
    }

    fn resolve_session(&self, requested_session: i32) -> Option<&HistorySession> {
        let current_session = i32::try_from(self.current_session_id()).ok()?;
        let target_session = if requested_session <= 0 {
            current_session + requested_session
        } else {
            requested_session
        };

        if target_session <= 0 {
            return None;
        }

        let target_session = u32::try_from(target_session).ok()?;
        self.sessions
            .iter()
            .find(|session| session.id == target_session)
    }

    fn tail_entries(&self, n: usize) -> Vec<HistoryReplyEntry> {
        let mut entries = self
            .entry_refs()
            .rev()
            .take(n)
            .map(|(session_id, entry)| HistoryReplyEntry::from_entry(session_id, entry))
            .collect::<Vec<_>>();
        entries.reverse();
        entries
    }

    fn range_entries(
        &self,
        requested_session: i32,
        start: i32,
        stop: Option<i32>,
    ) -> Vec<HistoryReplyEntry> {
        let Some(session) = self.resolve_session(requested_session) else {
            return Vec::new();
        };

        if session.id == self.current_session_id() {
            return self.current_session_range_entries(start, stop);
        }

        session
            .entries
            .iter()
            .filter(|entry| {
                i32::try_from(entry.line).ok().is_some_and(|line| {
                    line >= start && stop.is_none_or(|stop_line| line < stop_line)
                })
            })
            .map(|entry| HistoryReplyEntry::from_entry(session.id, entry))
            .collect()
    }

    fn current_session_range_entries(
        &self,
        start: i32,
        stop: Option<i32>,
    ) -> Vec<HistoryReplyEntry> {
        let session = self.current_session();
        let line_count = i32::try_from(session.entries.len())
            .ok()
            .and_then(|count| count.checked_add(1))
            .unwrap_or(i32::MAX);
        let start = normalize_history_index(start, line_count);
        let stop = stop
            .map(|stop_line| normalize_history_index(stop_line, line_count))
            .unwrap_or(line_count);

        if start >= stop {
            return Vec::new();
        }

        let mut entries = Vec::new();
        if start == 0 {
            entries.push(HistoryReplyEntry {
                session: 0,
                line: 0,
                input: String::new(),
                output: None,
            });
        }

        let first_line = start.max(1);
        for line in first_line..stop {
            let Some(index) = usize::try_from(line - 1).ok() else {
                continue;
            };
            let Some(entry) = session.entries.get(index) else {
                continue;
            };
            entries.push(HistoryReplyEntry::from_entry(0, entry));
        }

        entries
    }

    fn search_entries(
        &self,
        pattern: &str,
        n: Option<usize>,
        unique: bool,
    ) -> Vec<HistoryReplyEntry> {
        if unique {
            return self.search_entries_unique(pattern, n);
        }

        let Some(limit) = n else {
            return self
                .entry_refs()
                .filter(|(_, entry)| matches_history_pattern(&entry.input, pattern))
                .map(|(session_id, entry)| HistoryReplyEntry::from_entry(session_id, entry))
                .collect();
        };

        if limit == 0 {
            return Vec::new();
        }

        let mut matches = VecDeque::with_capacity(limit);
        for (session_id, entry) in self.entry_refs() {
            if !matches_history_pattern(&entry.input, pattern) {
                continue;
            }
            if matches.len() == limit {
                let _ = matches.pop_front();
            }
            matches.push_back(HistoryReplyEntry::from_entry(session_id, entry));
        }

        matches.into_iter().collect()
    }

    fn search_entries_unique(&self, pattern: &str, n: Option<usize>) -> Vec<HistoryReplyEntry> {
        struct LatestMatch<'a> {
            ordinal: usize,
            session_id: u32,
            entry: &'a HistoryEntry,
        }

        let mut latest_matches = HashMap::new();
        let mut ordinal = 0usize;
        for (session_id, entry) in self.entry_refs() {
            if !matches_history_pattern(&entry.input, pattern) {
                continue;
            }
            latest_matches.insert(
                entry.input.as_str(),
                LatestMatch {
                    ordinal,
                    session_id,
                    entry,
                },
            );
            ordinal = ordinal.saturating_add(1);
        }

        let mut matches = latest_matches.into_values().collect::<Vec<_>>();
        matches.sort_unstable_by_key(|entry| entry.ordinal);
        if let Some(limit) = n {
            keep_last_entries(&mut matches, limit);
        }
        matches
            .into_iter()
            .map(|entry| HistoryReplyEntry::from_entry(entry.session_id, entry.entry))
            .collect()
    }
}

impl HistorySession {
    fn new(id: u32) -> Self {
        Self {
            id,
            entries: Vec::new(),
        }
    }
}

impl HistoryReplyEntry {
    fn from_entry(session: u32, entry: &HistoryEntry) -> Self {
        Self {
            session,
            line: entry.line,
            input: entry.input.clone(),
            output: entry.output.clone(),
        }
    }

    fn as_value(&self, include_output: bool) -> Value {
        if include_output {
            json!([self.session, self.line, [self.input, self.output]])
        } else {
            json!([self.session, self.line, self.input])
        }
    }
}

fn history_output(outcome: &ExecutionOutcome) -> Option<String> {
    outcome
        .result
        .as_ref()
        .and_then(|result| result.data.get("text/plain"))
        .and_then(Value::as_str)
        .map(str::to_owned)
}

fn matches_history_pattern(input: &str, pattern: &str) -> bool {
    let pattern = pattern.chars().collect::<Vec<_>>();
    let input = input.chars().collect::<Vec<_>>();
    let mut input_index = 0usize;
    let mut pattern_index = 0usize;
    let mut last_star_index = None;
    let mut input_index_after_star = 0usize;

    while input_index < input.len() {
        if pattern_index < pattern.len()
            && (pattern[pattern_index] == '?' || pattern[pattern_index] == input[input_index])
        {
            input_index += 1;
            pattern_index += 1;
            continue;
        }

        if pattern_index < pattern.len() && pattern[pattern_index] == '*' {
            last_star_index = Some(pattern_index);
            pattern_index += 1;
            input_index_after_star = input_index;
            continue;
        }

        let Some(star_index) = last_star_index else {
            return false;
        };
        pattern_index = star_index + 1;
        input_index_after_star += 1;
        input_index = input_index_after_star;
    }

    while pattern_index < pattern.len() {
        if pattern[pattern_index] != '*' {
            return false;
        }
        pattern_index += 1;
    }

    true
}

fn keep_last_entries<T>(entries: &mut Vec<T>, limit: usize) {
    if entries.len() > limit {
        let drop_count = entries.len() - limit;
        entries.drain(0..drop_count);
    }
}

fn normalize_history_index(index: i32, line_count: i32) -> i32 {
    let adjusted = if index < 0 {
        index.saturating_add(line_count)
    } else {
        index
    };
    adjusted.clamp(0, line_count)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store_with_inputs(sessions: &[&[&str]]) -> HistoryStore {
        HistoryStore {
            sessions: sessions
                .iter()
                .enumerate()
                .map(|(session_index, inputs)| HistorySession {
                    id: u32::try_from(session_index + 1).unwrap(),
                    entries: inputs
                        .iter()
                        .enumerate()
                        .map(|(line_index, input)| HistoryEntry {
                            line: u32::try_from(line_index + 1).unwrap(),
                            input: (*input).to_owned(),
                            output: None,
                        })
                        .collect(),
                })
                .collect(),
        }
    }

    #[test]
    fn tail_entries_returns_last_entries_in_chronological_order() {
        let store = store_with_inputs(&[&["a = 1", "b = 2"], &["c = 3", "d = 4"]]);

        let entries = store.tail_entries(3);

        assert_eq!(
            entries
                .into_iter()
                .map(|entry| (entry.session, entry.line, entry.input))
                .collect::<Vec<_>>(),
            vec![
                (1, 2, "b = 2".to_owned()),
                (2, 1, "c = 3".to_owned()),
                (2, 2, "d = 4".to_owned()),
            ]
        );
    }

    #[test]
    fn search_entries_unique_keeps_latest_match_for_each_input() {
        let store = store_with_inputs(&[&["alpha", "beta"], &["alpha", "gamma", "beta"]]);

        let entries = store.search_entries("*", None, true);

        assert_eq!(
            entries
                .into_iter()
                .map(|entry| (entry.session, entry.line, entry.input))
                .collect::<Vec<_>>(),
            vec![
                (2, 1, "alpha".to_owned()),
                (2, 2, "gamma".to_owned()),
                (2, 3, "beta".to_owned()),
            ]
        );
    }

    #[test]
    fn search_entries_unique_applies_limit_after_latest_deduplication() {
        let store = store_with_inputs(&[&["alpha", "beta"], &["alpha", "gamma", "beta"]]);

        let entries = store.search_entries("*", Some(2), true);

        assert_eq!(
            entries
                .into_iter()
                .map(|entry| (entry.session, entry.line, entry.input))
                .collect::<Vec<_>>(),
            vec![(2, 2, "gamma".to_owned()), (2, 3, "beta".to_owned()),]
        );
    }

    #[test]
    fn matches_history_pattern_supports_wildcards() {
        assert!(matches_history_pattern("", ""));
        assert!(matches_history_pattern("", "*"));
        assert!(matches_history_pattern("value", "v*e"));
        assert!(matches_history_pattern("value", "v?l*"));
        assert!(matches_history_pattern("alpha", "a**a"));
        assert!(matches_history_pattern("你好", "你?"));
        assert!(!matches_history_pattern("", "?"));
        assert!(!matches_history_pattern("value", "v?z*"));
        assert!(!matches_history_pattern("你好", "?你"));
    }
}
