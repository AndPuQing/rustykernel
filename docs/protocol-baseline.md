# Protocol Baseline

This document records the protocol surface that `rustykernel` implements today.
It is intended to be a concrete baseline for the current code in `src/kernel.rs`
and the integration coverage in `tests/`.

The current `ipykernel` comparison status is tracked separately in
`docs/ipykernel-compat-matrix.md`. Confirmed behavior deltas are tracked in
`docs/known-behavior-differences.md`.

## Transport And Framing

- Jupyter protocol version: `5.3`
- Language: `python`
- ZeroMQ sockets bound from the connection file: `shell`, `iopub`, `stdin`,
  `control`, `hb`
- Message signing: `hmac-sha256` and empty-key unsigned mode
- Heartbeat: echoes frames on `hb`

## Implemented Request Surface

### Shell Channel

- `kernel_info_request` -> `kernel_info_reply`
- `connect_request` -> `connect_reply`
- `execute_request` -> `execute_reply`
- `complete_request` -> `complete_reply`
- `inspect_request` -> `inspect_reply`
- `history_request` -> `history_reply`
- `is_complete_request` -> `is_complete_reply`
- `comm_info_request` -> `comm_info_reply`
- `shutdown_request` -> `shutdown_reply`

### Control Channel

- `kernel_info_request` -> `kernel_info_reply`
- `connect_request` -> `connect_reply`
- `interrupt_request` -> `interrupt_reply`
- `debug_request` -> `debug_reply`
- `usage_request` -> `usage_reply`
- `shutdown_request` -> `shutdown_reply`

### Stdin Channel

- `input_reply` is accepted as the response to a pending `input_request`
- standalone `input_reply` frames are accepted and ignored so they do not stop
  the kernel

## IOPub Messages Published Today

- `status`
  - publishes `starting` during startup
  - publishes `busy` / `idle` around shell and control requests
- `execute_input`
- `stream`
- `display_data`
- `update_display_data`
- `execute_result`
- `error`

## Current Execution Semantics

- Python code runs in a long-lived worker process and preserves namespace state
  across normal cell execution.
- `stdout` and `stderr` are captured from the Python worker and emitted as
  `stream` messages.
- Successful expression results are emitted as `execute_result`.
- Rich display hooks are supported for common notebook repr paths, including:
  `text/html`, `text/markdown`, `text/latex`, `image/svg+xml`,
  `application/json`, and `_repr_mimebundle_()`.
- `from IPython.display import display, update_display` works through the worker
  shim and publishes `display_data` / `update_display_data`.
- `user_expressions` are evaluated and returned in `execute_reply`.
- Completions are backed by Jedi when available, otherwise `rlcompleter`, and
  populate `_jupyter_types_experimental`.
- Inspection returns `text/plain`, `text/markdown`, and `text/html` payloads
  plus summary metadata such as `type_name`, `signature`, and `doc_summary`.
- `is_complete_request` returns syntax-aware `complete`, `incomplete`, and
  `invalid` statuses.
- History is stored in memory per kernel runtime and supports `tail`, `range`,
  and `search` access modes.
- `interrupt_request` attempts to interrupt the active Python execution in
  place, preserving the live namespace, execution counter progression, and
  in-memory history.
- `usage_request` returns a control-plane resource snapshot for the running
  kernel process tree, including hostname, pid, kernel CPU/memory totals, host
  CPU count, and host virtual-memory totals.
- `debug_request` now supports the current debugger control-plane surface for
  `debugInfo`, `initialize`, `attach`, `disconnect`, `dumpCell`,
  `setBreakpoints`, `configurationDone`, `evaluate`, `source`, `threads`,
  `stackTrace`, `scopes`, `variables`, `continue`, `next`, `stepIn`, and
  `stepOut`.
- when `debugpy` is available, `initialize`, `attach`, `configurationDone`,
  `setBreakpoints`, `threads`, `stackTrace`, `scopes`, `variables`,
  `continue`, `next`, `stepIn`, and `stepOut` are now handled by Rust's
  `DebugSession` against the in-process debugpy DAP endpoint; the worker no
  longer proxies those live debug commands.
- real in-process debugpy DAP events are now forwarded to Jupyter as
  `debug_event` IOPub messages in real time during execution rather than being
  delayed until the next execution boundary.
- `stopped`, `continued`, `terminated`, `exited`, and `initialized` events now
  drive the kernel-side debug state machine, and `debugInfo.stoppedThreads` is
  emitted from Rust-owned state.
- synthetic execute-boundary `stopped` events now carry a small
  rustykernel-specific snapshot for stack/scopes/variables, which Rust caches
  and can use to answer `stackTrace`, `scopes`, and `variables` without a
  worker fallback path.
- when a previously dumped cell is executed, its temporary file path is now
  injected into IPython's compilation cache so real debugpy breakpoints, stack
  frames, and source paths can line up with the dumped file.

## Current Placeholders And Known Limits In Surface Area

- comms are tracked in-memory and exposed through `comm_info_request`
- shell-side `comm_open` / `comm_msg` / `comm_close` are routed into a minimal
  worker comm manager and can emit `comm_*` events back on iopub
- restart-style `shutdown_request` also restarts the worker, resets the
  execution counter, and starts a new in-memory history session
- unsupported request types fall back to `*_reply` with:
  - `status: "error"`
  - `ename: "NotImplemented"`
  - `evalue: "unsupported message type: <msg_type>"`

## Requests Not Implemented Yet

The current runtime does not implement full handlers for common protocol
extensions such as:

- debugger features still outside the currently covered baseline, such as a
  real `pause` flow and broader lifecycle hardening beyond the now-tested live
  `continue` / `next` / `stepIn` / `stepOut` paths
- subshell requests

## Test Coverage That Anchors This Baseline

- Rust runtime tests in `src/kernel.rs` cover request routing, execution, rich
  output, stdin, history, interrupt, and shutdown behavior.
- Python integration tests in `tests/test_python_api.py` cover the installed
  runtime surface for `connect_request`, `execute_request`, rich output,
  `complete_request`, `inspect_request`, `history_request`,
  `is_complete_request`, stdin input, interrupt, and restart semantics.
- `tests/test_protocol_smoke.py` provides a shallow protocol smoke suite for
  `kernel_info_request`, `execute_request`, `complete_request`,
  `inspect_request`, `history_request`, `is_complete_request`,
  `shutdown_request`, `interrupt_request`, `usage_request`, and `debug_request`.
- `tests/test_ipykernel_compat.py` currently compares `kernel_info_request`,
  `execute_request`, `comm_info_request`, `complete_request`,
  `inspect_request`, and `is_complete_request` against `ipykernel`. The current
  classification of those checks is documented in
  `docs/ipykernel-compat-matrix.md`.
