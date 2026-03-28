# Known Behavior Differences From ipykernel

This document records behavior differences that are already known today, so the
project does not accidentally regress into treating them as parity.

It only includes differences that are either explicit in the current runtime
implementation or already exercised by local tests. Comparison coverage status
for the broader surface is tracked separately in
`docs/ipykernel-compat-matrix.md`.

## Confirmed Runtime Differences

### Comm support is still minimal

Current `rustykernel` behavior:

- `comm_info_request` reflects the live in-memory comm registry
- shell-side `comm_open` / `comm_msg` / `comm_close` are handled and can drive
  Python-side targets registered through `rustykernel.comm`
- Python code can emit `comm_open` / `comm_msg` / `comm_close` back to the
  frontend through the same shim

This narrows the protocol gap, but it is still not a full `ipykernel`-grade
comm environment. In particular, broader widget and IPython integration is not
present yet.

### The execution stack now uses real IPython, but the kernel bridge is still custom

Current `rustykernel` behavior:

- the worker now executes code through a real `IPython.core.interactiveshell.InteractiveShell`
- `get_ipython()`, `%magic`, top-level `await`, `user_expressions`,
  `payload_manager`, display formatting, and `IPython.display` objects all
  come from the real IPython runtime
- `rustykernel` still owns the outer Jupyter bridge: worker process startup,
  shell/control/stdin sockets, message signing, IOPub publishing, interrupt
  handling, and comm plumbing
- the worker still carries a few targeted compatibility patches around the
  IPython shell boundary, such as display/event capture and a `%matplotlib inline`
  fallback when `matplotlib` itself is absent

This is implemented in `python/rustykernel/worker_main.py` by embedding a real
IPython shell and adapting its output/payload hooks to `rustykernel`'s message
transport.

In practice, notebook execution semantics are much closer to `ipykernel` than
before, but exact parity still depends on how closely the surrounding bridge
matches `ipykernel`'s kernel-side wiring.

### Syntax-error formatting is semantically close, but not raw-identical

Current `rustykernel` behavior:

- `SyntaxError` replies and `error` IOPub messages line up with `ipykernel`
  on core semantics such as `status`, `ename`, `user_expressions`, `payload`,
  and the final highlighted source snippet
- the raw `evalue` filename and the full traceback frame list are still not
  byte-for-byte identical, because the embedded shell currently surfaces some
  extra IPython preprocessing frames before the final syntax snippet

The compatibility suite therefore compares syntax errors semantically rather
than requiring exact raw traceback equality.

### Unsupported requests fall back to a generic `NotImplemented` reply

Current `rustykernel` behavior:

- unsupported message types are routed to `send_unsupported_reply()`
- the reply body is:
  - `status: "error"`
  - `ename: "NotImplemented"`
  - `evalue: "unsupported message type: <msg_type>"`
  - `traceback: []`

That is the current local fallback behavior for requests that `ipykernel`
implements but `rustykernel` does not yet support.

### Raw `kernel_info_reply` parity is not claimed

Current `rustykernel` behavior:

- `implementation`, `implementation_version`, and `banner` are
  `rustykernel`-specific
- `supported_features` currently advertises `["debugger"]`
- `debugger` is currently `true`
- `kernel_info_reply` and kernelspec metadata now advertise debugger support,
  but not subshell support

`rustykernel` now accepts a broader control-channel `debug_request` surface for
`debugInfo`, `initialize`, `attach`, `disconnect`, `dumpCell`,
`setBreakpoints`, `configurationDone`, `evaluate`, `source`, `threads`,
`stackTrace`, `scopes`, `variables`, `continue`, `next`, `stepIn`, and
`stepOut`. When the in-process `debugpy` path is available, those live
stop/step/stack/variables commands are now owned by Rust's `DebugSession`
instead of being proxied by the local worker shim.

When the in-process `debugpy` path is active, DAP events are now forwarded as
Jupyter `debug_event` messages in real time, and kernel-side debug state is
updated from `initialized`, `stopped`, `continued`, `terminated`, and `exited`
events. Real live `continue` / `next` / `stepIn` / `stepOut` end-to-end
coverage is now present in both Rust runtime tests and Python black-box smoke
tests. In particular, the current Python worker still serializes requests
through one stdin/protocol loop, so commands that require stronger
mid-execution re-entrancy beyond the currently covered stop/step flows (for
example a future real `pause`) still need a more re-entrant worker IPC design.
For execute-boundary synthetic breakpoint stops, the worker now only emits a
compact snapshot payload and Rust answers
`stackTrace`/`scopes`/`variables` from its own cache.

The current `ipykernel` comparison test deliberately normalizes
`kernel_info_reply` and does not require raw field-for-field equality.

## Environment-Sensitive Differences

### Completion backend depends on local availability of Jedi

Current `rustykernel` behavior:

- when `jedi` is available, completions report `metadata.backend == "jedi"`
- otherwise the worker falls back to `rlcompleter`

That means completion ordering and metadata can vary by environment. The
current project setup installs Jedi, but the runtime still contains an explicit
fallback path that is not designed to match `ipykernel` exactly.

## Areas Not Yet Locked To Exact Parity

The following surfaces are implemented or partially compared, but the project
does not yet claim exact `ipykernel` equivalence for them:

- raw `complete_request` payloads
- raw `inspect_request` payloads
- `execute_request` stream behavior and broader error-case coverage
- `execute_request` rich display update behavior
- stdin request/reply flow
- `shutdown_request`

Those coverage boundaries are documented in `docs/ipykernel-compat-matrix.md`.
