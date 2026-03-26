# rustykernel

`rustykernel` is a scaffold for a Rust-first Python kernel runtime that can
grow toward `VS Code + Jupyter` compatibility without starting from a pure
Python `ipykernel` layout.

Today it already supports a small but real Jupyter message loop backed by a
persistent Python worker, rather than only static metadata stubs.

## Quickstart

```bash
uv venv
uv sync
uv run python -m rustykernel --json
```

`uv run` will only rebuild the Rust extension when the editable install is
considered stale. For a `maturin` project that means the Rust and Python source
files must be covered by `tool.uv.cache-keys`, and the project must be installed
through the project environment (`uv sync`).

To install a real Jupyter kernelspec into the current environment, run:

```bash
uv run python -m rustykernel install --sys-prefix
```

That writes a `kernel.json` using `python -m rustykernel -f {connection_file}`
as the launcher, so Jupyter frontends can discover and start the kernel like a
normal installed runtime.

To bind a real Jupyter connection file and keep the process alive, run:

```bash
uv run python -m rustykernel -f /path/to/kernel-connection.json
```

That startup path now binds the `shell`, `iopub`, `stdin`, `control`, and
`hb` sockets from the connection file. The runtime now runs a signed Jupyter
message loop across `shell`, `control`, and `stdin`, publishes `status`,
`execute_input`, `stream`, `execute_result`, and `error` messages on `iopub`,
and continues to echo heartbeat frames on `hb`.

## Current protocol surface

The maintained baseline lives in [docs/protocol-baseline.md](docs/protocol-baseline.md).
That document is the source of truth for what the runtime actually implements
today. The current `ipykernel` comparison classification lives in
[docs/ipykernel-compat-matrix.md](docs/ipykernel-compat-matrix.md). Confirmed
known deltas from `ipykernel` are tracked in
[docs/known-behavior-differences.md](docs/known-behavior-differences.md).

In summary, `rustykernel` currently binds the standard Jupyter `shell`,
`iopub`, `stdin`, `control`, and `hb` sockets and implements:

- shell requests: `kernel_info_request`, `connect_request`,
  `execute_request`, `complete_request`, `inspect_request`,
  `history_request`, `is_complete_request`, `comm_info_request`,
  `comm_open`, `comm_msg`, `comm_close`, `shutdown_request`
- control requests: `kernel_info_request`, `connect_request`,
  `interrupt_request`, `usage_request`, `shutdown_request`
- stdin handling for `input_reply`
- iopub publishing for `status`, `execute_input`, `stream`, `display_data`,
  `update_display_data`, `execute_result`, `error`, `comm_open`,
  `comm_msg`, and `comm_close`

Current execution behavior:

- Python code runs in a long-lived worker process and preserves state across
  normal cell execution
- stdout/stderr are captured and published as Jupyter `stream` messages
- expression results are published as `execute_result`, including rich MIME
  bundles when objects expose notebook repr hooks
- execution now runs through a real embedded `IPython InteractiveShell`
- `get_ipython()` returns that live shell
- top-level `await` works, and `%autoawait off` can disable that behavior
- notebook-oriented magics now come from the real IPython stack, with a small
  `rustykernel` fallback for `%matplotlib inline` when `matplotlib` is absent
- `IPython.display` now uses the real IPython module, including
  `display`, `update_display`, `clear_output`, `DisplayHandle`, raw mimebundle
  display, and common display objects such as `HTML`, `Markdown`, `JSON`,
  `Javascript`, `Latex`, `SVG`, and `Pretty`
- `get_ipython().set_next_input(...)` is reflected back through
  `execute_reply.payload`
- exceptions are published as `error`
- completions strip callable parentheses and include
  `_jupyter_types_experimental` metadata
- completions are backed by Jedi when available
- inspection returns `text/plain` / `text/markdown` / `text/html` bundles plus
  summary metadata
- `comm_info_request` reports the live in-memory comm registry
- minimal comm hooks are available through `rustykernel.comm.Comm` and
  `rustykernel.comm.register_target`
- `interrupt_request` tries to stop the active execution without resetting the
  live worker namespace, execution counter, or history
- `usage_request` reports kernel/host resource usage snapshots on the control
  channel
- restart-style `shutdown_request` still resets the worker state

If you want an explicit rebuild without waiting for `uv` to resync the project,
run:

```bash
uv run maturin develop
```

Test coverage currently includes Rust-side protocol/runtime tests plus Python
integration tests for `connect_request`, `execute_request`,
`complete_request`, `inspect_request`, `history_request`,
`is_complete_request`, stdin input, interrupt, restart, a protocol smoke suite
for the core request types, and the current `ipykernel` compatibility checks
listed in the baseline document.

`cargo test` exercises the Rust layer by default. For the installed Python
surface, run:

```bash
uv run pytest
```

Only if you specifically need to compile the PyO3 bindings into the Rust test
harness as well, use:

```bash
LD_LIBRARY_PATH="$(uv run python -c 'import sysconfig; print(sysconfig.get_config_var(\"LIBDIR\"))')" cargo test --features python
```

## Layout

- `src/`: Rust extension module exposed as `rustykernel._core`
- `python/rustykernel/`: Python package, CLI entrypoint, future kernel launcher
- `tests/`: Python-side integration tests for the installed extension module

## Next steps

- Improve kernelspec resources and launcher ergonomics beyond the minimal installed `kernel.json`
- Implement richer `inspect_request` / `complete_request` behavior closer to notebook clients
- Improve protocol coverage for more frontend-driven requests as compatibility needs become clearer
