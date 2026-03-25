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

To bind a real Jupyter connection file and keep the process alive, run:

```bash
uv run python -m rustykernel --connection-file /path/to/kernel-connection.json
```

That startup path now binds the `shell`, `iopub`, `stdin`, `control`, and
`hb` sockets from the connection file. The runtime now runs a signed Jupyter
message loop across `shell`, `control`, and `stdin`, publishes `status`,
`execute_input`, `stream`, `execute_result`, and `error` messages on `iopub`,
and continues to echo heartbeat frames on `hb`.

## Current protocol surface

Implemented today:

- `kernel_info_request`
- `execute_request` with persistent Python state across cells
- `complete_request`
- `inspect_request`
- `is_complete_request` (currently simple placeholder behavior)
- `history_request` and `comm_info_request` placeholder replies
- `interrupt_request` on the control channel
- `shutdown_request` on shell/control
- `input_reply` accepted on the stdin channel

Current execution behavior:

- Python code runs in a long-lived worker process
- stdout/stderr are captured and published as Jupyter `stream` messages
- expression results are published as `execute_result`
- exceptions are published as `error`
- `interrupt_request` / restart-style `shutdown_request` reset worker state

If you want an explicit rebuild without waiting for `uv` to resync the project,
run:

```bash
uv run maturin develop
```

Test coverage currently includes Rust-side protocol/runtime tests plus Python
integration tests for execution, completion, inspection, interrupt, and
shutdown behavior.

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

- Make `is_complete_request` syntax-aware instead of always returning `complete`
- Implement richer `inspect_request` / `complete_request` behavior closer to notebook clients
- Add `display_data` / `display_id` support beyond plain `text/plain` results
- Add real stdin request plumbing from kernel to frontend instead of only accepting `input_reply`
- Improve protocol coverage for more frontend-driven requests as compatibility needs become clearer
