# rustykernel

`rustykernel` is a scaffold for a Rust-first Python kernel runtime that can
grow toward `VS Code + Jupyter` compatibility without starting from a pure
Python `ipykernel` layout.

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
message loop across `shell`, `control`, and `stdin`, publishes `status` and
`execute_input` messages on `iopub`, and continues to echo heartbeat frames on
`hb`.

If you want an explicit rebuild without waiting for `uv` to resync the project,
run:

```bash
uv run maturin develop
```

`cargo test` exercises the pure Rust layer by default. If you need to compile
the PyO3 bindings in tests as well, use:

```bash
LD_LIBRARY_PATH="$(uv run python -c 'import sysconfig; print(sysconfig.get_config_var(\"LIBDIR\"))')" cargo test --features python
```

## Layout

- `src/`: Rust extension module exposed as `rustykernel._core`
- `python/rustykernel/`: Python package, CLI entrypoint, future kernel launcher

## Next steps

- Add real execution, display data, and stdin request plumbing behind the new message loop
- Expand shell/control coverage beyond the current baseline request handlers
- Keep Python as a thin execution shim until protocol compatibility is stable
