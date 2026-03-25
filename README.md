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

- Add kernel lifecycle management around the parsed connection file
- Implement Jupyter message types and channel state machines in Rust
- Keep Python as a thin execution shim until protocol compatibility is stable
