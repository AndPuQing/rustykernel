# rustykernel

`rustykernel` is a Rust-first Python kernel runtime aimed at `Jupyter` and
`VS Code` compatibility without inheriting the full `ipykernel` process model.

The project is already beyond the "metadata-only scaffold" phase: it runs a
real signed Jupyter message loop, keeps a persistent Python worker alive across
cells, and executes code through an embedded `IPython InteractiveShell`.

## Quickstart

```bash
uv venv
uv sync
uv run python -m rustykernel --json
```

Install a kernelspec into the current environment:

```bash
uv run python -m rustykernel install --sys-prefix
```

Run against an actual Jupyter connection file:

```bash
uv run python -m rustykernel -f /path/to/kernel-connection.json
```

If you need a forced rebuild of the extension:

```bash
uv run maturin develop
```

## Current Status

The runtime currently supports the core Jupyter socket set (`shell`, `iopub`,
`stdin`, `control`, `hb`) and a usable notebook execution path:

- persistent Python state across cells
- real IPython execution, `get_ipython()`, magics, and top-level `await`
- stdout/stderr stream publishing plus rich display output
- completion, inspection, history, stdin input, interrupt, and restart flows
- minimal comm support
- control-plane `usage_request`
- debugger baseline on the control channel, including live step/continue/pause
- subshell control requests and routed subshell execution
- inline matplotlib as the current graphics target

Detailed planning and progress are intentionally kept in one place:
[TODO.md](TODO.md).

## Testing

Rust runtime coverage:

```bash
cargo test
```

Python integration and compatibility coverage:

```bash
uv run pytest
```

Only if you explicitly need PyO3 bindings inside the Rust test harness:

```bash
LD_LIBRARY_PATH="$(uv run python -c 'import sysconfig; print(sysconfig.get_config_var(\"LIBDIR\"))')" cargo test --features python
```

## Layout

- `src/`: Rust kernel runtime and protocol/control-plane implementation
- `python/rustykernel/`: Python package, CLI, kernelspec install path, worker
- `tests/`: protocol smoke, notebook semantics, and `ipykernel` comparison tests
