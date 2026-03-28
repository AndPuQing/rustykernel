from __future__ import annotations

import asyncio
import builtins
import codecs
import contextlib
import ctypes
import dataclasses
import getpass
import html
import hashlib
import inspect
import io
import json
import keyword
import linecache
import os
import queue
import re
import rlcompleter
import signal
import sys
import threading
import time
import tempfile
import token as token_types
import tokenize
import traceback
import types
import uuid
from io import StringIO
from typing import Optional

from IPython import get_ipython as ipython_get_ipython
from IPython.core.displayhook import DisplayHook as IPythonDisplayHook
from IPython.core.displaypub import DisplayPublisher as IPythonDisplayPublisher
from IPython.core.error import UsageError
from IPython.core.interactiveshell import InteractiveShell
from IPython.core.payload import PayloadManager as IPythonPayloadManager

try:
    import jedi
except ImportError:  # pragma: no cover - fallback for minimal environments
    jedi = None


namespace: dict[str, object] = {"__name__": "__main__"}
completer = rlcompleter.Completer(namespace)
COMPLETION_RE = re.compile(r"([A-Za-z_][A-Za-z0-9_\.]*)$")
COMM_TARGETS: dict[str, object] = {}
COMMS: dict[str, "Comm"] = {}
AUTOAWAIT_ENABLED = True
AUTOAWAIT_RUNNER = "asyncio"
_PROTOCOL_STREAM: io.TextIOBase | None = None
_PROTOCOL_LOCK = threading.Lock()
_FD_CAPTURE_LOCK = threading.Lock()
_REQUEST_CONTEXT = threading.local()
_INPUT_WAITERS_LOCK = threading.Lock()
_INPUT_WAITERS: dict[int, queue.Queue[dict[str, object]]] = {}
_INTERRUPT_FLAGS_LOCK = threading.Lock()
_INTERRUPT_FLAGS: set[str | None] = set()
_MAIN_THREAD_ID = threading.get_ident()
_LIBC = ctypes.CDLL(None)
_LIBC.fflush.argtypes = [ctypes.c_void_p]
_LIBC.fflush.restype = ctypes.c_int
_PY_ASYNC_EXC = ctypes.pythonapi.PyThreadState_SetAsyncExc
_PY_ASYNC_EXC.argtypes = [ctypes.c_ulong, ctypes.py_object]
_PY_ASYNC_EXC.restype = ctypes.c_int
STREAM_FDS = {"stdout": 1, "stderr": 2}

try:
    import debugpy
except ImportError:  # pragma: no cover - dependency may be absent
    debugpy = None

if hasattr(signal, "SIGUSR1"):
    signal.signal(signal.SIGUSR1, lambda signum, frame: None)


SubshellId = str | None


@dataclasses.dataclass
class RequestContext:
    request_id: int
    subshell_id: SubshellId
    display_events: list[dict[str, object]] = dataclasses.field(default_factory=list)
    comm_events: list[dict[str, object]] = dataclasses.field(default_factory=list)
    payloads: list[dict[str, object]] = dataclasses.field(default_factory=list)
    execute_result: dict[str, object] | None = None
    stream_chunks: dict[str, list[str]] = dataclasses.field(
        default_factory=lambda: {"stdout": [], "stderr": []}
    )
    input_queue: queue.Queue[dict[str, object]] = dataclasses.field(
        default_factory=queue.Queue
    )


def current_request_context() -> RequestContext:
    context = getattr(_REQUEST_CONTEXT, "current", None)
    if context is None:
        raise RuntimeError(
            "request-local state is only available while handling a request"
        )
    return context


def try_current_request_context() -> RequestContext | None:
    return getattr(_REQUEST_CONTEXT, "current", None)


def record_stream_chunk(name: str, text: str) -> None:
    context = try_current_request_context()
    if context is None:
        return
    context.stream_chunks[name].append(text)


def emit_live_stream(name: str, text: str) -> None:
    context = try_current_request_context()
    if context is None or context.request_id <= 0:
        return
    emit_protocol_message(
        {
            "id": context.request_id,
            "event": "stream",
            "name": name,
            "text": text,
        }
    )


@contextlib.contextmanager
def bind_request_context(context: RequestContext):
    previous = getattr(_REQUEST_CONTEXT, "current", None)
    _REQUEST_CONTEXT.current = context
    with _INPUT_WAITERS_LOCK:
        _INPUT_WAITERS[context.request_id] = context.input_queue
    try:
        yield context
    finally:
        with _INPUT_WAITERS_LOCK:
            _INPUT_WAITERS.pop(context.request_id, None)
        if previous is None:
            if hasattr(_REQUEST_CONTEXT, "current"):
                delattr(_REQUEST_CONTEXT, "current")
        else:
            _REQUEST_CONTEXT.current = previous


def interrupt_requested(subshell_id: SubshellId) -> bool:
    with _INTERRUPT_FLAGS_LOCK:
        if subshell_id not in _INTERRUPT_FLAGS:
            return False
        _INTERRUPT_FLAGS.discard(subshell_id)
        return True


def request_interrupt(subshell_id: SubshellId) -> None:
    with _INTERRUPT_FLAGS_LOCK:
        _INTERRUPT_FLAGS.add(subshell_id)


def clear_interrupt(subshell_id: SubshellId) -> None:
    with _INTERRUPT_FLAGS_LOCK:
        _INTERRUPT_FLAGS.discard(subshell_id)


def protocol_stdout() -> io.TextIOBase:
    global _PROTOCOL_STREAM

    if _PROTOCOL_STREAM is not None:
        return _PROTOCOL_STREAM

    protocol_fd = os.environ.get("RUSTYKERNEL_PROTOCOL_FD")
    if protocol_fd is None:
        return sys.__stdout__ if sys.__stdout__ is not None else sys.stdout

    _PROTOCOL_STREAM = os.fdopen(
        int(protocol_fd),
        "w",
        buffering=1,
        encoding="utf-8",
        closefd=False,
    )
    return _PROTOCOL_STREAM


def protocol_stdin() -> io.TextIOBase:
    return sys.__stdin__ if sys.__stdin__ is not None else sys.stdin


def emit_protocol_message(message: object) -> None:
    with _PROTOCOL_LOCK:
        stream = protocol_stdout()
        stream.write(json.dumps(message) + "\n")
        stream.flush()


def flush_standard_streams() -> None:
    for stream in (sys.__stdout__, sys.__stderr__):
        if stream is None:
            continue
        try:
            stream.flush()
        except Exception:
            pass
    _LIBC.fflush(None)


def write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    total = 0
    while total < len(view):
        written = os.write(fd, view[total:])
        total += written


class WorkerBinaryStream(io.RawIOBase):
    def __init__(self, fd: int) -> None:
        self._fd = fd

    def writable(self) -> bool:
        return True

    def fileno(self) -> int:
        return self._fd

    def isatty(self) -> bool:
        return os.isatty(self._fd)

    def write(self, data: bytes | bytearray | memoryview) -> int:
        chunk = bytes(data)
        if not chunk:
            return 0
        write_all(self._fd, chunk)
        return len(chunk)

    def flush(self) -> None:
        flush_standard_streams()


class WorkerStream(io.TextIOBase):
    def __init__(self, fd: int, name: str) -> None:
        self._fd = fd
        self._name = name
        self._buffer = WorkerBinaryStream(fd)

    @property
    def buffer(self) -> WorkerBinaryStream:
        return self._buffer

    @property
    def encoding(self) -> str:
        return "utf-8"

    @property
    def errors(self) -> str:
        return "replace"

    def writable(self) -> bool:
        return True

    def isatty(self) -> bool:
        return os.isatty(self._fd)

    def fileno(self) -> int:
        return self._fd

    def flush(self) -> None:
        flush_standard_streams()

    def write(self, data: str) -> int:
        if not isinstance(data, str):
            raise TypeError(f"write() argument must be str, not {type(data)}")

        if not data:
            return 0

        context = try_current_request_context()
        if context is not None and context.request_id > 0:
            record_stream_chunk(self._name, data)
            emit_live_stream(self._name, data)
            return len(data)

        write_all(self._fd, data.encode(self.encoding, self.errors))
        return len(data)


class FdCapture:
    def __init__(self, request_id: int | None = None) -> None:
        self._request_id = request_id
        self._context = None
        self._buffer_lock = threading.Lock()
        self._emit_lock = threading.Lock()
        self._buffers: dict[str, list[str]] = {"stdout": [], "stderr": []}
        self._pending: dict[str, str] = {"stdout": "", "stderr": ""}
        self._saved_fds: dict[str, int] = {}
        self._read_fds: dict[str, int] = {}
        self._threads: list[threading.Thread] = []

    def __enter__(self) -> FdCapture:
        self._context = try_current_request_context()
        flush_standard_streams()
        for name, fd in STREAM_FDS.items():
            self._saved_fds[name] = os.dup(fd)
            read_fd, write_fd = os.pipe()
            self._read_fds[name] = read_fd
            os.dup2(write_fd, fd)
            os.close(write_fd)
            thread = threading.Thread(
                target=self._watch_stream,
                args=(name, read_fd),
                daemon=True,
            )
            thread.start()
            self._threads.append(thread)
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> None:
        flush_standard_streams()
        for name, fd in STREAM_FDS.items():
            saved_fd = self._saved_fds.pop(name)
            os.dup2(saved_fd, fd)
            os.close(saved_fd)
        for thread in self._threads:
            thread.join()

    @property
    def stdout(self) -> str:
        return "".join(self._buffers["stdout"])

    @property
    def stderr(self) -> str:
        return "".join(self._buffers["stderr"])

    def _watch_stream(self, name: str, read_fd: int) -> None:
        decoder = codecs.getincrementaldecoder("utf-8")("replace")
        try:
            while True:
                chunk = os.read(read_fd, 4096)
                if not chunk:
                    break
                text = decoder.decode(chunk)
                if text:
                    self._handle_text(name, text, final=False)
            tail = decoder.decode(b"", final=True)
            if tail:
                self._handle_text(name, tail, final=False)
            self._handle_text(name, "", final=True)
        finally:
            os.close(read_fd)

    def _handle_text(self, name: str, text: str, *, final: bool) -> None:
        with self._buffer_lock:
            self._buffers[name].append(text)
            if self._context is not None:
                self._context.stream_chunks[name].append(text)
            combined = self._pending[name] + text
            emit_chunks: list[str] = []
            while True:
                newline = combined.find("\n")
                if newline == -1:
                    break
                emit_chunks.append(combined[: newline + 1])
                combined = combined[newline + 1 :]
            if final and combined:
                emit_chunks.append(combined)
                combined = ""
            self._pending[name] = combined

        if self._request_id is None:
            return

        for chunk in emit_chunks:
            with self._emit_lock:
                emit_live_stream(name, chunk)


def install_streams() -> None:
    sys.stdout = WorkerStream(STREAM_FDS["stdout"], "stdout")
    sys.stderr = WorkerStream(STREAM_FDS["stderr"], "stderr")


def completion_kind(value: object) -> str:
    if inspect.ismodule(value):
        return "module"
    if inspect.isclass(value):
        return "class"
    if inspect.isroutine(value):
        return "function"
    return "instance"


def normalize_completion(candidate: str) -> str:
    return candidate.rstrip("(")


def cursor_line_column(code: str, cursor_pos: int) -> tuple[int, int]:
    safe_cursor_pos = max(0, min(cursor_pos, len(code)))
    before = code[:safe_cursor_pos]
    line = before.count("\n") + 1
    if "\n" in before:
        column = len(before.rsplit("\n", 1)[-1])
    else:
        column = len(before)
    return line, column


def completion_span(code: str, cursor_pos: int) -> tuple[int, int]:
    safe_cursor_pos = max(0, min(cursor_pos, len(code)))
    prefix = code[:safe_cursor_pos]
    match = COMPLETION_RE.search(prefix)
    if not match:
        return safe_cursor_pos, safe_cursor_pos
    return match.start(1), safe_cursor_pos


def absolute_offsets(code: str) -> list[int]:
    offsets = [0]
    total = 0
    for line in code.splitlines(keepends=True):
        total += len(line)
        offsets.append(total)
    if not code.endswith(("\n", "\r")):
        offsets.append(len(code))
    return offsets


def to_absolute(offsets: list[int], position: tuple[int, int]) -> int:
    line, column = position
    line_index = max(0, min(line - 1, len(offsets) - 1))
    return offsets[line_index] + column


def token_at_cursor(code: str, cursor_pos: int) -> str:
    safe_cursor_pos = max(0, min(cursor_pos, len(code)))
    offsets = absolute_offsets(code)

    try:
        raw_tokens = list(tokenize.generate_tokens(StringIO(code).readline))
    except (tokenize.TokenError, IndentationError):
        match = COMPLETION_RE.search(code[:safe_cursor_pos])
        return match.group(1) if match else ""

    significant = [
        tok
        for tok in raw_tokens
        if tok.type
        not in {
            token_types.ENDMARKER,
            token_types.NEWLINE,
            token_types.NL,
            token_types.INDENT,
            token_types.DEDENT,
        }
    ]

    names: list[dict[str, object]] = []
    token_index = 0
    while token_index < len(significant):
        tok = significant[token_index]
        if tok.type != token_types.NAME:
            token_index += 1
            continue

        start = to_absolute(offsets, tok.start)
        end = to_absolute(offsets, tok.end)
        parts = [tok.string]
        last_index = token_index
        probe = token_index + 1

        while (
            probe + 1 < len(significant)
            and significant[probe].type == token_types.OP
            and significant[probe].string == "."
            and significant[probe + 1].type == token_types.NAME
        ):
            parts.extend([".", significant[probe + 1].string])
            end = to_absolute(offsets, significant[probe + 1].end)
            last_index = probe + 1
            probe += 2

        names.append(
            {
                "text": "".join(parts),
                "start": start,
                "end": end,
                "end_index": last_index,
            }
        )
        token_index = last_index + 1

    call_ranges: list[dict[str, object]] = []
    open_calls: list[dict[str, object]] = []
    name_by_end_index = {int(name["end_index"]): name for name in names}

    for index, tok in enumerate(significant):
        if tok.type == token_types.OP and tok.string == "(":
            caller = name_by_end_index.get(index - 1)
            open_calls.append(
                {
                    "name": caller["text"] if caller is not None else "",
                    "open": to_absolute(offsets, tok.start),
                }
            )
        elif tok.type == token_types.OP and tok.string == ")" and open_calls:
            current = open_calls.pop()
            current["close"] = to_absolute(offsets, tok.end)
            call_ranges.append(current)

    for current in open_calls:
        current["close"] = len(code)
        call_ranges.append(current)

    containing_calls = [
        call
        for call in call_ranges
        if int(call["open"]) <= safe_cursor_pos <= int(call["close"]) and call["name"]
    ]
    if containing_calls:
        containing_calls.sort(key=lambda call: int(call["open"]), reverse=True)
        return str(containing_calls[0]["name"])

    for name in names:
        if int(name["start"]) <= safe_cursor_pos <= int(name["end"]):
            return str(name["text"])

    for name in reversed(names):
        if int(name["end"]) == safe_cursor_pos:
            return str(name["text"])
        if int(name["end"]) < safe_cursor_pos:
            return str(name["text"])

    return ""


def safe_signature_text(value: object, label: str) -> Optional[str]:
    try:
        signature = str(inspect.signature(value))
    except (TypeError, ValueError):
        signature = None
    if signature is not None:
        return f"{label}{signature}"

    text_signature = getattr(value, "__text_signature__", None)
    if text_signature:
        normalized = text_signature.replace("$module, ", "").replace("$self, ", "")
        normalized = normalized.replace("$module", "").replace("$self", "")
        return f"{label}{normalized}"

    doc = inspect.getdoc(value)
    if doc:
        first_line = doc.splitlines()[0].strip()
        if "(" in first_line and ")" in first_line:
            prefix, suffix = first_line.split("(", 1)
            if prefix and not first_line.startswith(label):
                return f"{label}({suffix}"
            return first_line

    return None


def rich_display_data(value: object) -> tuple[dict[str, object], dict[str, object]]:
    data: dict[str, object] = {"text/plain": repr(value)}
    metadata: dict[str, object] = {}

    mimebundle = getattr(value, "_repr_mimebundle_", None)
    if callable(mimebundle):
        try:
            bundle = mimebundle()
        except TypeError:
            bundle = mimebundle(None, None)
        except BaseException:
            bundle = None

        if isinstance(bundle, tuple) and bundle:
            raw_data = bundle[0]
            raw_metadata = bundle[1] if len(bundle) > 1 else {}
        else:
            raw_data = bundle
            raw_metadata = {}

        if isinstance(raw_data, dict):
            data.update({str(key): value for key, value in raw_data.items()})
        if isinstance(raw_metadata, dict):
            metadata.update({str(key): value for key, value in raw_metadata.items()})

    rich_repr_names = [
        ("text/html", "_repr_html_"),
        ("text/markdown", "_repr_markdown_"),
        ("text/latex", "_repr_latex_"),
        ("image/svg+xml", "_repr_svg_"),
        ("application/json", "_repr_json_"),
        ("application/javascript", "_repr_javascript_"),
    ]

    for mime_type, method_name in rich_repr_names:
        if mime_type in data:
            continue
        method = getattr(value, method_name, None)
        if not callable(method):
            continue
        try:
            rendered = method()
        except BaseException:
            continue
        if rendered is None:
            continue
        if mime_type == "application/json" and not isinstance(
            rendered, (dict, list, str, int, float, bool)
        ):
            continue
        data[mime_type] = rendered

    return data, metadata


class DisplayFormatter:
    def format(self, value: object) -> tuple[dict[str, object], dict[str, object]]:
        return rich_display_data(value)


def _format_duration(seconds: float) -> str:
    if seconds < 1e-3:
        return f"{seconds * 1_000_000:.0f} us"
    if seconds < 1:
        return f"{seconds * 1_000:.3f} ms"
    return f"{seconds:.3f} s"


def _parse_magic_invocation(source: str, prefix: str) -> tuple[str, str]:
    stripped = source.strip()
    if not stripped.startswith(prefix):
        raise UsageError(f"expected {prefix!r} magic prefix")

    body = stripped[len(prefix) :].strip()
    if not body:
        raise UsageError("magic name is required")

    name, _, args = body.partition(" ")
    return name, args.lstrip()


class RustyInteractiveShell:
    def __init__(self) -> None:
        self.display_formatter = DisplayFormatter()
        self.user_ns = namespace
        self.user_module = None
        self.autoawait = True
        self.loop_runner = AUTOAWAIT_RUNNER
        self.active_eventloop: str | None = None
        self._line_magics: dict[str, object] = {}
        self._cell_magics: dict[str, object] = {}
        self._register_default_magics()

    def __repr__(self) -> str:
        return "<RustyInteractiveShell>"

    @property
    def magics_manager(self) -> types.SimpleNamespace:
        return types.SimpleNamespace(
            magics={
                "line": dict(self._line_magics),
                "cell": dict(self._cell_magics),
            }
        )

    def register_magic_function(
        self,
        function: object,
        magic_kind: str = "line",
        magic_name: object = None,
    ) -> None:
        name = str(magic_name or getattr(function, "__name__", "")).strip()
        if not name:
            raise UsageError("magic_name is required")
        if magic_kind == "line":
            self._line_magics[name] = function
        elif magic_kind == "cell":
            self._cell_magics[name] = function
        else:
            raise UsageError(f"unsupported magic kind: {magic_kind}")

    def run_line_magic(self, magic_name: str, line: str) -> object:
        magic = self._line_magics.get(str(magic_name))
        if magic is None:
            raise UsageError(f"line magic %{magic_name} not found")
        return magic(str(line))

    def run_cell_magic(self, magic_name: str, line: str, cell: str) -> object:
        magic = self._cell_magics.get(str(magic_name))
        if magic is None:
            raise UsageError(f"cell magic %%{magic_name} not found")
        return magic(str(line), str(cell))

    def set_next_input(self, text: str, replace: bool = False) -> None:
        payload = {
            "text": str(text),
            "replace": bool(replace),
        }
        context = current_request_context()
        self.user_ns["_rustykernel_next_input"] = payload
        context.payloads[:] = [
            item for item in context.payloads if item.get("source") != "set_next_input"
        ]
        context.payloads.append(
            {
                "source": "set_next_input",
                "text": payload["text"],
                "replace": payload["replace"],
            }
        )

    def object_inspect(self, name: object) -> dict[str, object]:
        symbol = str(name)
        try:
            value = eval(symbol, namespace, namespace)
        except BaseException:
            return {"found": False}

        data, metadata = self.display_formatter.format(value)
        return {
            "found": True,
            "string_form": repr(value),
            "type_name": type(value).__name__,
            "data": data,
            "metadata": metadata,
        }

    def _register_default_magics(self) -> None:
        self.register_magic_function(self._magic_pwd, "line", "pwd")
        self.register_magic_function(self._magic_cd, "line", "cd")
        self.register_magic_function(self._magic_lsmagic, "line", "lsmagic")
        self.register_magic_function(self._magic_time, "line", "time")
        self.register_magic_function(self._magic_timeit, "line", "timeit")
        self.register_magic_function(self._magic_autoawait, "line", "autoawait")
        self.register_magic_function(self._magic_matplotlib, "line", "matplotlib")
        self.register_magic_function(self._cell_magic_time, "cell", "time")
        self.register_magic_function(self._cell_magic_timeit, "cell", "timeit")

    def _magic_pwd(self, line: str) -> str:
        if line.strip():
            raise UsageError("%pwd does not accept arguments")
        return os.getcwd()

    def _magic_cd(self, line: str) -> str:
        target = line.strip()
        if not target:
            return os.getcwd()

        path = os.path.abspath(os.path.expanduser(target))
        os.chdir(path)
        return os.getcwd()

    def _magic_lsmagic(self, line: str) -> dict[str, list[str]]:
        if line.strip():
            raise UsageError("%lsmagic does not accept arguments")
        return {
            "line": sorted(self._line_magics),
            "cell": sorted(self._cell_magics),
        }

    def _magic_time(self, line: str) -> object:
        return self._run_timed_code(line)

    def _cell_magic_time(self, line: str, cell: str) -> object:
        if line.strip():
            raise UsageError("%%time does not accept arguments")
        return self._run_timed_code(cell)

    def _magic_timeit(self, line: str) -> None:
        if not line.strip():
            raise UsageError("%timeit requires code to run")
        self._run_timeit(line)
        return None

    def _cell_magic_timeit(self, line: str, cell: str) -> None:
        if line.strip():
            raise UsageError("%%timeit does not accept arguments")
        self._run_timeit(cell)
        return None

    def _magic_autoawait(self, line: str) -> str:
        global AUTOAWAIT_ENABLED, AUTOAWAIT_RUNNER

        argument = line.strip().lower()
        if argument in {"", "asyncio", "on", "true", "1"}:
            AUTOAWAIT_ENABLED = True
            AUTOAWAIT_RUNNER = "asyncio"
            self.autoawait = True
            self.loop_runner = AUTOAWAIT_RUNNER
        elif argument in {"off", "false", "0", "no"}:
            AUTOAWAIT_ENABLED = False
            self.autoawait = False
            self.loop_runner = "sync"
        else:
            raise UsageError(
                "rustykernel currently only supports %autoawait asyncio or %autoawait off"
            )

        status = "on" if AUTOAWAIT_ENABLED else "off"
        runner = AUTOAWAIT_RUNNER if AUTOAWAIT_ENABLED else "sync"
        return f"autoawait={status}, runner={runner}"

    def _magic_matplotlib(self, line: str) -> None:
        backend = line.strip().lower()
        if backend not in {"", "inline"}:
            raise UsageError(
                "rustykernel currently only accepts %matplotlib inline as a compatibility no-op"
            )
        self.active_eventloop = "inline"
        self.user_ns["_rustykernel_matplotlib_backend"] = "inline"
        return None

    def _run_timed_code(self, code: str) -> object:
        start = time.perf_counter()
        try:
            return run_interactive_code(code)
        finally:
            elapsed = time.perf_counter() - start
            print(f"Wall time: {_format_duration(elapsed)}")

    def _run_timeit(self, code: str, *, repeat: int = 3) -> None:
        loops = 1
        while loops < 100_000:
            elapsed = self._timeit_batch(code, loops)
            if elapsed >= 0.02:
                break
            loops *= 10

        timings = [self._timeit_batch(code, loops) for _ in range(repeat)]
        best = min(timings) / loops
        print(f"{loops} loops, best of {repeat}: {_format_duration(best)} per loop")

    def _timeit_batch(self, code: str, loops: int) -> float:
        start = time.perf_counter()
        sink = io.StringIO()
        for _ in range(loops):
            with contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
                run_interactive_code_silently(code)
        return time.perf_counter() - start


class RustyDisplayHook(IPythonDisplayHook):
    def write_output_prompt(self) -> None:
        return None

    def write_format_data(self, format_dict, md_dict=None) -> None:
        current_request_context().execute_result = {
            "data": dict(format_dict),
            "metadata": dict(md_dict or {}),
        }


class RustyDisplayPublisher(IPythonDisplayPublisher):
    def publish(
        self,
        data,
        metadata=None,
        source=None,
        *,
        transient=None,
        update=False,
        **kwargs,
    ) -> None:
        current_request_context().display_events.append(
            {
                "msg_type": "update_display_data" if update else "display_data",
                "data": dict(data),
                "metadata": dict(metadata or {}),
                "transient": dict(transient or {}),
            }
        )

    def clear_output(self, wait: bool = False) -> None:
        current_request_context().display_events.append(
            {
                "msg_type": "clear_output",
                "content": {"wait": bool(wait)},
            }
        )


class RequestScopedPayloadManager(IPythonPayloadManager):
    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._fallback_payloads: list[dict[str, object]] = []

    def _payloads(self) -> list[dict[str, object]]:
        try:
            return current_request_context().payloads
        except RuntimeError:
            return self._fallback_payloads

    def write_payload(self, data: dict[str, object], single: bool = True) -> None:
        payloads = self._payloads()
        source = data.get("source")
        if single and source is not None:
            payloads[:] = [item for item in payloads if item.get("source") != source]
        payloads.append(dict(data))

    def read_payload(self) -> list[dict[str, object]]:
        return list(self._payloads())

    def clear_payload(self) -> None:
        self._payloads().clear()


def build_interactive_shell() -> InteractiveShell:
    global namespace, completer

    user_module = types.ModuleType("__main__")
    namespace = user_module.__dict__
    namespace["__name__"] = "__main__"
    completer = rlcompleter.Completer(namespace)

    InteractiveShell.clear_instance()
    shell = InteractiveShell.instance(user_ns=namespace, user_module=user_module)

    displayhook = RustyDisplayHook(shell=shell, cache_size=shell.cache_size)
    shell.displayhook = displayhook
    shell.display_trap.hook = displayhook
    shell.display_pub = RustyDisplayPublisher(shell=shell)
    shell.payload_manager = RequestScopedPayloadManager(parent=shell)
    shell.showtraceback = lambda *args, **kwargs: None  # type: ignore[method-assign]
    shell.showsyntaxerror = lambda *args, **kwargs: None  # type: ignore[method-assign]

    # Match ipykernel's notebook default: when users import matplotlib without
    # explicitly selecting a backend, prefer the inline backend.
    if not os.environ.get("MPLBACKEND"):
        os.environ["MPLBACKEND"] = "module://matplotlib_inline.backend_inline"

    def rustykernel_enable_gui(self: InteractiveShell, gui: object = None) -> None:
        normalized = "inline" if gui in (None, "") else str(gui).lower()
        if normalized != "inline":
            raise UsageError("rustykernel only supports the inline matplotlib backend")
        self.active_eventloop = "inline"
        self.user_ns["_rustykernel_matplotlib_backend"] = "inline"
        return None

    shell.enable_gui = types.MethodType(  # type: ignore[method-assign]
        rustykernel_enable_gui, shell
    )

    def rustykernel_set_next_input(text: str, replace: bool = False) -> None:
        shell.rl_next_input = text
        shell.payload_manager.write_payload(
            {
                "source": "set_next_input",
                "text": text,
                "replace": replace,
            }
        )

    shell.set_next_input = rustykernel_set_next_input  # type: ignore[method-assign]

    matplotlib_magic = shell.find_line_magic("matplotlib")
    if callable(matplotlib_magic):

        def rustykernel_matplotlib(line: str):
            backend = line.strip().lower()
            if backend not in {"", "inline"}:
                raise UsageError("rustykernel only supports %matplotlib inline")
            try:
                return matplotlib_magic("inline")
            except ModuleNotFoundError as exc:
                if exc.name == "matplotlib" and backend in {"", "inline"}:
                    shell.active_eventloop = "inline"
                    shell.user_ns["_rustykernel_matplotlib_backend"] = "inline"
                    return None
                raise

        shell.register_magic_function(rustykernel_matplotlib, "line", "matplotlib")

    return shell


INTERACTIVE_SHELL = build_interactive_shell()


def publish_display_event(
    value: object,
    *,
    msg_type: str,
    transient: Optional[dict[str, object]] = None,
    content: Optional[dict[str, object]] = None,
) -> None:
    if content is not None:
        current_request_context().display_events.append(
            {"msg_type": msg_type, "content": content}
        )
        return

    data, metadata = rich_display_data(value)
    current_request_context().display_events.append(
        {
            "msg_type": msg_type,
            "data": data,
            "metadata": metadata,
            "transient": transient or {},
        }
    )


def publish_comm_event(
    msg_type: str,
    *,
    comm_id: str,
    data: object = None,
    metadata: object = None,
    target_name: Optional[str] = None,
) -> None:
    content: dict[str, object] = {
        "comm_id": comm_id,
        "data": {} if data is None else data,
        "metadata": {} if metadata is None else metadata,
    }
    if target_name is not None:
        content["target_name"] = target_name
    current_request_context().comm_events.append(
        {"msg_type": msg_type, "content": content}
    )


class DisplayHandle:
    def __init__(self, display_id: str):
        self.display_id = display_id

    def display(self, *values: object, **kwargs: object) -> "DisplayHandle":
        display(*values, display_id=self.display_id, **kwargs)
        return self

    def update(self, value: object, **kwargs: object) -> "DisplayHandle":
        update_display(value, display_id=self.display_id, **kwargs)
        return self


class _MimeDisplayObject:
    mime_type: str = ""

    def __init__(self, data: object = "", metadata: Optional[dict[str, object]] = None):
        self.data = data
        self.metadata = {} if metadata is None else dict(metadata)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.data!r})"

    def _repr_mimebundle_(self) -> tuple[dict[str, object], dict[str, object]]:
        return {
            "text/plain": repr(self),
            self.mime_type: self.data,
        }, self.metadata


class HTML(_MimeDisplayObject):
    mime_type = "text/html"


class Markdown(_MimeDisplayObject):
    mime_type = "text/markdown"


class Latex(_MimeDisplayObject):
    mime_type = "text/latex"


class SVG(_MimeDisplayObject):
    mime_type = "image/svg+xml"


class Javascript(_MimeDisplayObject):
    mime_type = "application/javascript"


class Pretty(_MimeDisplayObject):
    mime_type = "text/plain"


class JSONDisplayObject:
    def __init__(self, data: object, metadata: Optional[dict[str, object]] = None):
        self.data = data
        self.metadata = {} if metadata is None else dict(metadata)

    def __repr__(self) -> str:
        return f"JSON({self.data!r})"

    def _repr_mimebundle_(self) -> tuple[dict[str, object], dict[str, object]]:
        return {
            "text/plain": repr(self),
            "application/json": self.data,
        }, self.metadata


def _normalize_display_id(display_id: object) -> Optional[str]:
    if display_id is None or display_id is False:
        return None
    if display_id is True:
        return str(uuid.uuid4())
    return str(display_id)


def _display_data_and_metadata(
    value: object,
    *,
    raw: bool = False,
    metadata: Optional[dict[str, object]] = None,
) -> tuple[dict[str, object], dict[str, object]]:
    if raw:
        if not isinstance(value, dict):
            raise TypeError("raw display expects a mimebundle dict")
        return {str(key): bundle for key, bundle in value.items()}, metadata or {}

    data, rich_metadata = rich_display_data(value)
    if metadata:
        rich_metadata = {**rich_metadata, **metadata}
    return data, rich_metadata


def _publish_display_bundle(
    data: dict[str, object],
    metadata: dict[str, object],
    *,
    msg_type: str,
    transient: Optional[dict[str, object]] = None,
) -> None:
    current_request_context().display_events.append(
        {
            "msg_type": msg_type,
            "data": data,
            "metadata": metadata,
            "transient": transient or {},
        }
    )


def display(
    *values: object,
    display_id: object = None,
    raw: bool = False,
    metadata: Optional[dict[str, object]] = None,
) -> Optional[DisplayHandle]:
    normalized_display_id = _normalize_display_id(display_id)
    transient: dict[str, object] = {}
    if normalized_display_id is not None:
        transient["display_id"] = normalized_display_id

    for value in values:
        data, rich_metadata = _display_data_and_metadata(
            value,
            raw=raw,
            metadata=metadata,
        )
        _publish_display_bundle(
            data,
            rich_metadata,
            msg_type="display_data",
            transient=transient,
        )

    if normalized_display_id is None:
        return None
    return DisplayHandle(normalized_display_id)


def update_display(
    value: object,
    *,
    display_id: object,
    raw: bool = False,
    metadata: Optional[dict[str, object]] = None,
) -> None:
    normalized_display_id = _normalize_display_id(display_id)
    if normalized_display_id is None:
        raise ValueError("display_id is required for update_display")

    data, rich_metadata = _display_data_and_metadata(
        value,
        raw=raw,
        metadata=metadata,
    )
    _publish_display_bundle(
        data,
        rich_metadata,
        msg_type="update_display_data",
        transient={"display_id": normalized_display_id},
    )


def clear_output(wait: bool = False) -> None:
    publish_display_event(
        None,
        msg_type="clear_output",
        content={"wait": bool(wait)},
    )


def request_input(prompt: object = "", *, password: bool = False) -> str:
    context = current_request_context()

    emit_protocol_message(
        {
            "id": context.request_id,
            "event": "input_request",
            "prompt": str(prompt),
            "password": password,
        }
    )

    if threading.get_ident() == _MAIN_THREAD_ID:
        while True:
            raw_line = protocol_stdin().readline()
            if not raw_line:
                raise EOFError("stdin channel closed")
            if not raw_line.strip():
                continue

            response = json.loads(raw_line)
            if response.get("kind") != "input_reply":
                continue
            if int(response.get("id", -1)) != context.request_id:
                continue

            error = response.get("error")
            if error:
                raise EOFError(str(error))
            return str(response.get("value", ""))

    while True:
        response = context.input_queue.get()

        error = response.get("error")
        if error:
            raise EOFError(str(error))
        return str(response.get("value", ""))


def install_input_api() -> None:
    builtins.input = request_input
    getpass.getpass = lambda prompt="Password: ", stream=None: request_input(
        prompt, password=True
    )
    namespace["input"] = request_input


def install_display_api() -> None:
    from IPython import display as display_module

    exported_names = [
        "display",
        "update_display",
        "clear_output",
        "DisplayHandle",
        "HTML",
        "Markdown",
        "JSON",
        "Javascript",
        "Latex",
        "SVG",
        "Pretty",
        "Image",
    ]
    for name in exported_names:
        if hasattr(display_module, name):
            namespace[name] = getattr(display_module, name)


def get_ipython() -> InteractiveShell:
    return ipython_get_ipython()


def install_ipython_api() -> None:
    namespace["get_ipython"] = get_ipython
    builtins.get_ipython = get_ipython


class Comm:
    def __init__(
        self,
        *,
        target_name: str = "",
        comm_id: object = None,
        data: object = None,
        metadata: object = None,
        primary: bool = True,
    ):
        self.comm_id = str(comm_id or uuid.uuid4())
        self.target_name = str(target_name)
        self.primary = primary
        self._closed = False
        self._msg_callback = None
        self._close_callback = None

        COMMS[self.comm_id] = self
        if primary:
            publish_comm_event(
                "comm_open",
                comm_id=self.comm_id,
                target_name=self.target_name,
                data=data,
                metadata=metadata,
            )

    def send(self, data: object = None, metadata: object = None) -> None:
        if self._closed:
            raise RuntimeError(f"Cannot send on closed comm {self.comm_id}")
        publish_comm_event(
            "comm_msg",
            comm_id=self.comm_id,
            data=data,
            metadata=metadata,
        )

    def close(self, data: object = None, metadata: object = None) -> None:
        if self._closed:
            return
        self._closed = True
        COMMS.pop(self.comm_id, None)
        publish_comm_event(
            "comm_close",
            comm_id=self.comm_id,
            data=data,
            metadata=metadata,
        )

    def on_msg(self, callback):
        self._msg_callback = callback
        return callback

    def on_close(self, callback):
        self._close_callback = callback
        return callback

    def handle_msg(self, msg: dict[str, object]) -> None:
        if callable(self._msg_callback):
            self._msg_callback(msg)

    def handle_close(self, msg: dict[str, object]) -> None:
        if callable(self._close_callback):
            self._close_callback(msg)


def register_target(target_name: object, callback: object) -> None:
    COMM_TARGETS[str(target_name)] = callback


def unregister_target(target_name: object) -> None:
    COMM_TARGETS.pop(str(target_name), None)


def install_comm_api() -> None:
    namespace["Comm"] = Comm
    namespace["register_comm_target"] = register_target
    namespace["unregister_comm_target"] = unregister_target

    rustykernel_module = sys.modules.get("rustykernel")
    if rustykernel_module is None:
        try:
            __import__("rustykernel")
            rustykernel_module = sys.modules.get("rustykernel")
        except ImportError:
            rustykernel_module = types.ModuleType("rustykernel")
            rustykernel_module.__path__ = []
            sys.modules["rustykernel"] = rustykernel_module

    comm_module = types.ModuleType("rustykernel.comm")
    comm_module.Comm = Comm
    comm_module.register_target = register_target
    comm_module.unregister_target = unregister_target
    comm_module.__all__ = ["Comm", "register_target", "unregister_target"]
    sys.modules["rustykernel.comm"] = comm_module
    rustykernel_module.comm = comm_module


def run_interactive_code_silently(code: str) -> None:
    INTERACTIVE_SHELL.run_cell(code, store_history=False, silent=True)


def run_interactive_code(code: str) -> object:
    return INTERACTIVE_SHELL.run_cell(code, store_history=False, silent=False)


def format_execution_error(error: BaseException) -> tuple[str, str, list[str]]:
    if isinstance(error, SyntaxError):
        traceback_lines = INTERACTIVE_SHELL.SyntaxTB.structured_traceback(
            type(error), error, error.__traceback__
        )
    else:
        traceback_lines = INTERACTIVE_SHELL.InteractiveTB.structured_traceback(
            type(error), error, error.__traceback__
        )
    return error.__class__.__name__, str(error), list(traceback_lines)


@contextlib.contextmanager
def override_cell_filename(source_path: str):
    compiler = INTERACTIVE_SHELL.compile
    original_cache = compiler.cache

    def cache_with_debug_filename(
        transformed_code: str, number: int = 0, raw_code: object | None = None
    ) -> str:
        compiler._filename_map[source_path] = number
        linecache.cache[source_path] = (
            len(transformed_code),
            None,
            [line + "\n" for line in transformed_code.splitlines()],
            source_path,
        )
        return source_path

    compiler.cache = cache_with_debug_filename  # type: ignore[assignment]
    try:
        yield
    finally:
        compiler.cache = original_cache  # type: ignore[assignment]


def execute_with_ipython(
    code: str,
    *,
    execution_count: int,
    silent: bool,
    store_history: bool,
) -> object:
    global AUTOAWAIT_ENABLED

    current_request_context().execute_result = None
    INTERACTIVE_SHELL.execution_count = max(1, execution_count)
    AUTOAWAIT_ENABLED = bool(getattr(INTERACTIVE_SHELL, "autoawait", True))
    INTERACTIVE_SHELL.payload_manager.clear_payload()

    preprocessing_exc_tuple = None
    try:
        transformed_cell = INTERACTIVE_SHELL.transform_cell(code)
    except Exception:
        transformed_cell = code
        preprocessing_exc_tuple = sys.exc_info()

    source_path = DEBUG_STATE.source_path_for_code(code)
    should_run_async = (
        hasattr(INTERACTIVE_SHELL, "run_cell_async")
        and hasattr(INTERACTIVE_SHELL, "should_run_async")
        and INTERACTIVE_SHELL.should_run_async(
            code,
            transformed_cell=transformed_cell,
            preprocessing_exc_tuple=preprocessing_exc_tuple,
        )
    )
    if source_path is not None:
        with override_cell_filename(source_path):
            if should_run_async:
                return asyncio.run(
                    INTERACTIVE_SHELL.run_cell_async(
                        code,
                        store_history=store_history,
                        silent=silent,
                        transformed_cell=transformed_cell,
                        preprocessing_exc_tuple=preprocessing_exc_tuple,
                    )
                )

            return INTERACTIVE_SHELL.run_cell(
                code,
                store_history=store_history,
                silent=silent,
            )

    if should_run_async:
        return asyncio.run(
            INTERACTIVE_SHELL.run_cell_async(
                code,
                store_history=store_history,
                silent=silent,
                transformed_cell=transformed_cell,
                preprocessing_exc_tuple=preprocessing_exc_tuple,
            )
        )

    return INTERACTIVE_SHELL.run_cell(
        code,
        store_history=store_history,
        silent=silent,
    )


install_streams()
install_display_api()
install_ipython_api()
install_input_api()
install_comm_api()


def evaluate_user_expressions(user_expressions: object) -> dict[str, object]:
    if not isinstance(user_expressions, dict):
        return {}
    return dict(INTERACTIVE_SHELL.user_expressions(user_expressions))


def comm_message(
    comm_id: str,
    *,
    target_name: Optional[str] = None,
    data: object = None,
    metadata: object = None,
) -> dict[str, object]:
    content: dict[str, object] = {
        "comm_id": comm_id,
        "data": {} if data is None else data,
        "metadata": {} if metadata is None else metadata,
    }
    if target_name is not None:
        content["target_name"] = target_name
    return {"content": content}


def handle_comm_open(
    comm_id: str,
    target_name: str,
    data: object = None,
    metadata: object = None,
) -> dict[str, object]:
    context = RequestContext(request_id=0, subshell_id=None)

    with bind_request_context(context):
        with FdCapture() as capture:
            callback = COMM_TARGETS.get(target_name)
            if callback is None:
                print(f"No such comm target registered: {target_name}", file=sys.stderr)
                publish_comm_event("comm_close", comm_id=comm_id)
            else:
                comm = Comm(comm_id=comm_id, target_name=target_name, primary=False)
                try:
                    callback(
                        comm,
                        comm_message(
                            comm_id,
                            target_name=target_name,
                            data=data,
                            metadata=metadata,
                        ),
                    )
                except BaseException:
                    traceback.print_exc()
                    comm.close()

    return {
        "stdout": capture.stdout,
        "stderr": capture.stderr,
        "events": list(context.comm_events),
        "registered": comm_id in COMMS,
    }


def handle_comm_msg(
    comm_id: str, data: object = None, metadata: object = None
) -> dict[str, object]:
    context = RequestContext(request_id=0, subshell_id=None)

    with bind_request_context(context):
        with FdCapture() as capture:
            comm = COMMS.get(comm_id)
            if comm is None:
                print(f"No such comm: {comm_id}", file=sys.stderr)
            else:
                try:
                    comm.handle_msg(comm_message(comm_id, data=data, metadata=metadata))
                except BaseException:
                    traceback.print_exc()

    return {
        "stdout": capture.stdout,
        "stderr": capture.stderr,
        "events": list(context.comm_events),
        "registered": comm_id in COMMS,
    }


def handle_comm_close(
    comm_id: str, data: object = None, metadata: object = None
) -> dict[str, object]:
    context = RequestContext(request_id=0, subshell_id=None)

    with bind_request_context(context):
        with FdCapture() as capture:
            comm = COMMS.get(comm_id)
            if comm is None:
                print(f"No such comm: {comm_id}", file=sys.stderr)
            else:
                comm._closed = True
                del COMMS[comm_id]
                try:
                    comm.handle_close(
                        comm_message(comm_id, data=data, metadata=metadata)
                    )
                except BaseException:
                    traceback.print_exc()

    return {
        "stdout": capture.stdout,
        "stderr": capture.stderr,
        "events": list(context.comm_events),
        "registered": comm_id in COMMS,
    }


def execute(
    code: str,
    request_id: int,
    subshell_id: SubshellId,
    user_expressions: object | None = None,
    *,
    execution_count: int,
    silent: bool,
    store_history: bool,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "status": "ok",
        "stdout": "",
        "stderr": "",
        "displays": [],
        "comm_events": [],
        "debug_events": [],
        "payload": [],
        "result": None,
        "user_expressions": {},
        "ename": None,
        "evalue": None,
        "traceback": [],
    }

    context = RequestContext(request_id=request_id, subshell_id=subshell_id)

    try:
        with bind_request_context(context):
            # fd-level stdout/stderr capture redirects process-global file
            # descriptors, so overlapping captures across subshell lanes are not
            # safe. Serialize the capture window to avoid deadlocks/cross-talk
            # while preserving per-subshell routing at the control plane.
            with _FD_CAPTURE_LOCK:
                with FdCapture(request_id=request_id):
                    if interrupt_requested(subshell_id):
                        raise KeyboardInterrupt()

                    result = execute_with_ipython(
                        code,
                        execution_count=execution_count,
                        silent=silent,
                        store_history=store_history,
                    )

                    error = (
                        result.error_before_exec
                        if result.error_before_exec is not None
                        else result.error_in_exec
                    )
                    if error is None:
                        payload["user_expressions"] = evaluate_user_expressions(
                            user_expressions
                        )
                    else:
                        payload["status"] = "error"
                        (
                            payload["ename"],
                            payload["evalue"],
                            payload["traceback"],
                        ) = format_execution_error(error)
    except BaseException as exc:
        payload["status"] = "error"
        payload["ename"] = exc.__class__.__name__
        payload["evalue"] = str(exc)
        payload["traceback"] = traceback.format_exception(
            type(exc), exc, exc.__traceback__
        )

    sys.stdout.flush()
    sys.stderr.flush()
    payload["stdout"] = "".join(context.stream_chunks["stdout"])
    payload["stderr"] = "".join(context.stream_chunks["stderr"])
    payload["displays"] = list(context.display_events)
    payload["comm_events"] = list(context.comm_events)
    payload["debug_events"] = DEBUG_STATE.capture_breakpoint_stop(code)
    payload["payload"] = list(context.payloads)
    clear_interrupt(subshell_id)
    context.payloads.clear()
    if payload["status"] == "ok" and context.execute_result is not None and not silent:
        payload["result"] = context.execute_result
    return payload


def complete(code: str, cursor_pos: int) -> dict[str, object]:
    cursor_start, cursor_end = completion_span(code, cursor_pos)

    if jedi is not None:
        try:
            line, column = cursor_line_column(code, cursor_pos)
            interpreter = jedi.Interpreter(code, [namespace])
            jedi_completions = interpreter.complete(line, column)

            matches: list[str] = []
            seen: set[str] = set()
            completion_types: list[dict[str, object]] = []
            for item in jedi_completions:
                completed = f"{code[:cursor_end]}{item.complete}"
                normalized = normalize_completion(completed)
                if normalized in seen:
                    continue
                seen.add(normalized)
                matches.append(normalized)
                signature = None
                try:
                    signature = safe_signature_text(
                        eval(normalized, namespace, namespace), normalized
                    )
                except BaseException:
                    if getattr(item, "type", None) == "function":
                        signature = None
                completion_types.append(
                    {
                        "start": cursor_start,
                        "end": cursor_end,
                        "text": normalized,
                        "type": getattr(item, "type", None) or "unknown",
                        "signature": signature,
                    }
                )

            matches.sort()
            completion_types.sort(key=lambda item: str(item["text"]))
            return {
                "status": "ok",
                "matches": matches,
                "cursor_start": cursor_start,
                "cursor_end": cursor_end,
                "metadata": {
                    "_jupyter_types_experimental": completion_types,
                    "backend": "jedi",
                },
            }
        except BaseException:
            pass

    safe_cursor_pos = max(0, min(cursor_pos, len(code)))
    prefix = code[:safe_cursor_pos]
    match = COMPLETION_RE.search(prefix)
    token = match.group(1) if match else ""

    matches: list[str] = []
    seen: set[str] = set()
    completion_types: list[dict[str, object]] = []
    state = 0
    while True:
        candidate = completer.complete(token, state)
        if candidate is None:
            break
        normalized = normalize_completion(candidate)
        if normalized not in seen:
            seen.add(normalized)
            matches.append(normalized)
            item_type = "keyword" if normalized in keyword.kwlist else None
            signature = None
            if item_type is None:
                try:
                    value = eval(normalized, namespace, namespace)
                    item_type = completion_kind(value)
                    signature = safe_signature_text(value, normalized)
                except BaseException:
                    item_type = "unknown"
            completion_types.append(
                {
                    "start": cursor_start,
                    "end": cursor_end,
                    "text": normalized,
                    "type": item_type,
                    "signature": signature,
                }
            )
        state += 1

    matches.sort()
    completion_types.sort(key=lambda item: str(item["text"]))

    return {
        "status": "ok",
        "matches": matches,
        "cursor_start": cursor_start,
        "cursor_end": cursor_end,
        "metadata": {
            "_jupyter_types_experimental": completion_types,
            "backend": "rlcompleter",
        },
    }


def inspect_symbol(code: str, cursor_pos: int, detail_level: int) -> dict[str, object]:
    token = token_at_cursor(code, cursor_pos)

    if not token:
        return {
            "status": "ok",
            "found": False,
            "data": {},
            "metadata": {},
        }

    try:
        value = eval(token, namespace, namespace)
    except BaseException:
        return {
            "status": "ok",
            "found": False,
            "data": {},
            "metadata": {},
        }

    lines: list[str] = [repr(value)]
    type_name = type(value).__name__
    qualname = type(value).__qualname__
    lines.append(f"type: {qualname}")
    metadata: dict[str, object] = {"type_name": type_name}
    markdown_lines: list[str] = [f"`{token}`", "", f"**type:** `{qualname}`"]

    signature = safe_signature_text(value, token)
    if signature:
        lines.append(f"signature: {signature}")
        metadata["signature"] = signature
        markdown_lines.extend(["", "```python", signature, "```"])

    doc = inspect.getdoc(value)
    if doc:
        metadata["doc_summary"] = doc.splitlines()[0]
        if detail_level > 0:
            lines.append("")
            lines.append(doc)
            markdown_lines.extend(["", doc])
        else:
            lines.append("")
            lines.append(doc.splitlines()[0])
            markdown_lines.extend(["", doc.splitlines()[0]])

    module_name = getattr(value, "__module__", None)
    if module_name:
        metadata["module"] = module_name

    try:
        source_file = inspect.getsourcefile(value) or inspect.getfile(value)
    except (TypeError, OSError):
        source_file = None
    if source_file:
        metadata["source_file"] = source_file
        lines.append(f"source: {source_file}")
        markdown_lines.extend(["", f"source: `{source_file}`"])

    if detail_level > 0:
        try:
            source = inspect.getsource(value).rstrip()
        except (OSError, TypeError):
            source = None
        if source:
            metadata["source"] = source
            lines.extend(["", source])
            markdown_lines.extend(["", "```python", source, "```"])

    return {
        "status": "ok",
        "found": True,
        "data": {
            "text/plain": "\n".join(lines),
            "text/markdown": "\n".join(markdown_lines),
            "text/html": "<br/>".join(
                html.escape(line) if line else "" for line in markdown_lines
            ),
        },
        "metadata": metadata,
    }


def completion_indent(code: str) -> str:
    stripped = code.rstrip()
    if not stripped:
        return ""

    last_line = stripped.splitlines()[-1]
    leading = len(last_line) - len(last_line.lstrip(" "))
    base_indent = " " * leading
    if last_line.rstrip().endswith(":"):
        return base_indent + "    "
    return base_indent


def is_complete(code: str) -> dict[str, object]:
    status, indent = INTERACTIVE_SHELL.input_transformer_manager.check_complete(code)
    return {
        "status": str(status),
        "indent": ""
        if indent is None
        else (" " * int(indent) if isinstance(indent, int) else str(indent)),
    }


def kernel_info_payload() -> dict[str, object]:
    version_info = sys.version_info
    return {
        "language_version": sys.version.split()[0],
        "language_version_major": version_info.major,
        "language_version_minor": version_info.minor,
    }


class ExecutionLane:
    def __init__(self, subshell_id: SubshellId) -> None:
        self.subshell_id = subshell_id
        self.requests: queue.Queue[dict[str, object] | None] = queue.Queue()
        self._active_lock = threading.Lock()
        self._active_request_id: int | None = None
        self.thread = threading.Thread(
            target=self._run,
            name="main-shell" if subshell_id is None else f"subshell-{subshell_id}",
            daemon=True,
        )
        self.thread.start()

    def submit(self, request: dict[str, object]) -> None:
        self.requests.put(request)

    def stop(self) -> None:
        self.requests.put(None)
        self.thread.join()

    def interrupt(self) -> None:
        with self._active_lock:
            active_request_id = self._active_request_id
        if active_request_id is None:
            return
        request_interrupt(self.subshell_id)
        ident = self.thread.ident
        if ident is None:
            return
        result = _PY_ASYNC_EXC(ident, KeyboardInterrupt)
        if result > 1:
            _PY_ASYNC_EXC(ident, None)
            raise RuntimeError("failed to deliver KeyboardInterrupt to execution lane")

    def _run(self) -> None:
        while True:
            request = self.requests.get()
            if request is None:
                return
            request_id = int(request["id"])
            with self._active_lock:
                self._active_request_id = request_id
            try:
                response = {
                    "id": request_id,
                    **execute(
                        str(request.get("code", "")),
                        request_id,
                        self.subshell_id,
                        request.get("user_expressions", {}),
                        execution_count=int(request.get("execution_count", 0)),
                        silent=bool(request.get("silent", False)),
                        store_history=bool(request.get("store_history", True)),
                    ),
                }
            except BaseException as exc:
                response = {
                    "id": request_id,
                    "status": "error",
                    "stdout": "",
                    "stderr": "",
                    "displays": [],
                    "comm_events": [],
                    "debug_events": [],
                    "payload": [],
                    "result": None,
                    "user_expressions": {},
                    "ename": exc.__class__.__name__,
                    "evalue": str(exc),
                    "traceback": traceback.format_exception(
                        type(exc), exc, exc.__traceback__
                    ),
                }
            finally:
                with self._active_lock:
                    self._active_request_id = None
            emit_protocol_message(response)


class SubshellManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._lanes: dict[SubshellId, ExecutionLane] = {None: ExecutionLane(None)}

    def create_subshell(self) -> str:
        subshell_id = str(uuid.uuid4())
        with self._lock:
            self._lanes[subshell_id] = ExecutionLane(subshell_id)
        return subshell_id

    def delete_subshell(self, subshell_id: str) -> None:
        with self._lock:
            lane = self._lanes.pop(subshell_id)
        lane.stop()

    def list_subshells(self) -> list[str]:
        with self._lock:
            return sorted(
                str(subshell_id)
                for subshell_id in self._lanes
                if subshell_id is not None
            )

    def submit_execute(
        self, request: dict[str, object], subshell_id: SubshellId
    ) -> bool:
        with self._lock:
            lane = self._lanes.get(subshell_id)
        if lane is None:
            return False
        lane.submit(request)
        return True

    def interrupt_all(self) -> None:
        with self._lock:
            lanes = list(self._lanes.values())
        for lane in lanes:
            try:
                lane.interrupt()
            except BaseException:
                continue

    def interrupt_subshells(self) -> None:
        with self._lock:
            lanes = [
                lane
                for subshell_id, lane in self._lanes.items()
                if subshell_id is not None
            ]
        for lane in lanes:
            try:
                lane.interrupt()
            except BaseException:
                continue


class DebugpyDAPClient:
    def __init__(self) -> None:
        self.endpoint: tuple[str, int] | None = None

    def ensure_endpoint(self) -> tuple[str, int]:
        if self.endpoint is None:
            if debugpy is None:
                raise RuntimeError("debugpy is not available")
            self.endpoint = tuple(debugpy.listen(("127.0.0.1", 0)))  # type: ignore[arg-type]
        return self.endpoint

    def close(self) -> None:
        return None


class DebugState:
    def __init__(self) -> None:
        self.available = debugpy is not None
        self.dap = DebugpyDAPClient() if self.available else None
        self.breakpoints: dict[str, list[dict[str, object]]] = {}
        self.cell_sources: dict[str, str] = {}
        self.tmp_dir = tempfile.mkdtemp(prefix="rustykernel-debug-")

    def source_path_for_code(self, code: str) -> str | None:
        digest = hashlib.sha1(code.encode("utf-8")).hexdigest()[:16]
        return self.cell_sources.get(digest)

    def _visible_namespace(self) -> dict[str, object]:
        return {
            key: value
            for key, value in namespace.items()
            if not (key.startswith("__") and key.endswith("__"))
        }

    def capture_breakpoint_stop(self, code: str) -> list[dict[str, object]]:
        source_path = self.source_path_for_code(code)
        breakpoints = (
            [] if source_path is None else self.breakpoints.get(source_path, [])
        )

        if source_path is None:
            return []

        if not breakpoints:
            return []

        line = 1
        if isinstance(breakpoints[0], dict) and isinstance(
            breakpoints[0].get("line"), int
        ):
            line = int(breakpoints[0]["line"])

        visible_namespace = self._visible_namespace()
        locals_ref = 1000
        globals_ref = 1001
        variables = [
            {
                "name": key,
                "value": repr(value),
                "type": type(value).__name__,
                "variablesReference": 0,
            }
            for key, value in sorted(visible_namespace.items())
        ]
        return [
            {
                "msg_type": "debug_event",
                "content": {
                    "seq": 0,
                    "type": "event",
                    "event": "stopped",
                    "body": {
                        "reason": "breakpoint",
                        "threadId": 1,
                        "allThreadsStopped": True,
                        "rustykernel": {
                            "stackFrames": [
                                {
                                    "id": 1,
                                    "name": "<module>",
                                    "line": line,
                                    "column": 1,
                                    "source": {"path": source_path},
                                }
                            ],
                            "scopesByFrame": {
                                "1": [
                                    {
                                        "name": "Locals",
                                        "presentationHint": "locals",
                                        "variablesReference": locals_ref,
                                        "expensive": False,
                                    },
                                    {
                                        "name": "Globals",
                                        "presentationHint": "globals",
                                        "variablesReference": globals_ref,
                                        "expensive": False,
                                    },
                                ]
                            },
                            "variablesByRef": {
                                str(locals_ref): variables,
                                str(globals_ref): variables,
                            },
                        },
                    },
                },
            }
        ]

    def debug_info(self, request: dict[str, object]) -> dict[str, object]:
        request_seq = int(request.get("seq", 0))
        breakpoint_list = [
            {"source": path, "breakpoints": breakpoints}
            for path, breakpoints in sorted(self.breakpoints.items())
        ]
        return {
            "type": "response",
            "request_seq": request_seq,
            "success": True,
            "command": "debugInfo",
            "body": {
                "isStarted": False,
                "hashMethod": "Murmur2",
                "hashSeed": 0,
                "tmpFilePrefix": self.tmp_dir + os.sep,
                "tmpFileSuffix": ".py",
                "breakpoints": breakpoint_list,
                "stoppedThreads": [],
                "richRendering": True,
                "exceptionPaths": ["Python Exceptions"],
                "copyToGlobals": True,
            },
        }

    def listen_endpoint(self) -> dict[str, object]:
        if self.dap is None:
            return {
                "available": False,
                "host": "",
                "port": 0,
            }
        host, port = self.dap.ensure_endpoint()
        return {
            "available": True,
            "host": host,
            "port": port,
        }

    def _unavailable_reply(
        self, request: dict[str, object], *, message: str = "debugpy is not available"
    ) -> dict[str, object]:
        return {
            "type": "response",
            "request_seq": int(request.get("seq", 0)),
            "success": False,
            "command": str(request.get("command", "")),
            "message": message,
        }

    def handle(self, request: object) -> dict[str, object]:
        if not isinstance(request, dict):
            return self._unavailable_reply({}, message="invalid debug request")

        command = str(request.get("command", ""))
        arguments = request.get("arguments", {})
        if not isinstance(arguments, dict):
            arguments = {}

        if command == "debugInfo":
            return self.debug_info(request)

        if not self.available:
            return self._unavailable_reply(request)

        request_seq = int(request.get("seq", 0))

        if command == "disconnect":
            self.breakpoints.clear()
            if self.dap is not None:
                self.dap.close()
            return {
                "seq": request_seq,
                "type": "response",
                "request_seq": request_seq,
                "success": True,
                "command": command,
                "body": {},
            }

        if command == "dumpCell":
            code = str(arguments.get("code", ""))
            digest = hashlib.sha1(code.encode("utf-8")).hexdigest()[:16]
            source_path = os.path.join(self.tmp_dir, f"cell_{digest}.py")
            with open(source_path, "w", encoding="utf-8") as handle:
                handle.write(code)
            self.cell_sources[digest] = source_path
            return {
                "type": "response",
                "request_seq": request_seq,
                "success": True,
                "command": command,
                "body": {"sourcePath": source_path},
            }

        if command == "setBreakpoints":
            source = arguments.get("source", {})
            if not isinstance(source, dict):
                source = {}
            source_path = str(source.get("path", ""))
            requested = arguments.get("breakpoints", [])
            if not isinstance(requested, list):
                requested = []
            normalized = []
            for item in requested:
                if isinstance(item, dict):
                    line = item.get("line")
                    if isinstance(line, int):
                        normalized.append({"line": line})
            self.breakpoints[source_path] = normalized
            return {
                "type": "response",
                "request_seq": request_seq,
                "success": True,
                "command": command,
                "body": {
                    "breakpoints": [
                        {
                            "verified": True,
                            "line": item["line"],
                            "source": {"path": source_path},
                        }
                        for item in normalized
                    ]
                },
            }

        if command == "evaluate":
            return {
                "seq": request_seq,
                "type": "response",
                "request_seq": request_seq,
                "success": True,
                "command": command,
                "body": {"result": "", "variablesReference": 0},
            }

        if command == "source":
            source = arguments.get("source", {})
            if not isinstance(source, dict):
                source = {}
            source_path = str(source.get("path", ""))
            if os.path.isfile(source_path):
                with open(source_path, encoding="utf-8") as handle:
                    content = handle.read()
                return {
                    "type": "response",
                    "request_seq": request_seq,
                    "success": True,
                    "command": command,
                    "body": {"content": content},
                }
            return {
                "type": "response",
                "request_seq": request_seq,
                "success": False,
                "command": command,
                "message": "source unavailable",
                "body": {},
            }

        return self._unavailable_reply(
            request, message=f"debug command not yet implemented: {command}"
        )


DEBUG_STATE = DebugState()
SUBSHELL_MANAGER = SubshellManager()


def invalid_subshell_reply(request_id: int, subshell_id: object) -> dict[str, object]:
    return {
        "id": request_id,
        "status": "error",
        "stdout": "",
        "stderr": "",
        "displays": [],
        "comm_events": [],
        "debug_events": [],
        "payload": [],
        "result": None,
        "user_expressions": {},
        "ename": "ValueError",
        "evalue": f"unknown subshell_id: {subshell_id}",
        "traceback": [],
    }


stdin = protocol_stdin()
while True:
    try:
        raw_line = stdin.readline()
    except KeyboardInterrupt:
        SUBSHELL_MANAGER.interrupt_all()
        continue

    if raw_line == "":
        break
    if not raw_line.strip():
        continue

    request = json.loads(raw_line)
    kind = request.get("kind", "execute")
    if kind == "complete":
        response = {
            "id": request["id"],
            **complete(request.get("code", ""), int(request.get("cursor_pos", 0))),
        }
    elif kind == "is_complete":
        response = {
            "id": request["id"],
            **is_complete(request.get("code", "")),
        }
    elif kind == "inspect":
        response = {
            "id": request["id"],
            **inspect_symbol(
                request.get("code", ""),
                int(request.get("cursor_pos", 0)),
                int(request.get("detail_level", 0)),
            ),
        }
    elif kind == "kernel_info":
        response = {
            "id": request["id"],
            **kernel_info_payload(),
        }
    elif kind == "debug_listen":
        response = {
            "id": request["id"],
            **DEBUG_STATE.listen_endpoint(),
        }
    elif kind == "debug":
        response = {
            "id": request["id"],
            **DEBUG_STATE.handle(request.get("message", {})),
        }
    elif kind == "comm_open":
        response = {
            "id": request["id"],
            **handle_comm_open(
                str(request.get("comm_id", "")),
                str(request.get("target_name", "")),
                request.get("data", {}),
                request.get("metadata", {}),
            ),
        }
    elif kind == "comm_msg":
        response = {
            "id": request["id"],
            **handle_comm_msg(
                str(request.get("comm_id", "")),
                request.get("data", {}),
                request.get("metadata", {}),
            ),
        }
    elif kind == "comm_close":
        response = {
            "id": request["id"],
            **handle_comm_close(
                str(request.get("comm_id", "")),
                request.get("data", {}),
                request.get("metadata", {}),
            ),
        }
    elif kind == "input_reply":
        request_id = int(request.get("id", -1))
        with _INPUT_WAITERS_LOCK:
            waiter = _INPUT_WAITERS.get(request_id)
        if waiter is not None:
            waiter.put(
                {
                    "value": request.get("value", ""),
                    "error": request.get("error"),
                }
            )
        continue
    elif kind == "create_subshell":
        response = {
            "id": request["id"],
            "status": "ok",
            "subshell_id": SUBSHELL_MANAGER.create_subshell(),
        }
    elif kind == "delete_subshell":
        subshell_id = str(request.get("subshell_id", ""))
        try:
            SUBSHELL_MANAGER.delete_subshell(subshell_id)
            response = {
                "id": request["id"],
                "status": "ok",
            }
        except KeyError:
            response = {
                "id": request["id"],
                "status": "error",
                "evalue": f"unknown subshell_id: {subshell_id}",
            }
    elif kind == "list_subshell":
        response = {
            "id": request["id"],
            "status": "ok",
            "subshell_id": SUBSHELL_MANAGER.list_subshells(),
        }
    elif kind == "interrupt":
        SUBSHELL_MANAGER.interrupt_subshells()
        response = {"id": request["id"], "status": "ok"}
    elif kind == "execute":
        subshell_id = request.get("subshell_id")
        normalized_subshell_id = None if subshell_id is None else str(subshell_id)
        if normalized_subshell_id is None:
            response = {
                "id": request["id"],
                **execute(
                    request.get("code", ""),
                    int(request["id"]),
                    None,
                    request.get("user_expressions", {}),
                    execution_count=int(request.get("execution_count", 0)),
                    silent=bool(request.get("silent", False)),
                    store_history=bool(request.get("store_history", True)),
                ),
            }
        elif not SUBSHELL_MANAGER.submit_execute(request, normalized_subshell_id):
            response = invalid_subshell_reply(
                int(request["id"]), normalized_subshell_id
            )
        else:
            continue
    else:
        response = {
            "id": request["id"],
            "status": "error",
            "evalue": f"unknown worker request kind: {kind}",
        }
    emit_protocol_message(response)
