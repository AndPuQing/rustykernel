import ast
import builtins
import codeop
import contextlib
import getpass
import html
import inspect
import io
import json
import keyword
import re
import rlcompleter
import sys
import token as token_types
import tokenize
import traceback
import types
import uuid
from io import StringIO
from typing import Optional

try:
    import jedi
except ImportError:  # pragma: no cover - fallback for minimal environments
    jedi = None


namespace: dict[str, object] = {"__name__": "__main__"}
completer = rlcompleter.Completer(namespace)
COMPLETION_RE = re.compile(r"([A-Za-z_][A-Za-z0-9_\.]*)$")
DISPLAY_EVENTS: list[dict[str, object]] = []
CURRENT_REQUEST_ID: Optional[int] = None


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


def publish_display_event(
    value: object, *, msg_type: str, transient: Optional[dict[str, object]] = None
) -> None:
    data, metadata = rich_display_data(value)
    DISPLAY_EVENTS.append(
        {
            "msg_type": msg_type,
            "data": data,
            "metadata": metadata,
            "transient": transient or {},
        }
    )


class DisplayHandle:
    def __init__(self, display_id: str):
        self.display_id = display_id

    def display(self, value: object) -> "DisplayHandle":
        display(value, display_id=self.display_id)
        return self

    def update(self, value: object) -> "DisplayHandle":
        update_display(value, display_id=self.display_id)
        return self


def _normalize_display_id(display_id: object) -> Optional[str]:
    if display_id is None or display_id is False:
        return None
    if display_id is True:
        return str(uuid.uuid4())
    return str(display_id)


def display(value: object, *, display_id: object = None) -> Optional[DisplayHandle]:
    normalized_display_id = _normalize_display_id(display_id)
    transient: dict[str, object] = {}
    if normalized_display_id is not None:
        transient["display_id"] = normalized_display_id

    publish_display_event(value, msg_type="display_data", transient=transient)

    if normalized_display_id is None:
        return None
    return DisplayHandle(normalized_display_id)


def update_display(value: object, *, display_id: object) -> None:
    normalized_display_id = _normalize_display_id(display_id)
    if normalized_display_id is None:
        raise ValueError("display_id is required for update_display")

    publish_display_event(
        value,
        msg_type="update_display_data",
        transient={"display_id": normalized_display_id},
    )


def request_input(prompt: object = "", *, password: bool = False) -> str:
    if CURRENT_REQUEST_ID is None:
        raise RuntimeError("input() is only available while handling a request")

    sys.stdout.write(
        json.dumps(
            {
                "id": CURRENT_REQUEST_ID,
                "event": "input_request",
                "prompt": str(prompt),
                "password": password,
            }
        )
        + "\n"
    )
    sys.stdout.flush()

    while True:
        raw_line = sys.stdin.readline()
        if not raw_line:
            raise EOFError("stdin channel closed")
        if not raw_line.strip():
            continue

        response = json.loads(raw_line)
        if response.get("kind") != "input_reply":
            continue
        if int(response.get("id", -1)) != CURRENT_REQUEST_ID:
            continue

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
    namespace["display"] = display
    namespace["update_display"] = update_display
    namespace["DisplayHandle"] = DisplayHandle

    ipython_module = sys.modules.get("IPython")
    if ipython_module is None:
        ipython_module = types.ModuleType("IPython")
        ipython_module.__path__ = []
        sys.modules["IPython"] = ipython_module

    display_module = types.ModuleType("IPython.display")
    display_module.display = display
    display_module.update_display = update_display
    display_module.DisplayHandle = DisplayHandle
    display_module.__all__ = ["display", "update_display", "DisplayHandle"]
    sys.modules["IPython.display"] = display_module
    ipython_module.display = display_module


install_display_api()
install_input_api()


def execute(code: str, request_id: int) -> dict[str, object]:
    stdout_buffer = io.StringIO()
    stderr_buffer = io.StringIO()
    payload: dict[str, object] = {
        "status": "ok",
        "stdout": "",
        "stderr": "",
        "displays": [],
        "result": None,
        "ename": None,
        "evalue": None,
        "traceback": [],
    }

    global CURRENT_REQUEST_ID
    DISPLAY_EVENTS.clear()
    CURRENT_REQUEST_ID = request_id

    try:
        tree = ast.parse(code, filename="<rustykernel>", mode="exec")
        body = list(tree.body)
        expr = None
        if body and isinstance(body[-1], ast.Expr):
            expr = ast.Expression(body.pop().value)
        module = ast.Module(body=body, type_ignores=[])

        with contextlib.redirect_stdout(stdout_buffer), contextlib.redirect_stderr(
            stderr_buffer
        ):
            if body:
                exec(compile(module, "<rustykernel>", "exec"), namespace, namespace)
            if expr is not None:
                value = eval(
                    compile(expr, "<rustykernel>", "eval"), namespace, namespace
                )
                if value is not None:
                    data, metadata = rich_display_data(value)
                    payload["result"] = {
                        "data": data,
                        "metadata": metadata,
                    }
    except BaseException as exc:
        payload["status"] = "error"
        payload["ename"] = exc.__class__.__name__
        payload["evalue"] = str(exc)
        payload["traceback"] = traceback.format_exception(
            type(exc), exc, exc.__traceback__
        )

    payload["stdout"] = stdout_buffer.getvalue()
    payload["stderr"] = stderr_buffer.getvalue()
    payload["displays"] = list(DISPLAY_EVENTS)
    CURRENT_REQUEST_ID = None
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
    try:
        compiled = codeop.compile_command(code, filename="<rustykernel>", symbol="exec")
    except (SyntaxError, OverflowError, ValueError, TypeError):
        return {
            "status": "invalid",
            "indent": "",
        }

    if compiled is None:
        return {
            "status": "incomplete",
            "indent": completion_indent(code),
        }

    return {
        "status": "complete",
        "indent": "",
    }


for raw_line in sys.stdin:
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
    elif kind == "input_reply":
        continue
    else:
        response = {
            "id": request["id"],
            **execute(request.get("code", ""), int(request["id"])),
        }
    sys.stdout.write(json.dumps(response) + "\n")
    sys.stdout.flush()
