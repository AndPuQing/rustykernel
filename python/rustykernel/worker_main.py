import ast
import contextlib
import io
import json
import inspect
import re
import rlcompleter
import sys
import traceback


namespace: dict[str, object] = {"__name__": "__main__"}
completer = rlcompleter.Completer(namespace)
COMPLETION_RE = re.compile(r"([A-Za-z_][A-Za-z0-9_\.]*)$")


def execute(code: str) -> dict[str, object]:
    stdout_buffer = io.StringIO()
    stderr_buffer = io.StringIO()
    payload: dict[str, object] = {
        "status": "ok",
        "stdout": "",
        "stderr": "",
        "result": None,
        "ename": None,
        "evalue": None,
        "traceback": [],
    }

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
                value = eval(compile(expr, "<rustykernel>", "eval"), namespace, namespace)
                if value is not None:
                    payload["result"] = {
                        "data": {"text/plain": repr(value)},
                        "metadata": {},
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
    return payload


def complete(code: str, cursor_pos: int) -> dict[str, object]:
    safe_cursor_pos = max(0, min(cursor_pos, len(code)))
    prefix = code[:safe_cursor_pos]
    match = COMPLETION_RE.search(prefix)
    token = match.group(1) if match else ""
    cursor_start = match.start(1) if match else safe_cursor_pos
    cursor_end = safe_cursor_pos

    matches: list[str] = []
    seen: set[str] = set()
    state = 0
    while True:
        candidate = completer.complete(token, state)
        if candidate is None:
            break
        if candidate not in seen:
            seen.add(candidate)
            matches.append(candidate)
        state += 1

    return {
        "status": "ok",
        "matches": matches,
        "cursor_start": cursor_start,
        "cursor_end": cursor_end,
        "metadata": {},
    }


def inspect_symbol(code: str, cursor_pos: int, detail_level: int) -> dict[str, object]:
    safe_cursor_pos = max(0, min(cursor_pos, len(code)))
    prefix = code[:safe_cursor_pos]
    match = COMPLETION_RE.search(prefix)
    token = match.group(1) if match else ""

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

    try:
        signature = str(inspect.signature(value))
    except (TypeError, ValueError):
        signature = None
    if signature:
        lines.append(f"signature: {token}{signature}")

    doc = inspect.getdoc(value)
    if doc:
        if detail_level > 0:
            lines.append("")
            lines.append(doc)
        else:
            lines.append("")
            lines.append(doc.splitlines()[0])

    return {
        "status": "ok",
        "found": True,
        "data": {"text/plain": "\n".join(lines)},
        "metadata": {"type_name": type_name},
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
    elif kind == "inspect":
        response = {
            "id": request["id"],
            **inspect_symbol(
                request.get("code", ""),
                int(request.get("cursor_pos", 0)),
                int(request.get("detail_level", 0)),
            ),
        }
    else:
        response = {"id": request["id"], **execute(request.get("code", ""))}
    sys.stdout.write(json.dumps(response) + "\n")
    sys.stdout.flush()
