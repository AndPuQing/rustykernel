import ast
import contextlib
import io
import json
import sys
import traceback


namespace: dict[str, object] = {"__name__": "__main__"}


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


for raw_line in sys.stdin:
    if not raw_line.strip():
        continue
    request = json.loads(raw_line)
    response = {"id": request["id"], **execute(request.get("code", ""))}
    sys.stdout.write(json.dumps(response) + "\n")
    sys.stdout.flush()
