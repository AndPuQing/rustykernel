from __future__ import annotations

import importlib.util
import re
import sys
import time
from collections.abc import Iterator
from pathlib import Path

import pytest
import zmq

from tests.compat_harness import (
    KernelCommand,
    RunningKernel,
    client_request,
    recv_iopub_messages_for_parent,
    recv_message,
    run_stdin_flow,
    running_kernel,
    sign_message,
)


pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("ipykernel") is None,
    reason="ipykernel is not installed",
)

ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*m")
CELL_IN_RE = re.compile(r"Cell In\[(\d+)\]")


@pytest.fixture()
def zmq_context() -> Iterator[zmq.Context]:
    context = zmq.Context()
    yield context
    context.term()


def collect_kernel_info(kernel: RunningKernel) -> dict[str, object]:
    return kernel.request("kernel_info_request", {})["content"]


def collect_comm_info(kernel: RunningKernel) -> dict[str, object]:
    return kernel.request("comm_info_request", {})["content"]


def collect_connect_reply(kernel: RunningKernel) -> dict[str, object]:
    return kernel.request("connect_request", {})["content"]


def collect_complete_reply(kernel: RunningKernel) -> dict[str, object]:
    kernel.execute("text = 'hello'")
    return kernel.request(
        "complete_request",
        {
            "code": "text.st",
            "cursor_pos": 7,
        },
    )["content"]


def collect_inspect_reply(kernel: RunningKernel) -> dict[str, object]:
    kernel.execute("value = 40")
    return kernel.request(
        "inspect_request",
        {
            "code": "value",
            "cursor_pos": 5,
            "detail_level": 1,
        },
    )["content"]


def collect_is_complete_replies(kernel: RunningKernel) -> list[dict[str, object]]:
    return [
        kernel.request("is_complete_request", {"code": code})["content"]
        for code in ["value = 1", "for i in range(3):", "1+"]
    ]


def collect_history_reply(kernel: RunningKernel) -> dict[str, object]:
    kernel.execute("value = 40")
    kernel.execute("value + 2")
    return kernel.request(
        "history_request",
        {
            "hist_access_type": "range",
            "output": True,
            "raw": True,
            "session": 0,
            "start": 0,
            "stop": 3,
        },
    )["content"]


def collect_shutdown_flow(
    kernel: RunningKernel,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    header, frames = client_request(
        "compat-session",
        "shutdown_request",
        {"restart": False},
    )
    signature = sign_message(str(kernel.payload["key"]), frames)
    kernel.control.send_multipart([b"<IDS|MSG>", signature, *frames])
    reply = recv_message(kernel.control, str(kernel.payload["key"]))
    published = recv_iopub_messages_for_parent(
        kernel.iopub,
        str(kernel.payload["key"]),
        header["msg_id"],
    )
    return reply["content"], published


def collect_execute_result(
    kernel: RunningKernel,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    kernel.execute("value = 40")
    return kernel.execute("value + 2")


def collect_execute_error(
    kernel: RunningKernel,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    return kernel.execute("1 / 0")


def collect_execute_exception(
    kernel: RunningKernel, code: str
) -> tuple[dict[str, object], list[dict[str, object]]]:
    return kernel.execute(code)


def collect_execute_syntax_error(
    kernel: RunningKernel,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    return kernel.execute("for i in range(2) print(i)")


def collect_execute_user_expressions(
    kernel: RunningKernel,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    header, frames = client_request(
        "compat-session",
        "execute_request",
        {
            "code": "value = 21",
            "silent": False,
            "store_history": True,
            "allow_stdin": False,
            "user_expressions": {
                "double": "value * 2",
                "missing": "missing_name",
                "syntax": "1 +",
            },
            "stop_on_error": True,
        },
    )
    signature = sign_message(str(kernel.payload["key"]), frames)
    kernel.shell.send_multipart([b"<IDS|MSG>", signature, *frames])
    reply = recv_message(kernel.shell, str(kernel.payload["key"]))
    published = recv_iopub_messages_for_parent(
        kernel.iopub, str(kernel.payload["key"]), header["msg_id"]
    )
    return reply, published


def collect_rich_display_flow(
    kernel: RunningKernel,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    return kernel.execute(
        """
from IPython.display import HTML, JSON, display

display(HTML("<b>hi</b>"))
display(JSON({"a": 1}))
"done"
"""
    )


def collect_stream_flow(
    kernel: RunningKernel,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    return kernel.execute(
        """
import sys
print("out")
print("err", file=sys.stderr)
"""
    )


def collect_chunked_stream_flow(
    kernel: RunningKernel,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    return kernel.execute(
        """
import os
import subprocess
import sys

print("py-out")
sys.stdout.flush()
os.write(1, b"fd-out\\n")
print("py-err", file=sys.stderr)
sys.stderr.flush()
os.write(2, b"fd-err\\n")
subprocess.run(
    [
        sys.executable,
        "-c",
        "import sys; print('child-out'); sys.stdout.flush(); print('child-err', file=sys.stderr); sys.stderr.flush()",
    ],
    check=True,
)
"""
    )


def collect_interrupt_flow(
    kernel: RunningKernel,
) -> tuple[
    dict[str, object],
    list[dict[str, object]],
    dict[str, object],
    list[dict[str, object]],
]:
    kernel.execute("value = 1")

    execute_header, execute_frames = client_request(
        "compat-session",
        "execute_request",
        {
            "code": "import time\ntime.sleep(30)",
            "silent": False,
            "store_history": True,
            "allow_stdin": False,
            "user_expressions": {},
            "stop_on_error": True,
        },
    )
    execute_signature = sign_message(str(kernel.payload["key"]), execute_frames)
    kernel.shell.send_multipart([b"<IDS|MSG>", execute_signature, *execute_frames])
    time.sleep(0.2)

    interrupt_header, interrupt_frames = client_request(
        "compat-session",
        "interrupt_request",
        {},
    )
    interrupt_signature = sign_message(str(kernel.payload["key"]), interrupt_frames)
    kernel.control.send_multipart(
        [b"<IDS|MSG>", interrupt_signature, *interrupt_frames]
    )
    interrupt_reply = recv_message(kernel.control, str(kernel.payload["key"]))
    interrupt_messages = recv_iopub_messages_for_parent(
        kernel.iopub,
        str(kernel.payload["key"]),
        interrupt_header["msg_id"],
    )

    execute_reply = recv_message(kernel.shell, str(kernel.payload["key"]))
    execute_messages = recv_iopub_messages_for_parent(
        kernel.iopub,
        str(kernel.payload["key"]),
        execute_header["msg_id"],
    )
    return (
        interrupt_reply["content"],
        interrupt_messages,
        execute_reply["content"],
        execute_messages,
    )


def extract_traceback_cell_block(traceback: list[str]) -> list[str]:
    cleaned = [strip_ansi(line) for line in traceback]
    for entry in reversed(cleaned):
        lines = entry.splitlines()
        if any("Cell In[" in line for line in lines):
            return lines
    raise AssertionError(f"missing Cell In[...] block in traceback: {cleaned!r}")


def extract_execution_count_from_cell_line(cell_line: str) -> int:
    match = CELL_IN_RE.search(cell_line)
    if match is None:
        raise AssertionError(f"missing Cell In[...] execution count: {cell_line!r}")
    return int(match.group(1))


def normalize_runtime_traceback_structure(traceback: list[str]) -> dict[str, object]:
    cleaned = [strip_ansi(line) for line in traceback]
    assert len(cleaned) >= 4
    cell_block = extract_traceback_cell_block(traceback)
    assert len(cell_block) >= 2
    highlighted_code_line = re.sub(r"^---->\s*\d+\s*", "", cell_block[1]).rstrip()
    return {
        "line_count": len(cleaned),
        "separator": cleaned[0],
        "headline": cleaned[1],
        "cell_prefix": cell_block[0],
        "highlighted_code_line": highlighted_code_line,
        "in_execution_count": extract_execution_count_from_cell_line(cell_block[0]),
        "tail": cleaned[-1],
    }


def normalize_syntax_traceback_structure(traceback: list[str]) -> dict[str, object]:
    cell_block = extract_traceback_cell_block(traceback)
    assert len(cell_block) >= 4
    return {
        "cell_line": cell_block[0],
        "source_line": cell_block[1].strip(),
        "caret_line": cell_block[2],
        "in_execution_count": extract_execution_count_from_cell_line(cell_block[0]),
        "tail": cell_block[-1],
    }


def normalize_kernel_info(content: dict[str, object]) -> dict[str, object]:
    language_info = dict(content["language_info"])
    help_links = {link["text"]: link["url"] for link in content["help_links"]}
    return {
        "status": content["status"],
        "protocol_version": content["protocol_version"],
        "language": content.get("language", language_info["name"]),
        "language_info": {
            "name": language_info["name"],
            "version": language_info["version"],
            "mimetype": language_info["mimetype"],
            "codemirror_mode": language_info["codemirror_mode"],
            "pygments_lexer": language_info["pygments_lexer"],
            "nbconvert_exporter": language_info["nbconvert_exporter"],
            "file_extension": language_info["file_extension"],
        },
        "python_reference": help_links["Python Reference"],
    }


def normalize_connect_reply(
    content: dict[str, object], payload: dict[str, object]
) -> dict[str, object]:
    return {
        "status": content["status"],
        "shell_port": content["shell_port"] == payload["shell_port"],
        "iopub_port": content["iopub_port"] == payload["iopub_port"],
        "stdin_port": content["stdin_port"] == payload["stdin_port"],
        "control_port": content["control_port"] == payload["control_port"],
        "hb_port": content["hb_port"] == payload["hb_port"],
    }


def normalize_execute_reply(content: dict[str, object]) -> dict[str, object]:
    return {
        "status": content["status"],
        "execution_count": content["execution_count"],
        "user_expressions": content["user_expressions"],
        "payload": content["payload"],
    }


def normalize_user_expression_reply(content: dict[str, object]) -> dict[str, object]:
    user_expressions = {}
    for name, value in content["user_expressions"].items():
        entry = {"status": value["status"]}
        if "data" in value:
            entry["data"] = value["data"]
        if "metadata" in value:
            entry["metadata"] = value["metadata"]
        if "ename" in value:
            entry["ename"] = value["ename"]
        if "evalue" in value:
            entry["evalue"] = value["evalue"]
        if "traceback" in value:
            entry["traceback"] = value["traceback"]
        user_expressions[name] = entry

    return {
        "status": content["status"],
        "execution_count": content["execution_count"],
        "user_expressions": user_expressions,
        "payload": content["payload"],
    }


def normalize_execute_error_reply(content: dict[str, object]) -> dict[str, object]:
    return {
        "status": content["status"],
        "execution_count": content["execution_count"],
        "ename": content["ename"],
        "evalue": content["evalue"],
        "user_expressions": content["user_expressions"],
        "payload": content["payload"],
        "traceback": content["traceback"],
    }


def normalize_exception_reply(content: dict[str, object]) -> dict[str, object]:
    return {
        "status": content["status"],
        "execution_count": content["execution_count"],
        "ename": content["ename"],
        "evalue": content["evalue"],
        "user_expressions": content["user_expressions"],
        "payload": content["payload"],
        "traceback_tail": strip_ansi(content["traceback"][-1]),
    }


def normalize_syntax_error_reply(content: dict[str, object]) -> dict[str, object]:
    return {
        "status": content["status"],
        "execution_count": content["execution_count"],
        "ename": content["ename"],
        "evalue_message": content["evalue"].split(" (", 1)[0],
        "user_expressions": content["user_expressions"],
        "payload": content["payload"],
        "traceback_tail": strip_ansi(content["traceback"][-1]),
    }


def normalize_syntax_error_content(content: dict[str, object]) -> dict[str, object]:
    return {
        "ename": content["ename"],
        "evalue_message": content["evalue"].split(" (", 1)[0],
        "traceback_tail": strip_ansi(content["traceback"][-1]),
    }


def normalize_exception_content(content: dict[str, object]) -> dict[str, object]:
    return {
        "ename": content["ename"],
        "evalue": content["evalue"],
        "traceback_tail": strip_ansi(content["traceback"][-1]),
    }


def normalize_iopub_messages(
    messages: list[dict[str, object]],
) -> list[tuple[str, object]]:
    normalized = []
    for message in messages:
        msg_type = message["header"]["msg_type"]
        content = message["content"]
        if msg_type == "status":
            normalized.append((msg_type, content["execution_state"]))
        elif msg_type == "execute_input":
            normalized.append(
                (
                    msg_type,
                    {
                        "code": content["code"],
                        "execution_count": content["execution_count"],
                    },
                )
            )
        elif msg_type == "execute_result":
            normalized.append(
                (
                    msg_type,
                    {
                        "text/plain": content["data"]["text/plain"],
                        "execution_count": content["execution_count"],
                    },
                )
            )
        elif msg_type == "display_data":
            normalized.append(
                (
                    msg_type,
                    {
                        "data": dict(content["data"]),
                        "metadata": dict(content.get("metadata", {})),
                        "transient": dict(content.get("transient", {})),
                    },
                )
            )
        elif msg_type == "error":
            normalized.append((msg_type, normalize_exception_content(content)))
        else:
            normalized.append((msg_type, content))
    return normalized


def strip_optional_execute_prelude(
    normalized: list[tuple[str, object]],
) -> list[tuple[str, object]]:
    index = 0
    if index < len(normalized) and normalized[index] == ("status", "busy"):
        index += 1
    if index < len(normalized) and normalized[index][0] == "execute_input":
        index += 1
    return normalized[index:]


def normalize_syntax_error_iopub_messages(
    messages: list[dict[str, object]],
) -> list[tuple[str, object]]:
    normalized = []
    for message in messages:
        msg_type = message["header"]["msg_type"]
        content = message["content"]
        if msg_type == "status":
            normalized.append((msg_type, content["execution_state"]))
        elif msg_type == "execute_input":
            normalized.append(
                (
                    msg_type,
                    {
                        "code": content["code"],
                        "execution_count": content["execution_count"],
                    },
                )
            )
        elif msg_type == "error":
            normalized.append((msg_type, normalize_syntax_error_content(content)))
        else:
            normalized.append((msg_type, content))
    return normalized


def normalize_stream_iopub_messages(
    messages: list[dict[str, object]],
) -> dict[str, object]:
    stream_text: dict[str, str] = {"stdout": "", "stderr": ""}
    prefix: list[tuple[str, object]] = []
    suffix: list[tuple[str, object]] = []

    for message in messages:
        msg_type = message["header"]["msg_type"]
        content = message["content"]
        if msg_type == "stream":
            stream_text[str(content["name"])] += str(content["text"])
            continue
        normalized = normalize_iopub_messages([message])[0]
        if normalized == ("status", "idle"):
            suffix.append(normalized)
        else:
            prefix.append(normalized)

    return {
        "prefix": prefix,
        "streams": stream_text,
        "suffix": suffix,
    }


def normalize_stream_message_sequence(
    messages: list[dict[str, object]],
) -> list[dict[str, str]]:
    return [
        {
            "name": str(message["content"]["name"]),
            "text": str(message["content"]["text"]),
        }
        for message in messages
        if message["header"]["msg_type"] == "stream"
    ]


def normalize_traceback_lines(traceback: list[str]) -> list[str]:
    return [strip_ansi(line) for line in traceback]


def normalize_completion_reply(content: dict[str, object]) -> dict[str, object]:
    metadata = content["metadata"]["_jupyter_types_experimental"]
    return {
        "status": content["status"],
        "matches": sorted(
            strip_completion_qualifier(match) for match in content["matches"]
        ),
        "types": {
            strip_completion_qualifier(item["text"]): item["type"] for item in metadata
        },
        "has_signature": {
            strip_completion_qualifier(item["text"]): bool(item.get("signature"))
            for item in metadata
        },
    }


def strip_completion_qualifier(text: str) -> str:
    return text.splitlines()[-1].rsplit(".", 1)[-1]


def normalize_is_complete_replies(
    replies: list[dict[str, object]],
) -> list[dict[str, object]]:
    return [
        {
            "status": reply["status"],
            "indent": reply.get("indent", ""),
        }
        for reply in replies
    ]


def normalize_inspect_reply(content: dict[str, object]) -> dict[str, object]:
    text_plain = strip_ansi(str(content.get("data", {}).get("text/plain", "")))
    return {
        "status": content["status"],
        "found": content["found"],
        "type_name": extract_type_name(text_plain),
        "value_repr": extract_value_repr(text_plain),
        "doc_summary": extract_doc_summary(text_plain),
    }


def strip_ansi(text: str) -> str:
    return ANSI_ESCAPE_RE.sub("", text)


def extract_type_name(text: str) -> str | None:
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.lower().startswith("type:"):
            return stripped.split(":", 1)[1].strip()
    return None


def extract_value_repr(text: str) -> str | None:
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.lower().startswith(
            ("type:", "docstring:", "signature:")
        ):
            continue
        if stripped.lower().startswith("string form:"):
            return stripped.split(":", 1)[1].strip()
        return stripped
    return None


def extract_doc_summary(text: str) -> str | None:
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.lower().startswith("signature:"):
            continue
        if re.match(r"^[A-Za-z_][A-Za-z0-9_]*\(.*\)\s*->", stripped):
            return stripped
    return None


def kernel_commands() -> tuple[KernelCommand, KernelCommand]:
    return (
        KernelCommand(
            name="rustykernel",
            argv=[sys.executable, "-m", "rustykernel", "-f"],
        ),
        KernelCommand(
            name="ipykernel",
            argv=[sys.executable, "-m", "ipykernel_launcher", "-f"],
        ),
    )


def test_kernel_info_reply_matches_ipykernel(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    rustykernel_command, ipykernel_command = kernel_commands()
    with running_kernel(
        rustykernel_command, tmp_path / "rusty-connection.json", zmq_context
    ) as rustykernel:
        rustykernel_info = collect_kernel_info(rustykernel)
    with running_kernel(
        ipykernel_command, tmp_path / "ipykernel-connection.json", zmq_context
    ) as ipykernel:
        ipykernel_info = collect_kernel_info(ipykernel)

    assert normalize_kernel_info(rustykernel_info) == normalize_kernel_info(
        ipykernel_info
    )


def test_connect_reply_matches_ipykernel_semantics(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    rustykernel_command, ipykernel_command = kernel_commands()
    with running_kernel(
        rustykernel_command, tmp_path / "rusty-connection.json", zmq_context
    ) as rustykernel:
        rustykernel_reply = collect_connect_reply(rustykernel)
        rustykernel_payload = rustykernel.payload
    with running_kernel(
        ipykernel_command, tmp_path / "ipykernel-connection.json", zmq_context
    ) as ipykernel:
        ipykernel_reply = collect_connect_reply(ipykernel)
        ipykernel_payload = ipykernel.payload

    assert normalize_connect_reply(
        rustykernel_reply, rustykernel_payload
    ) == normalize_connect_reply(ipykernel_reply, ipykernel_payload)


def test_execute_result_flow_matches_ipykernel(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    rustykernel_command, ipykernel_command = kernel_commands()
    with running_kernel(
        rustykernel_command, tmp_path / "rusty-connection.json", zmq_context
    ) as rustykernel:
        rustykernel_reply, rustykernel_messages = collect_execute_result(rustykernel)
    with running_kernel(
        ipykernel_command, tmp_path / "ipykernel-connection.json", zmq_context
    ) as ipykernel:
        ipykernel_reply, ipykernel_messages = collect_execute_result(ipykernel)

    assert normalize_execute_reply(
        rustykernel_reply["content"]
    ) == normalize_execute_reply(ipykernel_reply["content"])
    assert normalize_stream_iopub_messages(
        rustykernel_messages
    ) == normalize_stream_iopub_messages(ipykernel_messages)


def test_history_reply_matches_ipykernel_for_range_query(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    rustykernel_command, ipykernel_command = kernel_commands()
    with running_kernel(
        rustykernel_command, tmp_path / "rusty-connection.json", zmq_context
    ) as rustykernel:
        rustykernel_reply = collect_history_reply(rustykernel)
    with running_kernel(
        ipykernel_command, tmp_path / "ipykernel-connection.json", zmq_context
    ) as ipykernel:
        ipykernel_reply = collect_history_reply(ipykernel)

    assert rustykernel_reply == ipykernel_reply


def test_shutdown_request_matches_ipykernel(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    rustykernel_command, ipykernel_command = kernel_commands()
    with running_kernel(
        rustykernel_command, tmp_path / "rusty-connection.json", zmq_context
    ) as rustykernel:
        rustykernel_reply, rustykernel_messages = collect_shutdown_flow(rustykernel)
    with running_kernel(
        ipykernel_command, tmp_path / "ipykernel-connection.json", zmq_context
    ) as ipykernel:
        ipykernel_reply, ipykernel_messages = collect_shutdown_flow(ipykernel)

    assert rustykernel_reply == ipykernel_reply
    assert normalize_stream_iopub_messages(
        rustykernel_messages
    ) == normalize_stream_iopub_messages(ipykernel_messages)


def test_execute_error_flow_matches_ipykernel(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    rustykernel_command, ipykernel_command = kernel_commands()
    with running_kernel(
        rustykernel_command, tmp_path / "rusty-connection.json", zmq_context
    ) as rustykernel:
        rustykernel_reply, rustykernel_messages = collect_execute_error(rustykernel)
    with running_kernel(
        ipykernel_command, tmp_path / "ipykernel-connection.json", zmq_context
    ) as ipykernel:
        ipykernel_reply, ipykernel_messages = collect_execute_error(ipykernel)

    assert normalize_execute_error_reply(
        rustykernel_reply["content"]
    ) == normalize_execute_error_reply(ipykernel_reply["content"])
    assert rustykernel_reply["content"]["execution_count"] == 1
    assert ipykernel_reply["content"]["execution_count"] == 1
    assert normalize_runtime_traceback_structure(
        rustykernel_reply["content"]["traceback"]
    ) == normalize_runtime_traceback_structure(ipykernel_reply["content"]["traceback"])
    assert (
        normalize_runtime_traceback_structure(
            rustykernel_reply["content"]["traceback"]
        )["in_execution_count"]
        == rustykernel_reply["content"]["execution_count"]
    )
    assert (
        normalize_runtime_traceback_structure(ipykernel_reply["content"]["traceback"])[
            "in_execution_count"
        ]
        == ipykernel_reply["content"]["execution_count"]
    )
    assert normalize_iopub_messages(rustykernel_messages) == normalize_iopub_messages(
        ipykernel_messages
    )


@pytest.mark.parametrize(
    ("code", "expected_ename"),
    [
        ("import definitely_missing_module_xyz", "ModuleNotFoundError"),
        ("assert 1 == 2", "AssertionError"),
        ("{}[1]", "KeyError"),
        ("missing_name", "NameError"),
        ("len(1)", "TypeError"),
        ('int("abc")', "ValueError"),
        ("(1).missing_attr", "AttributeError"),
        ("from math import definitely_missing_name", "ImportError"),
    ],
)
def test_execute_additional_exception_flows_match_ipykernel(
    tmp_path: Path,
    zmq_context: zmq.Context,
    code: str,
    expected_ename: str,
) -> None:
    rustykernel_command, ipykernel_command = kernel_commands()
    with running_kernel(
        rustykernel_command, tmp_path / "rusty-connection.json", zmq_context
    ) as rustykernel:
        rustykernel_reply, rustykernel_messages = collect_execute_exception(
            rustykernel, code
        )
    with running_kernel(
        ipykernel_command, tmp_path / "ipykernel-connection.json", zmq_context
    ) as ipykernel:
        ipykernel_reply, ipykernel_messages = collect_execute_exception(ipykernel, code)

    assert rustykernel_reply["content"]["ename"] == expected_ename
    assert ipykernel_reply["content"]["ename"] == expected_ename
    assert normalize_exception_reply(
        rustykernel_reply["content"]
    ) == normalize_exception_reply(ipykernel_reply["content"])
    assert normalize_runtime_traceback_structure(
        rustykernel_reply["content"]["traceback"]
    ) == normalize_runtime_traceback_structure(ipykernel_reply["content"]["traceback"])
    assert strip_optional_execute_prelude(
        normalize_iopub_messages(rustykernel_messages)
    ) == strip_optional_execute_prelude(normalize_iopub_messages(ipykernel_messages))


def test_execute_syntax_error_flow_matches_ipykernel_semantics(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    rustykernel_command, ipykernel_command = kernel_commands()
    with running_kernel(
        rustykernel_command, tmp_path / "rusty-connection.json", zmq_context
    ) as rustykernel:
        rustykernel_reply, rustykernel_messages = collect_execute_syntax_error(
            rustykernel
        )
    with running_kernel(
        ipykernel_command, tmp_path / "ipykernel-connection.json", zmq_context
    ) as ipykernel:
        ipykernel_reply, ipykernel_messages = collect_execute_syntax_error(ipykernel)

    assert normalize_syntax_error_reply(
        rustykernel_reply["content"]
    ) == normalize_syntax_error_reply(ipykernel_reply["content"])
    assert rustykernel_reply["content"]["execution_count"] == 1
    assert ipykernel_reply["content"]["execution_count"] == 1
    assert normalize_syntax_traceback_structure(
        rustykernel_reply["content"]["traceback"]
    ) == normalize_syntax_traceback_structure(ipykernel_reply["content"]["traceback"])
    assert (
        normalize_syntax_traceback_structure(rustykernel_reply["content"]["traceback"])[
            "in_execution_count"
        ]
        == rustykernel_reply["content"]["execution_count"]
    )
    assert (
        normalize_syntax_traceback_structure(ipykernel_reply["content"]["traceback"])[
            "in_execution_count"
        ]
        == ipykernel_reply["content"]["execution_count"]
    )
    assert normalize_syntax_error_iopub_messages(
        rustykernel_messages
    ) == normalize_syntax_error_iopub_messages(ipykernel_messages)


def test_execute_user_expressions_match_ipykernel(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    rustykernel_command, ipykernel_command = kernel_commands()
    with running_kernel(
        rustykernel_command, tmp_path / "rusty-connection.json", zmq_context
    ) as rustykernel:
        rustykernel_reply, rustykernel_messages = collect_execute_user_expressions(
            rustykernel
        )
    with running_kernel(
        ipykernel_command, tmp_path / "ipykernel-connection.json", zmq_context
    ) as ipykernel:
        ipykernel_reply, ipykernel_messages = collect_execute_user_expressions(
            ipykernel
        )

    assert normalize_user_expression_reply(
        rustykernel_reply["content"]
    ) == normalize_user_expression_reply(ipykernel_reply["content"])
    assert strip_optional_execute_prelude(
        normalize_iopub_messages(rustykernel_messages)
    ) == strip_optional_execute_prelude(normalize_iopub_messages(ipykernel_messages))


def test_execute_user_expression_tracebacks_match_ipykernel_details(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    rustykernel_command, ipykernel_command = kernel_commands()
    with running_kernel(
        rustykernel_command, tmp_path / "rusty-connection.json", zmq_context
    ) as rustykernel:
        rustykernel_reply, rustykernel_messages = collect_execute_user_expressions(
            rustykernel
        )
    with running_kernel(
        ipykernel_command, tmp_path / "ipykernel-connection.json", zmq_context
    ) as ipykernel:
        ipykernel_reply, ipykernel_messages = collect_execute_user_expressions(
            ipykernel
        )

    for expression_name in ("missing", "syntax"):
        assert normalize_traceback_lines(
            rustykernel_reply["content"]["user_expressions"][expression_name][
                "traceback"
            ]
        ) == normalize_traceback_lines(
            ipykernel_reply["content"]["user_expressions"][expression_name]["traceback"]
        )
    assert normalize_iopub_messages(rustykernel_messages) == normalize_iopub_messages(
        ipykernel_messages
    )


def test_rich_display_flow_matches_ipykernel(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    rustykernel_command, ipykernel_command = kernel_commands()
    with running_kernel(
        rustykernel_command, tmp_path / "rusty-connection.json", zmq_context
    ) as rustykernel:
        rustykernel_reply, rustykernel_messages = collect_rich_display_flow(rustykernel)
    with running_kernel(
        ipykernel_command, tmp_path / "ipykernel-connection.json", zmq_context
    ) as ipykernel:
        ipykernel_reply, ipykernel_messages = collect_rich_display_flow(ipykernel)

    assert normalize_execute_reply(
        rustykernel_reply["content"]
    ) == normalize_execute_reply(ipykernel_reply["content"])
    assert strip_optional_execute_prelude(
        normalize_iopub_messages(rustykernel_messages)
    ) == strip_optional_execute_prelude(normalize_iopub_messages(ipykernel_messages))


def test_stream_flow_matches_ipykernel(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    rustykernel_command, ipykernel_command = kernel_commands()
    with running_kernel(
        rustykernel_command, tmp_path / "rusty-connection.json", zmq_context
    ) as rustykernel:
        rustykernel_reply, rustykernel_messages = collect_stream_flow(rustykernel)
    with running_kernel(
        ipykernel_command, tmp_path / "ipykernel-connection.json", zmq_context
    ) as ipykernel:
        ipykernel_reply, ipykernel_messages = collect_stream_flow(ipykernel)

    assert normalize_execute_reply(
        rustykernel_reply["content"]
    ) == normalize_execute_reply(ipykernel_reply["content"])
    assert normalize_stream_iopub_messages(
        rustykernel_messages
    ) == normalize_stream_iopub_messages(ipykernel_messages)


def test_chunked_stream_flow_matches_ipykernel_publication_details(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    rustykernel_command, ipykernel_command = kernel_commands()
    with running_kernel(
        rustykernel_command, tmp_path / "rusty-connection.json", zmq_context
    ) as rustykernel:
        rustykernel_reply, rustykernel_messages = collect_chunked_stream_flow(
            rustykernel
        )
    with running_kernel(
        ipykernel_command, tmp_path / "ipykernel-connection.json", zmq_context
    ) as ipykernel:
        ipykernel_reply, ipykernel_messages = collect_chunked_stream_flow(ipykernel)

    assert normalize_execute_reply(
        rustykernel_reply["content"]
    ) == normalize_execute_reply(ipykernel_reply["content"])
    rustykernel_streams = normalize_stream_iopub_messages(rustykernel_messages)[
        "streams"
    ]

    assert rustykernel_streams == {
        "stdout": "py-out\nfd-out\nchild-out\n",
        "stderr": "py-err\nfd-err\nchild-err\n",
    }


def test_stdin_flow_matches_ipykernel() -> None:
    rustykernel_command, ipykernel_command = kernel_commands()
    rustykernel_input_request, rustykernel_reply, rustykernel_messages = run_stdin_flow(
        rustykernel_command
    )
    ipykernel_input_request, ipykernel_reply, ipykernel_messages = run_stdin_flow(
        ipykernel_command
    )

    assert {
        "prompt": rustykernel_input_request["content"]["prompt"],
        "password": rustykernel_input_request["content"]["password"],
    } == {
        "prompt": ipykernel_input_request["content"]["prompt"],
        "password": ipykernel_input_request["content"]["password"],
    }
    assert normalize_execute_reply(
        rustykernel_reply["content"]
    ) == normalize_execute_reply(ipykernel_reply["content"])
    assert strip_optional_execute_prelude(
        normalize_iopub_messages(rustykernel_messages)
    ) == strip_optional_execute_prelude(normalize_iopub_messages(ipykernel_messages))


def test_password_stdin_flow_matches_ipykernel() -> None:
    rustykernel_command, ipykernel_command = kernel_commands()
    code = "import getpass\nsecret = getpass.getpass('Password: ')\nsecret"
    rustykernel_input_request, rustykernel_reply, rustykernel_messages = run_stdin_flow(
        rustykernel_command,
        code=code,
        input_value="s3cr3t",
        socket_identity=b"compat-password-stdin-rustykernel",
    )
    ipykernel_input_request, ipykernel_reply, ipykernel_messages = run_stdin_flow(
        ipykernel_command,
        code=code,
        input_value="s3cr3t",
        socket_identity=b"compat-password-stdin-ipykernel",
    )

    assert {
        "prompt": rustykernel_input_request["content"]["prompt"],
        "password": rustykernel_input_request["content"]["password"],
    } == {
        "prompt": ipykernel_input_request["content"]["prompt"],
        "password": ipykernel_input_request["content"]["password"],
    }
    assert normalize_execute_reply(
        rustykernel_reply["content"]
    ) == normalize_execute_reply(ipykernel_reply["content"])
    assert strip_optional_execute_prelude(
        normalize_iopub_messages(rustykernel_messages)
    ) == strip_optional_execute_prelude(normalize_iopub_messages(ipykernel_messages))


def test_interrupt_request_matches_ipykernel_semantics(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    rustykernel_command, ipykernel_command = kernel_commands()
    with running_kernel(
        rustykernel_command, tmp_path / "rusty-connection.json", zmq_context
    ) as rustykernel:
        (
            rustykernel_interrupt_reply,
            rustykernel_interrupt_messages,
            rustykernel_execute_reply,
            rustykernel_execute_messages,
        ) = collect_interrupt_flow(rustykernel)
    with running_kernel(
        ipykernel_command, tmp_path / "ipykernel-connection.json", zmq_context
    ) as ipykernel:
        (
            ipykernel_interrupt_reply,
            ipykernel_interrupt_messages,
            ipykernel_execute_reply,
            ipykernel_execute_messages,
        ) = collect_interrupt_flow(ipykernel)

    assert rustykernel_interrupt_reply == ipykernel_interrupt_reply
    assert normalize_iopub_messages(
        rustykernel_interrupt_messages
    ) == normalize_iopub_messages(ipykernel_interrupt_messages)
    assert normalize_execute_error_reply(
        rustykernel_execute_reply
    ) == normalize_execute_error_reply(ipykernel_execute_reply)
    assert normalize_iopub_messages(
        rustykernel_execute_messages
    ) == normalize_iopub_messages(ipykernel_execute_messages)


def test_nested_runtime_traceback_matches_ipykernel(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    rustykernel_command, ipykernel_command = kernel_commands()
    code = "def outer():\n    def inner():\n        1 / 0\n    inner()\nouter()"
    with running_kernel(
        rustykernel_command, tmp_path / "rusty-connection.json", zmq_context
    ) as rustykernel:
        rustykernel_reply, rustykernel_messages = collect_execute_exception(
            rustykernel, code
        )
    with running_kernel(
        ipykernel_command, tmp_path / "ipykernel-connection.json", zmq_context
    ) as ipykernel:
        ipykernel_reply, ipykernel_messages = collect_execute_exception(ipykernel, code)

    assert normalize_execute_error_reply(
        rustykernel_reply["content"]
    ) == normalize_execute_error_reply(ipykernel_reply["content"])
    assert normalize_traceback_lines(
        rustykernel_reply["content"]["traceback"]
    ) == normalize_traceback_lines(ipykernel_reply["content"]["traceback"])
    rustykernel_error = next(
        message
        for message in rustykernel_messages
        if message["header"]["msg_type"] == "error"
    )
    ipykernel_error = next(
        message
        for message in ipykernel_messages
        if message["header"]["msg_type"] == "error"
    )
    assert normalize_traceback_lines(
        rustykernel_error["content"]["traceback"]
    ) == normalize_traceback_lines(ipykernel_error["content"]["traceback"])


def test_comm_info_reply_matches_ipykernel(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    rustykernel_command, ipykernel_command = kernel_commands()
    with running_kernel(
        rustykernel_command, tmp_path / "rusty-connection.json", zmq_context
    ) as rustykernel:
        rustykernel_reply = collect_comm_info(rustykernel)
    with running_kernel(
        ipykernel_command, tmp_path / "ipykernel-connection.json", zmq_context
    ) as ipykernel:
        ipykernel_reply = collect_comm_info(ipykernel)

    assert rustykernel_reply == ipykernel_reply


def test_complete_reply_matches_ipykernel_after_normalization(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    rustykernel_command, ipykernel_command = kernel_commands()
    with running_kernel(
        rustykernel_command, tmp_path / "rusty-connection.json", zmq_context
    ) as rustykernel:
        rustykernel_reply = collect_complete_reply(rustykernel)
    with running_kernel(
        ipykernel_command, tmp_path / "ipykernel-connection.json", zmq_context
    ) as ipykernel:
        ipykernel_reply = collect_complete_reply(ipykernel)

    assert normalize_completion_reply(rustykernel_reply) == normalize_completion_reply(
        ipykernel_reply
    )


def test_inspect_reply_matches_ipykernel_core_semantics(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    rustykernel_command, ipykernel_command = kernel_commands()
    with running_kernel(
        rustykernel_command, tmp_path / "rusty-connection.json", zmq_context
    ) as rustykernel:
        rustykernel_reply = collect_inspect_reply(rustykernel)
    with running_kernel(
        ipykernel_command, tmp_path / "ipykernel-connection.json", zmq_context
    ) as ipykernel:
        ipykernel_reply = collect_inspect_reply(ipykernel)

    assert normalize_inspect_reply(rustykernel_reply) == normalize_inspect_reply(
        ipykernel_reply
    )


def test_is_complete_reply_matches_ipykernel(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    rustykernel_command, ipykernel_command = kernel_commands()
    with running_kernel(
        rustykernel_command, tmp_path / "rusty-connection.json", zmq_context
    ) as rustykernel:
        rustykernel_replies = collect_is_complete_replies(rustykernel)
    with running_kernel(
        ipykernel_command, tmp_path / "ipykernel-connection.json", zmq_context
    ) as ipykernel:
        ipykernel_replies = collect_is_complete_replies(ipykernel)

    assert normalize_is_complete_replies(
        rustykernel_replies
    ) == normalize_is_complete_replies(ipykernel_replies)
