from __future__ import annotations

import hashlib
import hmac
import importlib.util
import json
import re
import socket
import subprocess
import sys
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

import pytest
import zmq


pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("ipykernel") is None,
    reason="ipykernel is not installed",
)

ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*m")


@dataclass(frozen=True)
class KernelCommand:
    name: str
    argv: list[str]


@dataclass
class RunningKernel:
    name: str
    payload: dict[str, object]
    process: subprocess.Popen[bytes]
    shell: zmq.Socket
    control: zmq.Socket
    iopub: zmq.Socket

    def request(self, msg_type: str, content: dict[str, object]) -> dict[str, object]:
        header, frames = client_request("compat-session", msg_type, content)
        signature = sign_message(str(self.payload["key"]), frames)
        self.shell.send_multipart([b"<IDS|MSG>", signature, *frames])
        reply = recv_message(self.shell, str(self.payload["key"]))
        self.drain_iopub()
        reply["request_header"] = header
        return reply

    def execute(self, code: str) -> tuple[dict[str, object], list[dict[str, object]]]:
        header, frames = client_request(
            "compat-session",
            "execute_request",
            {
                "code": code,
                "silent": False,
                "store_history": True,
                "allow_stdin": False,
                "user_expressions": {},
                "stop_on_error": True,
            },
        )
        signature = sign_message(str(self.payload["key"]), frames)
        self.shell.send_multipart([b"<IDS|MSG>", signature, *frames])
        reply = recv_message(self.shell, str(self.payload["key"]))
        published = recv_iopub_messages_for_parent(
            self.iopub, str(self.payload["key"]), header["msg_id"]
        )
        return reply, published

    def drain_iopub(self) -> None:
        while True:
            try:
                recv_message(self.iopub, str(self.payload["key"]))
            except zmq.error.Again:
                return


def reserve_tcp_ports(count: int) -> list[int]:
    listeners = []
    try:
        for _ in range(count):
            listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listener.bind(("127.0.0.1", 0))
            listeners.append(listener)
        return [listener.getsockname()[1] for listener in listeners]
    finally:
        for listener in listeners:
            listener.close()


def connection_payload() -> dict[str, object]:
    shell_port, iopub_port, stdin_port, control_port, hb_port = reserve_tcp_ports(5)
    return {
        "transport": "tcp",
        "ip": "127.0.0.1",
        "shell_port": shell_port,
        "iopub_port": iopub_port,
        "stdin_port": stdin_port,
        "control_port": control_port,
        "hb_port": hb_port,
        "signature_scheme": "hmac-sha256",
        "key": "secret",
        "kernel_name": "python-test",
    }


def write_connection_file(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def sign_message(key: str, parts: list[bytes]) -> bytes:
    if not key:
        return b""

    digest = hmac.new(key.encode("utf-8"), digestmod=hashlib.sha256)
    for part in parts:
        digest.update(part)
    return digest.hexdigest().encode("ascii")


def client_request(
    session: str, msg_type: str, content: dict[str, object]
) -> tuple[dict[str, object], list[bytes]]:
    header = {
        "msg_id": str(uuid.uuid4()),
        "session": session,
        "username": "compat-client",
        "date": "2026-03-26T00:00:00.000Z",
        "msg_type": msg_type,
        "version": "5.3",
    }
    header_frame = json.dumps(header).encode("utf-8")
    parent_frame = b"{}"
    metadata_frame = b"{}"
    content_frame = json.dumps(content).encode("utf-8")
    return header, [header_frame, parent_frame, metadata_frame, content_frame]


def recv_message(socket: zmq.Socket, key: str) -> dict[str, object]:
    frames = socket.recv_multipart()
    delimiter = frames.index(b"<IDS|MSG>")
    message_frames = frames[delimiter + 1 :]
    signature = message_frames[0]
    payload_frames = message_frames[1:5]
    assert sign_message(key, payload_frames) == signature
    return {
        "header": json.loads(payload_frames[0]),
        "parent_header": json.loads(payload_frames[1]),
        "metadata": json.loads(payload_frames[2]),
        "content": json.loads(payload_frames[3]),
    }


def recv_iopub_messages_for_parent(
    socket: zmq.Socket, key: str, parent_msg_id: str, timeout_s: float = 3.0
) -> list[dict[str, object]]:
    deadline = time.monotonic() + timeout_s
    messages = []
    idle_seen = False
    while time.monotonic() < deadline and not idle_seen:
        try:
            message = recv_message(socket, key)
        except zmq.error.Again:
            continue
        if message["parent_header"].get("msg_id") != parent_msg_id:
            continue
        messages.append(message)
        idle_seen = (
            message["header"]["msg_type"] == "status"
            and message["content"].get("execution_state") == "idle"
        )
    assert idle_seen, "timed out waiting for parent idle status"
    return messages


@pytest.fixture()
def zmq_context() -> Iterator[zmq.Context]:
    context = zmq.Context()
    yield context
    context.term()


@contextmanager
def running_kernel(
    command: KernelCommand,
    connection_file: Path,
    zmq_context: zmq.Context,
) -> Iterator[RunningKernel]:
    payload = connection_payload()
    write_connection_file(connection_file, payload)
    process = subprocess.Popen(
        [*command.argv, str(connection_file)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    shell = zmq_context.socket(zmq.DEALER)
    shell.connect(f"tcp://127.0.0.1:{payload['shell_port']}")
    shell.setsockopt(zmq.RCVTIMEO, 3000)

    control = zmq_context.socket(zmq.DEALER)
    control.connect(f"tcp://127.0.0.1:{payload['control_port']}")
    control.setsockopt(zmq.RCVTIMEO, 3000)

    iopub = zmq_context.socket(zmq.SUB)
    iopub.setsockopt(zmq.SUBSCRIBE, b"")
    iopub.connect(f"tcp://127.0.0.1:{payload['iopub_port']}")
    iopub.setsockopt(zmq.RCVTIMEO, 250)

    kernel = RunningKernel(
        name=command.name,
        payload=payload,
        process=process,
        shell=shell,
        control=control,
        iopub=iopub,
    )

    try:
        wait_for_ready(kernel)
        yield kernel
    finally:
        shell.close(0)
        control.close(0)
        iopub.close(0)
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def wait_for_ready(kernel: RunningKernel) -> None:
    deadline = time.monotonic() + 10.0
    while time.monotonic() < deadline:
        try:
            reply = kernel.request("kernel_info_request", {})
        except zmq.error.Again:
            time.sleep(0.1)
            continue
        if reply["header"]["msg_type"] == "kernel_info_reply":
            return
    raise AssertionError(f"{kernel.name} did not answer kernel_info_request")


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


def collect_execute_syntax_error(
    kernel: RunningKernel,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    return kernel.execute("for i in range(2) print(i)")


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
        else:
            normalized.append((msg_type, content))
    return normalized


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
    assert normalize_iopub_messages(rustykernel_messages) == normalize_iopub_messages(
        ipykernel_messages
    )


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
    assert normalize_iopub_messages(rustykernel_messages) == normalize_iopub_messages(
        ipykernel_messages
    )


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
    assert normalize_iopub_messages(rustykernel_messages) == normalize_iopub_messages(
        ipykernel_messages
    )


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
    assert normalize_syntax_error_iopub_messages(
        rustykernel_messages
    ) == normalize_syntax_error_iopub_messages(ipykernel_messages)


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
