from __future__ import annotations

import hashlib
import hmac
import json
import socket
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Literal

import rustykernel
import zmq


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


def write_connection_file(tmp_path: Path, payload: dict[str, object]) -> Path:
    path = tmp_path / "connection.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def sign_message(key: str, parts: list[bytes]) -> bytes:
    if not key:
        return b""

    digest = hmac.new(key.encode("utf-8"), digestmod=hashlib.sha256)
    for part in parts:
        digest.update(part)
    return digest.hexdigest().encode("ascii")


def client_request(
    session: str,
    msg_type: str,
    content: dict[str, object],
    *,
    header_overrides: dict[str, object] | None = None,
) -> tuple[dict[str, object], list[bytes]]:
    header = {
        "msg_id": str(uuid.uuid4()),
        "session": session,
        "username": "python-test-client",
        "date": "2026-03-26T00:00:00.000Z",
        "msg_type": msg_type,
        "version": "5.3",
    }
    if header_overrides:
        header.update(header_overrides)
    header_frame = json.dumps(header).encode("utf-8")
    parent_frame = b"{}"
    metadata_frame = b"{}"
    content_frame = json.dumps(content).encode("utf-8")
    return header, [header_frame, parent_frame, metadata_frame, content_frame]


def send_client_message(
    socket: zmq.Socket,
    key: str,
    session: str,
    msg_type: str,
    content: dict[str, object],
    *,
    header_overrides: dict[str, object] | None = None,
) -> dict[str, object]:
    header, frames = client_request(
        session,
        msg_type,
        content,
        header_overrides=header_overrides,
    )
    signature = sign_message(key, frames)
    socket.send_multipart([b"<IDS|MSG>", signature, *frames])
    return header


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
    socket: zmq.Socket,
    key: str,
    parent_msg_id: str,
    timeout_s: float = 3.0,
) -> list[dict[str, object]]:
    deadline = time.monotonic() + timeout_s
    messages = []
    idle_seen = False
    while time.monotonic() < deadline and not idle_seen:
        message = recv_message(socket, key)
        if message["parent_header"].get("msg_id") != parent_msg_id:
            continue
        messages.append(message)
        idle_seen = (
            message["header"]["msg_type"] == "status"
            and message["content"].get("execution_state") == "idle"
        )

    assert idle_seen, "timed out waiting for parent idle status"
    return messages


def recv_iopub_messages_until_parent_predicate(
    socket: zmq.Socket,
    key: str,
    parent_msg_id: str,
    predicate,
    timeout_s: float = 10.0,
) -> list[dict[str, object]]:
    deadline = time.monotonic() + timeout_s
    messages = []
    while time.monotonic() < deadline:
        message = recv_message(socket, key)
        if message["parent_header"].get("msg_id") != parent_msg_id:
            continue
        messages.append(message)
        if predicate(message):
            return messages

    raise AssertionError("timed out waiting for matching parent iopub message")


def wait_for_kernel_stop(
    kernel: object, timeout_s: float = 3.0, poll_interval_s: float = 0.05
) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if getattr(kernel, "is_running") is False:
            return
        time.sleep(poll_interval_s)
    raise AssertionError("kernel did not stop within timeout")


@dataclass
class KernelClient:
    payload: dict[str, object]
    kernel: object
    shell: zmq.Socket
    control: zmq.Socket
    iopub: zmq.Socket
    session: str = "smoke-session"

    def request(
        self,
        channel: Literal["shell", "control"],
        msg_type: str,
        content: dict[str, object],
    ) -> tuple[dict[str, object], list[dict[str, object]]]:
        socket = self.shell if channel == "shell" else self.control
        header = send_client_message(
            socket,
            str(self.payload["key"]),
            self.session,
            msg_type,
            content,
        )
        reply = recv_message(socket, str(self.payload["key"]))
        published = recv_iopub_messages_for_parent(
            self.iopub,
            str(self.payload["key"]),
            str(header["msg_id"]),
        )
        return reply, published

    def debug_request(
        self,
        seq: int,
        command: str,
        arguments: dict[str, object] | None = None,
    ) -> tuple[dict[str, object], list[dict[str, object]]]:
        content: dict[str, object] = {
            "seq": seq,
            "type": "request",
            "command": command,
        }
        if arguments is not None:
            content["arguments"] = arguments
        return self.request("control", "debug_request", content)

    def debug_request_and_drain(
        self,
        seq: int,
        command: str,
        arguments: dict[str, object] | None = None,
    ) -> dict[str, object]:
        reply, _ = self.debug_request(seq, command, arguments)
        return reply

    def top_frame_and_locals(
        self, seq: int, thread_id: int
    ) -> tuple[dict[str, object], list[dict[str, object]]]:
        threads_reply = self.debug_request_and_drain(seq, "threads", {})
        assert threads_reply["content"]["success"] is True
        assert any(
            thread["id"] == thread_id
            for thread in threads_reply["content"]["body"].get("threads", [])
        )

        stack_reply = self.debug_request_and_drain(
            seq + 1, "stackTrace", {"threadId": thread_id}
        )
        assert stack_reply["content"]["success"] is True
        frame = stack_reply["content"]["body"]["stackFrames"][0]

        scopes_reply = self.debug_request_and_drain(
            seq + 2, "scopes", {"frameId": frame["id"]}
        )
        assert scopes_reply["content"]["success"] is True
        locals_ref = scopes_reply["content"]["body"]["scopes"][0]["variablesReference"]

        variables_reply = self.debug_request_and_drain(
            seq + 3, "variables", {"variablesReference": locals_ref}
        )
        assert variables_reply["content"]["success"] is True
        return frame, variables_reply["content"]["body"]["variables"]


@contextmanager
def running_kernel_client(
    tmp_path: Path, zmq_context: zmq.Context
) -> Iterator[KernelClient]:
    payload = connection_payload()
    path = write_connection_file(tmp_path, payload)
    kernel = rustykernel.start_kernel(str(path))

    shell = zmq_context.socket(zmq.DEALER)
    shell.connect(kernel.endpoints.shell)
    control = zmq_context.socket(zmq.DEALER)
    control.connect(kernel.endpoints.control)
    iopub = zmq_context.socket(zmq.SUB)
    iopub.setsockopt(zmq.SUBSCRIBE, b"")
    iopub.connect(kernel.endpoints.iopub)

    shell.setsockopt(zmq.RCVTIMEO, 10000)
    control.setsockopt(zmq.RCVTIMEO, 10000)
    iopub.setsockopt(zmq.RCVTIMEO, 10000)
    time.sleep(0.1)

    client = KernelClient(
        payload=payload,
        kernel=kernel,
        shell=shell,
        control=control,
        iopub=iopub,
    )

    try:
        yield client
    finally:
        shell.close(0)
        control.close(0)
        iopub.close(0)
        kernel.stop()
