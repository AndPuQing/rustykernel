from __future__ import annotations

import hashlib
import hmac
import json
import socket
import subprocess
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

import zmq


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
    stdin: zmq.Socket | None = None

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
    return header, [
        json.dumps(header).encode("utf-8"),
        b"{}",
        b"{}",
        json.dumps(content).encode("utf-8"),
    ]


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


def recv_message_retry(
    socket: zmq.Socket, key: str, timeout_s: float = 5.0
) -> dict[str, object]:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        try:
            return recv_message(socket, key)
        except zmq.error.Again:
            time.sleep(0.05)
    raise AssertionError("timed out waiting for message")


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


def send_message(
    socket: zmq.Socket,
    key: str,
    msg_type: str,
    content: dict[str, object],
    *,
    session: str = "compat-session",
) -> dict[str, object]:
    header, frames = client_request(session, msg_type, content)
    signature = sign_message(key, frames)
    socket.send_multipart([b"<IDS|MSG>", signature, *frames])
    return header


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
        time.sleep(0.1)
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


def run_stdin_flow(
    command: KernelCommand,
    *,
    code: str = "name = input('Name: ')\nname",
    input_value: str = "Ada",
    socket_identity: bytes = b"compat-stdin-client",
) -> tuple[dict[str, object], dict[str, object], list[dict[str, object]]]:
    payload = connection_payload()
    with TemporaryDirectory() as td:
        connection_file = Path(td) / "stdin-compat-connection.json"
        write_connection_file(connection_file, payload)
        process = subprocess.Popen(
            [*command.argv, str(connection_file)],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        context = zmq.Context()
        shell = context.socket(zmq.DEALER)
        shell.setsockopt(zmq.IDENTITY, socket_identity)
        shell.connect(f"tcp://127.0.0.1:{payload['shell_port']}")
        shell.setsockopt(zmq.RCVTIMEO, 3000)

        stdin = context.socket(zmq.DEALER)
        stdin.setsockopt(zmq.IDENTITY, socket_identity)
        stdin.connect(f"tcp://127.0.0.1:{payload['stdin_port']}")
        stdin.setsockopt(zmq.RCVTIMEO, 3000)

        iopub = context.socket(zmq.SUB)
        iopub.setsockopt(zmq.SUBSCRIBE, b"")
        iopub.connect(f"tcp://127.0.0.1:{payload['iopub_port']}")
        iopub.setsockopt(zmq.RCVTIMEO, 250)

        try:
            time.sleep(0.1)
            ready_deadline = time.monotonic() + 10.0
            while time.monotonic() < ready_deadline:
                try:
                    send_message(shell, str(payload["key"]), "kernel_info_request", {})
                    ready_reply = recv_message(shell, str(payload["key"]))
                except zmq.error.Again:
                    time.sleep(0.1)
                    continue
                if ready_reply["header"]["msg_type"] == "kernel_info_reply":
                    break
            else:
                raise AssertionError(
                    f"{command.name} did not answer kernel_info_request"
                )

            time.sleep(0.1)
            execute_header = send_message(
                shell,
                str(payload["key"]),
                "execute_request",
                {
                    "code": code,
                    "silent": False,
                    "store_history": True,
                    "allow_stdin": True,
                    "user_expressions": {},
                    "stop_on_error": True,
                },
            )
            input_request = recv_message_retry(
                stdin, str(payload["key"]), timeout_s=5.0
            )
            send_message(
                stdin,
                str(payload["key"]),
                "input_reply",
                {"value": input_value},
            )
            execute_reply = recv_message_retry(
                shell, str(payload["key"]), timeout_s=5.0
            )
            published = recv_iopub_messages_for_parent(
                iopub, str(payload["key"]), execute_header["msg_id"], timeout_s=4.0
            )
            return input_request, execute_reply, published
        finally:
            shell.close(0)
            stdin.close(0)
            iopub.close(0)
            context.term()
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
