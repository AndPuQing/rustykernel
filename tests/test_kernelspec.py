from __future__ import annotations

import hashlib
import hmac
import json
import os
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest
import rustykernel
import zmq
from jupyter_core.paths import jupyter_data_dir


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
    session: str, msg_type: str, content: dict[str, object]
) -> tuple[dict[str, object], list[bytes]]:
    header = {
        "msg_id": str(uuid.uuid4()),
        "session": session,
        "username": "python-test-client",
        "date": "2026-03-26T00:00:00.000Z",
        "msg_type": msg_type,
        "version": "5.3",
    }
    parent_header: dict[str, object] = {}
    metadata: dict[str, object] = {}
    header_frame = json.dumps(header).encode("utf-8")
    parent_frame = json.dumps(parent_header).encode("utf-8")
    metadata_frame = json.dumps(metadata).encode("utf-8")
    content_frame = json.dumps(content).encode("utf-8")
    return header, [header_frame, parent_frame, metadata_frame, content_frame]


def send_client_message(
    socket: zmq.Socket,
    key: str,
    session: str,
    msg_type: str,
    content: dict[str, object],
) -> dict[str, object]:
    header, frames = client_request(session, msg_type, content)
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


@pytest.fixture()
def zmq_context() -> zmq.Context:
    context = zmq.Context()
    yield context
    context.term()


def test_make_rustykernel_cmd() -> None:
    command = rustykernel.make_rustykernel_cmd()

    assert command == [sys.executable, "-m", "rustykernel", "-f", "{connection_file}"]


def test_get_kernel_dict() -> None:
    kernel_dict = rustykernel.get_kernel_dict()

    assert kernel_dict["argv"] == rustykernel.make_rustykernel_cmd()
    assert kernel_dict["display_name"] == "Python (rustykernel)"
    assert kernel_dict["language"] == "python"
    assert kernel_dict["metadata"] == {"debugger": True}
    assert (
        kernel_dict["kernel_protocol_version"]
        == rustykernel.runtime_info().protocol_version
    )


def test_write_kernel_spec(tmp_path: Path) -> None:
    spec_path = Path(rustykernel.write_kernel_spec(tmp_path / "spec"))

    kernel_json = spec_path / "kernel.json"
    assert kernel_json.exists()
    payload = json.loads(kernel_json.read_text(encoding="utf-8"))
    assert payload["argv"] == rustykernel.make_rustykernel_cmd()


def test_install_prefix_writes_kernel_spec(tmp_path: Path) -> None:
    destination = Path(
        rustykernel.install(
            prefix=str(tmp_path),
            kernel_name="rusty-prefix",
            display_name="Rusty Prefix",
        )
    )

    kernel_json = destination / "kernel.json"
    payload = json.loads(kernel_json.read_text(encoding="utf-8"))
    assert destination == tmp_path / "share" / "jupyter" / "kernels" / "rusty-prefix"
    assert payload["display_name"] == "Rusty Prefix"


def test_install_user_writes_kernel_spec(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))

    destination = Path(rustykernel.install(user=True))
    kernel_json = destination / "kernel.json"

    assert destination == Path(jupyter_data_dir()) / "kernels" / rustykernel.KERNEL_NAME
    assert kernel_json.exists()


def test_module_cli_install_prefix(tmp_path: Path) -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "rustykernel",
            "install",
            "--prefix",
            str(tmp_path),
            "--name",
            "rusty-cli",
            "--display-name",
            "Rusty CLI",
        ],
        capture_output=True,
        check=True,
        text=True,
    )

    destination = Path(result.stdout.strip())
    kernel_json = destination / "kernel.json"
    payload = json.loads(kernel_json.read_text(encoding="utf-8"))
    assert destination == tmp_path / "share" / "jupyter" / "kernels" / "rusty-cli"
    assert payload["display_name"] == "Rusty CLI"


def test_module_cli_install_sys_prefix_uses_active_prefix(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    from rustykernel import app as rustykernel_app

    calls: dict[str, object] = {}

    def fake_install(**kwargs: object) -> str:
        calls.update(kwargs)
        return "/tmp/rustykernel-spec"

    monkeypatch.setattr(rustykernel_app, "install_kernel_spec", fake_install)
    monkeypatch.setattr(rustykernel_app.sys, "prefix", "/tmp/active-prefix")

    exit_code = rustykernel_app.main(["install", "--sys-prefix"])

    assert exit_code == 0
    assert calls == {
        "user": False,
        "kernel_name": "rustykernel",
        "display_name": None,
        "prefix": "/tmp/active-prefix",
    }
    assert capsys.readouterr().out.strip() == "/tmp/rustykernel-spec"


def test_kernelspec_argv_starts_a_real_kernel(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    install_root = tmp_path / "prefix"
    spec_destination = Path(
        rustykernel.install(prefix=str(install_root), kernel_name="rusty-launch")
    )
    kernel_json = json.loads(
        (spec_destination / "kernel.json").read_text(encoding="utf-8")
    )

    connection = connection_payload()
    connection_file = write_connection_file(tmp_path, connection)
    argv = [
        str(connection_file) if part == "{connection_file}" else part
        for part in kernel_json["argv"]
    ]

    process = subprocess.Popen(
        argv,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=os.environ.copy(),
    )

    shell = zmq_context.socket(zmq.DEALER)
    shell.connect(f"tcp://127.0.0.1:{connection['shell_port']}")
    shell.setsockopt(zmq.RCVTIMEO, 250)

    iopub = zmq_context.socket(zmq.SUB)
    iopub.setsockopt(zmq.SUBSCRIBE, b"")
    iopub.connect(f"tcp://127.0.0.1:{connection['iopub_port']}")
    iopub.setsockopt(zmq.RCVTIMEO, 1000)

    try:
        deadline = time.monotonic() + 10.0
        info_reply: dict[str, object] | None = None
        pending_msg_ids: set[str] = set()
        while time.monotonic() < deadline:
            header = send_client_message(
                shell,
                str(connection["key"]),
                "python-test-session",
                "kernel_info_request",
                {},
            )
            pending_msg_ids.add(header["msg_id"])
            try:
                candidate = recv_message(shell, str(connection["key"]))
            except zmq.error.Again:
                time.sleep(0.1)
                continue
            if candidate["parent_header"].get("msg_id") in pending_msg_ids:
                info_reply = candidate
                break

        assert info_reply is not None, "kernel did not answer kernel_info_request"
        assert info_reply["header"]["msg_type"] == "kernel_info_reply"
        content = info_reply["content"]
        assert content["implementation"] == "rustykernel"
        assert content["protocol_version"] == "5.3"
        assert content["language"] == "python"
        assert content["language_version"] == content["language_info"]["version"]
        assert content["language_info"]["name"] == "python"
        assert content["language_info"]["mimetype"] == "text/x-python"
        assert content["language_info"]["file_extension"] == ".py"
        assert content["language_info"]["nbconvert_exporter"] == "python"
        assert content["language_info"]["codemirror_mode"]["name"] == "ipython"
        assert content["help_links"][0]["text"] == "Python Reference"
        assert content["debugger"] is True
        assert content["supported_features"] == ["debugger"]

        execute_header = send_client_message(
            shell,
            str(connection["key"]),
            "python-test-session",
            "execute_request",
            {
                "code": "40 + 2",
                "silent": False,
                "store_history": True,
                "allow_stdin": False,
                "user_expressions": {},
                "stop_on_error": True,
            },
        )
        execute_reply = recv_message(shell, str(connection["key"]))
        assert execute_reply["content"]["status"] == "ok"

        seen_result = False
        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline and not seen_result:
            message = recv_message(iopub, str(connection["key"]))
            if message["parent_header"].get("msg_id") != execute_header["msg_id"]:
                continue
            if message["header"]["msg_type"] == "execute_result":
                assert message["content"]["data"]["text/plain"] == "42"
                seen_result = True

        assert seen_result, "kernel did not publish execute_result"
    finally:
        shell.close(0)
        iopub.close(0)
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)
