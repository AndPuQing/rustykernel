from __future__ import annotations

import json
import hashlib
import hmac
import socket
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest
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
    socket: zmq.Socket, key: str, session: str, msg_type: str, content: dict[str, object]
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


def recv_iopub_messages_for_parent(
    socket: zmq.Socket, key: str, parent_msg_id: str, expected_count: int
) -> list[dict[str, object]]:
    deadline = time.monotonic() + 3.0
    messages = []
    while len(messages) < expected_count:
        assert time.monotonic() < deadline, "timed out waiting for iopub messages"
        message = recv_message(socket, key)
        if message["parent_header"].get("msg_id") == parent_msg_id:
            messages.append(message)
    return messages


@pytest.fixture()
def zmq_context() -> zmq.Context:
    context = zmq.Context()
    yield context
    context.term()


def test_healthcheck_and_runtime_info() -> None:
    info = rustykernel.runtime_info()

    assert rustykernel.healthcheck() == "ok"
    assert info.implementation == "rustykernel"
    assert info.implementation_version == rustykernel.__version__
    assert info.language == "python"
    assert info.protocol_version == "5.3"


def test_parse_connection_file_returns_python_objects(tmp_path: Path) -> None:
    payload = connection_payload()
    path = write_connection_file(tmp_path, payload)

    connection = rustykernel.parse_connection_file(str(path))

    assert connection.transport == payload["transport"]
    assert connection.ip == payload["ip"]
    assert connection.shell_port == payload["shell_port"]
    assert connection.iopub_port == payload["iopub_port"]
    assert connection.stdin_port == payload["stdin_port"]
    assert connection.control_port == payload["control_port"]
    assert connection.hb_port == payload["hb_port"]
    assert connection.signature_scheme == payload["signature_scheme"]
    assert connection.key == payload["key"]
    assert connection.kernel_name == payload["kernel_name"]


def test_parse_connection_file_maps_invalid_config_to_value_error(
    tmp_path: Path,
) -> None:
    payload = connection_payload()
    payload["transport"] = ""
    path = write_connection_file(tmp_path, payload)

    with pytest.raises(ValueError, match="transport must not be empty"):
        rustykernel.parse_connection_file(str(path))


def test_start_kernel_exposes_running_kernel_lifecycle(tmp_path: Path) -> None:
    payload = connection_payload()
    path = write_connection_file(tmp_path, payload)

    kernel = rustykernel.start_kernel(str(path))
    try:
        assert kernel.is_running is True
        assert kernel.connection.kernel_name == payload["kernel_name"]
        assert kernel.endpoints.shell == f"tcp://127.0.0.1:{payload['shell_port']}"
        assert kernel.endpoints.control == f"tcp://127.0.0.1:{payload['control_port']}"
    finally:
        kernel.stop()

    assert kernel.is_running is False


def test_module_cli_prints_json_description() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "rustykernel", "--json"],
        capture_output=True,
        check=True,
        text=True,
    )

    payload = json.loads(result.stdout)

    assert payload["implementation"] == "rustykernel"
    assert payload["language"] == "python"
    assert payload["healthcheck"] == "ok"
    assert payload["connection_file"] is None
    assert payload["connection"] is None


def test_execute_request_persists_state_and_publishes_result(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    payload = connection_payload()
    path = write_connection_file(tmp_path, payload)
    kernel = rustykernel.start_kernel(str(path))

    shell = zmq_context.socket(zmq.DEALER)
    shell.connect(kernel.endpoints.shell)
    iopub = zmq_context.socket(zmq.SUB)
    iopub.setsockopt(zmq.SUBSCRIBE, b"")
    iopub.connect(kernel.endpoints.iopub)
    shell.setsockopt(zmq.RCVTIMEO, 2000)
    iopub.setsockopt(zmq.RCVTIMEO, 2000)
    time.sleep(0.1)

    try:
        first_header = send_client_message(
            shell,
            str(payload["key"]),
            "python-test-session",
            "execute_request",
            {
                "code": "value = 40",
                "silent": False,
                "store_history": True,
                "allow_stdin": False,
                "user_expressions": {},
                "stop_on_error": True,
            },
        )
        first_reply = recv_message(shell, str(payload["key"]))
        assert first_reply["header"]["msg_type"] == "execute_reply"
        assert first_reply["content"]["status"] == "ok"
        recv_iopub_messages_for_parent(iopub, str(payload["key"]), first_header["msg_id"], 3)

        second_header = send_client_message(
            shell,
            str(payload["key"]),
            "python-test-session",
            "execute_request",
            {
                "code": "value + 2",
                "silent": False,
                "store_history": True,
                "allow_stdin": False,
                "user_expressions": {},
                "stop_on_error": True,
            },
        )
        second_reply = recv_message(shell, str(payload["key"]))
        assert second_reply["content"]["status"] == "ok"
        assert second_reply["content"]["execution_count"] == 2

        published = recv_iopub_messages_for_parent(
            iopub, str(payload["key"]), second_header["msg_id"], 4
        )
        assert [message["header"]["msg_type"] for message in published] == [
            "status",
            "execute_input",
            "execute_result",
            "status",
        ]
        assert published[2]["content"]["data"]["text/plain"] == "42"
    finally:
        shell.close(0)
        iopub.close(0)
        kernel.stop()


def test_execute_request_publishes_rich_mimebundle_results(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    payload = connection_payload()
    path = write_connection_file(tmp_path, payload)
    kernel = rustykernel.start_kernel(str(path))

    shell = zmq_context.socket(zmq.DEALER)
    shell.connect(kernel.endpoints.shell)
    iopub = zmq_context.socket(zmq.SUB)
    iopub.setsockopt(zmq.SUBSCRIBE, b"")
    iopub.connect(kernel.endpoints.iopub)
    shell.setsockopt(zmq.RCVTIMEO, 2000)
    iopub.setsockopt(zmq.RCVTIMEO, 2000)
    time.sleep(0.1)

    try:
        execute_header = send_client_message(
            shell,
            str(payload["key"]),
            "python-test-session",
            "execute_request",
            {
                "code": """
class RichValue:
    def __repr__(self):
        return "RichValue(text)"

    def _repr_html_(self):
        return "<b>rich</b>"

    def _repr_markdown_(self):
        return "**rich**"

    def _repr_mimebundle_(self, include=None, exclude=None):
        return ({"application/json": {"value": 7}}, {"application/json": {"expanded": True}})

RichValue()
""",
                "silent": False,
                "store_history": True,
                "allow_stdin": False,
                "user_expressions": {},
                "stop_on_error": True,
            },
        )
        execute_reply = recv_message(shell, str(payload["key"]))
        assert execute_reply["content"]["status"] == "ok"

        published = recv_iopub_messages_for_parent(
            iopub, str(payload["key"]), execute_header["msg_id"], 4
        )
        assert [message["header"]["msg_type"] for message in published] == [
            "status",
            "execute_input",
            "execute_result",
            "status",
        ]
        result = published[2]["content"]
        assert result["data"]["text/plain"] == "RichValue(text)"
        assert result["data"]["text/html"] == "<b>rich</b>"
        assert result["data"]["text/markdown"] == "**rich**"
        assert result["data"]["application/json"] == {"value": 7}
        assert result["metadata"]["application/json"] == {"expanded": True}
    finally:
        shell.close(0)
        iopub.close(0)
        kernel.stop()


def test_execute_request_supports_input_via_stdin_channel(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    payload = connection_payload()
    path = write_connection_file(tmp_path, payload)
    kernel = rustykernel.start_kernel(str(path))

    shell = zmq_context.socket(zmq.DEALER)
    shell.setsockopt(zmq.IDENTITY, b"python-stdin-client")
    shell.connect(kernel.endpoints.shell)
    stdin = zmq_context.socket(zmq.DEALER)
    stdin.setsockopt(zmq.IDENTITY, b"python-stdin-client")
    stdin.connect(kernel.endpoints.stdin)
    iopub = zmq_context.socket(zmq.SUB)
    iopub.setsockopt(zmq.SUBSCRIBE, b"")
    iopub.connect(kernel.endpoints.iopub)
    shell.setsockopt(zmq.RCVTIMEO, 2000)
    stdin.setsockopt(zmq.RCVTIMEO, 2000)
    iopub.setsockopt(zmq.RCVTIMEO, 2000)
    time.sleep(0.1)

    try:
        execute_header = send_client_message(
            shell,
            str(payload["key"]),
            "python-test-session",
            "execute_request",
            {
                "code": "name = input('Name: ')\nname",
                "silent": False,
                "store_history": True,
                "allow_stdin": True,
                "user_expressions": {},
                "stop_on_error": True,
            },
        )

        input_request = recv_message(stdin, str(payload["key"]))
        assert input_request["header"]["msg_type"] == "input_request"
        assert input_request["content"]["prompt"] == "Name: "
        assert input_request["content"]["password"] is False

        send_client_message(
            stdin,
            str(payload["key"]),
            "python-test-session",
            "input_reply",
            {"value": "Ada"},
        )

        execute_reply = recv_message(shell, str(payload["key"]))
        assert execute_reply["header"]["msg_type"] == "execute_reply"
        assert execute_reply["content"]["status"] == "ok"

        published = recv_iopub_messages_for_parent(
            iopub, str(payload["key"]), execute_header["msg_id"], 4
        )
        assert [message["header"]["msg_type"] for message in published] == [
            "status",
            "execute_input",
            "execute_result",
            "status",
        ]
        assert published[2]["content"]["data"]["text/plain"] == "'Ada'"
    finally:
        shell.close(0)
        stdin.close(0)
        iopub.close(0)
        kernel.stop()


def test_execute_request_publishes_display_data_and_updates(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    payload = connection_payload()
    path = write_connection_file(tmp_path, payload)
    kernel = rustykernel.start_kernel(str(path))

    shell = zmq_context.socket(zmq.DEALER)
    shell.connect(kernel.endpoints.shell)
    iopub = zmq_context.socket(zmq.SUB)
    iopub.setsockopt(zmq.SUBSCRIBE, b"")
    iopub.connect(kernel.endpoints.iopub)
    shell.setsockopt(zmq.RCVTIMEO, 2000)
    iopub.setsockopt(zmq.RCVTIMEO, 2000)
    time.sleep(0.1)

    try:
        execute_header = send_client_message(
            shell,
            str(payload["key"]),
            "python-test-session",
            "execute_request",
            {
                "code": """
from IPython.display import display, update_display

class RichValue:
    def __init__(self, label):
        self.label = label

    def __repr__(self):
        return f"RichValue({self.label})"

    def _repr_html_(self):
        return f"<b>{self.label}</b>"

handle = display(RichValue('first'), display_id=True)
update_display(RichValue('second'), display_id=handle.display_id)
_ = handle.update(RichValue('third'))
""",
                "silent": False,
                "store_history": True,
                "allow_stdin": False,
                "user_expressions": {},
                "stop_on_error": True,
            },
        )
        execute_reply = recv_message(shell, str(payload["key"]))
        assert execute_reply["content"]["status"] == "ok"

        published = recv_iopub_messages_for_parent(
            iopub, str(payload["key"]), execute_header["msg_id"], 6
        )
        assert [message["header"]["msg_type"] for message in published] == [
            "status",
            "execute_input",
            "display_data",
            "update_display_data",
            "update_display_data",
            "status",
        ]

        first_display = published[2]["content"]
        display_id = first_display["transient"]["display_id"]
        assert first_display["data"]["text/plain"] == "RichValue(first)"
        assert first_display["data"]["text/html"] == "<b>first</b>"

        second_display = published[3]["content"]
        assert second_display["transient"]["display_id"] == display_id
        assert second_display["data"]["text/plain"] == "RichValue(second)"

        third_display = published[4]["content"]
        assert third_display["transient"]["display_id"] == display_id
        assert third_display["data"]["text/plain"] == "RichValue(third)"
    finally:
        shell.close(0)
        iopub.close(0)
        kernel.stop()


def test_complete_request_returns_python_matches(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    payload = connection_payload()
    path = write_connection_file(tmp_path, payload)
    kernel = rustykernel.start_kernel(str(path))

    shell = zmq_context.socket(zmq.DEALER)
    shell.connect(kernel.endpoints.shell)
    iopub = zmq_context.socket(zmq.SUB)
    iopub.setsockopt(zmq.SUBSCRIBE, b"")
    iopub.connect(kernel.endpoints.iopub)
    shell.setsockopt(zmq.RCVTIMEO, 2000)
    iopub.setsockopt(zmq.RCVTIMEO, 2000)
    time.sleep(0.1)

    try:
        define_header = send_client_message(
            shell,
            str(payload["key"]),
            "python-test-session",
            "execute_request",
            {
                "code": "value = 40",
                "silent": False,
                "store_history": True,
                "allow_stdin": False,
                "user_expressions": {},
                "stop_on_error": True,
            },
        )
        define_reply = recv_message(shell, str(payload["key"]))
        assert define_reply["content"]["status"] == "ok"
        recv_iopub_messages_for_parent(iopub, str(payload["key"]), define_header["msg_id"], 3)

        complete_header = send_client_message(
            shell,
            str(payload["key"]),
            "python-test-session",
            "complete_request",
            {
                "code": "val",
                "cursor_pos": 3,
            },
        )
        complete_reply = recv_message(shell, str(payload["key"]))
        assert complete_reply["header"]["msg_type"] == "complete_reply"
        assert complete_reply["content"]["status"] == "ok"
        assert "value" in complete_reply["content"]["matches"]
        assert complete_reply["content"]["cursor_start"] == 0
        assert complete_reply["content"]["cursor_end"] == 3
        assert complete_reply["content"]["metadata"]["backend"] == "jedi"
        assert any(
            item["text"] == "value"
            for item in complete_reply["content"]["metadata"][
                "_jupyter_types_experimental"
            ]
        )

        published = recv_iopub_messages_for_parent(
            iopub, str(payload["key"]), complete_header["msg_id"], 2
        )
        assert [message["content"]["execution_state"] for message in published] == [
            "busy",
            "idle",
        ]
    finally:
        shell.close(0)
        iopub.close(0)
        kernel.stop()


def test_inspect_request_returns_python_details(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    payload = connection_payload()
    path = write_connection_file(tmp_path, payload)
    kernel = rustykernel.start_kernel(str(path))

    shell = zmq_context.socket(zmq.DEALER)
    shell.connect(kernel.endpoints.shell)
    iopub = zmq_context.socket(zmq.SUB)
    iopub.setsockopt(zmq.SUBSCRIBE, b"")
    iopub.connect(kernel.endpoints.iopub)
    shell.setsockopt(zmq.RCVTIMEO, 2000)
    iopub.setsockopt(zmq.RCVTIMEO, 2000)
    time.sleep(0.1)

    try:
        define_header = send_client_message(
            shell,
            str(payload["key"]),
            "python-test-session",
            "execute_request",
            {
                "code": "value = 40",
                "silent": False,
                "store_history": True,
                "allow_stdin": False,
                "user_expressions": {},
                "stop_on_error": True,
            },
        )
        define_reply = recv_message(shell, str(payload["key"]))
        assert define_reply["content"]["status"] == "ok"
        recv_iopub_messages_for_parent(iopub, str(payload["key"]), define_header["msg_id"], 3)

        inspect_header = send_client_message(
            shell,
            str(payload["key"]),
            "python-test-session",
            "inspect_request",
            {
                "code": "value",
                "cursor_pos": 5,
                "detail_level": 1,
            },
        )
        inspect_reply = recv_message(shell, str(payload["key"]))
        assert inspect_reply["header"]["msg_type"] == "inspect_reply"
        assert inspect_reply["content"]["status"] == "ok"
        assert inspect_reply["content"]["found"] is True
        assert inspect_reply["content"]["data"]["text/plain"].startswith("40\ntype: int")
        assert inspect_reply["content"]["metadata"]["type_name"] == "int"
        assert inspect_reply["content"]["metadata"]["doc_summary"] == "int([x]) -> integer"
        assert "**type:** `int`" in inspect_reply["content"]["data"]["text/markdown"]

        published = recv_iopub_messages_for_parent(
            iopub, str(payload["key"]), inspect_header["msg_id"], 2
        )
        assert [message["content"]["execution_state"] for message in published] == [
            "busy",
            "idle",
        ]
    finally:
        shell.close(0)
        iopub.close(0)
        kernel.stop()


def test_complete_request_reports_attribute_function_matches(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    payload = connection_payload()
    path = write_connection_file(tmp_path, payload)
    kernel = rustykernel.start_kernel(str(path))

    shell = zmq_context.socket(zmq.DEALER)
    shell.connect(kernel.endpoints.shell)
    iopub = zmq_context.socket(zmq.SUB)
    iopub.setsockopt(zmq.SUBSCRIBE, b"")
    iopub.connect(kernel.endpoints.iopub)
    shell.setsockopt(zmq.RCVTIMEO, 2000)
    iopub.setsockopt(zmq.RCVTIMEO, 2000)
    time.sleep(0.1)

    try:
        define_header = send_client_message(
            shell,
            str(payload["key"]),
            "python-test-session",
            "execute_request",
            {
                "code": "text = 'hello'",
                "silent": False,
                "store_history": True,
                "allow_stdin": False,
                "user_expressions": {},
                "stop_on_error": True,
            },
        )
        define_reply = recv_message(shell, str(payload["key"]))
        assert define_reply["content"]["status"] == "ok"
        recv_iopub_messages_for_parent(iopub, str(payload["key"]), define_header["msg_id"], 3)

        complete_header = send_client_message(
            shell,
            str(payload["key"]),
            "python-test-session",
            "complete_request",
            {
                "code": "text.st",
                "cursor_pos": 7,
            },
        )
        complete_reply = recv_message(shell, str(payload["key"]))
        assert "text.startswith" in complete_reply["content"]["matches"]
        assert all(
            not match.endswith("(") for match in complete_reply["content"]["matches"]
        )
        assert any(
            item["text"] == "text.startswith" and item["type"] == "function"
            and "signature" in item
            for item in complete_reply["content"]["metadata"][
                "_jupyter_types_experimental"
            ]
        )

        published = recv_iopub_messages_for_parent(
            iopub, str(payload["key"]), complete_header["msg_id"], 2
        )
        assert [message["content"]["execution_state"] for message in published] == [
            "busy",
            "idle",
        ]
    finally:
        shell.close(0)
        iopub.close(0)
        kernel.stop()


def test_inspect_request_prioritizes_callable_context(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    payload = connection_payload()
    path = write_connection_file(tmp_path, payload)
    kernel = rustykernel.start_kernel(str(path))

    shell = zmq_context.socket(zmq.DEALER)
    shell.connect(kernel.endpoints.shell)
    iopub = zmq_context.socket(zmq.SUB)
    iopub.setsockopt(zmq.SUBSCRIBE, b"")
    iopub.connect(kernel.endpoints.iopub)
    shell.setsockopt(zmq.RCVTIMEO, 2000)
    iopub.setsockopt(zmq.RCVTIMEO, 2000)
    time.sleep(0.1)

    try:
        define_header = send_client_message(
            shell,
            str(payload["key"]),
            "python-test-session",
            "execute_request",
            {
                "code": "value = 40",
                "silent": False,
                "store_history": True,
                "allow_stdin": False,
                "user_expressions": {},
                "stop_on_error": True,
            },
        )
        define_reply = recv_message(shell, str(payload["key"]))
        assert define_reply["content"]["status"] == "ok"
        recv_iopub_messages_for_parent(iopub, str(payload["key"]), define_header["msg_id"], 3)

        inspect_header = send_client_message(
            shell,
            str(payload["key"]),
            "python-test-session",
            "inspect_request",
            {
                "code": "str(value)",
                "cursor_pos": 8,
                "detail_level": 0,
            },
        )
        inspect_reply = recv_message(shell, str(payload["key"]))
        assert inspect_reply["content"]["status"] == "ok"
        assert inspect_reply["content"]["found"] is True
        assert inspect_reply["content"]["metadata"]["signature"] == "str(object='') -> str"
        assert (
            "signature: str(object='') -> str"
            in inspect_reply["content"]["data"]["text/plain"]
        )
        assert "type:" in inspect_reply["content"]["data"]["text/html"]

        published = recv_iopub_messages_for_parent(
            iopub, str(payload["key"]), inspect_header["msg_id"], 2
        )
        assert [message["content"]["execution_state"] for message in published] == [
            "busy",
            "idle",
        ]
    finally:
        shell.close(0)
        iopub.close(0)
        kernel.stop()


def test_is_complete_request_is_syntax_aware(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    payload = connection_payload()
    path = write_connection_file(tmp_path, payload)
    kernel = rustykernel.start_kernel(str(path))

    shell = zmq_context.socket(zmq.DEALER)
    shell.connect(kernel.endpoints.shell)
    iopub = zmq_context.socket(zmq.SUB)
    iopub.setsockopt(zmq.SUBSCRIBE, b"")
    iopub.connect(kernel.endpoints.iopub)
    shell.setsockopt(zmq.RCVTIMEO, 2000)
    iopub.setsockopt(zmq.RCVTIMEO, 2000)
    time.sleep(0.1)

    try:
        complete_header = send_client_message(
            shell,
            str(payload["key"]),
            "python-test-session",
            "is_complete_request",
            {"code": "value = 1"},
        )
        complete_reply = recv_message(shell, str(payload["key"]))
        assert complete_reply["header"]["msg_type"] == "is_complete_reply"
        assert complete_reply["content"]["status"] == "complete"
        assert complete_reply["content"]["indent"] == ""
        published = recv_iopub_messages_for_parent(
            iopub, str(payload["key"]), complete_header["msg_id"], 2
        )
        assert [message["content"]["execution_state"] for message in published] == [
            "busy",
            "idle",
        ]

        incomplete_header = send_client_message(
            shell,
            str(payload["key"]),
            "python-test-session",
            "is_complete_request",
            {"code": "for i in range(3):"},
        )
        incomplete_reply = recv_message(shell, str(payload["key"]))
        assert incomplete_reply["content"]["status"] == "incomplete"
        assert incomplete_reply["content"]["indent"] == "    "
        published = recv_iopub_messages_for_parent(
            iopub, str(payload["key"]), incomplete_header["msg_id"], 2
        )
        assert [message["content"]["execution_state"] for message in published] == [
            "busy",
            "idle",
        ]

        invalid_header = send_client_message(
            shell,
            str(payload["key"]),
            "python-test-session",
            "is_complete_request",
            {"code": "1+"},
        )
        invalid_reply = recv_message(shell, str(payload["key"]))
        assert invalid_reply["content"]["status"] == "invalid"
        assert invalid_reply["content"]["indent"] == ""
        published = recv_iopub_messages_for_parent(
            iopub, str(payload["key"]), invalid_header["msg_id"], 2
        )
        assert [message["content"]["execution_state"] for message in published] == [
            "busy",
            "idle",
        ]
    finally:
        shell.close(0)
        iopub.close(0)
        kernel.stop()


def test_control_restart_clears_worker_state(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    payload = connection_payload()
    path = write_connection_file(tmp_path, payload)
    kernel = rustykernel.start_kernel(str(path))

    shell = zmq_context.socket(zmq.DEALER)
    shell.connect(kernel.endpoints.shell)
    control = zmq_context.socket(zmq.DEALER)
    control.connect(kernel.endpoints.control)
    shell.setsockopt(zmq.RCVTIMEO, 2000)
    control.setsockopt(zmq.RCVTIMEO, 2000)
    time.sleep(0.1)

    try:
        send_client_message(
            shell,
            str(payload["key"]),
            "python-test-session",
            "execute_request",
            {
                "code": "value = 7",
                "silent": False,
                "store_history": True,
                "allow_stdin": False,
                "user_expressions": {},
                "stop_on_error": True,
            },
        )
        define_reply = recv_message(shell, str(payload["key"]))
        assert define_reply["content"]["status"] == "ok"

        send_client_message(
            control,
            str(payload["key"]),
            "python-test-session",
            "shutdown_request",
            {"restart": True},
        )
        restart_reply = recv_message(control, str(payload["key"]))
        assert restart_reply["header"]["msg_type"] == "shutdown_reply"
        assert restart_reply["content"]["restart"] is True
        assert kernel.is_running is True

        send_client_message(
            shell,
            str(payload["key"]),
            "python-test-session",
            "execute_request",
            {
                "code": "value",
                "silent": False,
                "store_history": True,
                "allow_stdin": False,
                "user_expressions": {},
                "stop_on_error": True,
            },
        )
        probe_reply = recv_message(shell, str(payload["key"]))
        assert probe_reply["content"]["status"] == "error"
        assert probe_reply["content"]["ename"] == "NameError"
    finally:
        shell.close(0)
        control.close(0)
        kernel.stop()


def test_control_interrupt_restarts_worker_state(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    payload = connection_payload()
    path = write_connection_file(tmp_path, payload)
    kernel = rustykernel.start_kernel(str(path))

    shell = zmq_context.socket(zmq.DEALER)
    shell.connect(kernel.endpoints.shell)
    control = zmq_context.socket(zmq.DEALER)
    control.connect(kernel.endpoints.control)
    shell.setsockopt(zmq.RCVTIMEO, 2000)
    control.setsockopt(zmq.RCVTIMEO, 2000)
    time.sleep(0.1)

    try:
        send_client_message(
            shell,
            str(payload["key"]),
            "python-test-session",
            "execute_request",
            {
                "code": "value = 123",
                "silent": False,
                "store_history": True,
                "allow_stdin": False,
                "user_expressions": {},
                "stop_on_error": True,
            },
        )
        define_reply = recv_message(shell, str(payload["key"]))
        assert define_reply["content"]["status"] == "ok"

        send_client_message(
            control,
            str(payload["key"]),
            "python-test-session",
            "interrupt_request",
            {},
        )
        interrupt_reply = recv_message(control, str(payload["key"]))
        assert interrupt_reply["header"]["msg_type"] == "interrupt_reply"
        assert interrupt_reply["content"]["status"] == "ok"
        assert kernel.is_running is True

        send_client_message(
            shell,
            str(payload["key"]),
            "python-test-session",
            "execute_request",
            {
                "code": "value",
                "silent": False,
                "store_history": True,
                "allow_stdin": False,
                "user_expressions": {},
                "stop_on_error": True,
            },
        )
        probe_reply = recv_message(shell, str(payload["key"]))
        assert probe_reply["content"]["status"] == "error"
        assert probe_reply["content"]["ename"] == "NameError"
    finally:
        shell.close(0)
        control.close(0)
        kernel.stop()
