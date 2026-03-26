from __future__ import annotations

import time
from pathlib import Path

import pytest
import zmq

from tests.support import (
    recv_message,
    recv_iopub_messages_for_parent,
    running_kernel_client,
    send_client_message,
    wait_for_kernel_stop,
)


@pytest.fixture()
def zmq_context() -> zmq.Context:
    context = zmq.Context()
    yield context
    context.term()


def test_kernel_info_request_smoke(tmp_path: Path, zmq_context: zmq.Context) -> None:
    with running_kernel_client(tmp_path, zmq_context) as client:
        reply, published = client.request("shell", "kernel_info_request", {})

    assert reply["header"]["msg_type"] == "kernel_info_reply"
    assert reply["content"]["status"] == "ok"
    assert reply["content"]["protocol_version"] == "5.3"
    assert reply["content"]["language"] == "python"
    assert reply["content"]["debugger"] is False
    assert reply["content"]["supported_features"] == []
    assert [message["content"]["execution_state"] for message in published] == [
        "busy",
        "idle",
    ]


def test_usage_request_smoke(tmp_path: Path, zmq_context: zmq.Context) -> None:
    with running_kernel_client(tmp_path, zmq_context) as client:
        reply, published = client.request("control", "usage_request", {})

    assert reply["header"]["msg_type"] == "usage_reply"
    assert isinstance(reply["content"]["hostname"], str)
    assert reply["content"]["hostname"]
    assert isinstance(reply["content"]["pid"], int)
    assert reply["content"]["pid"] > 0
    assert isinstance(reply["content"]["kernel_cpu"], (float, int))
    assert isinstance(reply["content"]["kernel_memory"], int)
    assert reply["content"]["kernel_memory"] > 0
    assert isinstance(reply["content"]["cpu_count"], int)
    assert reply["content"]["cpu_count"] > 0

    host_virtual_memory = reply["content"]["host_virtual_memory"]
    assert isinstance(host_virtual_memory, dict)
    assert host_virtual_memory["total"] >= host_virtual_memory["used"]
    assert host_virtual_memory["total"] >= host_virtual_memory["available"]
    assert host_virtual_memory["total"] > 0

    if "host_cpu_percent" in reply["content"]:
        assert isinstance(reply["content"]["host_cpu_percent"], (float, int))

    assert [message["content"]["execution_state"] for message in published] == [
        "busy",
        "idle",
    ]


def test_execute_request_smoke(tmp_path: Path, zmq_context: zmq.Context) -> None:
    with running_kernel_client(tmp_path, zmq_context) as client:
        reply, published = client.request(
            "shell",
            "execute_request",
            {
                "code": "1 + 2",
                "silent": False,
                "store_history": True,
                "allow_stdin": False,
                "user_expressions": {},
                "stop_on_error": True,
            },
        )

    assert reply["header"]["msg_type"] == "execute_reply"
    assert reply["content"]["status"] == "ok"
    assert reply["content"]["execution_count"] == 1
    assert [message["header"]["msg_type"] for message in published] == [
        "status",
        "execute_input",
        "execute_result",
        "status",
    ]
    assert published[2]["content"]["data"]["text/plain"] == "3"


def test_fd_and_subprocess_output_are_published_as_streams(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    with running_kernel_client(tmp_path, zmq_context) as client:
        reply, published = client.request(
            "shell",
            "execute_request",
            {
                "code": """
import faulthandler
import os
import subprocess
import sys

print("py-out")
os.write(1, b"fd-out\\n")
os.write(2, b"fd-err\\n")
subprocess.run(
    [
        sys.executable,
        "-c",
        "import sys; print('child-out'); print('child-err', file=sys.stderr)",
    ],
    check=True,
)
faulthandler.dump_traceback()
40 + 2
""",
                "silent": False,
                "store_history": True,
                "allow_stdin": False,
                "user_expressions": {},
                "stop_on_error": True,
            },
        )

    assert reply["header"]["msg_type"] == "execute_reply"
    assert reply["content"]["status"] == "ok"
    assert published[0]["header"]["msg_type"] == "status"
    assert published[1]["header"]["msg_type"] == "execute_input"
    assert published[-2]["header"]["msg_type"] == "execute_result"
    assert published[-1]["header"]["msg_type"] == "status"

    stream_messages = [
        message
        for message in published[2:-2]
        if message["header"]["msg_type"] == "stream"
    ]
    assert stream_messages

    stdout_text = "".join(
        message["content"]["text"]
        for message in stream_messages
        if message["content"]["name"] == "stdout"
    )
    stderr_text = "".join(
        message["content"]["text"]
        for message in stream_messages
        if message["content"]["name"] == "stderr"
    )

    assert "py-out\n" in stdout_text
    assert "fd-out\n" in stdout_text
    assert "child-out\n" in stdout_text
    assert "fd-err\n" in stderr_text
    assert "child-err\n" in stderr_text
    assert "Current thread" in stderr_text
    assert published[-2]["content"]["data"]["text/plain"] == "42"


def test_execute_request_publishes_python_stdout_as_stream(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    with running_kernel_client(tmp_path, zmq_context) as client:
        reply, published = client.request(
            "shell",
            "execute_request",
            {
                "code": "print('hello')\n40 + 2",
                "silent": False,
                "store_history": True,
                "allow_stdin": False,
                "user_expressions": {},
                "stop_on_error": True,
            },
        )

    assert reply["header"]["msg_type"] == "execute_reply"
    assert reply["content"]["status"] == "ok"
    assert [message["header"]["msg_type"] for message in published] == [
        "status",
        "execute_input",
        "stream",
        "execute_result",
        "status",
    ]
    assert published[2]["content"] == {"name": "stdout", "text": "hello\n"}
    assert published[3]["content"]["data"]["text/plain"] == "42"


def test_execute_request_error_smoke(tmp_path: Path, zmq_context: zmq.Context) -> None:
    with running_kernel_client(tmp_path, zmq_context) as client:
        reply, published = client.request(
            "shell",
            "execute_request",
            {
                "code": "1 / 0",
                "silent": False,
                "store_history": True,
                "allow_stdin": False,
                "user_expressions": {},
                "stop_on_error": True,
            },
        )

    assert reply["header"]["msg_type"] == "execute_reply"
    assert reply["content"]["status"] == "error"
    assert reply["content"]["execution_count"] == 1
    assert reply["content"]["ename"] == "ZeroDivisionError"
    assert reply["content"]["user_expressions"] == {}
    assert reply["content"]["payload"] == []
    assert reply["content"]["traceback"]
    assert [message["header"]["msg_type"] for message in published] == [
        "status",
        "execute_input",
        "error",
        "status",
    ]
    assert published[2]["content"]["ename"] == "ZeroDivisionError"
    assert published[2]["content"]["evalue"] == "division by zero"


def test_complete_request_smoke(tmp_path: Path, zmq_context: zmq.Context) -> None:
    with running_kernel_client(tmp_path, zmq_context) as client:
        execute_reply, _ = client.request(
            "shell",
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
        assert execute_reply["content"]["status"] == "ok"

        reply, published = client.request(
            "shell",
            "complete_request",
            {"code": "val", "cursor_pos": 3},
        )

    assert reply["header"]["msg_type"] == "complete_reply"
    assert reply["content"]["status"] == "ok"
    assert "value" in reply["content"]["matches"]
    assert [message["content"]["execution_state"] for message in published] == [
        "busy",
        "idle",
    ]


def test_inspect_request_smoke(tmp_path: Path, zmq_context: zmq.Context) -> None:
    with running_kernel_client(tmp_path, zmq_context) as client:
        execute_reply, _ = client.request(
            "shell",
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
        assert execute_reply["content"]["status"] == "ok"

        reply, published = client.request(
            "shell",
            "inspect_request",
            {"code": "value", "cursor_pos": 5, "detail_level": 0},
        )

    assert reply["header"]["msg_type"] == "inspect_reply"
    assert reply["content"]["status"] == "ok"
    assert reply["content"]["found"] is True
    assert "text/plain" in reply["content"]["data"]
    assert [message["content"]["execution_state"] for message in published] == [
        "busy",
        "idle",
    ]


def test_history_request_smoke(tmp_path: Path, zmq_context: zmq.Context) -> None:
    with running_kernel_client(tmp_path, zmq_context) as client:
        first_reply, _ = client.request(
            "shell",
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
        assert first_reply["content"]["status"] == "ok"

        second_reply, _ = client.request(
            "shell",
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
        assert second_reply["content"]["status"] == "ok"

        reply, published = client.request(
            "shell",
            "history_request",
            {
                "hist_access_type": "range",
                "output": True,
                "raw": True,
                "session": 0,
                "start": 0,
                "stop": 3,
            },
        )

    assert reply["header"]["msg_type"] == "history_reply"
    assert reply["content"]["status"] == "ok"
    assert reply["content"]["history"] == [
        [0, 0, ["", None]],
        [0, 1, ["value = 40", None]],
        [0, 2, ["value + 2", "42"]],
    ]
    assert [message["content"]["execution_state"] for message in published] == [
        "busy",
        "idle",
    ]


def test_comm_info_request_reports_open_comms(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    with running_kernel_client(tmp_path, zmq_context) as client:
        setup_reply, _ = client.request(
            "shell",
            "execute_request",
            {
                "code": """
from rustykernel.comm import register_target

def target(comm, msg):
    pass

register_target("jupyter.widget", target)
""",
                "silent": False,
                "store_history": True,
                "allow_stdin": False,
                "user_expressions": {},
                "stop_on_error": True,
            },
        )
        assert setup_reply["content"]["status"] == "ok"

        open_header = send_client_message(
            client.shell,
            str(client.payload["key"]),
            client.session,
            "comm_open",
            {
                "comm_id": "comm-1",
                "target_name": "jupyter.widget",
                "data": {},
            },
        )
        open_published = recv_iopub_messages_for_parent(
            client.iopub,
            str(client.payload["key"]),
            str(open_header["msg_id"]),
        )

        reply, published = client.request("shell", "comm_info_request", {})
        filtered_reply, filtered_published = client.request(
            "shell",
            "comm_info_request",
            {"target_name": "other.target"},
        )

    assert [message["content"]["execution_state"] for message in open_published] == [
        "busy",
        "idle",
    ]
    assert reply["header"]["msg_type"] == "comm_info_reply"
    assert reply["content"] == {
        "status": "ok",
        "comms": {
            "comm-1": {
                "target_name": "jupyter.widget",
            }
        },
    }
    assert [message["content"]["execution_state"] for message in published] == [
        "busy",
        "idle",
    ]
    assert filtered_reply["content"] == {"status": "ok", "comms": {}}
    assert [
        message["content"]["execution_state"] for message in filtered_published
    ] == [
        "busy",
        "idle",
    ]


def test_comm_messages_reach_registered_target(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    with running_kernel_client(tmp_path, zmq_context) as client:
        setup_reply, _ = client.request(
            "shell",
            "execute_request",
            {
                "code": """
from rustykernel.comm import register_target
received = []

def target(comm, msg):
    received.append(("open", msg["content"]["data"]))
    comm.send({"phase": "opened", "value": msg["content"]["data"]["value"]})

    @comm.on_msg
    def _on_msg(msg):
        received.append(("msg", msg["content"]["data"]))
        comm.send({"phase": "echo", "value": msg["content"]["data"]["value"]})

    @comm.on_close
    def _on_close(msg):
        received.append(("close", msg["content"]["data"]))

register_target("echo.target", target)
""",
                "silent": False,
                "store_history": True,
                "allow_stdin": False,
                "user_expressions": {},
                "stop_on_error": True,
            },
        )
        assert setup_reply["content"]["status"] == "ok"

        open_header = send_client_message(
            client.shell,
            str(client.payload["key"]),
            client.session,
            "comm_open",
            {
                "comm_id": "comm-echo",
                "target_name": "echo.target",
                "data": {"value": 1},
                "metadata": {},
            },
        )
        open_published = recv_iopub_messages_for_parent(
            client.iopub,
            str(client.payload["key"]),
            str(open_header["msg_id"]),
        )

        msg_header = send_client_message(
            client.shell,
            str(client.payload["key"]),
            client.session,
            "comm_msg",
            {
                "comm_id": "comm-echo",
                "data": {"value": 2},
                "metadata": {},
            },
        )
        msg_published = recv_iopub_messages_for_parent(
            client.iopub,
            str(client.payload["key"]),
            str(msg_header["msg_id"]),
        )

        close_header = send_client_message(
            client.shell,
            str(client.payload["key"]),
            client.session,
            "comm_close",
            {
                "comm_id": "comm-echo",
                "data": {"reason": "done"},
                "metadata": {},
            },
        )
        close_published = recv_iopub_messages_for_parent(
            client.iopub,
            str(client.payload["key"]),
            str(close_header["msg_id"]),
        )

        probe_reply, probe_published = client.request(
            "shell",
            "execute_request",
            {
                "code": "received",
                "silent": False,
                "store_history": True,
                "allow_stdin": False,
                "user_expressions": {},
                "stop_on_error": True,
            },
        )

    assert [message["header"]["msg_type"] for message in open_published] == [
        "status",
        "comm_msg",
        "status",
    ]
    assert open_published[1]["content"]["data"] == {"phase": "opened", "value": 1}
    assert [message["header"]["msg_type"] for message in msg_published] == [
        "status",
        "comm_msg",
        "status",
    ]
    assert msg_published[1]["content"]["data"] == {"phase": "echo", "value": 2}
    assert [message["header"]["msg_type"] for message in close_published] == [
        "status",
        "status",
    ]
    assert probe_reply["content"]["status"] == "ok"
    assert probe_reply["content"]["execution_count"] == 2
    assert probe_reply["content"]["payload"] == []
    assert probe_published[2]["content"]["data"]["text/plain"] == (
        "[('open', {'value': 1}), ('msg', {'value': 2}), ('close', {'reason': 'done'})]"
    )


def test_execute_request_can_publish_comm_flow(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    with running_kernel_client(tmp_path, zmq_context) as client:
        reply, published = client.request(
            "shell",
            "execute_request",
            {
                "code": """
from rustykernel.comm import Comm
comm = Comm(target_name="kernel.target", data={"phase": "open"})
comm.send({"phase": "message"})
comm.close({"phase": "close"})
""",
                "silent": False,
                "store_history": True,
                "allow_stdin": False,
                "user_expressions": {},
                "stop_on_error": True,
            },
        )

    assert reply["header"]["msg_type"] == "execute_reply"
    assert reply["content"]["status"] == "ok"
    assert [message["header"]["msg_type"] for message in published] == [
        "status",
        "execute_input",
        "comm_open",
        "comm_msg",
        "comm_close",
        "status",
    ]
    assert published[2]["content"]["target_name"] == "kernel.target"
    assert published[2]["content"]["data"] == {"phase": "open"}
    assert published[3]["content"]["data"] == {"phase": "message"}
    assert published[4]["content"]["data"] == {"phase": "close"}


def test_is_complete_request_smoke(tmp_path: Path, zmq_context: zmq.Context) -> None:
    with running_kernel_client(tmp_path, zmq_context) as client:
        reply, published = client.request(
            "shell",
            "is_complete_request",
            {"code": "for i in range(3):"},
        )

    assert reply["header"]["msg_type"] == "is_complete_reply"
    assert reply["content"]["status"] == "incomplete"
    assert reply["content"]["indent"] == "    "
    assert [message["content"]["execution_state"] for message in published] == [
        "busy",
        "idle",
    ]


def test_shutdown_request_smoke(tmp_path: Path, zmq_context: zmq.Context) -> None:
    with running_kernel_client(tmp_path, zmq_context) as client:
        reply, published = client.request(
            "control",
            "shutdown_request",
            {"restart": False},
        )
        wait_for_kernel_stop(client.kernel)

    assert reply["header"]["msg_type"] == "shutdown_reply"
    assert reply["content"]["status"] == "ok"
    assert reply["content"]["restart"] is False
    assert [message["header"]["msg_type"] for message in published] == [
        "status",
        "shutdown_reply",
        "status",
    ]
    assert published[1]["content"] == {"status": "ok", "restart": False}


def test_interrupt_request_smoke(tmp_path: Path, zmq_context: zmq.Context) -> None:
    with running_kernel_client(tmp_path, zmq_context) as client:
        setup_reply, _ = client.request(
            "shell",
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
        assert setup_reply["content"]["status"] == "ok"

        execute_header = send_client_message(
            client.shell,
            str(client.payload["key"]),
            client.session,
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
        time.sleep(0.2)

        reply, published = client.request("control", "interrupt_request", {})
        shell_reply = recv_message(client.shell, str(client.payload["key"]))
        execute_published = recv_iopub_messages_for_parent(
            client.iopub,
            str(client.payload["key"]),
            str(execute_header["msg_id"]),
        )
        probe_reply, probe_published = client.request(
            "shell",
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
        history_reply, history_published = client.request(
            "shell",
            "history_request",
            {
                "hist_access_type": "tail",
                "output": False,
                "raw": True,
                "session": 0,
                "n": 10,
            },
        )

    assert reply["header"]["msg_type"] == "interrupt_reply"
    assert reply["content"]["status"] == "ok"
    assert [message["content"]["execution_state"] for message in published] == [
        "busy",
        "idle",
    ]
    assert shell_reply["header"]["msg_type"] == "execute_reply"
    assert shell_reply["content"]["status"] == "error"
    assert shell_reply["content"]["ename"] == "KeyboardInterrupt"
    assert shell_reply["content"]["execution_count"] == 2
    assert [message["header"]["msg_type"] for message in execute_published] == [
        "error",
        "status",
    ]
    assert execute_published[-2]["content"]["ename"] == "KeyboardInterrupt"
    assert probe_reply["header"]["msg_type"] == "execute_reply"
    assert probe_reply["content"]["status"] == "ok"
    assert probe_reply["content"]["execution_count"] == 3
    assert [message["header"]["msg_type"] for message in probe_published] == [
        "status",
        "execute_input",
        "execute_result",
        "status",
    ]
    assert [
        message["content"]["execution_state"]
        for message in probe_published
        if message["header"]["msg_type"] == "status"
    ] == [
        "busy",
        "idle",
    ]
    assert history_reply["content"]["history"] == [
        [1, 1, "value = 40"],
        [1, 2, "import time\ntime.sleep(30)"],
        [1, 3, "value"],
    ]
    assert [message["content"]["execution_state"] for message in history_published] == [
        "busy",
        "idle",
    ]
