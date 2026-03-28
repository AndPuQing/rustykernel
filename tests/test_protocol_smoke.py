from __future__ import annotations

import ast
import time
from pathlib import Path

import pytest
import zmq

from tests.support import (
    recv_message,
    recv_iopub_messages_for_parent,
    recv_iopub_messages_until_parent_predicate,
    running_kernel_client,
    send_client_message,
    wait_for_kernel_stop,
)


@pytest.fixture()
def zmq_context() -> zmq.Context:
    context = zmq.Context()
    yield context
    context.term()


def recv_iopub_messages_for_parents(
    socket: zmq.Socket,
    key: str,
    parent_msg_ids: list[str],
    timeout_s: float = 5.0,
) -> dict[str, list[dict[str, object]]]:
    pending = set(parent_msg_ids)
    messages = {parent_msg_id: [] for parent_msg_id in parent_msg_ids}
    deadline = time.monotonic() + timeout_s

    while pending and time.monotonic() < deadline:
        message = recv_message(socket, key)
        parent_msg_id = message["parent_header"].get("msg_id")
        if parent_msg_id not in messages:
            continue
        messages[parent_msg_id].append(message)
        if (
            message["header"]["msg_type"] == "status"
            and message["content"].get("execution_state") == "idle"
        ):
            pending.discard(parent_msg_id)

    assert not pending, f"timed out waiting for parent idle status: {sorted(pending)!r}"
    return messages


def test_kernel_info_request_smoke(tmp_path: Path, zmq_context: zmq.Context) -> None:
    with running_kernel_client(tmp_path, zmq_context) as client:
        reply, published = client.request("shell", "kernel_info_request", {})

    assert reply["header"]["msg_type"] == "kernel_info_reply"
    assert reply["content"]["status"] == "ok"
    assert reply["content"]["protocol_version"] == "5.3"
    assert reply["content"]["language"] == "python"
    assert reply["content"]["debugger"] is True
    assert reply["content"]["supported_features"] == [
        "debugger",
        "kernel subshells",
    ]
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


def test_subshell_control_requests_smoke(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    with running_kernel_client(tmp_path, zmq_context) as client:
        create_reply, create_published = client.request(
            "control", "create_subshell_request", {}
        )
        subshell_id = create_reply["content"]["subshell_id"]

        list_reply, list_published = client.request(
            "control", "list_subshell_request", {}
        )
        delete_reply, delete_published = client.request(
            "control",
            "delete_subshell_request",
            {"subshell_id": subshell_id},
        )
        empty_list_reply, empty_list_published = client.request(
            "control", "list_subshell_request", {}
        )

    assert create_reply["header"]["msg_type"] == "create_subshell_reply"
    assert create_reply["content"]["status"] == "ok"
    assert isinstance(subshell_id, str)
    assert subshell_id
    assert list_reply["content"] == {"status": "ok", "subshell_id": [subshell_id]}
    assert delete_reply["content"] == {"status": "ok"}
    assert empty_list_reply["content"] == {"status": "ok", "subshell_id": []}
    for published in (
        create_published,
        list_published,
        delete_published,
        empty_list_published,
    ):
        assert [message["content"]["execution_state"] for message in published] == [
            "busy",
            "idle",
        ]


def test_subshell_execute_request_shares_namespace_and_uses_distinct_thread(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    with running_kernel_client(tmp_path, zmq_context) as client:
        create_reply, _ = client.request("control", "create_subshell_request", {})
        subshell_id = create_reply["content"]["subshell_id"]

        main_reply, main_published = client.request(
            "shell",
            "execute_request",
            {"code": "import threading\nvalue = 6\nprint(threading.get_ident())"},
        )
        main_thread_id = int(
            next(
                message["content"]["text"]
                for message in main_published
                if message["header"]["msg_type"] == "stream"
            ).strip()
        )

        header = send_client_message(
            client.shell,
            str(client.payload["key"]),
            client.session,
            "execute_request",
            {
                "code": "import threading\nprint(value)\nprint(threading.get_ident())",
                "silent": False,
                "store_history": True,
            },
            header_overrides={"subshell_id": subshell_id},
        )
        shell_reply = recv_message(client.shell, str(client.payload["key"]))
        published = recv_iopub_messages_for_parent(
            client.iopub,
            str(client.payload["key"]),
            str(header["msg_id"]),
        )

    assert main_reply["content"]["status"] == "ok"
    assert shell_reply["content"]["status"] == "ok"
    assert shell_reply["parent_header"]["subshell_id"] == subshell_id
    stream_text = "".join(
        message["content"]["text"]
        for message in published
        if message["header"]["msg_type"] == "stream"
    )
    lines = [line for line in stream_text.splitlines() if line]
    assert lines[0] == "6"
    assert int(lines[1]) != main_thread_id


def test_subshell_execute_requests_complete_without_cross_talk(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    with running_kernel_client(tmp_path, zmq_context) as client:
        first_create_reply, _ = client.request("control", "create_subshell_request", {})
        second_create_reply, _ = client.request(
            "control", "create_subshell_request", {}
        )
        first_subshell_id = first_create_reply["content"]["subshell_id"]
        second_subshell_id = second_create_reply["content"]["subshell_id"]

        second_shell = zmq_context.socket(zmq.DEALER)
        second_shell.connect(client.kernel.endpoints.shell)
        second_shell.setsockopt(zmq.RCVTIMEO, 10000)
        time.sleep(0.1)

        first_header = send_client_message(
            client.shell,
            str(client.payload["key"]),
            client.session,
            "execute_request",
            {
                "code": """
import threading
import time

start = time.monotonic()
time.sleep(0.4)
end = time.monotonic()
(threading.get_ident(), start, end)
""",
                "silent": False,
                "store_history": True,
                "allow_stdin": False,
                "user_expressions": {},
                "stop_on_error": True,
            },
            header_overrides={"subshell_id": first_subshell_id},
        )
        second_header = send_client_message(
            second_shell,
            str(client.payload["key"]),
            "smoke-session-2",
            "execute_request",
            {
                "code": """
import threading
import time

start = time.monotonic()
time.sleep(0.4)
end = time.monotonic()
(threading.get_ident(), start, end)
""",
                "silent": False,
                "store_history": True,
                "allow_stdin": False,
                "user_expressions": {},
                "stop_on_error": True,
            },
            header_overrides={"subshell_id": second_subshell_id},
        )

        first_reply = recv_message(client.shell, str(client.payload["key"]))
        second_reply = recv_message(second_shell, str(client.payload["key"]))
        published_by_parent = recv_iopub_messages_for_parents(
            client.iopub,
            str(client.payload["key"]),
            [str(first_header["msg_id"]), str(second_header["msg_id"])],
        )
        first_published = published_by_parent[str(first_header["msg_id"])]
        second_published = published_by_parent[str(second_header["msg_id"])]

        second_shell.close(0)

    assert first_reply["content"]["status"] == "ok"
    assert second_reply["content"]["status"] == "ok"
    assert first_reply["parent_header"]["subshell_id"] == first_subshell_id
    assert second_reply["parent_header"]["subshell_id"] == second_subshell_id

    first_result = ast.literal_eval(
        next(
            message["content"]["data"]["text/plain"]
            for message in first_published
            if message["header"]["msg_type"] == "execute_result"
        )
    )
    second_result = ast.literal_eval(
        next(
            message["content"]["data"]["text/plain"]
            for message in second_published
            if message["header"]["msg_type"] == "execute_result"
        )
    )
    assert first_result[0] != second_result[0]
    assert first_result[2] > first_result[1]
    assert second_result[2] > second_result[1]


def test_subshell_execute_error_remains_isolated_from_concurrent_subshell(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    with running_kernel_client(tmp_path, zmq_context) as client:
        first_create_reply, _ = client.request("control", "create_subshell_request", {})
        second_create_reply, _ = client.request(
            "control", "create_subshell_request", {}
        )
        first_subshell_id = first_create_reply["content"]["subshell_id"]
        second_subshell_id = second_create_reply["content"]["subshell_id"]

        second_shell = zmq_context.socket(zmq.DEALER)
        second_shell.connect(client.kernel.endpoints.shell)
        second_shell.setsockopt(zmq.RCVTIMEO, 10000)
        time.sleep(0.1)

        first_header = send_client_message(
            client.shell,
            str(client.payload["key"]),
            client.session,
            "execute_request",
            {
                "code": """
import threading
import time

time.sleep(0.2)
raise RuntimeError("first boom")
""",
                "silent": False,
                "store_history": True,
                "allow_stdin": False,
                "user_expressions": {},
                "stop_on_error": True,
            },
            header_overrides={"subshell_id": first_subshell_id},
        )
        second_header = send_client_message(
            second_shell,
            str(client.payload["key"]),
            "smoke-session-2",
            "execute_request",
            {
                "code": """
import threading
import time

time.sleep(0.2)
threading.get_ident()
""",
                "silent": False,
                "store_history": True,
                "allow_stdin": False,
                "user_expressions": {},
                "stop_on_error": True,
            },
            header_overrides={"subshell_id": second_subshell_id},
        )

        first_reply = recv_message(client.shell, str(client.payload["key"]))
        second_reply = recv_message(second_shell, str(client.payload["key"]))
        published_by_parent = recv_iopub_messages_for_parents(
            client.iopub,
            str(client.payload["key"]),
            [str(first_header["msg_id"]), str(second_header["msg_id"])],
        )
        first_published = published_by_parent[str(first_header["msg_id"])]
        second_published = published_by_parent[str(second_header["msg_id"])]

        second_shell.close(0)

    assert first_reply["content"]["status"] == "error"
    assert second_reply["content"]["status"] == "ok"
    assert first_reply["parent_header"]["subshell_id"] == first_subshell_id
    assert second_reply["parent_header"]["subshell_id"] == second_subshell_id
    assert first_reply["content"]["ename"] == "RuntimeError"
    assert first_reply["content"]["evalue"] == "first boom"
    second_result = ast.literal_eval(
        next(
            message["content"]["data"]["text/plain"]
            for message in second_published
            if message["header"]["msg_type"] == "execute_result"
        )
    )

    assert [message["header"]["msg_type"] for message in first_published] == [
        "status",
        "execute_input",
        "error",
        "status",
    ]
    assert [message["header"]["msg_type"] for message in second_published] == [
        "status",
        "execute_input",
        "execute_result",
        "status",
    ]
    assert first_published[2]["content"]["ename"] == "RuntimeError"
    assert first_published[2]["content"]["evalue"] == "first boom"
    assert isinstance(second_result, int)


def test_subshells_are_cleared_after_restart(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    with running_kernel_client(tmp_path, zmq_context) as client:
        create_reply, _ = client.request("control", "create_subshell_request", {})
        subshell_id = create_reply["content"]["subshell_id"]

        restart_reply, restart_published = client.request(
            "control",
            "shutdown_request",
            {"restart": True},
        )
        list_reply, list_published = client.request(
            "control", "list_subshell_request", {}
        )
        probe_header = send_client_message(
            client.shell,
            str(client.payload["key"]),
            client.session,
            "execute_request",
            {
                "code": "41 + 1",
                "silent": False,
                "store_history": True,
                "allow_stdin": False,
                "user_expressions": {},
                "stop_on_error": True,
            },
            header_overrides={"subshell_id": subshell_id},
        )
        probe_reply = recv_message(client.shell, str(client.payload["key"]))
        probe_published = recv_iopub_messages_for_parent(
            client.iopub,
            str(client.payload["key"]),
            str(probe_header["msg_id"]),
        )

    assert restart_reply["content"] == {"status": "ok", "restart": True}
    assert [message["header"]["msg_type"] for message in restart_published] == [
        "status",
        "shutdown_reply",
        "status",
    ]

    assert list_reply["content"] == {"status": "ok", "subshell_id": []}
    assert [message["content"]["execution_state"] for message in list_published] == [
        "busy",
        "idle",
    ]

    assert probe_reply["content"]["status"] == "error"
    assert "unknown subshell_id" in probe_reply["content"]["evalue"]
    assert subshell_id in probe_reply["content"]["evalue"]
    assert [message["header"]["msg_type"] for message in probe_published] == [
        "status",
        "execute_input",
        "error",
        "status",
    ]


def test_delete_subshell_while_execute_is_running_waits_for_completion(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    with running_kernel_client(tmp_path, zmq_context) as client:
        create_reply, _ = client.request("control", "create_subshell_request", {})
        subshell_id = create_reply["content"]["subshell_id"]

        delete_control = zmq_context.socket(zmq.DEALER)
        delete_control.connect(client.kernel.endpoints.control)
        delete_control.setsockopt(zmq.RCVTIMEO, 10000)
        delete_iopub = zmq_context.socket(zmq.SUB)
        delete_iopub.setsockopt(zmq.SUBSCRIBE, b"")
        delete_iopub.connect(client.kernel.endpoints.iopub)
        delete_iopub.setsockopt(zmq.RCVTIMEO, 10000)
        time.sleep(0.1)

        execute_header = send_client_message(
            client.shell,
            str(client.payload["key"]),
            client.session,
            "execute_request",
            {
                "code": """
import time
print("started")
time.sleep(0.4)
print("finished")
"done"
""",
                "silent": False,
                "store_history": True,
                "allow_stdin": False,
                "user_expressions": {},
                "stop_on_error": True,
            },
            header_overrides={"subshell_id": subshell_id},
        )
        execute_started = recv_iopub_messages_until_parent_predicate(
            client.iopub,
            str(client.payload["key"]),
            str(execute_header["msg_id"]),
            lambda message: (
                message["header"]["msg_type"] == "stream"
                and "started" in message["content"].get("text", "")
            ),
        )

        delete_header = send_client_message(
            delete_control,
            str(client.payload["key"]),
            "delete-subshell-session",
            "delete_subshell_request",
            {"subshell_id": subshell_id},
        )
        delete_reply = recv_message(delete_control, str(client.payload["key"]))
        delete_published = recv_iopub_messages_for_parent(
            delete_iopub,
            str(client.payload["key"]),
            str(delete_header["msg_id"]),
        )

        execute_reply = recv_message(client.shell, str(client.payload["key"]))
        execute_tail = recv_iopub_messages_for_parent(
            client.iopub,
            str(client.payload["key"]),
            str(execute_header["msg_id"]),
        )
        probe_header = send_client_message(
            client.shell,
            str(client.payload["key"]),
            client.session,
            "execute_request",
            {
                "code": "40 + 2",
                "silent": False,
                "store_history": True,
                "allow_stdin": False,
                "user_expressions": {},
                "stop_on_error": True,
            },
            header_overrides={"subshell_id": subshell_id},
        )
        probe_reply = recv_message(client.shell, str(client.payload["key"]))
        probe_published = recv_iopub_messages_for_parent(
            client.iopub,
            str(client.payload["key"]),
            str(probe_header["msg_id"]),
        )

        delete_control.close(0)
        delete_iopub.close(0)

    execute_messages = execute_started + execute_tail
    execute_stream_text = "".join(
        message["content"]["text"]
        for message in execute_messages
        if message["header"]["msg_type"] == "stream"
    )

    assert execute_reply["content"]["status"] == "ok"
    assert execute_reply["parent_header"]["subshell_id"] == subshell_id
    assert "started" in execute_stream_text
    assert "finished" in execute_stream_text
    assert (
        ast.literal_eval(
            next(
                message["content"]["data"]["text/plain"]
                for message in execute_messages
                if message["header"]["msg_type"] == "execute_result"
            )
        )
        == "done"
    )

    assert delete_reply["content"] == {"status": "ok"}
    assert [message["content"]["execution_state"] for message in delete_published] == [
        "busy",
        "idle",
    ]

    assert probe_reply["content"]["status"] == "error"
    assert "unknown subshell_id" in probe_reply["content"]["evalue"]
    assert subshell_id in probe_reply["content"]["evalue"]
    assert [message["header"]["msg_type"] for message in probe_published] == [
        "status",
        "execute_input",
        "error",
        "status",
    ]


def test_debug_request_debug_info_smoke(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    with running_kernel_client(tmp_path, zmq_context) as client:
        reply, published = client.request(
            "control",
            "debug_request",
            {
                "seq": 1,
                "type": "request",
                "command": "debugInfo",
            },
        )

    assert reply["header"]["msg_type"] == "debug_reply"
    assert reply["content"]["type"] == "response"
    assert reply["content"]["request_seq"] == 1
    assert reply["content"]["success"] is True
    assert reply["content"]["command"] == "debugInfo"
    assert reply["content"]["body"]["isStarted"] is False
    assert reply["content"]["body"]["breakpoints"] == []
    assert reply["content"]["body"]["stoppedThreads"] == []
    assert reply["content"]["body"]["tmpFileSuffix"] == ".py"
    assert [message["content"]["execution_state"] for message in published] == [
        "busy",
        "idle",
    ]


def test_debug_request_initialize_and_breakpoint_bookkeeping_smoke(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    with running_kernel_client(tmp_path, zmq_context) as client:
        init_reply, _ = client.request(
            "control",
            "debug_request",
            {
                "seq": 1,
                "type": "request",
                "command": "initialize",
                "arguments": {
                    "clientID": "test-client",
                    "clientName": "test-client",
                    "adapterID": "",
                },
            },
        )
        attach_reply, _ = client.request(
            "control",
            "debug_request",
            {
                "seq": 2,
                "type": "request",
                "command": "attach",
                "arguments": {},
            },
        )
        dump_reply, _ = client.request(
            "control",
            "debug_request",
            {
                "seq": 3,
                "type": "request",
                "command": "dumpCell",
                "arguments": {"code": "def f():\n    return 42\nf()\n"},
            },
        )
        source_path = dump_reply["content"]["body"]["sourcePath"]
        set_reply, _ = client.request(
            "control",
            "debug_request",
            {
                "seq": 4,
                "type": "request",
                "command": "setBreakpoints",
                "arguments": {
                    "source": {"path": source_path},
                    "breakpoints": [{"line": 2}],
                    "sourceModified": False,
                },
            },
        )
        info_reply, _ = client.request(
            "control",
            "debug_request",
            {
                "seq": 5,
                "type": "request",
                "command": "debugInfo",
            },
        )
        disconnect_reply, _ = client.request(
            "control",
            "debug_request",
            {
                "seq": 6,
                "type": "request",
                "command": "disconnect",
                "arguments": {"restart": False, "terminateDebuggee": True},
            },
        )

    assert init_reply["header"]["msg_type"] == "debug_reply"
    assert init_reply["content"]["success"] is True
    assert init_reply["content"]["command"] == "initialize"
    assert attach_reply["content"]["success"] is True
    assert dump_reply["content"]["success"] is True
    assert source_path.endswith(".py")
    assert set_reply["content"]["success"] is True
    assert set_reply["content"]["body"]["breakpoints"][0]["verified"] is True
    assert info_reply["content"]["body"]["isStarted"] is True, (
        "attach should mark debugger session started"
    )
    assert source_path in {
        item["source"] for item in info_reply["content"]["body"]["breakpoints"]
    }
    assert disconnect_reply["content"]["success"] is True


def test_debug_breakpoint_event_and_variable_queries_smoke(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    code = "x = 1\ny = x + 1\nz = y + 1\n"

    with running_kernel_client(tmp_path, zmq_context) as client:
        dump_reply, _ = client.request(
            "control",
            "debug_request",
            {
                "seq": 1,
                "type": "request",
                "command": "dumpCell",
                "arguments": {"code": code},
            },
        )
        source_path = dump_reply["content"]["body"]["sourcePath"]
        client.request(
            "control",
            "debug_request",
            {
                "seq": 2,
                "type": "request",
                "command": "initialize",
                "arguments": {},
            },
        )
        client.request(
            "control",
            "debug_request",
            {
                "seq": 3,
                "type": "request",
                "command": "attach",
                "arguments": {},
            },
        )
        client.request(
            "control",
            "debug_request",
            {
                "seq": 4,
                "type": "request",
                "command": "setBreakpoints",
                "arguments": {
                    "source": {"path": source_path},
                    "breakpoints": [{"line": 2}],
                    "sourceModified": False,
                },
            },
        )
        client.request(
            "control",
            "debug_request",
            {
                "seq": 5,
                "type": "request",
                "command": "configurationDone",
                "arguments": {},
            },
        )

        execute_reply, published = client.request(
            "shell",
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

        stopped_event = next(
            (
                message
                for message in published
                if message["header"]["msg_type"] == "debug_event"
                and message["content"]["event"] == "stopped"
            ),
            None,
        )
        stack_reply, _ = client.request(
            "control",
            "debug_request",
            {
                "seq": 6,
                "type": "request",
                "command": "stackTrace",
                "arguments": {"threadId": 1},
            },
        )
        frame_id = None
        stack_frames = stack_reply["content"]["body"].get("stackFrames", [])
        scopes_reply = None
        variables_reply = None
        if stack_frames:
            frame_id = stack_frames[0]["id"]
            scopes_reply, _ = client.request(
                "control",
                "debug_request",
                {
                    "seq": 7,
                    "type": "request",
                    "command": "scopes",
                    "arguments": {"frameId": frame_id},
                },
            )
            locals_ref = scopes_reply["content"]["body"]["scopes"][0][
                "variablesReference"
            ]
            variables_reply, _ = client.request(
                "control",
                "debug_request",
                {
                    "seq": 8,
                    "type": "request",
                    "command": "variables",
                    "arguments": {"variablesReference": locals_ref},
                },
            )

    assert execute_reply["content"]["status"] == "ok"
    if stopped_event is not None:
        assert stopped_event["content"]["body"]["reason"] == "breakpoint"
        assert (
            stack_reply["content"]["body"]["stackFrames"][0]["source"]["path"]
            == source_path
        )
        assert stack_reply["content"]["body"]["stackFrames"][0]["line"] == 2
        variables = {
            item["name"]: item["value"]
            for item in variables_reply["content"]["body"]["variables"]  # type: ignore[index]
        }
        assert variables["x"] == "1"
        assert variables["y"] == "2"
    else:
        # Keep this smoke tolerant: this test exercises the shallow breakpoint
        # path, while stronger live stepping coverage now lives in the dedicated
        # live debugpy smoke below.
        assert stack_reply["content"]["success"] is True
        assert (
            any(message["header"]["msg_type"] == "debug_event" for message in published)
            or stack_reply["content"]["body"].get("stackFrames", []) == []
        )


def test_live_debugpy_continue_next_stepin_stepout_smoke(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    code = (
        "def inner():\n"
        "    value = 1\n"
        "    value += 1\n"
        "    return value\n"
        "\n"
        "result = inner()\n"
        "result += 10\n"
        "result\n"
    )

    with running_kernel_client(tmp_path, zmq_context) as client:
        dump_reply = client.debug_request_and_drain(1, "dumpCell", {"code": code})
        source_path = dump_reply["content"]["body"]["sourcePath"]

        init_reply = client.debug_request_and_drain(
            2,
            "initialize",
            {
                "clientID": "test-client",
                "clientName": "test-client",
                "adapterID": "python",
            },
        )
        attach_reply = client.debug_request_and_drain(3, "attach", {})
        set_reply = client.debug_request_and_drain(
            4,
            "setBreakpoints",
            {
                "source": {"path": source_path},
                "breakpoints": [{"line": 6}],
                "sourceModified": False,
            },
        )
        config_reply = client.debug_request_and_drain(5, "configurationDone", {})

        assert init_reply["content"]["success"] is True
        assert attach_reply["content"]["success"] is True
        assert set_reply["content"]["success"] is True
        assert config_reply["content"]["success"] is True

        shell_header = send_client_message(
            client.shell,
            str(client.payload["key"]),
            client.session,
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
        first_stop_messages = recv_iopub_messages_until_parent_predicate(
            client.iopub,
            str(client.payload["key"]),
            str(shell_header["msg_id"]),
            lambda message: (
                message["header"]["msg_type"] == "debug_event"
                and message["content"]["event"] == "stopped"
            ),
        )
        first_stopped = next(
            message
            for message in first_stop_messages
            if message["header"]["msg_type"] == "debug_event"
            and message["content"]["event"] == "stopped"
        )
        stopped_thread_id = first_stopped["content"]["body"].get("threadId", 1)
        frame, variables = client.top_frame_and_locals(6, int(stopped_thread_id))
        assert frame["source"]["path"] == source_path
        assert frame["line"] == 6
        assert isinstance(variables, list)

        continue_reply = client.debug_request_and_drain(
            10, "continue", {"threadId": int(stopped_thread_id)}
        )
        assert continue_reply["content"]["success"] is True
        execute_reply = recv_message(client.shell, str(client.payload["key"]))
        execute_tail = recv_iopub_messages_for_parent(
            client.iopub,
            str(client.payload["key"]),
            str(shell_header["msg_id"]),
        )
        assert execute_reply["content"]["status"] == "ok"
        assert any(
            message["header"]["msg_type"] == "execute_result"
            for message in execute_tail
        )

        shell_header = send_client_message(
            client.shell,
            str(client.payload["key"]),
            client.session,
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
        second_stop_messages = recv_iopub_messages_until_parent_predicate(
            client.iopub,
            str(client.payload["key"]),
            str(shell_header["msg_id"]),
            lambda message: (
                message["header"]["msg_type"] == "debug_event"
                and message["content"]["event"] == "stopped"
            ),
        )
        second_stopped = next(
            message
            for message in second_stop_messages
            if message["header"]["msg_type"] == "debug_event"
            and message["content"]["event"] == "stopped"
        )
        thread_id = int(second_stopped["content"]["body"].get("threadId", 1))

        frame, variables = client.top_frame_and_locals(20, thread_id)
        assert frame["source"]["path"] == source_path
        assert frame["line"] == 6
        assert isinstance(variables, list)

        step_in_reply = client.debug_request_and_drain(
            24, "stepIn", {"threadId": thread_id}
        )
        assert step_in_reply["content"]["success"] is True
        step_in_messages = recv_iopub_messages_until_parent_predicate(
            client.iopub,
            str(client.payload["key"]),
            str(shell_header["msg_id"]),
            lambda message: (
                message["header"]["msg_type"] == "debug_event"
                and message["content"]["event"] == "stopped"
            ),
        )
        thread_id = int(
            next(
                message["content"]["body"].get("threadId", 1)
                for message in step_in_messages
                if message["header"]["msg_type"] == "debug_event"
                and message["content"]["event"] == "stopped"
            )
        )
        frame, _ = client.top_frame_and_locals(25, thread_id)
        assert frame["source"]["path"] == source_path
        assert frame["line"] == 2

        next_reply = client.debug_request_and_drain(29, "next", {"threadId": thread_id})
        assert next_reply["content"]["success"] is True
        next_messages = recv_iopub_messages_until_parent_predicate(
            client.iopub,
            str(client.payload["key"]),
            str(shell_header["msg_id"]),
            lambda message: (
                message["header"]["msg_type"] == "debug_event"
                and message["content"]["event"] == "stopped"
            ),
        )
        thread_id = int(
            next(
                message["content"]["body"].get("threadId", 1)
                for message in next_messages
                if message["header"]["msg_type"] == "debug_event"
                and message["content"]["event"] == "stopped"
            )
        )
        frame, variables = client.top_frame_and_locals(30, thread_id)
        assert frame["source"]["path"] == source_path
        assert frame["line"] == 3
        assert {item["name"]: item["value"] for item in variables}["value"] == "1"

        step_out_reply = client.debug_request_and_drain(
            34, "stepOut", {"threadId": thread_id}
        )
        assert step_out_reply["content"]["success"] is True
        step_out_messages = recv_iopub_messages_until_parent_predicate(
            client.iopub,
            str(client.payload["key"]),
            str(shell_header["msg_id"]),
            lambda message: (
                message["header"]["msg_type"] == "debug_event"
                and message["content"]["event"] == "stopped"
            ),
        )
        thread_id = int(
            next(
                message["content"]["body"].get("threadId", 1)
                for message in step_out_messages
                if message["header"]["msg_type"] == "debug_event"
                and message["content"]["event"] == "stopped"
            )
        )
        frame, _ = client.top_frame_and_locals(35, thread_id)
        assert frame["source"]["path"] == source_path
        assert frame["line"] == 6

        continue_reply = client.debug_request_and_drain(
            39, "continue", {"threadId": thread_id}
        )
        assert continue_reply["content"]["success"] is True
        execute_reply = recv_message(client.shell, str(client.payload["key"]))
        execute_tail = recv_iopub_messages_for_parent(
            client.iopub,
            str(client.payload["key"]),
            str(shell_header["msg_id"]),
        )
        assert execute_reply["content"]["status"] == "ok"
        assert any(
            message["header"]["msg_type"] == "execute_result"
            for message in execute_tail
        )

        disconnect_reply = client.debug_request_and_drain(
            43,
            "disconnect",
            {"restart": False, "terminateDebuggee": True},
        )
        assert disconnect_reply["content"]["success"] is True


def test_live_debugpy_pause_in_subshell_smoke(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    code = (
        "def runner():\n"
        "    import time\n"
        "    total = 0\n"
        "    for _ in range(200):\n"
        "        total += 1\n"
        "        time.sleep(0.01)\n"
        "    return total\n"
        "\n"
        "result = runner()\n"
        "result\n"
    )

    with running_kernel_client(tmp_path, zmq_context) as client:
        create_reply, _ = client.request("control", "create_subshell_request", {})
        subshell_id = create_reply["content"]["subshell_id"]

        dump_reply = client.debug_request_and_drain(1, "dumpCell", {"code": code})
        source_path = dump_reply["content"]["body"]["sourcePath"]

        init_reply = client.debug_request_and_drain(
            2,
            "initialize",
            {
                "clientID": "test-client",
                "clientName": "test-client",
                "adapterID": "python",
            },
        )
        attach_reply = client.debug_request_and_drain(3, "attach", {})
        set_reply = client.debug_request_and_drain(
            4,
            "setBreakpoints",
            {
                "source": {"path": source_path},
                "breakpoints": [{"line": 2}],
                "sourceModified": False,
            },
        )
        config_reply = client.debug_request_and_drain(5, "configurationDone", {})

        assert init_reply["content"]["success"] is True
        assert attach_reply["content"]["success"] is True
        assert set_reply["content"]["success"] is True
        assert config_reply["content"]["success"] is True

        shell_header = send_client_message(
            client.shell,
            str(client.payload["key"]),
            client.session,
            "execute_request",
            {
                "code": code,
                "silent": False,
                "store_history": True,
                "allow_stdin": False,
                "user_expressions": {},
                "stop_on_error": True,
            },
            header_overrides={"subshell_id": subshell_id},
        )
        first_stop_messages = recv_iopub_messages_until_parent_predicate(
            client.iopub,
            str(client.payload["key"]),
            str(shell_header["msg_id"]),
            lambda message: (
                message["header"]["msg_type"] == "debug_event"
                and message["content"]["event"] == "stopped"
            ),
        )
        first_stopped = next(
            message
            for message in first_stop_messages
            if message["header"]["msg_type"] == "debug_event"
            and message["content"]["event"] == "stopped"
        )
        assert first_stopped["parent_header"]["subshell_id"] == subshell_id
        thread_id = int(first_stopped["content"]["body"].get("threadId", 1))
        frame, _ = client.top_frame_and_locals(6, thread_id)
        assert frame["source"]["path"] == source_path
        assert frame["line"] == 2

        continue_reply = client.debug_request_and_drain(
            10, "continue", {"threadId": thread_id}
        )
        assert continue_reply["content"]["success"] is True

        time.sleep(0.15)

        pause_reply = client.debug_request_and_drain(
            11, "pause", {"threadId": thread_id}
        )
        assert pause_reply["content"]["success"] is True
        pause_messages = recv_iopub_messages_until_parent_predicate(
            client.iopub,
            str(client.payload["key"]),
            str(shell_header["msg_id"]),
            lambda message: (
                message["header"]["msg_type"] == "debug_event"
                and message["content"]["event"] == "stopped"
                and message["content"]["body"].get("reason") == "pause"
            ),
        )
        pause_stopped = next(
            message
            for message in pause_messages
            if message["header"]["msg_type"] == "debug_event"
            and message["content"]["event"] == "stopped"
            and message["content"]["body"].get("reason") == "pause"
        )
        assert pause_stopped["parent_header"]["subshell_id"] == subshell_id
        paused_thread_id = int(pause_stopped["content"]["body"].get("threadId", 1))
        assert paused_thread_id == thread_id

        frame, _ = client.top_frame_and_locals(20, paused_thread_id)
        assert frame["source"]["path"] == source_path
        assert frame["line"] in {4, 5, 6}

        resume_reply = client.debug_request_and_drain(
            24, "continue", {"threadId": paused_thread_id}
        )
        assert resume_reply["content"]["success"] is True
        execute_reply = recv_message(client.shell, str(client.payload["key"]))
        execute_tail = recv_iopub_messages_for_parent(
            client.iopub,
            str(client.payload["key"]),
            str(shell_header["msg_id"]),
        )
        assert execute_reply["content"]["status"] == "ok"
        assert execute_reply["parent_header"]["subshell_id"] == subshell_id
        assert any(
            message["header"]["msg_type"] == "execute_result"
            for message in execute_tail
        )

        disconnect_reply = client.debug_request_and_drain(
            28,
            "disconnect",
            {"restart": False, "terminateDebuggee": True},
        )
        assert disconnect_reply["content"]["success"] is True


def test_interrupt_request_stops_running_debug_session_in_subshell(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    code = (
        "def runner():\n"
        "    import time\n"
        "    total = 0\n"
        "    for _ in range(300):\n"
        "        total += 1\n"
        "        time.sleep(0.01)\n"
        "    return total\n"
        "\n"
        "runner()\n"
    )

    with running_kernel_client(tmp_path, zmq_context) as client:
        create_reply, _ = client.request("control", "create_subshell_request", {})
        subshell_id = create_reply["content"]["subshell_id"]

        dump_reply = client.debug_request_and_drain(1, "dumpCell", {"code": code})
        assert dump_reply["content"]["success"] is True

        init_reply = client.debug_request_and_drain(
            2,
            "initialize",
            {
                "clientID": "test-client",
                "clientName": "test-client",
                "adapterID": "python",
            },
        )
        attach_reply = client.debug_request_and_drain(3, "attach", {})
        config_reply = client.debug_request_and_drain(4, "configurationDone", {})

        assert init_reply["content"]["success"] is True
        assert attach_reply["content"]["success"] is True
        assert config_reply["content"]["success"] is True

        execute_header = send_client_message(
            client.shell,
            str(client.payload["key"]),
            client.session,
            "execute_request",
            {
                "code": code,
                "silent": False,
                "store_history": True,
                "allow_stdin": False,
                "user_expressions": {},
                "stop_on_error": True,
            },
            header_overrides={"subshell_id": subshell_id},
        )
        time.sleep(0.2)

        interrupt_reply, interrupt_published = client.request(
            "control", "interrupt_request", {}
        )
        execute_reply = recv_message(client.shell, str(client.payload["key"]))
        execute_published = recv_iopub_messages_for_parent(
            client.iopub,
            str(client.payload["key"]),
            str(execute_header["msg_id"]),
        )
        probe_header = send_client_message(
            client.shell,
            str(client.payload["key"]),
            client.session,
            "execute_request",
            {
                "code": "40 + 2",
                "silent": False,
                "store_history": True,
                "allow_stdin": False,
                "user_expressions": {},
                "stop_on_error": True,
            },
            header_overrides={"subshell_id": subshell_id},
        )
        probe_reply = recv_message(client.shell, str(client.payload["key"]))
        probe_published = recv_iopub_messages_for_parent(
            client.iopub,
            str(client.payload["key"]),
            str(probe_header["msg_id"]),
        )

    assert interrupt_reply["header"]["msg_type"] == "interrupt_reply"
    assert interrupt_reply["content"]["status"] == "ok"
    assert [
        message["content"]["execution_state"] for message in interrupt_published
    ] == [
        "busy",
        "idle",
    ]

    assert execute_reply["content"]["status"] == "error"
    assert execute_reply["content"]["ename"] == "KeyboardInterrupt"
    assert execute_reply["parent_header"]["subshell_id"] == subshell_id
    assert any(
        message["header"]["msg_type"] == "error"
        and message["content"]["ename"] == "KeyboardInterrupt"
        for message in execute_published
    )
    assert execute_published[-1]["header"]["msg_type"] == "status"
    assert execute_published[-1]["content"]["execution_state"] == "idle"

    assert probe_reply["content"]["status"] == "ok"
    assert probe_reply["parent_header"]["subshell_id"] == subshell_id
    assert [message["header"]["msg_type"] for message in probe_published] == [
        "status",
        "execute_input",
        "execute_result",
        "status",
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


def test_shutdown_request_stops_kernel_with_running_subshell_execution(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    with running_kernel_client(tmp_path, zmq_context) as client:
        create_reply, _ = client.request("control", "create_subshell_request", {})
        subshell_id = create_reply["content"]["subshell_id"]

        execute_header = send_client_message(
            client.shell,
            str(client.payload["key"]),
            client.session,
            "execute_request",
            {
                "code": """
import time
print("started")
time.sleep(30)
""",
                "silent": False,
                "store_history": True,
                "allow_stdin": False,
                "user_expressions": {},
                "stop_on_error": True,
            },
            header_overrides={"subshell_id": subshell_id},
        )
        started_messages = recv_iopub_messages_until_parent_predicate(
            client.iopub,
            str(client.payload["key"]),
            str(execute_header["msg_id"]),
            lambda message: (
                message["header"]["msg_type"] == "stream"
                and "started" in message["content"].get("text", "")
            ),
        )
        assert any(
            message["header"]["msg_type"] == "stream"
            and "started" in message["content"].get("text", "")
            for message in started_messages
        )

        shutdown_reply, shutdown_published = client.request(
            "control",
            "shutdown_request",
            {"restart": False},
        )
        wait_for_kernel_stop(client.kernel)

    assert shutdown_reply["header"]["msg_type"] == "shutdown_reply"
    assert shutdown_reply["content"] == {"status": "ok", "restart": False}
    assert [message["header"]["msg_type"] for message in shutdown_published] == [
        "status",
        "shutdown_reply",
        "status",
    ]
    assert shutdown_published[1]["content"] == {"status": "ok", "restart": False}


def test_shutdown_request_stops_kernel_with_active_debug_session_in_subshell(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    code = 'import time\nprint("started")\ntime.sleep(30)\n'

    with running_kernel_client(tmp_path, zmq_context) as client:
        create_reply, _ = client.request("control", "create_subshell_request", {})
        subshell_id = create_reply["content"]["subshell_id"]

        dump_reply = client.debug_request_and_drain(1, "dumpCell", {"code": code})
        assert dump_reply["content"]["success"] is True

        init_reply = client.debug_request_and_drain(
            2,
            "initialize",
            {
                "clientID": "test-client",
                "clientName": "test-client",
                "adapterID": "python",
            },
        )
        attach_reply = client.debug_request_and_drain(3, "attach", {})
        config_reply = client.debug_request_and_drain(4, "configurationDone", {})

        assert init_reply["content"]["success"] is True
        assert attach_reply["content"]["success"] is True
        assert config_reply["content"]["success"] is True

        execute_header = send_client_message(
            client.shell,
            str(client.payload["key"]),
            client.session,
            "execute_request",
            {
                "code": code,
                "silent": False,
                "store_history": True,
                "allow_stdin": False,
                "user_expressions": {},
                "stop_on_error": True,
            },
            header_overrides={"subshell_id": subshell_id},
        )
        started_messages = recv_iopub_messages_until_parent_predicate(
            client.iopub,
            str(client.payload["key"]),
            str(execute_header["msg_id"]),
            lambda message: (
                message["header"]["msg_type"] == "stream"
                and "started" in message["content"].get("text", "")
            ),
        )
        assert any(
            message["header"]["msg_type"] == "stream"
            and "started" in message["content"].get("text", "")
            for message in started_messages
        )

        shutdown_reply, shutdown_published = client.request(
            "control",
            "shutdown_request",
            {"restart": False},
        )
        wait_for_kernel_stop(client.kernel)

    assert shutdown_reply["header"]["msg_type"] == "shutdown_reply"
    assert shutdown_reply["content"] == {"status": "ok", "restart": False}
    assert [message["header"]["msg_type"] for message in shutdown_published] == [
        "status",
        "shutdown_reply",
        "status",
    ]
    assert shutdown_published[1]["content"] == {"status": "ok", "restart": False}


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
