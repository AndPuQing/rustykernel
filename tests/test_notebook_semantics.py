from __future__ import annotations

import ast
from pathlib import Path

import pytest
import zmq

from tests.support import running_kernel_client


@pytest.fixture()
def zmq_context() -> zmq.Context:
    context = zmq.Context()
    yield context
    context.term()


def execute_request(code: str) -> dict[str, object]:
    return {
        "code": code,
        "silent": False,
        "store_history": True,
        "allow_stdin": False,
        "user_expressions": {},
        "stop_on_error": True,
    }


def test_get_ipython_imports_and_magic_registry(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    with running_kernel_client(tmp_path, zmq_context) as client:
        reply, published = client.request(
            "shell",
            "execute_request",
            execute_request(
                """
from IPython import get_ipython as root_get_ipython
from IPython.core.getipython import get_ipython as core_get_ipython

ip = get_ipython()
{
    "root": root_get_ipython() is ip,
    "core": core_get_ipython() is ip,
    "formatter": hasattr(ip, "display_formatter"),
    "line_magics": sorted(ip.magics_manager.magics["line"]),
    "cell_magics": sorted(ip.magics_manager.magics["cell"]),
}
"""
            ),
        )

    assert reply["content"]["status"] == "ok"
    assert [message["header"]["msg_type"] for message in published] == [
        "status",
        "execute_input",
        "execute_result",
        "status",
    ]

    result = ast.literal_eval(published[2]["content"]["data"]["text/plain"])
    assert result["root"] is True
    assert result["core"] is True
    assert result["formatter"] is True
    assert "autoawait" in result["line_magics"]
    assert "matplotlib" in result["line_magics"]
    assert "timeit" in result["line_magics"]
    assert "time" in result["cell_magics"]
    assert "timeit" in result["cell_magics"]


def test_line_magics_support_pwd_and_cd(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    with running_kernel_client(tmp_path, zmq_context) as client:
        reply, published = client.request(
            "shell",
            "execute_request",
            execute_request(
                f"""
%cd {tmp_path}
%pwd
"""
            ),
        )

    assert reply["content"]["status"] == "ok"
    assert [message["header"]["msg_type"] for message in published] == [
        "status",
        "execute_input",
        "stream",
        "execute_result",
        "status",
    ]
    assert str(tmp_path) in published[2]["content"]["text"]
    assert ast.literal_eval(published[3]["content"]["data"]["text/plain"]) == str(
        tmp_path
    )


def test_matplotlib_inline_magic_is_accepted(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    with running_kernel_client(tmp_path, zmq_context) as client:
        reply, published = client.request(
            "shell",
            "execute_request",
            execute_request(
                """
%matplotlib inline
"ok"
"""
            ),
        )

    assert reply["content"]["status"] == "ok"
    assert [message["header"]["msg_type"] for message in published] == [
        "status",
        "execute_input",
        "execute_result",
        "status",
    ]
    assert ast.literal_eval(published[2]["content"]["data"]["text/plain"]) == "ok"


def test_cell_magic_time_executes_cell_and_emits_timing(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    with running_kernel_client(tmp_path, zmq_context) as client:
        reply, published = client.request(
            "shell",
            "execute_request",
            execute_request(
                """
%%time
value = 40
value + 2
"""
            ),
        )

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
    assert "Wall time:" in "".join(
        message["content"]["text"] for message in stream_messages
    )
    assert published[-2]["content"]["data"]["text/plain"] == "42"


def test_timeit_magics_emit_timing_summary(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    with running_kernel_client(tmp_path, zmq_context) as client:
        line_reply, line_published = client.request(
            "shell",
            "execute_request",
            execute_request("%timeit -n1 -r1 value = 1 + 1"),
        )
        cell_reply, cell_published = client.request(
            "shell",
            "execute_request",
            execute_request(
                """
%%timeit -n1 -r1
value = 40 + 2
"""
            ),
        )

    assert line_reply["content"]["status"] == "ok"
    assert [message["header"]["msg_type"] for message in line_published] == [
        "status",
        "execute_input",
        "stream",
        "status",
    ]
    assert "per loop" in line_published[2]["content"]["text"]

    assert cell_reply["content"]["status"] == "ok"
    assert [message["header"]["msg_type"] for message in cell_published] == [
        "status",
        "execute_input",
        "stream",
        "status",
    ]
    assert "per loop" in cell_published[2]["content"]["text"]


def test_autoawait_supports_top_level_await_and_can_be_disabled(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    with running_kernel_client(tmp_path, zmq_context) as client:
        async_reply, async_published = client.request(
            "shell",
            "execute_request",
            execute_request(
                """
import asyncio
await asyncio.sleep(0.01)
41 + 1
"""
            ),
        )
        disable_reply, disable_published = client.request(
            "shell",
            "execute_request",
            execute_request(
                """
%autoawait off
get_ipython().autoawait
"""
            ),
        )
        disabled_await_reply, disabled_await_published = client.request(
            "shell",
            "execute_request",
            execute_request(
                """
import asyncio
await asyncio.sleep(0)
"""
            ),
        )

    assert async_reply["content"]["status"] == "ok"
    assert [message["header"]["msg_type"] for message in async_published] == [
        "status",
        "execute_input",
        "execute_result",
        "status",
    ]
    assert async_published[2]["content"]["data"]["text/plain"] == "42"

    assert disable_reply["content"]["status"] == "ok"
    assert [message["header"]["msg_type"] for message in disable_published] == [
        "status",
        "execute_input",
        "execute_result",
        "status",
    ]
    assert (
        ast.literal_eval(disable_published[2]["content"]["data"]["text/plain"]) is False
    )

    assert disabled_await_reply["content"]["status"] == "error"
    assert disabled_await_reply["content"]["ename"] == "SyntaxError"
    assert [message["header"]["msg_type"] for message in disabled_await_published] in [
        ["status", "execute_input", "error", "status"],
        ["status", "execute_input", "stream", "error", "status"],
    ]


def test_set_next_input_is_returned_once_in_execute_reply_payload(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    with running_kernel_client(tmp_path, zmq_context) as client:
        reply, published = client.request(
            "shell",
            "execute_request",
            execute_request(
                """
ip = get_ipython()
for _ in range(3):
    ip.set_next_input("Hello There")
"""
            ),
        )

    assert reply["content"]["status"] == "ok"
    assert reply["content"]["payload"] == [
        {
            "source": "set_next_input",
            "text": "Hello There",
            "replace": False,
        }
    ]
    assert [message["header"]["msg_type"] for message in published] == [
        "status",
        "execute_input",
        "status",
    ]


def test_clear_output_publishes_iopub_event(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    with running_kernel_client(tmp_path, zmq_context) as client:
        reply, published = client.request(
            "shell",
            "execute_request",
            execute_request(
                """
from IPython.display import clear_output, display

display("before")
clear_output(wait=True)
display("after")
"""
            ),
        )

    assert reply["content"]["status"] == "ok"
    assert [message["header"]["msg_type"] for message in published] == [
        "status",
        "execute_input",
        "display_data",
        "clear_output",
        "display_data",
        "status",
    ]
    assert published[2]["content"]["data"]["text/plain"] == "'before'"
    assert published[3]["content"] == {"wait": True}
    assert published[4]["content"]["data"]["text/plain"] == "'after'"


def test_ipython_display_objects_publish_expected_mimebundles(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    with running_kernel_client(tmp_path, zmq_context) as client:
        reply, published = client.request(
            "shell",
            "execute_request",
            execute_request(
                """
from IPython.display import HTML, Markdown, JSON, Javascript, Latex, SVG, Pretty, display

display(
    HTML("<b>bold</b>"),
    Markdown("**md**"),
    JSON({"answer": 42}),
    Javascript("console.log('ok')"),
    Latex("$x^2$"),
    SVG("<svg><circle r='2'/></svg>"),
    Pretty("plain text"),
)
"""
            ),
        )

    assert reply["content"]["status"] == "ok"
    assert [message["header"]["msg_type"] for message in published] == [
        "status",
        "execute_input",
        "display_data",
        "display_data",
        "display_data",
        "display_data",
        "display_data",
        "display_data",
        "display_data",
        "status",
    ]
    assert published[2]["content"]["data"]["text/html"] == "<b>bold</b>"
    assert published[3]["content"]["data"]["text/markdown"] == "**md**"
    assert published[4]["content"]["data"]["application/json"] == {"answer": 42}
    assert (
        published[5]["content"]["data"]["application/javascript"] == "console.log('ok')"
    )
    assert published[6]["content"]["data"]["text/latex"] == "$x^2$"
    assert (
        published[7]["content"]["data"]["image/svg+xml"] == '<svg><circle r="2"/></svg>'
    )
    assert published[8]["content"]["data"]["text/plain"] == "plain text"


def test_display_raw_mimebundle_and_update_display_raw(
    tmp_path: Path, zmq_context: zmq.Context
) -> None:
    with running_kernel_client(tmp_path, zmq_context) as client:
        reply, published = client.request(
            "shell",
            "execute_request",
            execute_request(
                """
from IPython.display import display, update_display

display({"text/plain": "raw", "text/html": "<i>raw</i>"}, raw=True)
handle = display({"text/plain": "before", "text/html": "<b>before</b>"}, raw=True, display_id=True)
update_display({"text/plain": "after", "text/html": "<b>after</b>"}, raw=True, display_id=handle.display_id)
"""
            ),
        )

    assert reply["content"]["status"] == "ok"
    assert [message["header"]["msg_type"] for message in published] == [
        "status",
        "execute_input",
        "display_data",
        "display_data",
        "update_display_data",
        "status",
    ]
    assert published[2]["content"]["data"] == {
        "text/plain": "raw",
        "text/html": "<i>raw</i>",
    }
    display_id = published[3]["content"]["transient"]["display_id"]
    assert published[3]["content"]["data"] == {
        "text/plain": "before",
        "text/html": "<b>before</b>",
    }
    assert published[4]["content"]["transient"]["display_id"] == display_id
    assert published[4]["content"]["data"] == {
        "text/plain": "after",
        "text/html": "<b>after</b>",
    }
