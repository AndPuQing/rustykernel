from __future__ import annotations

# ruff: noqa: E402

import argparse
import json
import statistics
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import zmq

from tests.compat_harness import (
    KernelCommand,
    client_request,
    recv_iopub_messages_for_parent,
    recv_message,
    running_kernel,
    sign_message,
)


@dataclass(frozen=True)
class MetricDefinition:
    name: str
    unit: str


METRICS: list[MetricDefinition] = [
    MetricDefinition("startup_ready", "ms"),
    MetricDefinition("empty_execute", "ms"),
    MetricDefinition("simple_execute", "ms"),
    MetricDefinition("arith_loop_execute", "ms"),
    MetricDefinition("large_result_execute", "ms"),
    MetricDefinition("rich_display_execute", "ms"),
    MetricDefinition("exception_execute", "ms"),
    MetricDefinition("syntax_error_execute", "ms"),
    MetricDefinition("matplotlib_execute", "ms"),
    MetricDefinition("complete_request", "ms"),
    MetricDefinition("inspect_request", "ms"),
    MetricDefinition("history_request", "ms"),
    MetricDefinition("repeated_small_total", "ms"),
    MetricDefinition("repeated_small_avg", "ms/op"),
    MetricDefinition("repeated_stateful_total", "ms"),
    MetricDefinition("repeated_stateful_avg", "ms/op"),
    MetricDefinition("stream_python", "ms"),
    MetricDefinition("stream_python_msgs", "count"),
    MetricDefinition("stream_fd", "ms"),
    MetricDefinition("stream_fd_msgs", "count"),
    MetricDefinition("stream_subprocess", "ms"),
    MetricDefinition("stream_subprocess_msgs", "count"),
    MetricDefinition("mixed_stream", "ms"),
    MetricDefinition("mixed_stream_msgs", "count"),
    MetricDefinition("interrupt_latency", "ms"),
]


def kernel_commands(python: str) -> tuple[KernelCommand, KernelCommand]:
    return (
        KernelCommand(name="rustykernel", argv=[python, "-m", "rustykernel", "-f"]),
        KernelCommand(
            name="ipykernel", argv=[python, "-m", "ipykernel_launcher", "-f"]
        ),
    )


def median_ms(samples: list[float]) -> float:
    return statistics.median(samples) * 1000.0


def execute_roundtrip_ms(kernel, code: str, repeat: int, warmup: int = 0) -> float:
    for _ in range(warmup):
        kernel.execute(code)
    samples: list[float] = []
    for _ in range(repeat):
        start = time.perf_counter()
        kernel.execute(code)
        samples.append(time.perf_counter() - start)
    return median_ms(samples)


def request_roundtrip_ms(
    kernel,
    msg_type: str,
    content: dict[str, object],
    repeat: int,
    warmup: int = 0,
) -> float:
    for _ in range(warmup):
        kernel.request(msg_type, content)
    samples: list[float] = []
    for _ in range(repeat):
        start = time.perf_counter()
        kernel.request(msg_type, content)
        samples.append(time.perf_counter() - start)
    return median_ms(samples)


def repeated_small_exec(kernel, iterations: int) -> tuple[float, float]:
    start = time.perf_counter()
    for i in range(iterations):
        kernel.execute(f"value = {i}\\nvalue + 1")
    total_ms = (time.perf_counter() - start) * 1000.0
    return total_ms, total_ms / iterations


def repeated_stateful_exec(kernel, iterations: int) -> tuple[float, float]:
    kernel.execute("counter = 0")
    start = time.perf_counter()
    for _ in range(iterations):
        kernel.execute("counter += 1\\ncounter")
    total_ms = (time.perf_counter() - start) * 1000.0
    return total_ms, total_ms / iterations


def execute_and_count_streams(kernel, code: str) -> tuple[float, int]:
    start = time.perf_counter()
    _, messages = kernel.execute(code)
    elapsed_ms = (time.perf_counter() - start) * 1000.0
    count = sum(1 for message in messages if message["header"]["msg_type"] == "stream")
    return elapsed_ms, count


def interrupt_latency_ms(kernel) -> float:
    execute_header, execute_frames = client_request(
        "bench-session",
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
        "bench-session", "interrupt_request", {}
    )
    interrupt_signature = sign_message(str(kernel.payload["key"]), interrupt_frames)
    start = time.perf_counter()
    kernel.control.send_multipart(
        [b"<IDS|MSG>", interrupt_signature, *interrupt_frames]
    )
    recv_message(kernel.control, str(kernel.payload["key"]))
    recv_iopub_messages_for_parent(
        kernel.iopub,
        str(kernel.payload["key"]),
        str(interrupt_header["msg_id"]),
        timeout_s=10.0,
    )
    recv_message(kernel.shell, str(kernel.payload["key"]))
    recv_iopub_messages_for_parent(
        kernel.iopub,
        str(kernel.payload["key"]),
        str(execute_header["msg_id"]),
        timeout_s=10.0,
    )
    return (time.perf_counter() - start) * 1000.0


def benchmark_kernel(
    command: KernelCommand,
    *,
    zmq_context: zmq.Context,
    temp_dir: Path,
    execute_repeat: int,
    request_repeat: int,
    execute_warmup: int,
    request_warmup: int,
    iterations: int,
    stream_lines: int,
    large_result_bytes: int,
) -> dict[str, float]:
    connection_file = temp_dir / f"{command.name}-connection.json"
    metrics: dict[str, float] = {}

    startup_begin = time.perf_counter()
    with running_kernel(command, connection_file, zmq_context) as kernel:
        metrics["startup_ready"] = (time.perf_counter() - startup_begin) * 1000.0

        kernel.execute("pass")
        kernel.execute("text = 'hello world'\nvalue = 42")

        metrics["empty_execute"] = execute_roundtrip_ms(
            kernel, "", execute_repeat, warmup=execute_warmup
        )
        metrics["simple_execute"] = execute_roundtrip_ms(
            kernel, "1 + 1", execute_repeat, warmup=execute_warmup
        )
        metrics["arith_loop_execute"] = execute_roundtrip_ms(
            kernel,
            "sum(i * i for i in range(5000))",
            execute_repeat,
            warmup=execute_warmup,
        )
        metrics["large_result_execute"] = execute_roundtrip_ms(
            kernel,
            repr("x" * large_result_bytes),
            execute_repeat,
            warmup=execute_warmup,
        )
        metrics["rich_display_execute"] = execute_roundtrip_ms(
            kernel,
            "from IPython.display import HTML, JSON, display\n"
            "display(HTML('<b>hi</b>'))\n"
            "display(JSON({'a': 1, 'b': [1, 2, 3]}))\n"
            "'done'",
            execute_repeat,
            warmup=execute_warmup,
        )
        metrics["exception_execute"] = execute_roundtrip_ms(
            kernel,
            "def outer():\n    def inner():\n        1 / 0\n    inner()\nouter()",
            execute_repeat,
            warmup=execute_warmup,
        )
        metrics["syntax_error_execute"] = execute_roundtrip_ms(
            kernel,
            "for i in range(2) print(i)",
            execute_repeat,
            warmup=execute_warmup,
        )
        metrics["matplotlib_execute"] = execute_roundtrip_ms(
            kernel,
            "import matplotlib.pyplot as plt\n"
            "plt.figure()\n"
            "plt.plot([1, 2, 3], [1, 4, 9])\n"
            "plt.title('bench')\n"
            "plt.show()",
            execute_repeat,
            warmup=execute_warmup,
        )

        metrics["complete_request"] = request_roundtrip_ms(
            kernel,
            "complete_request",
            {"code": "text.st", "cursor_pos": 7},
            request_repeat,
            warmup=request_warmup,
        )
        metrics["inspect_request"] = request_roundtrip_ms(
            kernel,
            "inspect_request",
            {"code": "value", "cursor_pos": 5, "detail_level": 1},
            request_repeat,
            warmup=request_warmup,
        )
        metrics["history_request"] = request_roundtrip_ms(
            kernel,
            "history_request",
            {
                "hist_access_type": "range",
                "output": True,
                "raw": True,
                "session": 0,
                "start": 0,
                "stop": 10,
            },
            request_repeat,
            warmup=request_warmup,
        )

        repeated_total_ms, repeated_avg_ms = repeated_small_exec(kernel, iterations)
        metrics["repeated_small_total"] = repeated_total_ms
        metrics["repeated_small_avg"] = repeated_avg_ms

        repeated_stateful_total_ms, repeated_stateful_avg_ms = repeated_stateful_exec(
            kernel, iterations
        )
        metrics["repeated_stateful_total"] = repeated_stateful_total_ms
        metrics["repeated_stateful_avg"] = repeated_stateful_avg_ms

        metrics["stream_python"], metrics["stream_python_msgs"] = (
            execute_and_count_streams(
                kernel,
                "import sys\n"
                f"for i in range({stream_lines}):\n"
                '    print(f"py-line-{i}")\n'
                "    if i % 50 == 0:\n"
                "        sys.stdout.flush()\n",
            )
        )
        metrics["stream_fd"], metrics["stream_fd_msgs"] = execute_and_count_streams(
            kernel,
            "import os\n"
            f"for i in range({stream_lines}):\n"
            '    os.write(1, f"fd-line-{i}\\n".encode())\n',
        )
        metrics["stream_subprocess"], metrics["stream_subprocess_msgs"] = (
            execute_and_count_streams(
                kernel,
                "import subprocess, sys\n"
                "subprocess.run([\n"
                "    sys.executable,\n"
                "    '-c',\n"
                f"    \"for i in range({stream_lines}): print('child-line-%d' % i)\",\n"
                "], check=True)",
            )
        )
        metrics["mixed_stream"], metrics["mixed_stream_msgs"] = (
            execute_and_count_streams(
                kernel,
                "import os, subprocess, sys\n"
                "print('py-out')\n"
                "sys.stdout.flush()\n"
                "os.write(1, b'fd-out\\n')\n"
                "print('py-err', file=sys.stderr)\n"
                "sys.stderr.flush()\n"
                "os.write(2, b'fd-err\\n')\n"
                "subprocess.run([sys.executable, '-c', \"import sys; print('child-out'); sys.stdout.flush(); print('child-err', file=sys.stderr); sys.stderr.flush()\"], check=True)",
            )
        )

        metrics["interrupt_latency"] = interrupt_latency_ms(kernel)

    return metrics


def summarize(
    rusty: dict[str, float], ipy: dict[str, float]
) -> list[dict[str, float | str]]:
    summary: list[dict[str, float | str]] = []
    for metric in METRICS:
        rusty_value = rusty[metric.name]
        ipy_value = ipy[metric.name]
        ratio = float("inf") if ipy_value == 0 else rusty_value / ipy_value
        summary.append(
            {
                "name": metric.name,
                "unit": metric.unit,
                "rustykernel": rusty_value,
                "ipykernel": ipy_value,
                "ratio_vs_ipykernel": ratio,
            }
        )
    return summary


def print_table(summary: list[dict[str, float | str]]) -> None:
    print(
        f"{'metric':<24} {'unit':<8} {'rustykernel':>14} {'ipykernel':>14} {'ratio':>10}",
        flush=True,
    )
    for metric in summary:
        print(
            f"{metric['name']:<24} {metric['unit']:<8} {float(metric['rustykernel']):>14.3f} {float(metric['ipykernel']):>14.3f} {float(metric['ratio_vs_ipykernel']):>10.3f}",
            flush=True,
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Benchmark rustykernel against ipykernel."
    )
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--execute-repeat", type=int, default=7)
    parser.add_argument("--request-repeat", type=int, default=7)
    parser.add_argument("--execute-warmup", type=int, default=2)
    parser.add_argument("--request-warmup", type=int, default=2)
    parser.add_argument("--iterations", type=int, default=50)
    parser.add_argument("--stream-lines", type=int, default=200)
    parser.add_argument("--large-result-bytes", type=int, default=20000)
    parser.add_argument("--json", action="store_true", dest="json_output")
    parser.add_argument("--output-json", type=Path)
    args = parser.parse_args()

    rusty_command, ipy_command = kernel_commands(args.python)

    with TemporaryDirectory() as td:
        zmq_context = zmq.Context()
        try:
            rusty = benchmark_kernel(
                rusty_command,
                zmq_context=zmq_context,
                temp_dir=Path(td),
                execute_repeat=args.execute_repeat,
                request_repeat=args.request_repeat,
                execute_warmup=args.execute_warmup,
                request_warmup=args.request_warmup,
                iterations=args.iterations,
                stream_lines=args.stream_lines,
                large_result_bytes=args.large_result_bytes,
            )
            ipy = benchmark_kernel(
                ipy_command,
                zmq_context=zmq_context,
                temp_dir=Path(td),
                execute_repeat=args.execute_repeat,
                request_repeat=args.request_repeat,
                execute_warmup=args.execute_warmup,
                request_warmup=args.request_warmup,
                iterations=args.iterations,
                stream_lines=args.stream_lines,
                large_result_bytes=args.large_result_bytes,
            )
        finally:
            zmq_context.term()

    payload = {
        "config": {
            "python": args.python,
            "execute_repeat": args.execute_repeat,
            "request_repeat": args.request_repeat,
            "execute_warmup": args.execute_warmup,
            "request_warmup": args.request_warmup,
            "iterations": args.iterations,
            "stream_lines": args.stream_lines,
            "large_result_bytes": args.large_result_bytes,
        },
        "rustykernel": rusty,
        "ipykernel": ipy,
        "summary": summarize(rusty, ipy),
    }

    if args.output_json is not None:
        args.output_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    if args.json_output:
        print(json.dumps(payload, indent=2))
    else:
        print_table(payload["summary"])
        if args.output_json is not None:
            print(f"\nWrote JSON report to {args.output_json}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
