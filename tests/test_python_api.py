from __future__ import annotations

import json
import socket
import subprocess
import sys
from pathlib import Path

import pytest
import rustykernel


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
