"""Thin Python entrypoints around the Rust extension module."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from typing import Any, Sequence

from ._core import ConnectionInfo, healthcheck, parse_connection_file, runtime_info


@dataclass(slots=True)
class KernelApp:
    connection_file: str | None = None

    def describe(self) -> dict[str, Any]:
        info = runtime_info()
        connection = self.load_connection()
        return {
            "implementation": info.implementation,
            "implementation_version": info.implementation_version,
            "language": info.language,
            "protocol_version": info.protocol_version,
            "connection_file": self.connection_file,
            "connection": connection,
            "healthcheck": healthcheck(),
        }

    def load_connection(self) -> dict[str, Any] | None:
        if self.connection_file is None:
            return None

        return connection_to_dict(parse_connection_file(self.connection_file))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="rustykernel",
        description="Rust-first Python kernel runtime scaffold.",
    )
    parser.add_argument(
        "--connection-file",
        help="Future Jupyter connection file path.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print runtime metadata as JSON.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    app = KernelApp(connection_file=args.connection_file)
    payload = app.describe()

    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        connection = payload["connection"]
        connection_summary = "no connection file loaded"
        if connection is not None:
            connection_summary = (
                f"{connection['transport']}://{connection['ip']} "
                f"(shell={connection['shell_port']}, iopub={connection['iopub_port']}, "
                f"stdin={connection['stdin_port']}, control={connection['control_port']}, "
                f"hb={connection['hb_port']})"
            )
        print(
            "rustykernel scaffold ready "
            f"(protocol {payload['protocol_version']}, "
            f"connection_file={payload['connection_file']!r}, "
            f"{connection_summary})"
        )

    return 0


def connection_to_dict(connection: ConnectionInfo) -> dict[str, Any]:
    return {
        "transport": connection.transport,
        "ip": connection.ip,
        "shell_port": connection.shell_port,
        "iopub_port": connection.iopub_port,
        "stdin_port": connection.stdin_port,
        "control_port": connection.control_port,
        "hb_port": connection.hb_port,
        "signature_scheme": connection.signature_scheme,
        "key": connection.key,
        "kernel_name": connection.kernel_name,
    }
