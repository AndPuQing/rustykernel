"""Thin Python entrypoints around the Rust extension module."""

from __future__ import annotations

import argparse
import json
import time
from dataclasses import dataclass
from typing import Any, Sequence

from ._core import (
    ConnectionInfo,
    healthcheck,
    parse_connection_file,
    runtime_info,
    start_kernel,
)


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

    def run(self) -> int:
        if self.connection_file is None:
            payload = self.describe()
            print(
                "rustykernel scaffold ready "
                f"(protocol {payload['protocol_version']}, "
                f"connection_file={payload['connection_file']!r}, "
                "no connection file loaded)"
            )
            return 0

        connection = connection_to_dict(parse_connection_file(self.connection_file))
        kernel = start_kernel(self.connection_file)
        print(
            "rustykernel channels bound "
            f"(shell={kernel.endpoints.shell}, "
            f"iopub={kernel.endpoints.iopub}, "
            f"stdin={kernel.endpoints.stdin}, "
            f"control={kernel.endpoints.control}, "
            f"hb={kernel.endpoints.hb}, "
            f"signature_scheme={connection['signature_scheme']})"
        )

        try:
            while kernel.is_running:
                time.sleep(0.25)
        except KeyboardInterrupt:
            pass
        finally:
            kernel.stop()

        return 0

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

    if args.json:
        payload = app.describe()
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0

    return app.run()


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
