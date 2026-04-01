"""Thin Python entrypoints around the Rust extension module."""

from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from typing import Any, Sequence

from ._core import (
    ConnectionInfo,
    healthcheck,
    parse_connection_file,
    runtime_info,
    start_kernel,
)
from .kernelspec import KERNEL_NAME
from .kernelspec import install as install_kernel_spec


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

        os.environ["RUSTYKERNEL_PYTHON_EXECUTABLE"] = sys.executable
        kernel = start_kernel(self.connection_file)

        try:
            while kernel.is_running:
                try:
                    kernel.wait_for_shutdown()
                except KeyboardInterrupt:
                    kernel.interrupt()
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
        "-f",
        "--connection-file",
        help="Jupyter connection file path.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print runtime metadata as JSON.",
    )
    subparsers = parser.add_subparsers(dest="command")
    install_parser = subparsers.add_parser(
        "install",
        help="Install the rustykernel kernelspec.",
    )
    install_parser.add_argument(
        "--user",
        action="store_true",
        help="Install for the current user instead of system-wide.",
    )
    install_parser.add_argument(
        "--prefix",
        help="Install into the given prefix.",
    )
    install_parser.add_argument(
        "--sys-prefix",
        action="store_const",
        const=sys.prefix,
        dest="prefix",
        help="Install into the active Python environment prefix.",
    )
    install_parser.add_argument(
        "--name",
        default=KERNEL_NAME,
        help="Kernel spec name to install.",
    )
    install_parser.add_argument(
        "--display-name",
        help="Display name shown by Jupyter frontends.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)

    if args.command == "install":
        destination = install_kernel_spec(
            user=args.user,
            kernel_name=args.name,
            display_name=args.display_name,
            prefix=args.prefix,
        )
        print(destination)
        return 0

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
