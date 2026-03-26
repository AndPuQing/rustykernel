"""Kernel spec helpers for installing rustykernel into Jupyter frontends."""

from __future__ import annotations

import json
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

from jupyter_client.kernelspec import KernelSpecManager

from ._core import runtime_info

KERNEL_NAME = "rustykernel"


def make_rustykernel_cmd(
    executable: str | None = None,
    extra_arguments: list[str] | None = None,
    python_arguments: list[str] | None = None,
) -> list[str]:
    """Build the kernel launch command used in kernel.json."""

    executable = executable or sys.executable
    extra_arguments = extra_arguments or []
    python_arguments = python_arguments or []
    return [
        executable,
        *python_arguments,
        "-m",
        "rustykernel",
        "-f",
        "{connection_file}",
        *extra_arguments,
    ]


def get_kernel_dict(
    extra_arguments: list[str] | None = None,
    python_arguments: list[str] | None = None,
) -> dict[str, Any]:
    """Construct the kernel.json payload."""

    info = runtime_info()
    return {
        "argv": make_rustykernel_cmd(
            extra_arguments=extra_arguments,
            python_arguments=python_arguments,
        ),
        "display_name": "Python (rustykernel)",
        "language": "python",
        "metadata": {"debugger": False},
        "kernel_protocol_version": info.protocol_version,
    }


def write_kernel_spec(
    path: Path | str | None = None,
    overrides: dict[str, Any] | None = None,
    extra_arguments: list[str] | None = None,
    python_arguments: list[str] | None = None,
) -> str:
    """Write a kernelspec directory and return its path."""

    if path is None:
        path = Path(tempfile.mkdtemp(prefix="rustykernel_")) / KERNEL_NAME
    else:
        path = Path(path)

    path.mkdir(parents=True, exist_ok=True)

    kernel_dict = get_kernel_dict(
        extra_arguments=extra_arguments,
        python_arguments=python_arguments,
    )
    if overrides:
        kernel_dict.update(overrides)

    kernel_json = path / "kernel.json"
    kernel_json.write_text(json.dumps(kernel_dict, indent=2), encoding="utf-8")
    return str(path)


def install(
    kernel_spec_manager: KernelSpecManager | None = None,
    user: bool = False,
    kernel_name: str = KERNEL_NAME,
    display_name: str | None = None,
    prefix: str | None = None,
    env: dict[str, str] | None = None,
) -> str:
    """Install the rustykernel kernelspec and return the destination path."""

    kernel_spec_manager = kernel_spec_manager or KernelSpecManager()

    overrides: dict[str, Any] = {}
    if display_name is not None:
        overrides["display_name"] = display_name
    elif kernel_name != KERNEL_NAME:
        overrides["display_name"] = kernel_name

    if env:
        overrides["env"] = env

    spec_path = write_kernel_spec(overrides=overrides)
    try:
        return kernel_spec_manager.install_kernel_spec(
            spec_path,
            kernel_name=kernel_name,
            user=user,
            prefix=prefix,
        )
    finally:
        shutil.rmtree(spec_path, ignore_errors=True)
