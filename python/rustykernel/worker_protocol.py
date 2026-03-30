from __future__ import annotations

import dataclasses
import json
from typing import Any, Literal

PROTOCOL_VERSION = 1

FrameType = Literal["request", "response", "event"]


class ProtocolError(RuntimeError):
    pass


@dataclasses.dataclass(slots=True)
class WorkerEnvelope:
    protocol_version: int
    frame_type: FrameType
    request_id: int
    seq: int | None
    body: dict[str, Any]


def request_envelope(request_id: int, body: dict[str, Any]) -> dict[str, Any]:
    return {
        "protocol_version": PROTOCOL_VERSION,
        "frame_type": "request",
        "request_id": request_id,
        "body": body,
    }


def response_envelope(request_id: int, body: dict[str, Any]) -> dict[str, Any]:
    return {
        "protocol_version": PROTOCOL_VERSION,
        "frame_type": "response",
        "request_id": request_id,
        "body": body,
    }


def event_envelope(request_id: int, seq: int, body: dict[str, Any]) -> dict[str, Any]:
    return {
        "protocol_version": PROTOCOL_VERSION,
        "frame_type": "event",
        "request_id": request_id,
        "seq": seq,
        "body": body,
    }


def request_body(request_type: str, **fields: Any) -> dict[str, Any]:
    return {"request_type": request_type, **fields}


def response_body(response_type: str, **fields: Any) -> dict[str, Any]:
    return {"response_type": response_type, **fields}


def event_body(event_type: str, **fields: Any) -> dict[str, Any]:
    return {"event_type": event_type, **fields}


def decode_envelope(raw_line: str) -> WorkerEnvelope:
    payload = json.loads(raw_line)
    if not isinstance(payload, dict):
        raise ProtocolError("protocol message must be a JSON object")

    protocol_version = int(payload.get("protocol_version", -1))
    if protocol_version != PROTOCOL_VERSION:
        raise ProtocolError(f"unsupported worker protocol version: {protocol_version}")

    frame_type = payload.get("frame_type")
    if frame_type not in {"request", "response", "event"}:
        raise ProtocolError(f"invalid worker frame_type: {frame_type!r}")

    request_id = int(payload.get("request_id", -1))
    if request_id < 0:
        raise ProtocolError(f"invalid worker request_id: {request_id}")

    seq = payload.get("seq")
    if frame_type == "event":
        if seq is None:
            raise ProtocolError("worker event missing seq")
        seq = int(seq)
    elif seq is not None:
        seq = int(seq)

    body = payload.get("body")
    if not isinstance(body, dict):
        raise ProtocolError("worker frame body must be an object")

    return WorkerEnvelope(
        protocol_version=protocol_version,
        frame_type=frame_type,
        request_id=request_id,
        seq=seq,
        body=body,
    )
