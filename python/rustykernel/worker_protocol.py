from __future__ import annotations

import dataclasses
import json
import struct
import zlib
from typing import Any, BinaryIO, Literal

PROTOCOL_VERSION = 2

_FLAG_COMPRESSED = 1 << 0
_HEADER = struct.Struct(">BQ")
_COMPRESSION_THRESHOLD = 128 * 1024
_MIN_COMPRESSION_SAVINGS = 256

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


def _encode_payload(message: dict[str, Any]) -> tuple[int, bytes]:
    payload = json.dumps(
        message,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")

    if len(payload) < _COMPRESSION_THRESHOLD:
        return 0, payload

    compressed = zlib.compress(payload, level=1)
    savings = len(payload) - len(compressed)
    if savings < _MIN_COMPRESSION_SAVINGS or compressed_len_too_close(
        len(payload), len(compressed)
    ):
        return 0, payload

    return _FLAG_COMPRESSED, compressed


def compressed_len_too_close(original_len: int, compressed_len: int) -> bool:
    return compressed_len * 10 > original_len * 7


def write_framed_message(stream: BinaryIO, message: dict[str, Any]) -> None:
    flags, payload = _encode_payload(message)
    stream.write(_HEADER.pack(flags, len(payload)))
    stream.write(payload)
    stream.flush()


def read_framed_message(stream: BinaryIO) -> bytes | None:
    header = stream.read(_HEADER.size)
    if header == b"":
        return None
    if len(header) != _HEADER.size:
        raise ProtocolError("truncated worker protocol header")

    flags, payload_len = _HEADER.unpack(header)
    payload = stream.read(payload_len)
    if len(payload) != payload_len:
        raise ProtocolError("truncated worker protocol payload")

    if flags & _FLAG_COMPRESSED:
        try:
            payload = zlib.decompress(payload)
        except zlib.error as exc:
            raise ProtocolError(
                f"failed to decompress worker protocol payload: {exc}"
            ) from exc

    if flags & ~_FLAG_COMPRESSED:
        raise ProtocolError(f"unknown worker protocol flags: {flags}")

    return payload


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


def decode_envelope(raw_message: bytes) -> WorkerEnvelope:
    try:
        payload = json.loads(raw_message)
    except json.JSONDecodeError as exc:
        raise ProtocolError(f"invalid worker protocol JSON: {exc}") from exc
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
