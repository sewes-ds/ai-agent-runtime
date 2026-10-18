"""Length-delimited JSON protocol for local Unix sockets."""

from __future__ import annotations

import asyncio
import json
import struct
from typing import Any

PROTOCOL_MAJOR = 1
PROTOCOL_MINOR = 1
MAX_FRAME_SIZE = 1024 * 1024
_HEADER = struct.Struct("!I")


async def read_frame(
    reader: asyncio.StreamReader,
    *,
    timeout: float = 10.0,
) -> dict[str, Any]:
    header = await asyncio.wait_for(reader.readexactly(_HEADER.size), timeout=timeout)
    (size,) = _HEADER.unpack(header)
    if size == 0 or size > MAX_FRAME_SIZE:
        raise ValueError(f"invalid frame size: {size}")
    payload = await asyncio.wait_for(reader.readexactly(size), timeout=timeout)
    decoded = json.loads(payload)
    if not isinstance(decoded, dict):
        raise ValueError("IPC payload must be an object")
    return decoded


async def write_frame(
    writer: asyncio.StreamWriter,
    payload: dict[str, Any],
    *,
    timeout: float = 10.0,
) -> None:
    encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True).encode()
    if not encoded or len(encoded) > MAX_FRAME_SIZE:
        raise ValueError(f"invalid frame size: {len(encoded)}")
    writer.write(_HEADER.pack(len(encoded)))
    writer.write(encoded)
    await asyncio.wait_for(writer.drain(), timeout=timeout)


def versioned(action: str, request_id: str, **payload: Any) -> dict[str, Any]:
    return {
        "protocol_major": PROTOCOL_MAJOR,
        "protocol_minor": PROTOCOL_MINOR,
        "action": action,
        "request_id": request_id,
        **payload,
    }


def validate_version(payload: dict[str, Any]) -> None:
    if payload.get("protocol_major") != PROTOCOL_MAJOR:
        raise ValueError("protocol major mismatch")
