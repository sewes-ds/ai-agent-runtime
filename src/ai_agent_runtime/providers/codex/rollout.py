"""Metadata-only parser for version-qualified Codex rollout events."""

from __future__ import annotations

import json
import os
import re
import stat
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

from ai_agent_runtime.domain import sha256_digest

QUALIFIED_CODEX_VERSIONS = frozenset({"codex-cli 0.156.1"})


class RolloutSignalKind(StrEnum):
    TURN_STARTED = "TURN_STARTED"
    TURN_CONTEXT = "TURN_CONTEXT"
    TURN_COMPLETED = "TURN_COMPLETED"


@dataclass(frozen=True, slots=True)
class RolloutSignal:
    kind: RolloutSignalKind
    end_offset: int
    turn_id: str | None
    model: str | None = None
    reasoning_effort: str | None = None
    evidence_digest: str | None = None


@dataclass(frozen=True, slots=True)
class RolloutScan:
    next_offset: int
    signals: tuple[RolloutSignal, ...]
    at_eof: bool
    skipped_oversized_records: int = 0


class RolloutContractError(ValueError):
    pass


_OUTER_TYPE = re.compile(rb'"type"\s*:\s*"([^"\\]+)"')
_SIGNAL_RECORD_TYPES = frozenset({b"event_msg", b"turn_context"})
_LIFECYCLE_EVENT_TYPES = frozenset(
    {b"task_started", b"turn_started", b"task_complete", b"turn_complete"}
)
_TYPE_PREFIX_BYTES = 8192


def _oversized_record_is_ignorable(prefix: bytes) -> bool:
    """Classify a qualified Codex record without retaining its large payload.

    Codex 0.156.1 emits the top-level ``type`` before ``payload``. Lifecycle
    evidence lives only in ``event_msg`` and ``turn_context`` records, both of
    which are bounded metadata records. Large response/tool records may contain
    arbitrary user data and are intentionally skipped rather than decoded.
    """

    matches = tuple(_OUTER_TYPE.finditer(prefix[:_TYPE_PREFIX_BYTES]))
    if not matches:
        raise RolloutContractError("oversized rollout record lacks a bounded outer type")
    outer_type = matches[0].group(1)
    if outer_type not in _SIGNAL_RECORD_TYPES:
        return True
    if outer_type == b"turn_context":
        return False
    if len(matches) < 2:
        raise RolloutContractError("oversized event record lacks a bounded event type")
    return matches[1].group(1) not in _LIFECYCLE_EVENT_TYPES


def _skip_oversized_record(fd: int, initial: bytes, *, max_bytes: int) -> tuple[int, bool]:
    """Return bytes through the record newline, using bounded memory.

    A missing newline means the writer has not completed the record yet; in
    that case the caller must retain its previous durable cursor.
    """

    if not _oversized_record_is_ignorable(initial):
        raise RolloutContractError("qualified lifecycle record exceeds maximum size")
    consumed = len(initial)
    while True:
        chunk = os.read(fd, max_bytes)
        newline = chunk.find(b"\n")
        if newline >= 0:
            return consumed + newline + 1, True
        consumed += len(chunk)
        if not chunk:
            return consumed, False


def scan_rollout(
    path: Path,
    *,
    codex_version: str,
    start_offset: int,
    expected_device: int,
    expected_inode: int,
    max_bytes: int = 4 * 1024 * 1024,
    max_line_bytes: int = 2 * 1024 * 1024,
) -> RolloutScan:
    """Read only complete JSONL records and retain no prompt/transcript content."""

    if codex_version not in QUALIFIED_CODEX_VERSIONS:
        raise RolloutContractError(f"unqualified Codex rollout version: {codex_version}")
    if start_offset < 0 or max_bytes < 1 or max_line_bytes < 1:
        raise RolloutContractError("invalid rollout scan bounds")
    flags = os.O_RDONLY | os.O_CLOEXEC
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    fd = os.open(path, flags)
    try:
        metadata = os.fstat(fd)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_uid != os.getuid():
            raise RolloutContractError("rollout must be an owned regular file")
        if metadata.st_dev != expected_device or metadata.st_ino != expected_inode:
            raise RolloutContractError("rollout identity changed")
        if start_offset > metadata.st_size:
            raise RolloutContractError("rollout was truncated")
        os.lseek(fd, start_offset, os.SEEK_SET)
        chunk = os.read(fd, max_bytes)
        file_size = metadata.st_size
        if b"\n" not in chunk and len(chunk) > max_line_bytes:
            skipped_length, complete = _skip_oversized_record(
                fd,
                chunk,
                max_bytes=max_bytes,
            )
            if not complete:
                return RolloutScan(
                    next_offset=start_offset,
                    signals=(),
                    at_eof=True,
                )
            next_offset = start_offset + skipped_length
            return RolloutScan(
                next_offset=next_offset,
                signals=(),
                at_eof=next_offset >= file_size,
                skipped_oversized_records=1,
            )
    finally:
        os.close(fd)
    complete_length = chunk.rfind(b"\n") + 1
    if complete_length == 0:
        if len(chunk) > max_line_bytes:
            raise RolloutContractError("rollout line exceeds maximum size")
        return RolloutScan(
            next_offset=start_offset,
            signals=(),
            at_eof=start_offset + len(chunk) >= file_size,
        )

    signals: list[RolloutSignal] = []
    consumed = 0
    skipped_oversized_records = 0
    for raw_line in chunk[:complete_length].splitlines(keepends=True):
        consumed += len(raw_line)
        if len(raw_line) > max_line_bytes:
            if not _oversized_record_is_ignorable(raw_line):
                raise RolloutContractError("qualified lifecycle record exceeds maximum size")
            skipped_oversized_records += 1
            continue
        try:
            record = json.loads(raw_line)
        except json.JSONDecodeError as exc:
            raise RolloutContractError("invalid complete rollout JSON record") from exc
        signal = _signal_from_record(record, end_offset=start_offset + consumed)
        if signal is not None:
            signals.append(signal)
    return RolloutScan(
        next_offset=start_offset + complete_length,
        signals=tuple(signals),
        at_eof=start_offset + complete_length >= file_size,
        skipped_oversized_records=skipped_oversized_records,
    )


def _signal_from_record(record: object, *, end_offset: int) -> RolloutSignal | None:
    if not isinstance(record, dict):
        return None
    payload = record.get("payload")
    if record.get("type") == "event_msg" and isinstance(payload, dict):
        event_type = payload.get("type")
        turn_id = _optional_string(payload.get("turn_id"))
        if event_type in {"task_started", "turn_started"}:
            return RolloutSignal(
                kind=RolloutSignalKind.TURN_STARTED,
                end_offset=end_offset,
                turn_id=turn_id,
            )
        if event_type in {"task_complete", "turn_complete"}:
            return RolloutSignal(
                kind=RolloutSignalKind.TURN_COMPLETED,
                end_offset=end_offset,
                turn_id=turn_id,
            )
        return None
    if record.get("type") != "turn_context" or not isinstance(payload, dict):
        return None
    model = _optional_string(payload.get("model"))
    effort = _optional_string(payload.get("effort"))
    turn_id = _optional_string(payload.get("turn_id"))
    if model is None or effort is None or turn_id is None:
        raise RolloutContractError("turn_context lacks qualified model/effort/turn identity")
    evidence = {
        "end_offset": end_offset,
        "event_type": "turn_context",
        "model": model,
        "reasoning_effort": effort,
        "turn_id": turn_id,
    }
    return RolloutSignal(
        kind=RolloutSignalKind.TURN_CONTEXT,
        end_offset=end_offset,
        turn_id=turn_id,
        model=model,
        reasoning_effort=effort,
        evidence_digest=sha256_digest(evidence),
    )


def _optional_string(value: Any) -> str | None:
    return value if isinstance(value, str) and value else None
