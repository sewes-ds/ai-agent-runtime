import json
import os
from pathlib import Path

import pytest

from ai_agent_runtime.providers.codex.rollout import (
    RolloutContractError,
    RolloutSignalKind,
    scan_rollout,
)


def _append(path: Path, record: object, *, newline: bool = True) -> None:
    with path.open("ab") as stream:
        stream.write(json.dumps(record).encode())
        if newline:
            stream.write(b"\n")


def test_rollout_parser_emits_only_qualified_metadata(tmp_path: Path) -> None:
    path = tmp_path / "rollout.jsonl"
    _append(path, {"type": "event_msg", "payload": {"type": "task_started", "turn_id": "t1"}})
    _append(
        path,
        {
            "type": "response_item",
            "payload": {"type": "message", "content": "secret prompt is ignored"},
        },
    )
    _append(
        path,
        {
            "type": "turn_context",
            "payload": {
                "turn_id": "t1",
                "model": "gpt-test",
                "effort": "high",
                "summary": "must never be retained",
            },
        },
    )
    _append(path, {"type": "event_msg", "payload": {"type": "task_complete", "turn_id": "t1"}})
    metadata = path.stat()
    result = scan_rollout(
        path,
        codex_version="codex-cli 0.156.1",
        start_offset=0,
        expected_device=metadata.st_dev,
        expected_inode=metadata.st_ino,
    )
    assert [signal.kind for signal in result.signals] == [
        RolloutSignalKind.TURN_STARTED,
        RolloutSignalKind.TURN_CONTEXT,
        RolloutSignalKind.TURN_COMPLETED,
    ]
    context = result.signals[1]
    assert context.model == "gpt-test"
    assert context.reasoning_effort == "high"
    assert "secret" not in repr(result)
    assert "summary" not in repr(result)


def test_rollout_parser_defers_partial_record_and_resumes_exactly_once(tmp_path: Path) -> None:
    path = tmp_path / "rollout.jsonl"
    _append(path, {"type": "event_msg", "payload": {"type": "task_started"}})
    partial = json.dumps(
        {
            "type": "turn_context",
            "payload": {"turn_id": "t1", "model": "gpt-test", "effort": "low"},
        }
    ).encode()
    with path.open("ab") as stream:
        stream.write(partial)
    metadata = path.stat()
    first = scan_rollout(
        path,
        codex_version="codex-cli 0.156.1",
        start_offset=0,
        expected_device=metadata.st_dev,
        expected_inode=metadata.st_ino,
    )
    assert [signal.kind for signal in first.signals] == [RolloutSignalKind.TURN_STARTED]
    with path.open("ab") as stream:
        stream.write(b"\n")
    second = scan_rollout(
        path,
        codex_version="codex-cli 0.156.1",
        start_offset=first.next_offset,
        expected_device=metadata.st_dev,
        expected_inode=metadata.st_ino,
    )
    assert [signal.kind for signal in second.signals] == [RolloutSignalKind.TURN_CONTEXT]
    assert second.signals[0].turn_id == "t1"


def test_rollout_parser_fails_closed_on_version_identity_and_schema(tmp_path: Path) -> None:
    path = tmp_path / "rollout.jsonl"
    _append(
        path,
        {"type": "turn_context", "payload": {"turn_id": "t1", "model": "gpt-test"}},
    )
    metadata = path.stat()
    with pytest.raises(RolloutContractError, match="unqualified"):
        scan_rollout(
            path,
            codex_version="codex-cli 9.9.9",
            start_offset=0,
            expected_device=metadata.st_dev,
            expected_inode=metadata.st_ino,
        )
    with pytest.raises(RolloutContractError, match="identity"):
        scan_rollout(
            path,
            codex_version="codex-cli 0.156.1",
            start_offset=0,
            expected_device=metadata.st_dev,
            expected_inode=metadata.st_ino + 1,
        )
    with pytest.raises(RolloutContractError, match="lacks qualified"):
        scan_rollout(
            path,
            codex_version="codex-cli 0.156.1",
            start_offset=0,
            expected_device=metadata.st_dev,
            expected_inode=metadata.st_ino,
        )


def test_rollout_parser_rejects_symlink(tmp_path: Path) -> None:
    target = tmp_path / "target.jsonl"
    target.write_text("{}\n", encoding="utf-8")
    link = tmp_path / "link.jsonl"
    link.symlink_to(target)
    metadata = target.stat()
    with pytest.raises(OSError):
        scan_rollout(
            link,
            codex_version="codex-cli 0.156.1",
            start_offset=0,
            expected_device=metadata.st_dev,
            expected_inode=metadata.st_ino,
        )
    assert os.path.islink(link)


def test_rollout_parser_skips_oversized_response_and_reaches_task_complete(
    tmp_path: Path,
) -> None:
    path = tmp_path / "rollout.jsonl"
    _append(
        path,
        {
            "type": "event_msg",
            "payload": {"type": "task_started", "turn_id": "t1"},
        },
    )
    _append(
        path,
        {
            "type": "response_item",
            "payload": {"type": "custom_tool_call_output", "output": "x" * (3 * 1024 * 1024)},
        },
    )
    _append(
        path,
        {
            "type": "event_msg",
            "payload": {"type": "task_complete", "turn_id": "t1"},
        },
    )
    metadata = path.stat()

    result = scan_rollout(
        path,
        codex_version="codex-cli 0.156.1",
        start_offset=0,
        expected_device=metadata.st_dev,
        expected_inode=metadata.st_ino,
    )

    assert result.next_offset == metadata.st_size
    assert result.at_eof is True
    assert result.skipped_oversized_records == 1
    assert [signal.kind for signal in result.signals] == [
        RolloutSignalKind.TURN_STARTED,
        RolloutSignalKind.TURN_COMPLETED,
    ]


def test_rollout_parser_skips_oversized_response_larger_than_scan_window(
    tmp_path: Path,
) -> None:
    path = tmp_path / "rollout.jsonl"
    _append(
        path,
        {
            "type": "response_item",
            "payload": {"type": "custom_tool_call_output", "output": "x" * (5 * 1024 * 1024)},
        },
    )
    _append(
        path,
        {
            "type": "event_msg",
            "payload": {"type": "task_complete", "turn_id": "t1"},
        },
    )
    metadata = path.stat()

    first = scan_rollout(
        path,
        codex_version="codex-cli 0.156.1",
        start_offset=0,
        expected_device=metadata.st_dev,
        expected_inode=metadata.st_ino,
    )
    second = scan_rollout(
        path,
        codex_version="codex-cli 0.156.1",
        start_offset=first.next_offset,
        expected_device=metadata.st_dev,
        expected_inode=metadata.st_ino,
    )

    assert first.skipped_oversized_records == 1
    assert first.next_offset < metadata.st_size
    assert [signal.kind for signal in second.signals] == [RolloutSignalKind.TURN_COMPLETED]
    assert second.next_offset == metadata.st_size


def test_rollout_parser_skips_oversized_non_lifecycle_event(tmp_path: Path) -> None:
    path = tmp_path / "rollout.jsonl"
    _append(
        path,
        {
            "type": "event_msg",
            "payload": {
                "type": "item_completed",
                "turn_id": "t1",
                "item": {"output": "x" * (3 * 1024 * 1024)},
            },
        },
    )
    _append(
        path,
        {
            "type": "event_msg",
            "payload": {"type": "task_complete", "turn_id": "t1"},
        },
    )
    metadata = path.stat()

    result = scan_rollout(
        path,
        codex_version="codex-cli 0.156.1",
        start_offset=0,
        expected_device=metadata.st_dev,
        expected_inode=metadata.st_ino,
    )

    assert result.skipped_oversized_records == 1
    assert [signal.kind for signal in result.signals] == [RolloutSignalKind.TURN_COMPLETED]
