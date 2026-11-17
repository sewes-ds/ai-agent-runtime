from __future__ import annotations

import hashlib
import json
import socket
from pathlib import Path
from typing import Any

import pytest

from ai_agent_runtime.config import RuntimeConfig
from ai_agent_runtime.domain import DesiredSpec, LifecycleState, RuntimeBinding
from ai_agent_runtime.health.doctor import collect_doctor_report
from ai_agent_runtime.process_safety import profile_fingerprint
from ai_agent_runtime.state.store import RuntimeStore


def _fixture(tmp_path: Path) -> tuple[RuntimeConfig, socket.socket]:
    project = tmp_path / "project"
    project.mkdir(mode=0o700)
    profiles = tmp_path / "profiles"
    profile = profiles / "work"
    profile.mkdir(parents=True, mode=0o700)
    auth = profile / "auth.json"
    auth.write_text("{}\n", encoding="utf-8")
    auth.chmod(0o600)
    binary = tmp_path / "codex-real"
    binary.write_text("#!/bin/sh\necho 'codex-cli 0.156.1'\n", encoding="utf-8")
    binary.chmod(0o700)
    topic_config = tmp_path / "topic_config.json"
    topic_config.write_text(
        json.dumps(
            {
                "topics": {
                    "42": {
                        "cwd": str(project),
                        "engine": "codex",
                        "exec_mode": "tmux",
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    tmux_state = tmp_path / "tmux-state.json"
    tmux_state.write_text(
        json.dumps(
            {
                "-1001:42": {
                    "session_name": "cc-n1001-42",
                    "cwd": str(project),
                    "provider": "codex",
                    "session_id": "session-1",
                }
            }
        ),
        encoding="utf-8",
    )
    state_dir = tmp_path / "state"
    socket_path = tmp_path / "run" / "runtime.sock"
    socket_path.parent.mkdir(mode=0o700)
    config = RuntimeConfig(
        agent_instance_id="agent-1",
        forum_chat_id=-1001,
        state_dir=state_dir,
        socket_path=socket_path,
        database_path=state_dir / "runtime.sqlite",
        real_codex=binary,
        real_codex_version="0.156.1",
        real_codex_sha256=f"sha256:{hashlib.sha256(binary.read_bytes()).hexdigest()}",
        profiles_root=profiles,
        topic_config_path=topic_config,
        tmux_state_path=tmux_state,
        upstream_sha="0" * 40,
        allowed_project_roots=(tmp_path,),
    )
    binding = RuntimeBinding(
        binding_id="binding-1",
        agent_instance_id="agent-1",
        chat_id=-1001,
        thread_id=42,
        project_id="project-1",
        canonical_cwd=str(project),
        expected_tmux_session="cc-n1001-42",
    )
    spec = DesiredSpec.build(
        binding_id="binding-1",
        agent_instance_id="agent-1",
        project_id="project-1",
        canonical_cwd=str(project),
        provider="codex",
        account_profile="work",
        model="gpt-test",
        reasoning_effort="high",
        generation_id="gen-1",
        revision=1,
    )
    store = RuntimeStore(config.database_path)
    store.upsert_binding(binding)
    store.put_effective_spec(
        spec,
        codex_home=profile,
        profile_fingerprint=profile_fingerprint(profile),
    )
    store.record_instance(
        instance_id="instance-1",
        binding_id="binding-1",
        wrapper_pid=100,
        wrapper_starttime=200,
        child_pid=101,
        child_starttime=201,
        tmux_session="cc-n1001-42",
        tmux_pane="%1",
        lifecycle=LifecycleState.IDLE,
        applied_digest=None,
    )
    store.close()
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    listener.bind(str(socket_path))
    socket_path.chmod(0o600)
    return config, listener


@pytest.mark.asyncio
async def test_doctor_proves_local_runtime_inputs_without_mutation(tmp_path: Path) -> None:
    config, listener = _fixture(tmp_path)
    store = RuntimeStore(config.database_path)
    store.record_instance(
        instance_id="failed-instance",
        binding_id="binding-1",
        wrapper_pid=300,
        wrapper_starttime=400,
        child_pid=None,
        child_starttime=None,
        tmux_session="failed-session-without-live-state",
        tmux_pane="%2",
        lifecycle=LifecycleState.START_FAILED,
        applied_digest=None,
    )
    store.close()

    async def status(_path: Path, payload: dict[str, Any]) -> dict[str, Any]:
        assert payload["action"] == "status"
        return {"ok": True, "daemon_boot_id": "boot-1", "status": {}}

    database_mtime = config.database_path.stat().st_mtime_ns
    topic_before = config.topic_config_path.read_bytes()
    try:
        report = await collect_doctor_report(config, status_requester=status)
    finally:
        listener.close()
    assert report["healthy"] is True
    assert report["mutations_performed"] is False
    assert all(check["ok"] is True for check in report["checks"].values())  # type: ignore[union-attr]
    assert config.database_path.stat().st_mtime_ns == database_mtime
    assert config.topic_config_path.read_bytes() == topic_before


@pytest.mark.asyncio
async def test_doctor_reports_config_drift_and_runtime_status_failure(tmp_path: Path) -> None:
    config, listener = _fixture(tmp_path)
    other = tmp_path / "other"
    other.mkdir()
    raw = json.loads(config.topic_config_path.read_text(encoding="utf-8"))
    raw["topics"]["42"]["cwd"] = str(other)
    config.topic_config_path.write_text(json.dumps(raw), encoding="utf-8")

    async def unavailable(_path: Path, _payload: dict[str, Any]) -> dict[str, Any]:
        raise ConnectionError("daemon unavailable")

    try:
        report = await collect_doctor_report(config, status_requester=unavailable)
    finally:
        listener.close()
    assert report["healthy"] is False
    checks = report["checks"]
    assert checks["runtime_status"]["ok"] is False  # type: ignore[index]
    assert checks["binding_correlation"]["ok"] is False  # type: ignore[index]
    assert "cwd drift" in str(checks["binding_correlation"]["detail"])  # type: ignore[index]
