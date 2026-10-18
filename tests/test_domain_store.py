import os
import time
from dataclasses import replace
from pathlib import Path

import pytest

from ai_agent_runtime.domain import DesiredSpec, RuntimeBinding
from ai_agent_runtime.state.store import RuntimeStore


def _spec(revision: int = 1, *, generation: str = "gen-1") -> DesiredSpec:
    return DesiredSpec.build(
        binding_id="binding-1",
        agent_instance_id="agent-1",
        project_id="project-1",
        canonical_cwd="/srv/project",
        provider="codex",
        account_profile="work",
        model="gpt-test",
        reasoning_effort="high",
        generation_id=generation,
        revision=revision,
    )


def test_spec_digest_is_stable_and_detects_tampering() -> None:
    spec = _spec()
    assert spec.verify_digest()
    altered = replace(spec, model="different")
    assert not altered.verify_digest()


def test_control_plane_sync_timestamp_is_durable(tmp_path: Path) -> None:
    path = tmp_path / "runtime.sqlite"
    store = RuntimeStore(path)
    assert store.control_plane_last_seen("agent-1") is None
    assert store.mark_control_plane_sync("agent-1", observed_at=1234.5) == 1234.5
    store.close()
    reopened = RuntimeStore(path)
    assert reopened.control_plane_last_seen("agent-1") == 1234.5
    reopened.close()


def test_dispatch_state_distinguishes_launch_and_applied_desire(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path / "runtime.sqlite")
    binding = RuntimeBinding(
        binding_id="binding-1",
        agent_instance_id="agent-1",
        chat_id=-1001,
        thread_id=42,
        project_id="project-1",
        canonical_cwd="/srv/project",
        expected_tmux_session="cc-n1001-42",
    )
    store.upsert_binding(binding)
    first = _spec()
    store.put_effective_spec(first, codex_home=tmp_path, profile_fingerprint="fp")
    store.record_instance(
        instance_id="instance-1",
        binding_id=binding.binding_id,
        wrapper_pid=100,
        wrapper_starttime=10,
        child_pid=101,
        child_starttime=11,
        tmux_session=binding.expected_tmux_session,
        tmux_pane="%1",
        lifecycle="IDLE",
        applied_digest=None,
        launch_spec={"desired": {"spec_digest": first.spec_digest}},
    )

    launched = store.dispatch_state(agent_instance_id="agent-1", chat_id=-1001, thread_id=42)
    assert launched is not None
    assert launched.live_instance_count == 1
    assert launched.launch_spec_digest == first.spec_digest
    assert launched.applied_spec_digest is None

    store.record_applied_spec(
        instance_id="instance-1",
        binding_id=binding.binding_id,
        spec=first,
        evidence={"verified": True},
        session_id="session-1",
        account_profile="work",
        rollout_path="/rollout",
        rollout_device=1,
        rollout_inode=2,
    )
    second = _spec(2)
    store.put_effective_spec(second, codex_home=tmp_path, profile_fingerprint="fp")

    pending = store.dispatch_state(agent_instance_id="agent-1", chat_id=-1001, thread_id=42)
    assert pending is not None
    assert pending.desired_spec_digest == second.spec_digest
    assert pending.applied_spec_digest == first.spec_digest


def test_store_rejects_revision_rollback_and_conflicting_digest(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path / "runtime.sqlite")
    store.upsert_binding(
        RuntimeBinding(
            binding_id="binding-1",
            agent_instance_id="agent-1",
            chat_id=-1001,
            thread_id=42,
            project_id="project-1",
            canonical_cwd="/srv/project",
            expected_tmux_session="cc-n1001-42",
        )
    )
    store.put_effective_spec(_spec(2), codex_home=tmp_path, profile_fingerprint="fp")
    with pytest.raises(ValueError, match="rollback"):
        store.put_effective_spec(_spec(1), codex_home=tmp_path, profile_fingerprint="fp")

    conflicting = DesiredSpec.build(
        binding_id="binding-1",
        agent_instance_id="agent-1",
        project_id="project-1",
        canonical_cwd="/srv/project",
        provider="codex",
        account_profile="work",
        model="different",
        reasoning_effort="high",
        generation_id="gen-1",
        revision=2,
    )
    with pytest.raises(ValueError, match="different digest"):
        store.put_effective_spec(
            conflicting,
            codex_home=tmp_path,
            profile_fingerprint="fp",
        )
    with pytest.raises(ValueError, match="authoritative snapshot"):
        store.put_effective_spec(
            _spec(1, generation="gen-2"),
            codex_home=tmp_path,
            profile_fingerprint="fp",
        )


def test_authoritative_snapshot_changes_generation_and_requires_explicit_retirement(
    tmp_path: Path,
) -> None:
    store = RuntimeStore(tmp_path / "runtime.sqlite")
    first = RuntimeBinding(
        binding_id="binding-1",
        agent_instance_id="agent-1",
        chat_id=-1001,
        thread_id=42,
        project_id="project-1",
        canonical_cwd="/srv/project",
        expected_tmux_session="cc-n1001-42",
        config_digest="sha256:config",
    )
    store.upsert_binding(first)
    store.put_effective_spec(_spec(), codex_home=tmp_path, profile_fingerprint="fp")

    second = RuntimeBinding(
        binding_id="binding-2",
        agent_instance_id="agent-1",
        chat_id=-1001,
        thread_id=43,
        project_id="project-2",
        canonical_cwd="/srv/project-2",
        expected_tmux_session="cc-n1001-43",
        config_digest="sha256:config",
    )
    second_spec = DesiredSpec.build(
        binding_id="binding-2",
        agent_instance_id="agent-1",
        project_id="project-2",
        canonical_cwd="/srv/project-2",
        provider="codex",
        account_profile="work",
        model="gpt-test",
        reasoning_effort="high",
        generation_id="gen-2",
        revision=1,
    )
    generation_two = (
        (first, _spec(generation="gen-2"), tmp_path, "fp"),
        (second, second_spec, tmp_path, "fp"),
    )
    store.apply_authoritative_snapshot(
        agent_instance_id="agent-1",
        generation_id="gen-2",
        snapshot_digest="sha256:snapshot-1",
        updates=generation_two,
        retired_binding_ids=frozenset(),
    )
    assert store.get_effective_spec("binding-1")[0].generation_id == "gen-2"  # type: ignore[index]
    assert store.get_effective_spec("binding-2")[0].generation_id == "gen-2"  # type: ignore[index]

    with pytest.raises(ValueError, match="omits existing"):
        store.apply_authoritative_snapshot(
            agent_instance_id="agent-1",
            generation_id="gen-2",
            snapshot_digest="sha256:snapshot-2",
            updates=(generation_two[0],),
            retired_binding_ids=frozenset(),
        )

    store.apply_authoritative_snapshot(
        agent_instance_id="agent-1",
        generation_id="gen-2",
        snapshot_digest="sha256:snapshot-3",
        updates=(generation_two[0],),
        retired_binding_ids=frozenset({"binding-2"}),
    )
    assert store.get_binding("binding-2").configured is False  # type: ignore[union-attr]
    assert store.get_effective_spec("binding-2") is None


def test_session_provenance_is_immutable(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path / "runtime.sqlite")
    for binding_id in ("binding-1", "binding-2"):
        store.upsert_binding(
            RuntimeBinding(
                binding_id=binding_id,
                agent_instance_id="agent-1",
                chat_id=-1001,
                thread_id=1 if binding_id == "binding-1" else 2,
                project_id="project-1",
                canonical_cwd="/srv/project",
                expected_tmux_session=f"session-{binding_id}",
            )
        )
    store.record_session_provenance(
        session_id="session-id",
        binding_id="binding-1",
        account_profile="work",
    )
    with pytest.raises(ValueError, match="immutable"):
        store.record_session_provenance(
            session_id="session-id",
            binding_id="binding-2",
            account_profile="work",
        )


def test_status_distinguishes_unverified_pending_and_exited_instances(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path / "runtime.sqlite")
    binding = RuntimeBinding(
        binding_id="binding-1",
        agent_instance_id="agent-1",
        chat_id=-1001,
        thread_id=42,
        project_id="project-1",
        canonical_cwd="/srv/project",
        expected_tmux_session="cc-n1001-42",
    )
    store.upsert_binding(binding)
    spec = _spec()
    store.put_effective_spec(spec, codex_home=tmp_path, profile_fingerprint="fp")
    store.record_instance(
        instance_id="instance-1",
        binding_id="binding-1",
        wrapper_pid=100,
        wrapper_starttime=10,
        child_pid=101,
        child_starttime=11,
        tmux_session="cc-n1001-42",
        tmux_pane="%1",
        lifecycle="IDLE",
        applied_digest=None,
        heartbeat_at=time.time(),
    )
    report = store.status_report("agent-1")
    assert report["bindings"][0]["health"] == "OUT_OF_SYNC"  # type: ignore[index]

    store.record_applied_spec(
        instance_id="instance-1",
        binding_id="binding-1",
        spec=spec,
        evidence={"verified": True},
        session_id="session-1",
        account_profile="work",
        rollout_path="/rollout",
        rollout_device=1,
        rollout_inode=2,
    )
    assert store.status_report("agent-1")["bindings"][0]["health"] == (  # type: ignore[index]
        "RUNTIME_HEALTHY"
    )
    store.put_effective_spec(_spec(2), codex_home=tmp_path, profile_fingerprint="fp")
    assert store.status_report("agent-1")["bindings"][0]["health"] == (  # type: ignore[index]
        "PENDING_CHANGE"
    )

    store.mark_instance_exited(
        instance_id="instance-1",
        binding_id="binding-1",
        wrapper_pid=100,
        wrapper_starttime=10,
        child_pid=101,
        child_starttime=11,
    )
    final = store.status_report("agent-1")["bindings"][0]  # type: ignore[index]
    assert final["health"] == "RUNTIME_HEALTHY"  # type: ignore[index]
    assert final["instances"][0]["lifecycle"] == "EXITED"  # type: ignore[index]


def test_absent_provisional_wrapper_becomes_terminal_start_failure(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path / "runtime.sqlite")
    binding = RuntimeBinding(
        binding_id="binding-1",
        agent_instance_id="agent-1",
        chat_id=-1001,
        thread_id=42,
        project_id="project-1",
        canonical_cwd="/srv/project",
        expected_tmux_session="cc-n1001-42",
    )
    store.upsert_binding(binding)
    store.put_effective_spec(_spec(), codex_home=tmp_path, profile_fingerprint="fp")
    store.record_instance(
        instance_id="instance-1",
        binding_id="binding-1",
        wrapper_pid=100,
        wrapper_starttime=10,
        child_pid=None,
        child_starttime=None,
        tmux_session="cc-n1001-42",
        tmux_pane="%1",
        lifecycle="PROVISIONAL_STARTING",
        applied_digest=None,
    )

    assert store.active_wrapper_identities() == (
        ("instance-1", 100, 10, os.getuid(), "PROVISIONAL_STARTING"),
    )
    assert store.mark_instance_wrapper_absent(
        instance_id="instance-1",
        wrapper_pid=100,
        wrapper_starttime=10,
    )
    report = store.status_report("agent-1")["bindings"][0]  # type: ignore[index]
    assert report["instances"][0]["lifecycle"] == "START_FAILED"  # type: ignore[index]
    assert report["health"] == "RUNTIME_HEALTHY"  # type: ignore[index]


def test_retirement_candidate_survives_late_child_exit_notification(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path / "runtime.sqlite")
    binding = RuntimeBinding(
        binding_id="binding-1",
        agent_instance_id="agent-1",
        chat_id=-1001,
        thread_id=42,
        project_id="project-1",
        canonical_cwd="/srv/project",
        expected_tmux_session="cc-n1001-42",
    )
    store.upsert_binding(binding)
    applied = _spec()
    store.put_effective_spec(applied, codex_home=tmp_path, profile_fingerprint="fp")
    store.record_instance(
        instance_id="instance-1",
        binding_id="binding-1",
        wrapper_pid=100,
        wrapper_starttime=10,
        child_pid=101,
        child_starttime=11,
        tmux_session="cc-n1001-42",
        tmux_pane="%1",
        lifecycle="IDLE",
        applied_digest=None,
        heartbeat_at=time.time(),
    )
    store.record_applied_spec(
        instance_id="instance-1",
        binding_id="binding-1",
        spec=applied,
        evidence={"verified": True},
        session_id="session-1",
        account_profile="work",
        rollout_path="/rollout",
        rollout_device=1,
        rollout_inode=2,
    )
    next_spec = _spec(2)
    store.put_effective_spec(next_spec, codex_home=tmp_path, profile_fingerprint="fp")

    store.record_instance(
        instance_id="instance-2",
        binding_id="binding-1",
        wrapper_pid=200,
        wrapper_starttime=20,
        child_pid=201,
        child_starttime=21,
        tmux_session="cc-n1001-42-duplicate",
        tmux_pane="%2",
        lifecycle="IDLE",
        applied_digest=None,
        heartbeat_at=time.time(),
    )
    assert store.retirement_candidates() == ()
    store.mark_instance_exited(
        instance_id="instance-2",
        binding_id="binding-1",
        wrapper_pid=200,
        wrapper_starttime=20,
        child_pid=201,
        child_starttime=21,
    )

    candidates = store.retirement_candidates()
    assert len(candidates) == 1
    store.mark_instance_retired(candidates[0])
    store.mark_instance_exited(
        instance_id="instance-1",
        binding_id="binding-1",
        wrapper_pid=100,
        wrapper_starttime=10,
        child_pid=101,
        child_starttime=11,
    )

    report = store.status_report("agent-1")["bindings"][0]  # type: ignore[index]
    assert report["health"] == "PENDING_CHANGE"  # type: ignore[index]
    assert report["instances"][0]["lifecycle"] == "RETIRED_FOR_RESTART"  # type: ignore[index]

    store.record_instance(
        instance_id="instance-3",
        binding_id="binding-1",
        wrapper_pid=300,
        wrapper_starttime=30,
        child_pid=301,
        child_starttime=31,
        tmux_session="cc-n1001-42-new",
        tmux_pane="%3",
        lifecycle="IDLE",
        applied_digest=None,
        heartbeat_at=time.time(),
    )
    store.record_applied_spec(
        instance_id="instance-3",
        binding_id="binding-1",
        spec=next_spec,
        evidence={"verified": True},
        session_id="session-3",
        account_profile="work",
        rollout_path="/rollout-3",
        rollout_device=3,
        rollout_inode=4,
    )

    relaunched = store.status_report("agent-1")["bindings"][0]  # type: ignore[index]
    assert relaunched["health"] == "RUNTIME_HEALTHY"  # type: ignore[index]


def test_stale_live_instance_heartbeat_cannot_report_healthy(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path / "runtime.sqlite")
    binding = RuntimeBinding(
        binding_id="binding-1",
        agent_instance_id="agent-1",
        chat_id=-1001,
        thread_id=42,
        project_id="project-1",
        canonical_cwd="/srv/project",
        expected_tmux_session="cc-n1001-42",
    )
    store.upsert_binding(binding)
    spec = _spec()
    store.put_effective_spec(spec, codex_home=tmp_path, profile_fingerprint="fp")
    store.record_instance(
        instance_id="instance-1",
        binding_id="binding-1",
        wrapper_pid=100,
        wrapper_starttime=10,
        child_pid=101,
        child_starttime=11,
        tmux_session="cc-n1001-42",
        tmux_pane="%1",
        lifecycle="IDLE",
        applied_digest=None,
        heartbeat_at=time.time() - 11,
    )
    store.record_applied_spec(
        instance_id="instance-1",
        binding_id="binding-1",
        spec=spec,
        evidence={"verified": True},
        session_id="session-1",
        account_profile="work",
        rollout_path="/rollout",
        rollout_device=1,
        rollout_inode=2,
    )

    instance = store.status_report("agent-1")["bindings"][0]  # type: ignore[index]
    assert instance["health"] == "OUT_OF_SYNC"  # type: ignore[index]
    assert instance["instances"][0]["heartbeat_stale"] is True  # type: ignore[index]
