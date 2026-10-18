import asyncio
import contextlib
import hashlib
import json
import os
import uuid
from dataclasses import replace
from pathlib import Path

import pytest

from ai_agent_runtime.config import RuntimeConfig
from ai_agent_runtime.daemon import server as server_module
from ai_agent_runtime.daemon.server import RuntimeServer
from ai_agent_runtime.domain import DesiredSpec, ManagementMode, RuntimeBinding, sha256_digest
from ai_agent_runtime.errors import ErrorCode, RuntimeFailureError
from ai_agent_runtime.ipc.client import request
from ai_agent_runtime.ipc.protocol import versioned
from ai_agent_runtime.process_safety import proc_starttime, profile_fingerprint
from ai_agent_runtime.telegram_upstream import topic_config as topic_config_adapter
from ai_agent_runtime.telegram_upstream.topic_config import read_topic_config


def test_prepare_dispatch_blocks_old_model_until_idle_recycle(tmp_path: Path) -> None:
    config = replace(
        RuntimeConfig.defaults(),
        agent_instance_id="agent-1",
        forum_chat_id=-1001,
        state_dir=tmp_path / "state",
        database_path=tmp_path / "state/runtime.sqlite",
        socket_path=tmp_path / "run/runtime.sock",
    )
    daemon = RuntimeServer(config)
    binding = RuntimeBinding(
        binding_id="binding-1",
        agent_instance_id="agent-1",
        chat_id=-1001,
        thread_id=42,
        project_id="project-1",
        canonical_cwd="/srv/project",
        expected_tmux_session="cc-n1001-42",
    )
    daemon.store.upsert_binding(binding)
    first = DesiredSpec.build(
        binding_id=binding.binding_id,
        agent_instance_id=binding.agent_instance_id,
        project_id=binding.project_id,
        canonical_cwd=binding.canonical_cwd,
        provider="codex",
        account_profile="work",
        model="gpt-old",
        reasoning_effort="high",
        generation_id="gen-1",
        revision=1,
    )
    daemon.store.put_effective_spec(first, codex_home=tmp_path, profile_fingerprint="fp")
    payload = {"agent_instance_id": "agent-1", "chat_id": -1001, "thread_id": 42}

    stopped = daemon._prepare_dispatch("stopped", payload)
    assert stopped["decision"] == "START_REQUIRED"
    assert stopped["desired"] == {"provider": "codex", "model": "gpt-old"}

    daemon.store.record_instance(
        instance_id="instance-1",
        binding_id=binding.binding_id,
        wrapper_pid=100,
        wrapper_starttime=10,
        child_pid=101,
        child_starttime=11,
        tmux_session=binding.expected_tmux_session,
        tmux_pane="%1",
        lifecycle="BUSY",
        applied_digest=first.spec_digest,
        launch_spec={"desired": {"spec_digest": first.spec_digest}},
    )
    daemon.store.record_applied_spec(
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
    assert daemon._prepare_dispatch("matching", payload)["decision"] == "ALLOW"

    second = DesiredSpec.build(
        binding_id=binding.binding_id,
        agent_instance_id=binding.agent_instance_id,
        project_id=binding.project_id,
        canonical_cwd=binding.canonical_cwd,
        provider="codex",
        account_profile="work",
        model="gpt-new",
        reasoning_effort="xhigh",
        generation_id="gen-1",
        revision=2,
    )
    daemon.store.put_effective_spec(second, codex_home=tmp_path, profile_fingerprint="fp")
    assert daemon._prepare_dispatch("busy", payload)["decision"] == "WAIT"

    daemon.store.record_instance(
        instance_id="instance-1",
        binding_id=binding.binding_id,
        wrapper_pid=100,
        wrapper_starttime=10,
        child_pid=101,
        child_starttime=11,
        tmux_session=binding.expected_tmux_session,
        tmux_pane="%1",
        lifecycle="IDLE",
        applied_digest=first.spec_digest,
    )
    idle = daemon._prepare_dispatch("idle", payload)
    assert idle["decision"] == "RECYCLE_REQUIRED"
    assert idle["desired"] == {"provider": "codex", "model": "gpt-new"}


@pytest.mark.usefixtures("allow_hosted_runner_python")
@pytest.mark.asyncio
@pytest.mark.parametrize("switched_name", [False, True])
@pytest.mark.parametrize("config_changed", [False, True])
async def test_clear_waits_for_new_upstream_state(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, switched_name: bool, config_changed: bool
) -> None:
    project = tmp_path / "project"
    project.mkdir(mode=0o700)
    profile = tmp_path / "profiles" / "work"
    profile.mkdir(parents=True, mode=0o700)
    (profile / "auth.json").write_text("{}", encoding="utf-8")
    (profile / "auth.json").chmod(0o600)
    real_codex = tmp_path / "codex-real"
    real_codex.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    real_codex.chmod(0o700)
    topic_config = tmp_path / "topic_config.json"
    topic_config.write_text(
        json.dumps(
            {"topics": {"42": {"cwd": str(project), "engine": "codex", "exec_mode": "tmux"}}}
        ),
        encoding="utf-8",
    )
    tmux_name = "cc-n1001-42-switch-18dba53c902bb319" if switched_name else "cc-n1001-42"
    tmux_state = tmp_path / "tmux_state.json"

    def write_state(session_id: str | None) -> None:
        tmux_state.write_text(
            json.dumps(
                {
                    "-1001:42": {
                        "session_name": tmux_name,
                        "cwd": str(project),
                        "provider": "codex",
                        "session_id": session_id,
                    }
                }
            ),
            encoding="utf-8",
        )

    write_state("old-session")
    config = RuntimeConfig(
        agent_instance_id="agent-1",
        forum_chat_id=-1001,
        state_dir=tmp_path / "runtime-state",
        socket_path=tmp_path / "run/runtime.sock",
        database_path=tmp_path / "runtime-state/runtime.sqlite",
        real_codex=real_codex,
        real_codex_version="test-1",
        real_codex_sha256=f"sha256:{hashlib.sha256(real_codex.read_bytes()).hexdigest()}",
        profiles_root=tmp_path / "profiles",
        topic_config_path=topic_config,
        tmux_state_path=tmux_state,
        upstream_sha="0" * 40,
        allowed_project_roots=(tmp_path,),
    )
    daemon = RuntimeServer(
        config,
        phase_b_timeout=30.0,
        identity_verifier=lambda _identity: None,
        child_verifier=lambda **_kwargs: {},
    )
    binding = RuntimeBinding(
        binding_id="binding-1",
        agent_instance_id="agent-1",
        chat_id=-1001,
        thread_id=42,
        project_id="project-1",
        canonical_cwd=str(project),
        expected_tmux_session="cc-n1001-42",
        config_digest=read_topic_config(topic_config).digest,
    )
    daemon.store.upsert_binding(binding)
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
        source_kind="cus_ui",
        source_ref="user:1",
    )
    daemon.store.put_effective_spec(
        spec,
        codex_home=profile,
        profile_fingerprint=profile_fingerprint(profile),
    )
    pid = os.getpid()
    identity = {
        "wrapper": {"pid": pid, "starttime": proc_starttime(pid), "uid": os.getuid()},
        "cwd": str(project),
        "tmux_session": tmux_name,
        "tmux_pane": "%1",
    }
    registration = daemon._register(
        str(uuid.uuid4()),
        {"agent_instance_id": "agent-1", "resume_session_id": None, **identity},
        pid,
        os.getuid(),
    )
    assert registration["ok"] is True
    registered = daemon.store.get_registered_instance(registration["instance_id"])
    assert registered is not None
    assert registered.launch_spec["replaced_session_id"] == "old-session"

    saw_old_state = asyncio.Event()
    original_reader = server_module.read_tmux_states

    def observe_old_state(path: Path):  # type: ignore[no-untyped-def]
        states = original_reader(path)
        if any(state.session_id == "old-session" for state in states):
            saw_old_state.set()
        return states

    monkeypatch.setattr(server_module, "read_tmux_states", observe_old_state)

    verification = asyncio.create_task(
        daemon._verify_started(
            str(uuid.uuid4()),
            {
                "agent_instance_id": "agent-1",
                "instance_id": registration["instance_id"],
                "binding_id": "binding-1",
                "child_pid": pid,
                "child_starttime": proc_starttime(pid),
                **identity,
            },
            pid,
            os.getuid(),
        )
    )
    await asyncio.wait_for(saw_old_state.wait(), timeout=30.0)
    assert not verification.done()
    write_state(None)
    if config_changed:
        topic_config.write_text(
            json.dumps(
                {
                    "topics": {
                        "42": {
                            "cwd": str(project),
                            "engine": "codex",
                            "exec_mode": "tmux",
                            "name": "Renamed during Phase B",
                        }
                    }
                }
            ),
            encoding="utf-8",
        )
        with pytest.raises(RuntimeFailureError) as drift:
            await verification
        assert drift.value.code is ErrorCode.CONFIG_DRIFT
        assert daemon.store.get_applied_evidence(registration["instance_id"]) is None
        return
    verified = await verification
    assert verified["ok"] is True
    assert verified["verified"] is True
    assert verified["session_id"] is None


@pytest.mark.usefixtures("allow_hosted_runner_python")
def test_resume_requires_same_binding_even_with_same_cwd_and_account(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir(mode=0o700)
    profile = tmp_path / "profiles" / "work"
    profile.mkdir(parents=True, mode=0o700)
    topic_config = tmp_path / "topic_config.json"
    topic_config.write_text(
        json.dumps(
            {
                "topics": {
                    str(thread): {
                        "cwd": str(project),
                        "engine": "codex",
                        "exec_mode": "tmux",
                    }
                    for thread in (42, 43)
                }
            }
        ),
        encoding="utf-8",
    )
    tmux_state = tmp_path / "tmux_state.json"
    tmux_state.write_text("{}", encoding="utf-8")
    real_codex = tmp_path / "codex-real"
    real_codex.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    real_codex.chmod(0o700)
    config = RuntimeConfig(
        agent_instance_id="agent-1",
        forum_chat_id=-1001,
        state_dir=tmp_path / "runtime-state",
        socket_path=tmp_path / "run/runtime.sock",
        database_path=tmp_path / "runtime-state/runtime.sqlite",
        real_codex=real_codex,
        real_codex_version="test-1",
        real_codex_sha256=f"sha256:{hashlib.sha256(real_codex.read_bytes()).hexdigest()}",
        profiles_root=tmp_path / "profiles",
        topic_config_path=topic_config,
        tmux_state_path=tmux_state,
        upstream_sha="0" * 40,
        allowed_project_roots=(tmp_path,),
    )
    daemon = RuntimeServer(config, identity_verifier=lambda _identity: None)
    try:
        digest = read_topic_config(topic_config).digest
        for thread in (42, 43):
            binding_id = f"binding-{thread}"
            daemon.store.upsert_binding(
                RuntimeBinding(
                    binding_id=binding_id,
                    agent_instance_id="agent-1",
                    chat_id=-1001,
                    thread_id=thread,
                    project_id="project-1",
                    canonical_cwd=str(project),
                    expected_tmux_session=f"cc-n1001-{thread}",
                    config_digest=digest,
                )
            )
            daemon.store.put_effective_spec(
                DesiredSpec.build(
                    binding_id=binding_id,
                    agent_instance_id="agent-1",
                    project_id="project-1",
                    canonical_cwd=str(project),
                    provider="codex",
                    account_profile="work",
                    model="gpt-test",
                    reasoning_effort="high",
                    generation_id="gen-1",
                    revision=1,
                ),
                codex_home=profile,
                profile_fingerprint="test-profile",
            )
        daemon.store.record_session_provenance(
            session_id="owned-by-42",
            binding_id="binding-42",
            account_profile="work",
        )
        pid = os.getpid()
        identity = {
            "wrapper": {"pid": pid, "starttime": proc_starttime(pid), "uid": os.getuid()},
            "cwd": str(project),
            "tmux_pane": "%1",
        }
        allowed = daemon._register(
            str(uuid.uuid4()),
            {
                "agent_instance_id": "agent-1",
                "resume_session_id": "owned-by-42",
                "tmux_session": "cc-n1001-42",
                **identity,
            },
            pid,
            os.getuid(),
        )
        assert allowed["binding_id"] == "binding-42"

        with pytest.raises(RuntimeFailureError) as rejected:
            daemon._register(
                str(uuid.uuid4()),
                {
                    "agent_instance_id": "agent-1",
                    "resume_session_id": "owned-by-42",
                    "tmux_session": "cc-n1001-43",
                    **identity,
                },
                pid,
                os.getuid(),
            )
        assert rejected.value.code is ErrorCode.RESUME_BINDING_MISMATCH
        assert daemon.store.session_owner("owned-by-42") == ("binding-42", "work")
        notices = daemon.store.claim_outbox(now=100.0)
        assert len(notices) == 1
        assert notices[0]["payload"]["notification_code"] == "RESUME_BINDING_MISMATCH"
        assert notices[0]["binding_id"] == "binding-43"
    finally:
        daemon.store.close()


@pytest.mark.usefixtures("allow_hosted_runner_python")
@pytest.mark.asyncio
async def test_real_unix_socket_phase_a_then_phase_b(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir(mode=0o700)
    profiles = tmp_path / "profiles"
    profile = profiles / "work"
    profile.mkdir(parents=True, mode=0o700)
    real_codex = tmp_path / "codex-real"
    real_codex.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    real_codex.chmod(0o700)
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
    tmux_state = tmp_path / "tmux_state.json"
    config = RuntimeConfig(
        agent_instance_id="agent-1",
        forum_chat_id=-1001,
        state_dir=tmp_path / "runtime-state",
        socket_path=tmp_path / "run" / "runtime.sock",
        database_path=tmp_path / "runtime-state" / "runtime.sqlite",
        real_codex=real_codex,
        real_codex_version="test-1",
        real_codex_sha256=f"sha256:{hashlib.sha256(real_codex.read_bytes()).hexdigest()}",
        profiles_root=profiles,
        topic_config_path=topic_config,
        tmux_state_path=tmux_state,
        upstream_sha="0" * 40,
        allowed_project_roots=(tmp_path,),
    )
    daemon = RuntimeServer(
        config,
        phase_b_timeout=1.0,
        identity_verifier=lambda _identity: None,
        child_verifier=lambda **kwargs: {
            "actual_codex_home": str(kwargs["codex_home"]),
            "actual_model": kwargs["model"],
            "actual_reasoning_effort": kwargs["reasoning_effort"],
        },
        evidence_interval=0.01,
    )
    binding = RuntimeBinding(
        binding_id="binding-1",
        agent_instance_id="agent-1",
        chat_id=-1001,
        thread_id=42,
        project_id="project-1",
        canonical_cwd=str(project),
        expected_tmux_session="cc-n1001-42",
        config_digest=read_topic_config(topic_config).digest,
    )
    daemon.store.upsert_binding(binding)
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
        source_kind="cus_ui",
        source_ref="user:1",
    )
    daemon.store.put_effective_spec(
        spec,
        codex_home=profile,
        profile_fingerprint=profile_fingerprint(profile),
    )

    server_task = asyncio.create_task(daemon.serve())
    for _ in range(100):
        if config.socket_path.exists():
            break
        await asyncio.sleep(0.01)
    assert config.socket_path.exists()
    pid = os.getpid()
    identity = {
        "wrapper": {"pid": pid, "starttime": proc_starttime(pid), "uid": os.getuid()},
        "cwd": str(project),
        "tmux_session": "cc-n1001-42",
        "tmux_pane": "%1",
    }
    registration = await request(
        config.socket_path,
        versioned(
            "register",
            str(uuid.uuid4()),
            agent_instance_id="agent-1",
            resume_session_id=None,
            **identity,
        ),
    )
    assert registration["ok"] is True
    assert registration["binding_id"] == "binding-1"
    assert registration["spec"]["spec_digest"] == spec.spec_digest

    rollout = tmp_path / "rollout.jsonl"
    rollout.write_text("{}\n", encoding="utf-8")
    rollout.chmod(0o600)
    tmux_state.write_text(
        json.dumps(
            {
                "-1001:42": {
                    "session_name": "cc-n1001-42",
                    "cwd": str(project),
                    "provider": "codex",
                    "session_id": "session-1",
                    "transcript_path": str(rollout),
                }
            }
        ),
        encoding="utf-8",
    )
    verified = await request(
        config.socket_path,
        versioned(
            "verify_started",
            str(uuid.uuid4()),
            agent_instance_id="agent-1",
            instance_id=registration["instance_id"],
            binding_id="binding-1",
            child_pid=pid,
            child_starttime=proc_starttime(pid),
            **identity,
        ),
    )
    assert verified["ok"] is True
    assert verified["applied"] is False
    assert verified["session_id"] == "session-1"

    def durable_result_count() -> int:
        row = daemon.store._connection.execute("SELECT COUNT(*) FROM request_results").fetchone()
        assert row is not None
        return int(row[0])

    assert durable_result_count() == 2  # register and verify_started
    for _ in range(100):
        if daemon.store.session_owner("session-1") is not None:
            break
        await asyncio.sleep(0.01)
    assert daemon.store.session_owner("session-1") == ("binding-1", "work")
    evidence = daemon.store.get_applied_evidence(registration["instance_id"])
    assert evidence is not None
    assert evidence["rollout_inode"] == rollout.stat().st_ino
    assert evidence["wrapper_identity"]["uid"] == os.getuid()
    assert str(evidence["wrapper_identity"]["executable"]["sha256"]).startswith("sha256:")
    applied_status = daemon.store.status_report("agent-1")["bindings"][0]["instances"][0][  # type: ignore[index]
        "applied"
    ]
    assert applied_status["effective"]["account_profile"] == "work"
    assert applied_status["effective"]["actual_model"] == "gpt-test"
    assert applied_status["effective"]["actual_reasoning_effort"] == "high"
    assert applied_status["effective"]["session_id"] == "session-1"
    assert applied_status["effective"]["rollout_inode"] == rollout.stat().st_ino

    # A daemon restart can happen before the wrapper's first heartbeat. The
    # persisted launch generation is still 0, while the first heartbeat is 1.
    launch_boot_id = daemon.boot_id
    daemon.boot_id = "restarted-before-first-heartbeat"
    first_after_restart = await request(
        config.socket_path,
        versioned(
            "heartbeat",
            str(uuid.uuid4()),
            agent_instance_id="agent-1",
            instance_id=registration["instance_id"],
            binding_id="binding-1",
            child_pid=pid,
            child_starttime=proc_starttime(pid),
            connection_generation=1,
            previous_daemon_boot_id=launch_boot_id,
            **identity,
        ),
    )
    assert first_after_restart["ok"] is True
    assert first_after_restart["resynchronized"] is True
    assert first_after_restart["connection_generation"] == 1

    heartbeat = await request(
        config.socket_path,
        versioned(
            "heartbeat",
            str(uuid.uuid4()),
            agent_instance_id="agent-1",
            instance_id=registration["instance_id"],
            binding_id="binding-1",
            child_pid=pid,
            child_starttime=proc_starttime(pid),
            connection_generation=1,
            previous_daemon_boot_id=daemon.boot_id,
            **identity,
        ),
    )
    assert heartbeat["ok"] is True
    assert heartbeat["resynchronized"] is False
    assert heartbeat["connection_generation"] == 1
    live_status = daemon.store.status_report("agent-1")["bindings"][0]  # type: ignore[index]
    assert live_status["instances"][0]["last_heartbeat_at"] is not None  # type: ignore[index]
    assert live_status["instances"][0]["daemon_boot_id"] == daemon.boot_id  # type: ignore[index]
    assert live_status["instances"][0]["connection_generation"] == 1  # type: ignore[index]

    previous_boot_id = daemon.boot_id
    daemon.boot_id = "new-daemon-boot-id"
    resynchronized = await request(
        config.socket_path,
        versioned(
            "heartbeat",
            str(uuid.uuid4()),
            agent_instance_id="agent-1",
            instance_id=registration["instance_id"],
            binding_id="binding-1",
            child_pid=pid,
            child_starttime=proc_starttime(pid),
            connection_generation=1,
            previous_daemon_boot_id=previous_boot_id,
            **identity,
        ),
    )
    assert resynchronized["ok"] is True
    assert resynchronized["resynchronized"] is True
    assert resynchronized["daemon_boot_id"] == daemon.boot_id
    assert resynchronized["connection_generation"] == 1

    # The daemon may have persisted the resync while its ACK was lost. A retry
    # with the same exact process identity and generation must return the new
    # boot ID, not strand the wrapper forever.
    lost_ack_retry = await request(
        config.socket_path,
        versioned(
            "heartbeat",
            str(uuid.uuid4()),
            agent_instance_id="agent-1",
            instance_id=registration["instance_id"],
            binding_id="binding-1",
            child_pid=pid,
            child_starttime=proc_starttime(pid),
            connection_generation=1,
            previous_daemon_boot_id=previous_boot_id,
            **identity,
        ),
    )
    assert lost_ack_retry["ok"] is True
    assert lost_ack_retry["resynchronized"] is True

    post_ack = await request(
        config.socket_path,
        versioned(
            "heartbeat",
            str(uuid.uuid4()),
            agent_instance_id="agent-1",
            instance_id=registration["instance_id"],
            binding_id="binding-1",
            child_pid=pid,
            child_starttime=proc_starttime(pid),
            connection_generation=2,
            previous_daemon_boot_id=daemon.boot_id,
            **identity,
        ),
    )
    assert post_ack["ok"] is True

    stale_boot = await request(
        config.socket_path,
        versioned(
            "heartbeat",
            str(uuid.uuid4()),
            agent_instance_id="agent-1",
            instance_id=registration["instance_id"],
            binding_id="binding-1",
            child_pid=pid,
            child_starttime=proc_starttime(pid),
            connection_generation=2,
            previous_daemon_boot_id=previous_boot_id,
            **identity,
        ),
    )
    assert stale_boot["ok"] is False
    assert stale_boot["error"]["code"] == "CORRELATION_MISMATCH"

    # A second restart can occur after the wrapper advanced to generation 3
    # but before that generation has been persisted by the daemon.
    second_previous_boot_id = daemon.boot_id
    daemon.boot_id = "another-rapid-daemon-restart"
    rapid_restart = await request(
        config.socket_path,
        versioned(
            "heartbeat",
            str(uuid.uuid4()),
            agent_instance_id="agent-1",
            instance_id=registration["instance_id"],
            binding_id="binding-1",
            child_pid=pid,
            child_starttime=proc_starttime(pid),
            connection_generation=3,
            previous_daemon_boot_id=second_previous_boot_id,
            **identity,
        ),
    )
    assert rapid_restart["ok"] is True
    assert rapid_restart["resynchronized"] is True
    assert durable_result_count() == 2  # heartbeats never grow the durable cache

    wrong_heartbeat = await request(
        config.socket_path,
        versioned(
            "heartbeat",
            str(uuid.uuid4()),
            agent_instance_id="agent-1",
            instance_id=registration["instance_id"],
            binding_id="binding-1",
            child_pid=pid,
            child_starttime=proc_starttime(pid) + 1,
            connection_generation=1,
            previous_daemon_boot_id=verified["daemon_boot_id"],
            **identity,
        ),
    )
    assert wrong_heartbeat["ok"] is False
    assert wrong_heartbeat["error"]["code"] == "CORRELATION_MISMATCH"

    mismatch_request_id = str(uuid.uuid4())
    resume_mismatch = await request(
        config.socket_path,
        versioned(
            "register",
            mismatch_request_id,
            agent_instance_id="agent-1",
            resume_session_id="foreign-session",
            **identity,
        ),
    )
    assert resume_mismatch["ok"] is False
    assert resume_mismatch["error"]["code"] == "RESUME_BINDING_MISMATCH"
    notification_events = daemon.store.claim_outbox(now=100.0)
    assert len(notification_events) == 1
    assert notification_events[0]["kind"] == "USER_NOTIFICATION_REQUIRED"
    assert notification_events[0]["payload"]["notification_code"] == ("RESUME_BINDING_MISMATCH")
    daemon.store.acknowledge_outbox(notification_events[0]["event_id"], "ACKED")

    exited = await request(
        config.socket_path,
        versioned(
            "child_exited",
            str(uuid.uuid4()),
            agent_instance_id="agent-1",
            instance_id=registration["instance_id"],
            binding_id="binding-1",
            child_pid=pid,
            child_starttime=proc_starttime(pid),
            exit_code=0,
            **identity,
        ),
    )
    assert exited["ok"] is True
    assert durable_result_count() == 3  # child_exited must survive a lost ACK
    status = await request(
        config.socket_path,
        versioned("status", str(uuid.uuid4())),
    )
    instance_status = status["status"]["bindings"][0]["instances"][0]
    assert instance_status["lifecycle"] == "EXITED"
    assert status["status"]["control_plane_sync"]["state"] == "DEGRADED_CUS_SYNC"
    assert status["status"]["bindings"][0]["health"] == "DEGRADED_CUS_SYNC"
    assert status["status"]["topic_discovery"]["status"] == "OK"
    assert status["status"]["topic_discovery"]["unbound_topics"] == []

    control_plane = await request(
        config.socket_path,
        versioned(
            "control_plane_heartbeat",
            str(uuid.uuid4()),
            agent_instance_id="agent-1",
        ),
    )
    assert control_plane["ok"] is True
    assert control_plane["sync_state"] == "IN_SYNC"
    synced_status = await request(
        config.socket_path,
        versioned("status", str(uuid.uuid4())),
    )
    assert synced_status["status"]["control_plane_sync"]["state"] == "IN_SYNC"
    assert synced_status["status"]["bindings"][0]["health"] == "RUNTIME_HEALTHY"
    empty_pull = await request(
        config.socket_path,
        versioned("outbox_pull", str(uuid.uuid4()), limit=50),
    )
    assert empty_pull["ok"] is True
    assert empty_pull["events"] == []
    assert durable_result_count() == 3  # repeatable polls never grow the durable cache

    topic_config.write_text(
        json.dumps(
            {
                "topics": {
                    "42": {
                        "cwd": str(project),
                        "engine": "codex",
                        "exec_mode": "subprocess",
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    daemon._reconcile_topic_config_once()
    unsupported = daemon.store.claim_outbox(now=1000.0)
    assert len(unsupported) == 1
    assert unsupported[0]["payload"]["notification_code"] == ("UNSUPPORTED_CODEX_SUBPROCESS")

    topic_config.write_text(
        json.dumps(
            {
                "topics": {
                    "42": {
                        "cwd": str(tmp_path / "unexpected-project"),
                        "engine": "codex",
                        "exec_mode": "tmux",
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    daemon._reconcile_topic_config_once()
    drifted = daemon.store.get_binding("binding-1")
    assert drifted is not None
    assert drifted.management_mode == ManagementMode.CONFIG_DRIFT
    assert daemon.store.status_report("agent-1")["bindings"][0]["health"] == (  # type: ignore[index]
        "CONFIG_DRIFT"
    )

    server_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await server_task


@pytest.mark.asyncio
async def test_apply_binding_is_durable_idempotent_and_never_uses_default_codex(
    tmp_path: Path,
) -> None:
    project = tmp_path / "project"
    project.mkdir(mode=0o700)
    profiles = tmp_path / "profiles"
    profile = profiles / "work"
    profile.mkdir(parents=True, mode=0o700)
    (profile / "auth.json").write_text("{}", encoding="utf-8")
    (profile / "auth.json").chmod(0o600)
    real_codex = tmp_path / "codex-real"
    real_codex.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    real_codex.chmod(0o700)
    topic_config = tmp_path / "topic_config.json"
    topic_config.write_text(
        json.dumps(
            {
                "topics": {
                    "42": {
                        "cwd": str(tmp_path),
                        "engine": "claude",
                        "exec_mode": "subprocess",
                        "unknown": "preserve-me",
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    config = RuntimeConfig(
        agent_instance_id="agent-1",
        forum_chat_id=-1001,
        state_dir=tmp_path / "runtime-state",
        socket_path=tmp_path / "run" / "runtime.sock",
        database_path=tmp_path / "runtime-state" / "runtime.sqlite",
        real_codex=real_codex,
        real_codex_version="test-1",
        real_codex_sha256=f"sha256:{hashlib.sha256(real_codex.read_bytes()).hexdigest()}",
        profiles_root=profiles,
        topic_config_path=topic_config,
        tmux_state_path=tmp_path / "tmux_state.json",
        upstream_sha="0" * 40,
        allowed_project_roots=(tmp_path,),
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
        source_kind="cus_ui",
        source_ref="user:1",
    )
    desired_payload = {
        "binding_id": spec.binding_id,
        "agent_instance_id": spec.agent_instance_id,
        "project_id": spec.project_id,
        "canonical_cwd": spec.canonical_cwd,
        "provider": spec.provider,
        "account_profile": spec.account_profile,
        "model": spec.model,
        "reasoning_effort": spec.reasoning_effort,
        "generation_id": spec.generation_id,
        "revision": spec.revision,
        "spec_digest": spec.spec_digest,
        "source_kind": spec.source_kind,
        "source_ref": spec.source_ref,
    }
    request_id = str(uuid.uuid4())
    payload = versioned(
        "apply_binding",
        request_id,
        binding={
            "binding_id": "binding-1",
            "agent_instance_id": "agent-1",
            "chat_id": -1001,
            "thread_id": 42,
            "project_id": "project-1",
            "canonical_cwd": str(project),
        },
        desired=desired_payload,
    )

    daemon = RuntimeServer(
        config,
        identity_verifier=lambda _identity: None,
        child_verifier=lambda **_kwargs: {},
    )
    server_task = asyncio.create_task(daemon.serve())
    for _ in range(100):
        if config.socket_path.exists():
            break
        await asyncio.sleep(0.01)
    applied = await request(config.socket_path, payload)
    assert applied["ok"] is True
    persisted = daemon.store.get_effective_spec("binding-1")
    assert persisted is not None
    assert persisted[0].source_kind == "cus_ui"
    assert persisted[0].source_ref == "user:1"
    topic = read_topic_config(topic_config).topic(42)
    assert topic is not None
    assert topic.cwd == str(project)
    assert topic.is_managed_candidate
    assert topic.raw["unknown"] == "preserve-me"
    backups = list((config.state_dir / "topic-config-backups").iterdir())
    assert len(backups) == 1

    before_stale = read_topic_config(topic_config).digest
    stale_spec = DesiredSpec.build(
        binding_id="binding-1",
        agent_instance_id="agent-1",
        project_id="project-1",
        canonical_cwd=str(project),
        provider="codex",
        account_profile="work",
        model="stale-model",
        reasoning_effort="low",
        generation_id="gen-1",
        revision=0,
    )
    stale_payload = versioned(
        "apply_binding",
        str(uuid.uuid4()),
        binding=payload["binding"],
        desired={
            "binding_id": stale_spec.binding_id,
            "agent_instance_id": stale_spec.agent_instance_id,
            "project_id": stale_spec.project_id,
            "canonical_cwd": stale_spec.canonical_cwd,
            "provider": stale_spec.provider,
            "account_profile": stale_spec.account_profile,
            "model": stale_spec.model,
            "reasoning_effort": stale_spec.reasoning_effort,
            "generation_id": stale_spec.generation_id,
            "revision": stale_spec.revision,
            "spec_digest": stale_spec.spec_digest,
        },
    )
    stale = await request(config.socket_path, stale_payload)
    assert stale["ok"] is False
    assert read_topic_config(topic_config).digest == before_stale
    assert len(list((config.state_dir / "topic-config-backups").iterdir())) == 1

    server_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await server_task

    restarted = RuntimeServer(
        config,
        identity_verifier=lambda _identity: None,
        child_verifier=lambda **_kwargs: {},
    )
    restarted_task = asyncio.create_task(restarted.serve())
    for _ in range(100):
        if config.socket_path.exists():
            break
        await asyncio.sleep(0.01)
    replayed = await request(config.socket_path, payload)
    assert replayed["ok"] is True
    assert replayed["local_effective"] == applied["local_effective"]
    assert len(list((config.state_dir / "topic-config-backups").iterdir())) == 1

    next_spec = DesiredSpec.build(
        binding_id="binding-1",
        agent_instance_id="agent-1",
        project_id="project-1",
        canonical_cwd=str(project),
        provider="codex",
        account_profile="work",
        model="gpt-next",
        reasoning_effort="medium",
        generation_id="gen-2",
        revision=1,
    )
    next_desired = {
        "binding_id": next_spec.binding_id,
        "agent_instance_id": next_spec.agent_instance_id,
        "project_id": next_spec.project_id,
        "canonical_cwd": next_spec.canonical_cwd,
        "provider": next_spec.provider,
        "account_profile": next_spec.account_profile,
        "model": next_spec.model,
        "reasoning_effort": next_spec.reasoning_effort,
        "generation_id": next_spec.generation_id,
        "revision": next_spec.revision,
        "spec_digest": next_spec.spec_digest,
    }
    snapshot_items = [{"binding": payload["binding"], "desired": next_desired}]
    snapshot_material = {
        "agent_instance_id": "agent-1",
        "generation_id": "gen-2",
        "bindings": snapshot_items,
        "retired_binding_ids": [],
    }
    snapshot = await request(
        config.socket_path,
        versioned(
            "apply_snapshot",
            str(uuid.uuid4()),
            **snapshot_material,
            snapshot_digest=sha256_digest(snapshot_material),
        ),
    )
    assert snapshot["ok"] is True
    assert snapshot["generation_id"] == "gen-2"
    assert restarted.store.get_effective_spec("binding-1")[0] == next_spec  # type: ignore[index]

    stale_snapshot_spec = DesiredSpec.build(
        binding_id="binding-1",
        agent_instance_id="agent-1",
        project_id="project-1",
        canonical_cwd=str(project),
        provider="codex",
        account_profile="work",
        model="stale-snapshot",
        reasoning_effort="low",
        generation_id="gen-2",
        revision=0,
    )
    stale_snapshot_desired = {
        **next_desired,
        "revision": stale_snapshot_spec.revision,
        "model": stale_snapshot_spec.model,
        "reasoning_effort": stale_snapshot_spec.reasoning_effort,
        "spec_digest": stale_snapshot_spec.spec_digest,
    }
    stale_snapshot_items = [{"binding": payload["binding"], "desired": stale_snapshot_desired}]
    stale_snapshot_material = {
        "agent_instance_id": "agent-1",
        "generation_id": "gen-2",
        "bindings": stale_snapshot_items,
        "retired_binding_ids": [],
    }
    backups_before_stale_snapshot = len(list((config.state_dir / "topic-config-backups").iterdir()))
    stale_snapshot = await request(
        config.socket_path,
        versioned(
            "apply_snapshot",
            str(uuid.uuid4()),
            **stale_snapshot_material,
            snapshot_digest=sha256_digest(stale_snapshot_material),
        ),
    )
    assert stale_snapshot["ok"] is False
    assert len(list((config.state_dir / "topic-config-backups").iterdir())) == (
        backups_before_stale_snapshot
    )

    gen_three = DesiredSpec.build(
        binding_id="binding-1",
        agent_instance_id="agent-1",
        project_id="project-1",
        canonical_cwd=str(project),
        provider="codex",
        account_profile="work",
        model="gpt-three",
        reasoning_effort="high",
        generation_id="gen-3",
        revision=1,
    )
    incremental_generation_change = await request(
        config.socket_path,
        versioned(
            "apply_binding",
            str(uuid.uuid4()),
            binding=payload["binding"],
            desired={
                **next_desired,
                "generation_id": gen_three.generation_id,
                "model": gen_three.model,
                "reasoning_effort": gen_three.reasoning_effort,
                "spec_digest": gen_three.spec_digest,
            },
        ),
    )
    assert incremental_generation_change["ok"] is False
    assert incremental_generation_change["error"]["code"] == "GENERATION_RECONCILE_REQUIRED"

    conflicting = dict(payload)
    conflicting["desired"] = {**desired_payload, "model": "different"}
    rejected = await request(config.socket_path, conflicting)
    assert rejected["ok"] is False
    assert rejected["error"]["code"] == "INVALID_REQUEST"
    restarted_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await restarted_task


@pytest.mark.asyncio
async def test_snapshot_preserves_unmanaged_binding_without_rewriting_topic(
    tmp_path: Path,
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    profile = tmp_path / "profiles" / "work"
    profile.mkdir(parents=True)
    (profile / "auth.json").write_text("{}", encoding="utf-8")
    (profile / "auth.json").chmod(0o600)
    real_codex = tmp_path / "codex-real"
    real_codex.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    real_codex.chmod(0o700)
    topic_config = tmp_path / "topic_config.json"
    topic_config.write_text(
        json.dumps(
            {
                "topics": {
                    "42": {
                        "cwd": str(project),
                        "engine": "claude",
                        "exec_mode": "subprocess",
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    config = RuntimeConfig(
        agent_instance_id="agent-1",
        forum_chat_id=-1001,
        state_dir=tmp_path / "state",
        socket_path=tmp_path / "run" / "runtime.sock",
        database_path=tmp_path / "state" / "runtime.sqlite",
        real_codex=real_codex,
        real_codex_version="test",
        real_codex_sha256=f"sha256:{hashlib.sha256(real_codex.read_bytes()).hexdigest()}",
        profiles_root=tmp_path / "profiles",
        topic_config_path=topic_config,
        tmux_state_path=tmp_path / "tmux-state.json",
        upstream_sha="0" * 40,
        allowed_project_roots=(tmp_path,),
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
    item = {
        "binding": {
            "binding_id": "binding-1",
            "agent_instance_id": "agent-1",
            "chat_id": -1001,
            "thread_id": 42,
            "project_id": "project-1",
            "canonical_cwd": str(project),
        },
        "desired": {
            "binding_id": spec.binding_id,
            "agent_instance_id": spec.agent_instance_id,
            "project_id": spec.project_id,
            "canonical_cwd": spec.canonical_cwd,
            "provider": spec.provider,
            "account_profile": spec.account_profile,
            "model": spec.model,
            "reasoning_effort": spec.reasoning_effort,
            "generation_id": spec.generation_id,
            "revision": spec.revision,
            "spec_digest": spec.spec_digest,
        },
    }
    material = {
        "agent_instance_id": "agent-1",
        "generation_id": "gen-1",
        "bindings": [],
        "retired_binding_ids": [],
        "unmanaged_bindings": [item],
    }
    daemon = RuntimeServer(config)
    response = daemon._apply_snapshot(
        "request-1",
        {**material, "snapshot_digest": sha256_digest(material)},
    )
    assert response["ok"] is True
    assert response["applied_bindings"] == []
    assert response["unmanaged_bindings"] == ["binding-1"]
    binding = daemon.store.get_binding("binding-1")
    assert binding is not None
    assert binding.management_mode == ManagementMode.UNMANAGED
    assert daemon.store.get_effective_spec("binding-1")[0] == spec  # type: ignore[index]
    topic = read_topic_config(topic_config).topic(42)
    assert topic is not None
    assert topic.engine == "claude"
    assert topic.exec_mode == "subprocess"
    assert not (config.state_dir / "topic-config-backups").exists()


def test_project_relocation_requires_explicit_complete_stopped_snapshot(tmp_path: Path) -> None:
    old_project = tmp_path / "old-project"
    old_project.mkdir()
    new_project = tmp_path / "new-project"
    new_project.mkdir()
    profile = tmp_path / "profiles" / "work"
    profile.mkdir(parents=True)
    (profile / "auth.json").write_text("{}", encoding="utf-8")
    (profile / "auth.json").chmod(0o600)
    real_codex = tmp_path / "codex-real"
    real_codex.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    real_codex.chmod(0o700)
    topic_config = tmp_path / "topic_config.json"
    topic_config.write_text(
        json.dumps(
            {
                "topics": {
                    "42": {
                        "cwd": str(old_project),
                        "engine": "codex",
                        "exec_mode": "tmux",
                        "unknown": "preserved",
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    config = RuntimeConfig(
        agent_instance_id="agent-1",
        forum_chat_id=-1001,
        state_dir=tmp_path / "state",
        socket_path=tmp_path / "run" / "runtime.sock",
        database_path=tmp_path / "state" / "runtime.sqlite",
        real_codex=real_codex,
        real_codex_version="test",
        real_codex_sha256=f"sha256:{hashlib.sha256(real_codex.read_bytes()).hexdigest()}",
        profiles_root=tmp_path / "profiles",
        topic_config_path=topic_config,
        tmux_state_path=tmp_path / "tmux-state.json",
        upstream_sha="0" * 40,
        allowed_project_roots=(tmp_path,),
    )
    daemon = RuntimeServer(config)
    original_binding = RuntimeBinding(
        binding_id="binding-1",
        agent_instance_id="agent-1",
        chat_id=-1001,
        thread_id=42,
        project_id="project-1",
        canonical_cwd=str(old_project),
        expected_tmux_session="cc-n1001-42",
        config_digest=read_topic_config(topic_config).digest,
    )
    daemon.store.upsert_binding(original_binding)
    original_spec = DesiredSpec.build(
        binding_id="binding-1",
        agent_instance_id="agent-1",
        project_id="project-1",
        canonical_cwd=str(old_project),
        provider="codex",
        account_profile="work",
        model="gpt-test",
        reasoning_effort="high",
        generation_id="gen-1",
        revision=1,
    )
    daemon.store.put_effective_spec(
        original_spec,
        codex_home=profile,
        profile_fingerprint=profile_fingerprint(profile),
    )
    relocated_spec = DesiredSpec.build(
        binding_id="binding-1",
        agent_instance_id="agent-1",
        project_id="project-1",
        canonical_cwd=str(new_project),
        provider="codex",
        account_profile="work",
        model="gpt-test",
        reasoning_effort="high",
        generation_id="gen-1",
        revision=2,
    )
    item = {
        "binding": {
            "binding_id": "binding-1",
            "agent_instance_id": "agent-1",
            "chat_id": -1001,
            "thread_id": 42,
            "project_id": "project-1",
            "canonical_cwd": str(new_project),
        },
        "desired": {
            "binding_id": relocated_spec.binding_id,
            "agent_instance_id": relocated_spec.agent_instance_id,
            "project_id": relocated_spec.project_id,
            "canonical_cwd": relocated_spec.canonical_cwd,
            "provider": relocated_spec.provider,
            "account_profile": relocated_spec.account_profile,
            "model": relocated_spec.model,
            "reasoning_effort": relocated_spec.reasoning_effort,
            "generation_id": relocated_spec.generation_id,
            "revision": relocated_spec.revision,
            "spec_digest": relocated_spec.spec_digest,
        },
    }
    implicit_material = {
        "agent_instance_id": "agent-1",
        "generation_id": "gen-1",
        "bindings": [item],
        "retired_binding_ids": [],
    }
    with pytest.raises(RuntimeFailureError) as implicit:
        daemon._apply_snapshot(
            "implicit",
            {**implicit_material, "snapshot_digest": sha256_digest(implicit_material)},
        )
    assert implicit.value.code.value == "PROJECT_RELOCATION_REQUIRED"

    relocation = {
        "relocation_id": "relocation-1",
        "agent_instance_id": "agent-1",
        "project_id": "project-1",
        "old_canonical_cwd": str(old_project),
        "new_canonical_cwd": str(new_project),
        "binding_ids": ["binding-1"],
    }
    explicit_material = {
        **implicit_material,
        "project_relocations": [relocation],
    }
    explicit = daemon._apply_snapshot(
        "explicit",
        {**explicit_material, "snapshot_digest": sha256_digest(explicit_material)},
    )
    assert explicit["ok"] is True
    assert explicit["project_relocations"] == ["relocation-1"]
    stored_binding = daemon.store.get_binding("binding-1")
    assert stored_binding is not None
    assert stored_binding.project_id == "project-1"
    assert stored_binding.canonical_cwd == str(new_project)
    stored_spec = daemon.store.get_effective_spec("binding-1")
    assert stored_spec is not None and stored_spec[0] == relocated_spec
    assert daemon.store.get_project_relocation("relocation-1") == relocation
    topic = read_topic_config(topic_config).topic(42)
    assert topic is not None
    assert topic.cwd == str(new_project)
    assert topic.raw["unknown"] == "preserved"


def test_project_relocation_denies_live_binding(tmp_path: Path) -> None:
    old_project = tmp_path / "old"
    old_project.mkdir()
    new_project = tmp_path / "new"
    new_project.mkdir()
    profile = tmp_path / "profiles" / "work"
    profile.mkdir(parents=True)
    (profile / "auth.json").write_text("{}", encoding="utf-8")
    (profile / "auth.json").chmod(0o600)
    real_codex = tmp_path / "codex-real"
    real_codex.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    real_codex.chmod(0o700)
    topic_config = tmp_path / "topic.json"
    topic_config.write_text(
        json.dumps(
            {"topics": {"42": {"cwd": str(old_project), "engine": "codex", "exec_mode": "tmux"}}}
        ),
        encoding="utf-8",
    )
    config = RuntimeConfig(
        agent_instance_id="agent-1",
        forum_chat_id=-1001,
        state_dir=tmp_path / "state",
        socket_path=tmp_path / "run/runtime.sock",
        database_path=tmp_path / "state/runtime.sqlite",
        real_codex=real_codex,
        real_codex_version="test",
        real_codex_sha256=f"sha256:{hashlib.sha256(real_codex.read_bytes()).hexdigest()}",
        profiles_root=tmp_path / "profiles",
        topic_config_path=topic_config,
        tmux_state_path=tmp_path / "state.json",
        upstream_sha="0" * 40,
        allowed_project_roots=(tmp_path,),
    )
    daemon = RuntimeServer(config)
    binding = RuntimeBinding(
        binding_id="binding-1",
        agent_instance_id="agent-1",
        chat_id=-1001,
        thread_id=42,
        project_id="project-1",
        canonical_cwd=str(old_project),
        expected_tmux_session="cc-n1001-42",
    )
    daemon.store.upsert_binding(binding)
    daemon.store.record_instance(
        instance_id="instance-1",
        binding_id="binding-1",
        wrapper_pid=100,
        wrapper_starttime=1,
        child_pid=101,
        child_starttime=2,
        tmux_session="cc-n1001-42",
        tmux_pane="%1",
        lifecycle="IDLE",
        applied_digest=None,
    )
    relocation = {
        "relocation_id": "relocation-1",
        "agent_instance_id": "agent-1",
        "project_id": "project-1",
        "old_canonical_cwd": str(old_project),
        "new_canonical_cwd": str(new_project),
        "binding_ids": ["binding-1"],
    }
    with pytest.raises(RuntimeFailureError, match="stopped"):
        daemon._validate_project_relocations([relocation])


def test_apply_binding_does_not_retry_over_concurrent_engine_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    project = tmp_path / "project"
    project.mkdir(mode=0o700)
    profile = tmp_path / "profiles" / "work"
    profile.mkdir(parents=True, mode=0o700)
    (profile / "auth.json").write_text("{}", encoding="utf-8")
    (profile / "auth.json").chmod(0o600)
    real_codex = tmp_path / "codex-real"
    real_codex.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    real_codex.chmod(0o700)
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
    config = RuntimeConfig(
        agent_instance_id="agent-1",
        forum_chat_id=-1001,
        state_dir=tmp_path / "state",
        socket_path=tmp_path / "run/runtime.sock",
        database_path=tmp_path / "state/runtime.sqlite",
        real_codex=real_codex,
        real_codex_version="test",
        real_codex_sha256=f"sha256:{hashlib.sha256(real_codex.read_bytes()).hexdigest()}",
        profiles_root=tmp_path / "profiles",
        topic_config_path=topic_config,
        tmux_state_path=tmp_path / "tmux-state.json",
        upstream_sha="0" * 40,
        allowed_project_roots=(tmp_path,),
    )
    daemon = RuntimeServer(config)
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
    payload = {
        "binding": {
            "binding_id": spec.binding_id,
            "agent_instance_id": spec.agent_instance_id,
            "chat_id": -1001,
            "thread_id": 42,
            "project_id": spec.project_id,
            "canonical_cwd": spec.canonical_cwd,
        },
        "desired": {
            "binding_id": spec.binding_id,
            "agent_instance_id": spec.agent_instance_id,
            "project_id": spec.project_id,
            "canonical_cwd": spec.canonical_cwd,
            "provider": spec.provider,
            "account_profile": spec.account_profile,
            "model": spec.model,
            "reasoning_effort": spec.reasoning_effort,
            "generation_id": spec.generation_id,
            "revision": spec.revision,
            "spec_digest": spec.spec_digest,
        },
    }
    real_exchange = topic_config_adapter._exchange_paths
    exchanges = 0

    def exchange(source: Path, target: Path) -> None:
        nonlocal exchanges
        exchanges += 1
        if exchanges == 1:
            upstream = json.loads(topic_config.read_text(encoding="utf-8"))
            upstream["topics"]["42"]["engine"] = "claude"
            replacement = tmp_path / "upstream.tmp"
            replacement.write_text(json.dumps(upstream), encoding="utf-8")
            replacement.replace(topic_config)
        real_exchange(source, target)

    monkeypatch.setattr(topic_config_adapter, "_exchange_paths", exchange)
    try:
        with pytest.raises(RuntimeFailureError) as failure:
            daemon._apply_binding(str(uuid.uuid4()), payload)
        assert failure.value.code.value == "CONFIG_DRIFT"
        assert exchanges == 2
        assert read_topic_config(topic_config).topic(42).engine == "claude"  # type: ignore[union-attr]
        assert daemon.store.get_binding(spec.binding_id) is None
        daemon.store.upsert_binding(
            RuntimeBinding(
                binding_id=spec.binding_id,
                agent_instance_id=spec.agent_instance_id,
                chat_id=-1001,
                thread_id=42,
                project_id=spec.project_id,
                canonical_cwd=spec.canonical_cwd,
                expected_tmux_session="cc-n1001-42",
            )
        )
        with pytest.raises(RuntimeFailureError) as repeat_failure:
            daemon._apply_binding(str(uuid.uuid4()), payload)
        assert repeat_failure.value.code.value == "CONFIG_DRIFT"
        assert exchanges == 2
        assert read_topic_config(topic_config).topic(42).engine == "claude"  # type: ignore[union-attr]
    finally:
        daemon.store.close()
