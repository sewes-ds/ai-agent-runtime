from __future__ import annotations

import hashlib
import json
import uuid
from pathlib import Path

import pytest

from ai_agent_runtime.claude_wrapper.main import _config_path, _rewrite_model
from ai_agent_runtime.config import RuntimeConfig
from ai_agent_runtime.daemon.server import RuntimeServer
from ai_agent_runtime.domain import DesiredSpec
from ai_agent_runtime.telegram_upstream.topic_config import read_topic_config


def _config(tmp_path: Path, topic_config: Path, credential: Path) -> RuntimeConfig:
    real_codex = tmp_path / "codex-real"
    real_codex.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    real_codex.chmod(0o700)
    return RuntimeConfig(
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
        deepseek_api_key_path=credential,
        allowed_project_roots=(tmp_path,),
    )


def test_claude_wrapper_replaces_only_runtime_owned_model() -> None:
    original = ["--resume", str(uuid.uuid4()), "--allowedTools", "Read", "--model=old"]
    rewritten = _rewrite_model(original, "deepseek-flash")
    assert rewritten == [
        "--resume",
        original[1],
        "--allowedTools",
        "Read",
        "--model",
        "deepseek-flash",
    ]


def test_claude_wrapper_finds_instance_config_from_both_stable_path_shapes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("AI_AGENT_RUNTIME_CONFIG", raising=False)
    expected = Path.home() / ".config/ai-agent-runtime/instances/ta_prod_01.toml"
    assert _config_path("/safe/bin/claude-wrapper@ta_prod_01") == expected
    assert _config_path("/safe/instances/ta_prod_01/bin/claude-wrapper") == expected


def test_apply_binding_projects_claude_provider_to_upstream_engine(tmp_path: Path) -> None:
    project = tmp_path / "project"
    project.mkdir(mode=0o700)
    credential = tmp_path / "deepseek.token"
    credential.write_text("secret-for-test\n", encoding="utf-8")
    credential.chmod(0o600)
    topic_config = tmp_path / "topic_config.json"
    topic_config.write_text(
        json.dumps(
            {
                "topics": {
                    "42": {
                        "name": "DeepSeek",
                        "cwd": str(project),
                        "engine": "codex",
                        "exec_mode": "tmux",
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    config = _config(tmp_path, topic_config, credential)
    config.profiles_root.mkdir(mode=0o700)
    daemon = RuntimeServer(config)
    spec = DesiredSpec.build(
        binding_id="binding-1",
        agent_instance_id="agent-1",
        project_id="project-1",
        canonical_cwd=str(project),
        provider="claude_code",
        account_profile="deepseek_default",
        model="deepseek-flash",
        reasoning_effort="max",
        generation_id="gen-1",
        revision=1,
    )
    response = daemon._apply_binding(
        "request-1",
        {
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
        },
    )
    try:
        assert response["ok"] is True
        topic = read_topic_config(topic_config).topic(42)
        assert topic is not None
        assert topic.engine == "claude"
        effective = daemon.store.get_effective_spec(spec.binding_id)
        assert effective is not None
        assert effective[0].provider == "claude_code"
        assert effective[1] == credential.resolve()
        assert effective[2].startswith("secret-v1:sha256:")
    finally:
        daemon.store.close()
