from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from ai_agent_runtime.deployment.audit import (
    DEPLOYMENT_MANIFEST_SCHEMA,
    audit_deployment_manifest,
    build_deployment_manifest,
)
from ai_agent_runtime.deployment.install import INSTALL_REPORT_SCHEMA
from ai_agent_runtime.migration.inventory import InventoryError, _digest, write_json_artifact


def _executable(path: Path, content: bytes) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    path.chmod(0o700)
    return f"sha256:{hashlib.sha256(content).hexdigest()}"


def _install_report(tmp_path: Path) -> tuple[Path, Path, Path]:
    report_path = tmp_path / "runtime-install.json"
    release = tmp_path / "runtime-release"
    runtime = release / "bin" / "ai-agent-runtime"
    wrapper = release / "bin" / "codex-wrapper"
    wheel = tmp_path / "runtime.whl"
    if not report_path.exists():
        _executable(runtime, b"#!/bin/sh\nexit 0\n")
        _executable(wrapper, b"#!/bin/sh\nexit 0\n")
        qualification = {
            name: release / "bin" / name
            for name in (
                "ai-agent-runtime-qualify-upstream",
                "ai-agent-runtime-qualify-retirement",
            )
        }
        for executable in qualification.values():
            _executable(executable, b"#!/bin/sh\nexit 0\n")
        wheel.write_bytes(b"runtime-wheel")
        installed_files = [
            {
                "path": str(path.relative_to(release)),
                "sha256": f"sha256:{hashlib.sha256(path.read_bytes()).hexdigest()}",
            }
            for path in sorted((runtime, wrapper, *qualification.values()))
        ]
        report: dict[str, object] = {
            "schema": INSTALL_REPORT_SCHEMA,
            "status": "INSTALLED_NOT_ACTIVATED",
            "release_id": release.name,
            "release_path": str(release),
            "wheel_path": str(wheel),
            "wheel_sha256": f"sha256:{hashlib.sha256(wheel.read_bytes()).hexdigest()}",
            "python_path": "/usr/bin/python3.12",
            "runtime_executable": str(runtime),
            "wrapper_executable": str(wrapper),
            "qualification_executables": {name: str(path) for name, path in qualification.items()},
            "installed_files": installed_files,
            "installed_files_digest": _digest(installed_files),
            "existing_releases_preserved": True,
            "activation_performed": False,
            "services_restarted": False,
            "production_cutover_authorized": False,
        }
        report["report_digest"] = _digest(report)
        write_json_artifact(report_path, report)
    return report_path, runtime, wrapper


def _instance(tmp_path: Path, *, instance_id: str = "agent-1", chat_id: int = -1001) -> dict:
    root = tmp_path / instance_id
    root.mkdir()
    _report, runtime, installed_wrapper = _install_report(tmp_path)
    codex = root / "codex-real"
    codex_digest = _executable(codex, b"#!/bin/sh\nexit 0\n")
    wrapper = root / f"codex-wrapper@{instance_id}"
    wrapper.symlink_to(installed_wrapper)
    checkout = root / "telegram"
    checkout.mkdir()
    config = root / "runtime.toml"
    config.write_text(
        f"""
[runtime]
agent_instance_id = "{instance_id}"
forum_chat_id = {chat_id}
state_dir = "{root / "state"}"
socket = "{root / "run/runtime.sock"}"
database = "{root / "state/runtime.sqlite"}"

[telegram_agent]
topic_config = "{root / "telegram/topic_config.json"}"
tmux_state = "{root / "telegram/tmux_sessions/state.json"}"
upstream_sha = "{"0" * 40}"
tmux_session_prefix = "cc-"

[codex]
real_binary = "{codex}"
version = "0.156.1"
sha256 = "{codex_digest}"
profiles_root = "{root / "profiles"}"

[policy]
allowed_project_roots = ["{root / "projects"}"]
retirement_enabled = false
retirement_qualification_report_id = ""
""".strip()
        + "\n",
        encoding="utf-8",
    )
    return {
        "agent_instance_id": instance_id,
        "forum_chat_id": chat_id,
        "telegram_bot_token_fingerprint": f"sha256:{'1' * 64}",
        "runtime_config": str(config),
        "runtime_executable": str(runtime),
        "wrapper_path": str(wrapper),
        "telegram_checkout": str(checkout),
        "runtime_service": {
            "unit_name": f"ai-agent-runtime@{instance_id}.service",
            "type": "notify",
            "restart": "always",
            "restart_sec_seconds": 2,
            "watchdog_sec_seconds": 30,
            "runtime_directory_mode": "0700",
            "runtime_directory_preserve": "yes",
            "umask": "0077",
            "socket_mode": "0600",
            "socket_accept": False,
            "config_path": str(config),
            "exec_start": [str(runtime), "--config", str(config), "serve"],
        },
        "telegram_service": {
            "unit_name": f"telegram-ai-agent@{instance_id}.service",
            "kill_mode": "process",
            "telegram_codex_bin": str(wrapper),
            "codex_auto_update_enabled": False,
        },
    }


def _qualified() -> dict[str, object]:
    return {
        "accepted": True,
        "report_id": "qualification-1",
        "release_tuple": {
            "telegram_agent_sha": "0" * 40,
            "codex_version": "0.156.1",
            "codex_binary_sha256": "placeholder",
            "runtime_release": "runtime-release",
            "runtime_wheel_sha256": f"sha256:{hashlib.sha256(b'runtime-wheel').hexdigest()}",
            "protocol_major": 1,
            "cus_connector_version": "runtime-v1",
        },
    }


def _manifest(
    tmp_path: Path,
    instances: list[dict],
    *,
    target: str = "staging",
) -> Path:
    qualification = tmp_path / "qualification.json"
    qualification.write_text("{}\n", encoding="utf-8")
    rollback = tmp_path / "rollback.json"
    rollback.write_text("{}\n", encoding="utf-8")
    install_report, _runtime, _wrapper = _install_report(tmp_path)
    material: dict[str, object] = {
        "schema": DEPLOYMENT_MANIFEST_SCHEMA,
        "target": target,
        "runtime_install_report": str(install_report),
        "qualification_manifest": str(qualification),
        "rollback_manifest": str(rollback),
        "agent_instances": instances,
    }
    material["manifest_digest"] = _digest(material)
    path = tmp_path / "deployment.json"
    write_json_artifact(path, material)
    return path


def test_deployment_manifest_builder_pins_exact_inputs_without_mutation(tmp_path: Path) -> None:
    qualification = tmp_path / "qualification.json"
    qualification.write_text("{}\n", encoding="utf-8")
    rollback = tmp_path / "rollback.json"
    rollback.write_text("{}\n", encoding="utf-8")
    install_report, _runtime, _wrapper = _install_report(tmp_path)
    instance = _instance(tmp_path)

    manifest = build_deployment_manifest(
        target="staging",
        runtime_install_report=install_report,
        qualification_manifest=qualification,
        rollback_manifest=rollback,
        agent_instances=[instance],
    )

    assert manifest["schema"] == DEPLOYMENT_MANIFEST_SCHEMA
    assert manifest["target"] == "staging"
    assert manifest["runtime_install_report"] == str(install_report.resolve())
    assert manifest["qualification_manifest"] == str(qualification.resolve())
    assert manifest["rollback_manifest"] == str(rollback.resolve())
    assert manifest["agent_instances"] == [instance]
    assert manifest["manifest_digest"] == _digest(
        {key: value for key, value in manifest.items() if key != "manifest_digest"}
    )
    assert list(tmp_path.glob("*.tmp")) == []


def test_deployment_manifest_builder_rejects_unsafe_inputs(tmp_path: Path) -> None:
    qualification = tmp_path / "qualification.json"
    qualification.write_text("{}\n", encoding="utf-8")
    rollback = tmp_path / "rollback.json"
    rollback.write_text("{}\n", encoding="utf-8")
    install_report, _runtime, _wrapper = _install_report(tmp_path)
    qualification_link = tmp_path / "qualification-link.json"
    qualification_link.symlink_to(qualification)

    with pytest.raises(InventoryError, match="target"):
        build_deployment_manifest(
            target="preview",
            runtime_install_report=install_report,
            qualification_manifest=qualification,
            rollback_manifest=rollback,
            agent_instances=[_instance(tmp_path)],
        )
    with pytest.raises(InventoryError, match="non-symlink"):
        build_deployment_manifest(
            target="staging",
            runtime_install_report=install_report,
            qualification_manifest=qualification_link,
            rollback_manifest=rollback,
            agent_instances=[{}],
        )
    with pytest.raises(InventoryError, match="AgentInstance"):
        build_deployment_manifest(
            target="staging",
            runtime_install_report=install_report,
            qualification_manifest=qualification,
            rollback_manifest=rollback,
            agent_instances=[],
        )


def _verifier_for(path: Path) -> dict[str, object]:
    result = _qualified()
    codex = path.parent / "agent-1" / "codex-real"
    release_tuple = result["release_tuple"]
    assert isinstance(release_tuple, dict)
    release_tuple["codex_binary_sha256"] = (
        f"sha256:{hashlib.sha256(codex.read_bytes()).hexdigest()}"
    )
    return result


def _rollback_verified(_path: Path) -> dict[str, object]:
    return {
        "accepted": True,
        "manifest_digest": f"sha256:{'2' * 64}",
        "control_period_end": "2099-01-01T00:00:00Z",
        "control_period_active": True,
    }


def test_deployment_audit_accepts_exact_isolated_staging_layout(tmp_path: Path) -> None:
    path = _manifest(tmp_path, [_instance(tmp_path)])
    report = audit_deployment_manifest(
        path,
        qualification_verifier=_verifier_for,
        rollback_verifier=_rollback_verified,
        git_head_reader=lambda _path: "0" * 40,
    )
    assert report["accepted"] is True
    assert report["status"] == "READY_FOR_STAGING"
    assert report["mutations_performed"] is False
    assert report["production_cutover_authorized"] is False


def test_deployment_audit_blocks_duplicate_forum_and_unsafe_systemd(tmp_path: Path) -> None:
    first = _instance(tmp_path, instance_id="agent-1", chat_id=-1001)
    second = _instance(tmp_path, instance_id="agent-2", chat_id=-1001)
    second["telegram_bot_token_fingerprint"] = f"sha256:{'2' * 64}"
    second["telegram_service"]["kill_mode"] = "control-group"
    path = _manifest(tmp_path, [first, second])
    report = audit_deployment_manifest(
        path,
        qualification_verifier=_verifier_for,
        rollback_verifier=_rollback_verified,
        git_head_reader=lambda _path: "0" * 40,
    )
    assert report["accepted"] is False
    assert report["status"] == "BLOCKED"
    assert "deployment_forum_chat_id_not_unique" in report["errors"]
    assert "agent-2:telegram_service:kill_mode_mismatch" in report["errors"]


def test_deployment_audit_requires_tmux_socket_survival_across_full_stop(
    tmp_path: Path,
) -> None:
    instance = _instance(tmp_path)
    instance["runtime_service"]["runtime_directory_preserve"] = "restart"
    path = _manifest(tmp_path, [instance])
    report = audit_deployment_manifest(
        path,
        qualification_verifier=_verifier_for,
        rollback_verifier=_rollback_verified,
        git_head_reader=lambda _path: "0" * 40,
    )
    assert report["accepted"] is False
    assert "agent-1:runtime_service:runtime_directory_preserve_mismatch" in report["errors"]


def test_runtime_unit_preserves_tmux_socket_on_full_stop() -> None:
    unit = (
        Path(__file__).resolve().parents[1] / "deploy/systemd/ai-agent-runtime@.service"
    ).read_text(encoding="utf-8")
    assert "RuntimeDirectoryPreserve=yes" in unit


def test_tmux_session_host_isolated_from_telegram_control_plane() -> None:
    root = Path(__file__).resolve().parents[1]
    tmux_unit = (root / "deploy/systemd/ai-agent-tmux@.service").read_text(encoding="utf-8")
    telegram_drop_in = (root / "deploy/systemd/telegram-ai-agent-runtime.conf").read_text(
        encoding="utf-8"
    )

    assert "ExecStart=/usr/bin/tmux -D" in tmux_unit
    assert "OOMScoreAdjust=800" in tmux_unit
    assert "RuntimeDirectoryPreserve=yes" in tmux_unit
    assert "Requires=ai-agent-tmux@agent_01.service" in telegram_drop_in
    assert "After=ai-agent-tmux@agent_01.service" in telegram_drop_in
    assert "KillMode=process" in telegram_drop_in


def test_deployment_audit_blocks_unqualified_release(tmp_path: Path) -> None:
    path = _manifest(tmp_path, [_instance(tmp_path)], target="production")

    def blocked(_path: Path) -> dict[str, object]:
        result = _verifier_for(_path)
        result["accepted"] = False
        return result

    report = audit_deployment_manifest(
        path,
        qualification_verifier=blocked,
        rollback_verifier=_rollback_verified,
        git_head_reader=lambda _path: "0" * 40,
    )
    assert report["accepted"] is False
    assert "release_not_qualified" in report["errors"]


def test_production_audit_rejects_qualification_only_disabled_hooks(tmp_path: Path) -> None:
    instance = _instance(tmp_path)
    config_path = Path(instance["runtime_config"])
    config_path.write_text(
        config_path.read_text(encoding="utf-8") + "\nqualification_disable_codex_hooks = true\n",
        encoding="utf-8",
    )
    path = _manifest(tmp_path, [instance], target="production")
    report = audit_deployment_manifest(
        path,
        qualification_verifier=_verifier_for,
        rollback_verifier=_rollback_verified,
        git_head_reader=lambda _path: "0" * 40,
    )
    assert report["accepted"] is False
    assert "agent-1:qualification_codex_hooks_disabled" in report["errors"]


def test_staging_audit_accepts_intact_tuple_with_only_real_evidence_missing(
    tmp_path: Path,
) -> None:
    path = _manifest(tmp_path, [_instance(tmp_path)], target="staging")

    def prepared(_path: Path) -> dict[str, object]:
        result = _verifier_for(_path)
        result.update(
            {
                "accepted": False,
                "errors": [
                    "real_codex_launch:evidence_missing",
                    "telegram_e2e:evidence_missing",
                ],
            }
        )
        return result

    report = audit_deployment_manifest(
        path,
        qualification_verifier=prepared,
        rollback_verifier=_rollback_verified,
        git_head_reader=lambda _path: "0" * 40,
    )
    assert report["accepted"] is True
    assert report["status"] == "READY_FOR_STAGING"
    assert report["target"] == "staging"


def test_staging_audit_rejects_invalid_existing_evidence(tmp_path: Path) -> None:
    path = _manifest(tmp_path, [_instance(tmp_path)], target="staging")

    def invalid(_path: Path) -> dict[str, object]:
        result = _verifier_for(_path)
        result.update(
            {
                "accepted": False,
                "errors": ["upstream_source_contract:artifact_digest_mismatch"],
            }
        )
        return result

    report = audit_deployment_manifest(
        path,
        qualification_verifier=invalid,
        rollback_verifier=_rollback_verified,
        git_head_reader=lambda _path: "0" * 40,
    )
    assert report["accepted"] is False
    assert "qualification_manifest_not_prepared" in report["errors"]


def test_deployment_audit_blocks_unverified_rollback_path(tmp_path: Path) -> None:
    path = _manifest(tmp_path, [_instance(tmp_path)])
    report = audit_deployment_manifest(
        path,
        qualification_verifier=_verifier_for,
        rollback_verifier=lambda _path: {"accepted": False},
        git_head_reader=lambda _path: "0" * 40,
    )
    assert report["accepted"] is False
    assert "rollback_path_not_verified" in report["errors"]


def test_deployment_audit_binds_qualification_to_exact_installed_release(
    tmp_path: Path,
) -> None:
    path = _manifest(tmp_path, [_instance(tmp_path)])

    def other_runtime_release(qualification_path: Path) -> dict[str, object]:
        result = _verifier_for(qualification_path)
        release_tuple = result["release_tuple"]
        assert isinstance(release_tuple, dict)
        release_tuple["runtime_release"] = "runtime-release-other-wheel"
        return result

    report = audit_deployment_manifest(
        path,
        qualification_verifier=other_runtime_release,
        rollback_verifier=_rollback_verified,
        git_head_reader=lambda _path: "0" * 40,
    )

    assert report["accepted"] is False
    assert "runtime_release_tuple_mismatch" in report["errors"]


def test_deployment_audit_binds_qualification_to_exact_runtime_wheel(tmp_path: Path) -> None:
    path = _manifest(tmp_path, [_instance(tmp_path)])

    def other_runtime_wheel(qualification_path: Path) -> dict[str, object]:
        result = _verifier_for(qualification_path)
        release_tuple = result["release_tuple"]
        assert isinstance(release_tuple, dict)
        release_tuple["runtime_wheel_sha256"] = f"sha256:{'9' * 64}"
        return result

    report = audit_deployment_manifest(
        path,
        qualification_verifier=other_runtime_wheel,
        rollback_verifier=_rollback_verified,
        git_head_reader=lambda _path: "0" * 40,
    )

    assert report["accepted"] is False
    assert "runtime_wheel_tuple_mismatch" in report["errors"]
