from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from ai_agent_runtime.deployment import prepare as preparation
from ai_agent_runtime.deployment import switch as switching
from ai_agent_runtime.deployment.install import INSTALL_REPORT_SCHEMA
from ai_agent_runtime.migration.inventory import InventoryError, _digest, write_json_artifact


def _executable(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    path.chmod(0o700)


def _install_report(tmp_path: Path, release_id: str = "runtime-1") -> tuple[Path, Path]:
    release = tmp_path / "releases" / release_id
    runtime = release / "bin" / "ai-agent-runtime"
    wrapper = release / "bin" / "codex-wrapper"
    claude_wrapper = release / "bin" / "claude-wrapper"
    _executable(runtime)
    _executable(wrapper)
    _executable(claude_wrapper)
    qualification = {
        name: release / "bin" / name
        for name in (
            "ai-agent-runtime-qualify-upstream",
            "ai-agent-runtime-qualify-retirement",
        )
    }
    for executable in qualification.values():
        _executable(executable)
    wheel = tmp_path / "runtime.whl"
    wheel.write_bytes(b"wheel")
    installed_files = [
        {
            "path": str(path.relative_to(release)),
            "sha256": f"sha256:{hashlib.sha256(path.read_bytes()).hexdigest()}",
        }
        for path in sorted((runtime, wrapper, claude_wrapper, *qualification.values()))
    ]
    report: dict[str, object] = {
        "schema": INSTALL_REPORT_SCHEMA,
        "status": "INSTALLED_NOT_ACTIVATED",
        "release_id": release_id,
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
    report_path = tmp_path / f"install-report-{release_id}.json"
    write_json_artifact(report_path, report)
    return report_path, release


def test_prepare_instance_links_resolves_both_entrypoints_to_one_verified_release(
    tmp_path: Path,
) -> None:
    report_path, release = _install_report(tmp_path)
    instance = tmp_path / "instances" / "ta_staging_01"
    runtime = tmp_path / "bin" / "ai-agent-runtime@ta_staging_01"
    wrapper = tmp_path / "bin" / "codex-wrapper@ta_staging_01"
    claude_wrapper = tmp_path / "bin" / "claude-wrapper@ta_staging_01"

    report = preparation.prepare_instance_links(
        install_report=report_path,
        agent_instance_id="ta_staging_01",
        instance_link=instance,
        runtime_link=runtime,
        wrapper_link=wrapper,
        claude_wrapper_link=claude_wrapper,
    )

    assert instance.is_symlink()
    assert instance.resolve(strict=True) == release
    assert runtime.resolve(strict=True) == release / "bin" / "ai-agent-runtime"
    assert wrapper.resolve(strict=True) == release / "bin" / "codex-wrapper"
    assert claude_wrapper.resolve(strict=True) == release / "bin" / "claude-wrapper"
    assert report["status"] == "PREPARED_NOT_STARTED"
    assert report["services_started"] is False
    assert report["production_cutover_authorized"] is False

    with pytest.raises(InventoryError, match="existing"):
        preparation.prepare_instance_links(
            install_report=report_path,
            agent_instance_id="ta_staging_01",
            instance_link=instance,
            runtime_link=runtime,
            wrapper_link=wrapper,
        )


def test_prepare_instance_links_rolls_back_only_new_links_on_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    report_path, _release = _install_report(tmp_path)
    instance = tmp_path / "instances" / "ta_staging_01"
    runtime = tmp_path / "bin" / "ai-agent-runtime@ta_staging_01"
    wrapper = tmp_path / "bin" / "codex-wrapper@ta_staging_01"
    original = preparation._create_symlink
    calls = 0

    def fail_third(link: Path, target: Path) -> None:
        nonlocal calls
        calls += 1
        if calls == 3:
            raise OSError("simulated link failure")
        original(link, target)

    monkeypatch.setattr(preparation, "_create_symlink", fail_third)
    with pytest.raises(OSError, match="simulated"):
        preparation.prepare_instance_links(
            install_report=report_path,
            agent_instance_id="ta_staging_01",
            instance_link=instance,
            runtime_link=runtime,
            wrapper_link=wrapper,
        )

    assert not instance.is_symlink()
    assert not runtime.is_symlink()
    assert not wrapper.is_symlink()


def _prepared_switch(tmp_path: Path) -> tuple[Path, Path, Path, Path, Path, Path, Path]:
    old_report, old_release = _install_report(tmp_path, "runtime-1")
    new_report, new_release = _install_report(tmp_path, "runtime-2")
    instance = tmp_path / "instances" / "ta_staging_01"
    runtime = tmp_path / "bin" / "ai-agent-runtime@ta_staging_01"
    wrapper = tmp_path / "bin" / "codex-wrapper@ta_staging_01"
    preparation.prepare_instance_links(
        install_report=old_report,
        agent_instance_id="ta_staging_01",
        instance_link=instance,
        runtime_link=runtime,
        wrapper_link=wrapper,
    )
    return old_report, new_report, old_release, new_release, instance, runtime, wrapper


def test_switch_instance_release_preserves_stable_entrypoints_and_can_reverse(
    tmp_path: Path,
) -> None:
    old, new, old_release, new_release, instance, runtime, wrapper = _prepared_switch(tmp_path)
    result = switching.switch_instance_release(
        old_install_report=old,
        new_install_report=new,
        agent_instance_id="ta_staging_01",
        instance_link=instance,
        runtime_link=runtime,
        wrapper_link=wrapper,
        intent_path=tmp_path / "switch-intent.json",
        result_path=tmp_path / "switch-result.json",
    )
    assert result["status"] == "SWITCHED_NOT_RESTARTED"
    assert result["services_restarted"] is False
    assert result["production_cutover_authorized"] is False
    assert instance.resolve(strict=True) == new_release
    assert runtime.resolve(strict=True) == new_release / "bin/ai-agent-runtime"
    assert wrapper.resolve(strict=True) == new_release / "bin/codex-wrapper"
    assert (tmp_path / "switch-intent.json").is_file()
    assert (tmp_path / "switch-result.json").is_file()

    switching.switch_instance_release(
        old_install_report=new,
        new_install_report=old,
        agent_instance_id="ta_staging_01",
        instance_link=instance,
        runtime_link=runtime,
        wrapper_link=wrapper,
        intent_path=tmp_path / "rollback-intent.json",
        result_path=tmp_path / "rollback-result.json",
    )
    assert instance.resolve(strict=True) == old_release


def test_switch_instance_release_fails_before_mutation_if_link_drifted(tmp_path: Path) -> None:
    old, new, old_release, _new_release, instance, runtime, wrapper = _prepared_switch(tmp_path)
    wrapper.unlink()
    wrapper.symlink_to(old_release / "bin/codex-wrapper")
    with pytest.raises(InventoryError, match="differs"):
        switching.switch_instance_release(
            old_install_report=old,
            new_install_report=new,
            agent_instance_id="ta_staging_01",
            instance_link=instance,
            runtime_link=runtime,
            wrapper_link=wrapper,
            intent_path=tmp_path / "switch-intent.json",
            result_path=tmp_path / "switch-result.json",
        )
    assert instance.resolve(strict=True) == old_release
    assert not (tmp_path / "switch-intent.json").exists()


def test_switch_instance_release_rolls_back_on_failed_result_write(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    old, new, old_release, _new_release, instance, runtime, wrapper = _prepared_switch(tmp_path)
    original_write = switching.write_json_artifact

    def fail_result(path: Path, payload: dict[str, object]) -> None:
        if path.name == "switch-result.json":
            raise OSError("simulated result write failure")
        original_write(path, payload)

    monkeypatch.setattr(switching, "write_json_artifact", fail_result)
    with pytest.raises(OSError, match="simulated"):
        switching.switch_instance_release(
            old_install_report=old,
            new_install_report=new,
            agent_instance_id="ta_staging_01",
            instance_link=instance,
            runtime_link=runtime,
            wrapper_link=wrapper,
            intent_path=tmp_path / "switch-intent.json",
            result_path=tmp_path / "switch-result.json",
        )
    assert instance.resolve(strict=True) == old_release
    assert (tmp_path / "switch-intent.json").is_file()
    assert not (tmp_path / "switch-result.json").exists()


def test_switch_instance_release_rolls_back_if_replace_raises_after_swap(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    old, new, old_release, _new_release, instance, runtime, wrapper = _prepared_switch(tmp_path)
    original_replace = switching._replace_link
    calls = 0

    def fail_after_first_replace(link: Path, target: Path) -> None:
        nonlocal calls
        calls += 1
        original_replace(link, target)
        if calls == 1:
            raise OSError("simulated post-replace failure")

    monkeypatch.setattr(switching, "_replace_link", fail_after_first_replace)
    with pytest.raises(OSError, match="simulated"):
        switching.switch_instance_release(
            old_install_report=old,
            new_install_report=new,
            agent_instance_id="ta_staging_01",
            instance_link=instance,
            runtime_link=runtime,
            wrapper_link=wrapper,
            intent_path=tmp_path / "switch-intent.json",
            result_path=tmp_path / "switch-result.json",
        )
    assert calls == 2
    assert instance.resolve(strict=True) == old_release
