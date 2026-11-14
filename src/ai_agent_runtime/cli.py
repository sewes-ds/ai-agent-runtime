"""Operator CLI for Runtime lifecycle and diagnostics."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import uuid
from pathlib import Path

from ai_agent_runtime.config import RuntimeConfig
from ai_agent_runtime.daemon.server import RuntimeServer
from ai_agent_runtime.deployment.audit import (
    audit_deployment_manifest,
    build_deployment_manifest,
)
from ai_agent_runtime.deployment.install import (
    install_runtime_release,
    verify_runtime_install_report,
)
from ai_agent_runtime.deployment.prepare import prepare_instance_links
from ai_agent_runtime.deployment.switch import switch_instance_release
from ai_agent_runtime.health.doctor import collect_doctor_report
from ai_agent_runtime.ipc.client import request
from ai_agent_runtime.ipc.protocol import versioned
from ai_agent_runtime.migration.binding_plan import (
    build_binding_migration_plan,
    read_binding_migration_decisions,
    read_binding_migration_plan,
)
from ai_agent_runtime.migration.fresh_context import build_fresh_context_preflight
from ai_agent_runtime.migration.inventory import (
    InventoryError,
    build_inventory,
    compare_inventories,
    read_inventory,
    write_inventory,
    write_json_artifact,
)
from ai_agent_runtime.migration.rollback import (
    build_rollback_manifest,
    verify_rollback_manifest,
)
from ai_agent_runtime.migration.session_provenance import (
    build_session_provenance_plan,
    read_session_provenance_plan,
)
from ai_agent_runtime.migration.session_storage import (
    build_session_storage_manifest,
    stage_session_storage,
)
from ai_agent_runtime.migration.topic_config_stage import stage_topic_config
from ai_agent_runtime.qualification.release_gate import (
    build_qualification_manifest,
    verify_qualification_manifest,
)
from ai_agent_runtime.qualification.run_plan import build_staging_run_plan
from ai_agent_runtime.state.store import RuntimeStore


def _load_config(path: str) -> RuntimeConfig:
    config = RuntimeConfig.load(Path(path).expanduser())
    config.validate()
    return config


async def _status(config: RuntimeConfig) -> int:
    response = await request(
        config.socket_path,
        versioned("status", str(uuid.uuid4())),
    )
    print(json.dumps(response, indent=2, sort_keys=True))
    return 0 if response.get("ok") else 1


async def _doctor(config: RuntimeConfig) -> int:
    report = await collect_doctor_report(config)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["healthy"] is True else 1


def _local_summary(config: RuntimeConfig) -> int:
    store = RuntimeStore(config.database_path)
    try:
        print(json.dumps(store.summary(), indent=2, sort_keys=True))
    finally:
        store.close()
    return 0


def _inventory_export(
    config: RuntimeConfig,
    *,
    output: str,
    cus_control_store: str | None,
    legacy_project_settings: str | None,
    skip_live_tmux: bool,
) -> int:
    live_tmux = (
        {"available": False, "reason": "operator_skipped", "panes": []} if skip_live_tmux else None
    )
    inventory = build_inventory(
        config,
        cus_control_store=Path(cus_control_store).expanduser() if cus_control_store else None,
        legacy_project_settings=(
            Path(legacy_project_settings).expanduser() if legacy_project_settings else None
        ),
        live_tmux=live_tmux,
    )
    target = Path(output).expanduser()
    write_inventory(target, inventory)
    print(json.dumps({"output": str(target), "inventory_digest": inventory["inventory_digest"]}))
    return 0


def _inventory_compare(
    *, baseline: str, candidate: str, binding_plan: str | None, output: str | None
) -> int:
    report = compare_inventories(
        read_inventory(Path(baseline).expanduser()),
        read_inventory(Path(candidate).expanduser()),
        binding_plan=(
            read_binding_migration_plan(Path(binding_plan).expanduser()) if binding_plan else None
        ),
    )
    if output:
        write_json_artifact(Path(output).expanduser(), report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["accepted"] else 1


def _inventory_fresh_context_preflight(
    config: RuntimeConfig, *, baseline: str, binding_plan: str, output: str | None
) -> int:
    report = build_fresh_context_preflight(
        config,
        baseline=read_inventory(Path(baseline).expanduser()),
        binding_plan=read_binding_migration_plan(Path(binding_plan).expanduser()),
    )
    if output:
        write_json_artifact(Path(output).expanduser(), report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["accepted"] else 1


def _inventory_stage_topic_config(
    *, baseline: str, binding_plan: str, decisions: str | None, target: str, output: str
) -> int:
    report_path = Path(output).expanduser()
    target_path = Path(target).expanduser()
    if report_path.resolve(strict=False) == target_path.resolve(strict=False):
        raise InventoryError("topic-config stage report and target must be different paths")
    report_parent = report_path.parent
    if not report_path.is_absolute() or not report_parent.is_dir() or report_parent.is_symlink():
        raise InventoryError("topic-config stage report needs an existing private directory")
    report_parent_metadata = report_parent.stat()
    if report_parent_metadata.st_uid != os.getuid() or report_parent_metadata.st_mode & 0o077:
        raise InventoryError("topic-config stage report parent must be owned and mode 0700")
    if report_path.exists() or report_path.is_symlink():
        raise InventoryError("refusing to overwrite topic-config stage report")
    report = stage_topic_config(
        baseline=read_inventory(Path(baseline).expanduser()),
        binding_plan=read_binding_migration_plan(Path(binding_plan).expanduser()),
        decisions=(
            read_binding_migration_decisions(Path(decisions).expanduser()) if decisions else None
        ),
        target=target_path,
    )
    write_json_artifact(report_path, report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


def _inventory_plan_bindings(
    *,
    inventory: str,
    generation_id: str,
    output: str,
    decisions: str | None,
) -> int:
    plan = build_binding_migration_plan(
        read_inventory(Path(inventory).expanduser()),
        generation_id=generation_id,
        decisions=(
            read_binding_migration_decisions(Path(decisions).expanduser()) if decisions else None
        ),
    )
    target = Path(output).expanduser()
    write_json_artifact(target, plan)
    print(
        json.dumps(
            {
                "output": str(target),
                "plan_digest": plan["plan_digest"],
                "accepted": plan["accepted"],
                "counts": plan["counts"],
            },
            sort_keys=True,
        )
    )
    return 0 if plan["accepted"] else 1


def _inventory_plan_session_provenance(
    *,
    inventory: str,
    binding_plan: str,
    output: str,
) -> int:
    plan = build_session_provenance_plan(
        read_inventory(Path(inventory).expanduser()),
        read_binding_migration_plan(Path(binding_plan).expanduser()),
    )
    target = Path(output).expanduser()
    write_json_artifact(target, plan)
    blockers = plan.get("blockers")
    if not isinstance(blockers, list):
        raise InventoryError("session provenance plan blockers are malformed")
    print(
        json.dumps(
            {
                "output": str(target),
                "plan_digest": plan["plan_digest"],
                "accepted": plan["accepted"],
                "record_count": plan["record_count"],
                "blocker_count": len(blockers),
                "mutations_performed": False,
            },
            sort_keys=True,
        )
    )
    return 0 if plan["accepted"] else 1


async def _provenance_import(config: RuntimeConfig, *, plan_path: str) -> int:
    plan = read_session_provenance_plan(Path(plan_path).expanduser())
    digest = str(plan["plan_digest"])
    response = await request(
        config.socket_path,
        versioned(
            "import_session_provenance",
            f"provenance-import:{digest.removeprefix('sha256:')}",
            plan=plan,
        ),
    )
    print(json.dumps(response, indent=2, sort_keys=True))
    return 0 if response.get("ok") else 1


def _parse_profiles(values: list[str]) -> dict[str, Path]:
    profiles: dict[str, Path] = {}
    for value in values:
        name, separator, raw_path = value.partition("=")
        if not separator or not name or not raw_path or name in profiles:
            raise InventoryError("--profile must be a unique NAME=/absolute/path pair")
        profiles[name] = Path(raw_path).expanduser()
    return profiles


def _storage_manifest(
    *,
    profiles: list[str],
    shared_root: str,
    includes: list[str],
    output: str,
) -> int:
    manifest = build_session_storage_manifest(
        profiles=_parse_profiles(profiles),
        shared_root=Path(shared_root).expanduser(),
        includes=tuple(includes) if includes else ("sessions", "archived_sessions"),
    )
    target = Path(output).expanduser()
    write_json_artifact(target, manifest)
    print(
        json.dumps(
            {
                "output": str(target),
                "manifest_digest": manifest["manifest_digest"],
                "status": manifest["status"],
                "qualified": False,
            },
            sort_keys=True,
        )
    )
    return 0 if not manifest["collisions"] else 1


def _storage_stage(*, manifest: str, output: str) -> int:
    target = Path(output).expanduser()
    if target.exists() or target.is_symlink():
        raise InventoryError(f"refusing to overwrite stage report: {target}")
    report = stage_session_storage(Path(manifest).expanduser())
    write_json_artifact(target, report)
    print(
        json.dumps(
            {
                "output": str(target),
                "status": report["status"],
                "stage_digest": report["stage_digest"],
                "activation_performed": False,
            },
            sort_keys=True,
        )
    )
    return 0


def _parse_artifacts(values: list[str]) -> dict[str, Path]:
    artifacts: dict[str, Path] = {}
    for value in values:
        name, separator, raw_path = value.partition("=")
        if not separator or not name or not raw_path or name in artifacts:
            raise InventoryError("--artifact must be a unique NAME=/absolute/path pair")
        artifacts[name] = Path(raw_path).expanduser()
    return artifacts


def _rollback_manifest(
    *,
    legacy_repo: str,
    legacy_service: str,
    legacy_codex_bin: str,
    artifacts: list[str],
    control_period_end: str,
    output: str,
) -> int:
    manifest = build_rollback_manifest(
        legacy_repo=Path(legacy_repo).expanduser(),
        legacy_service=legacy_service,
        legacy_codex_bin=Path(legacy_codex_bin).expanduser(),
        artifacts=_parse_artifacts(artifacts),
        control_period_end=control_period_end,
    )
    target = Path(output).expanduser()
    write_json_artifact(target, manifest)
    print(
        json.dumps(
            {
                "output": str(target),
                "manifest_digest": manifest["manifest_digest"],
                "status": manifest["status"],
                "cleanup_authorized": False,
            },
            sort_keys=True,
        )
    )
    return 0


def _rollback_verify(*, manifest: str, output: str | None) -> int:
    report = verify_rollback_manifest(Path(manifest).expanduser())
    if output:
        write_json_artifact(Path(output).expanduser(), report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["accepted"] else 1


def _read_release_tuple(path: str) -> dict[str, object]:
    source = Path(path).expanduser()
    try:
        value = json.loads(source.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise InventoryError(f"release tuple is not valid JSON: {source}") from exc
    if not isinstance(value, dict):
        raise InventoryError("release tuple must be a JSON object")
    return value


def _qualification_manifest(
    *, report_id: str, release_tuple: str, evidence: list[str], output: str
) -> int:
    manifest = build_qualification_manifest(
        report_id=report_id,
        release_tuple=_read_release_tuple(release_tuple),
        evidence_paths=[Path(path) for path in evidence],
    )
    target = Path(output).expanduser()
    write_json_artifact(target, manifest)
    print(
        json.dumps(
            {
                "output": str(target),
                "manifest_digest": manifest["manifest_digest"],
                "qualified": False,
            },
            sort_keys=True,
        )
    )
    return 0


def _qualification_plan(
    *,
    report_id: str,
    release_tuple: str,
    runtime_install_report: str,
    upstream_checkout: str,
    evidence_directory: str,
    manifest_output: str,
    report_output: str,
    output: str,
) -> int:
    release_tuple_path = Path(release_tuple).expanduser()
    plan = build_staging_run_plan(
        report_id=report_id,
        release_tuple=_read_release_tuple(release_tuple),
        release_tuple_path=release_tuple_path,
        runtime_install_report=Path(runtime_install_report),
        upstream_checkout=Path(upstream_checkout),
        evidence_directory=Path(evidence_directory),
        qualification_manifest_output=Path(manifest_output),
        qualification_report_output=Path(report_output),
    )
    target = Path(output).expanduser()
    write_json_artifact(target, plan)
    gates = plan["gates"]
    if not isinstance(gates, list):
        raise InventoryError("qualification plan gates are invalid")
    print(
        json.dumps(
            {
                "output": str(target),
                "plan_digest": plan["plan_digest"],
                "gate_count": len(gates),
                "qualification_complete": False,
                "production_cutover_authorized": False,
            },
            sort_keys=True,
        )
    )
    return 0


def _qualification_verify(*, manifest: str, output: str | None) -> int:
    report = verify_qualification_manifest(Path(manifest))
    if output:
        write_json_artifact(Path(output).expanduser(), report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["accepted"] else 1


def _deployment_audit(*, manifest: str, output: str | None) -> int:
    report = audit_deployment_manifest(Path(manifest))
    if output:
        write_json_artifact(Path(output).expanduser(), report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report["accepted"] else 1


def _deployment_manifest(
    *,
    target: str,
    runtime_install_report: str,
    qualification_manifest: str,
    rollback_manifest: str,
    instances: list[str],
    output: str,
) -> int:
    descriptors = [_read_release_tuple(path) for path in instances]
    manifest = build_deployment_manifest(
        target=target,
        runtime_install_report=Path(runtime_install_report),
        qualification_manifest=Path(qualification_manifest),
        rollback_manifest=Path(rollback_manifest),
        agent_instances=descriptors,
    )
    destination = Path(output).expanduser()
    write_json_artifact(destination, manifest)
    print(
        json.dumps(
            {
                "output": str(destination),
                "target": target,
                "manifest_digest": manifest["manifest_digest"],
                "instances": len(descriptors),
                "mutations_performed": False,
            },
            sort_keys=True,
        )
    )
    return 0


def _deployment_install_release(
    *,
    wheel: str,
    wheel_sha256: str,
    releases_root: str,
    release_id: str,
    python: str,
    output: str,
) -> int:
    report = install_runtime_release(
        wheel=Path(wheel),
        wheel_sha256=wheel_sha256,
        releases_root=Path(releases_root),
        release_id=release_id,
        python=Path(python),
    )
    destination = Path(output).expanduser()
    write_json_artifact(destination, report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


def _deployment_verify_install(*, report: str, output: str | None) -> int:
    verification = verify_runtime_install_report(Path(report))
    if output:
        write_json_artifact(Path(output).expanduser(), verification)
    print(json.dumps(verification, indent=2, sort_keys=True))
    return 0 if verification["accepted"] is True else 1


def _deployment_prepare_instance(
    *,
    install_report: str,
    agent_instance_id: str,
    instance_link: str,
    runtime_link: str,
    wrapper_link: str,
    claude_wrapper_link: str | None,
    output: str,
) -> int:
    report = prepare_instance_links(
        install_report=Path(install_report),
        agent_instance_id=agent_instance_id,
        instance_link=Path(instance_link),
        runtime_link=Path(runtime_link),
        wrapper_link=Path(wrapper_link),
        claude_wrapper_link=(
            Path(claude_wrapper_link) if claude_wrapper_link is not None else None
        ),
    )
    write_json_artifact(Path(output).expanduser(), report)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


def _deployment_switch_instance(
    *,
    old_install_report: str,
    new_install_report: str,
    agent_instance_id: str,
    instance_link: str,
    runtime_link: str,
    wrapper_link: str,
    intent: str,
    result: str,
) -> int:
    report = switch_instance_release(
        old_install_report=Path(old_install_report),
        new_install_report=Path(new_install_report),
        agent_instance_id=agent_instance_id,
        instance_link=Path(instance_link),
        runtime_link=Path(runtime_link),
        wrapper_link=Path(wrapper_link),
        intent_path=Path(intent),
        result_path=Path(result),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


def main() -> None:
    default_config = os.environ.get(
        "AI_AGENT_RUNTIME_CONFIG",
        str(Path.home() / ".config" / "ai-agent-runtime" / "config.toml"),
    )
    parser = argparse.ArgumentParser(prog="ai-agent-runtime")
    parser.add_argument("--config", default=default_config)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("serve")
    commands.add_parser("status")
    commands.add_parser("local-summary")
    commands.add_parser("doctor")
    inventory = commands.add_parser("inventory")
    inventory_commands = inventory.add_subparsers(dest="inventory_command", required=True)
    inventory_export = inventory_commands.add_parser("export")
    inventory_export.add_argument("--output", required=True)
    inventory_export.add_argument("--cus-control-store")
    inventory_export.add_argument("--legacy-project-settings")
    inventory_export.add_argument("--skip-live-tmux", action="store_true")
    inventory_compare = inventory_commands.add_parser("compare")
    inventory_compare.add_argument("--baseline", required=True)
    inventory_compare.add_argument("--candidate", required=True)
    inventory_compare.add_argument("--binding-plan")
    inventory_compare.add_argument("--output")
    fresh_preflight = inventory_commands.add_parser("fresh-context-preflight")
    fresh_preflight.add_argument("--baseline", required=True)
    fresh_preflight.add_argument("--binding-plan", required=True)
    fresh_preflight.add_argument("--output")
    stage_topics = inventory_commands.add_parser("stage-topic-config")
    stage_topics.add_argument("--baseline", required=True)
    stage_topics.add_argument("--binding-plan", required=True)
    stage_topics.add_argument("--decisions")
    stage_topics.add_argument("--target", required=True)
    stage_topics.add_argument("--output", required=True)
    inventory_plan = inventory_commands.add_parser("plan-bindings")
    inventory_plan.add_argument("--inventory", required=True)
    inventory_plan.add_argument("--generation-id", required=True)
    inventory_plan.add_argument("--decisions")
    inventory_plan.add_argument("--output", required=True)
    provenance_plan = inventory_commands.add_parser("plan-session-provenance")
    provenance_plan.add_argument("--inventory", required=True)
    provenance_plan.add_argument("--binding-plan", required=True)
    provenance_plan.add_argument("--output", required=True)
    provenance = commands.add_parser("provenance")
    provenance_commands = provenance.add_subparsers(dest="provenance_command", required=True)
    provenance_import = provenance_commands.add_parser("import")
    provenance_import.add_argument("--plan", required=True)
    storage = commands.add_parser("storage")
    storage_commands = storage.add_subparsers(dest="storage_command", required=True)
    storage_manifest = storage_commands.add_parser("manifest")
    storage_manifest.add_argument("--profile", action="append", default=[], required=True)
    storage_manifest.add_argument("--shared-root", required=True)
    storage_manifest.add_argument("--include-relative", action="append", default=[])
    storage_manifest.add_argument("--output", required=True)
    storage_stage = storage_commands.add_parser("stage")
    storage_stage.add_argument("--manifest", required=True)
    storage_stage.add_argument("--output", required=True)
    rollback = commands.add_parser("rollback")
    rollback_commands = rollback.add_subparsers(dest="rollback_command", required=True)
    rollback_manifest = rollback_commands.add_parser("manifest")
    rollback_manifest.add_argument("--legacy-repo", required=True)
    rollback_manifest.add_argument("--legacy-service", required=True)
    rollback_manifest.add_argument("--legacy-codex-bin", required=True)
    rollback_manifest.add_argument("--artifact", action="append", default=[], required=True)
    rollback_manifest.add_argument("--control-period-end", required=True)
    rollback_manifest.add_argument("--output", required=True)
    rollback_verify = rollback_commands.add_parser("verify")
    rollback_verify.add_argument("--manifest", required=True)
    rollback_verify.add_argument("--output")
    qualification = commands.add_parser("qualification")
    qualification_commands = qualification.add_subparsers(
        dest="qualification_command", required=True
    )
    qualification_plan = qualification_commands.add_parser("plan")
    qualification_plan.add_argument("--report-id", required=True)
    qualification_plan.add_argument("--release-tuple", required=True)
    qualification_plan.add_argument("--runtime-install-report", required=True)
    qualification_plan.add_argument("--upstream-checkout", required=True)
    qualification_plan.add_argument("--evidence-directory", required=True)
    qualification_plan.add_argument("--manifest-output", required=True)
    qualification_plan.add_argument("--report-output", required=True)
    qualification_plan.add_argument("--output", required=True)
    qualification_manifest = qualification_commands.add_parser("manifest")
    qualification_manifest.add_argument("--report-id", required=True)
    qualification_manifest.add_argument("--release-tuple", required=True)
    qualification_manifest.add_argument("--evidence", action="append", default=[], required=True)
    qualification_manifest.add_argument("--output", required=True)
    qualification_verify = qualification_commands.add_parser("verify")
    qualification_verify.add_argument("--manifest", required=True)
    qualification_verify.add_argument("--output")
    deployment = commands.add_parser("deployment")
    deployment_commands = deployment.add_subparsers(dest="deployment_command", required=True)
    deployment_manifest = deployment_commands.add_parser("manifest")
    deployment_manifest.add_argument("--target", choices=("staging", "production"), required=True)
    deployment_manifest.add_argument("--runtime-install-report", required=True)
    deployment_manifest.add_argument("--qualification-manifest", required=True)
    deployment_manifest.add_argument("--rollback-manifest", required=True)
    deployment_manifest.add_argument("--instance", action="append", default=[], required=True)
    deployment_manifest.add_argument("--output", required=True)
    deployment_install = deployment_commands.add_parser("install-release")
    deployment_install.add_argument("--wheel", required=True)
    deployment_install.add_argument("--wheel-sha256", required=True)
    deployment_install.add_argument("--releases-root", required=True)
    deployment_install.add_argument("--release-id", required=True)
    deployment_install.add_argument("--python", required=True)
    deployment_install.add_argument("--output", required=True)
    deployment_verify_install = deployment_commands.add_parser("verify-install")
    deployment_verify_install.add_argument("--report", required=True)
    deployment_verify_install.add_argument("--output")
    deployment_prepare = deployment_commands.add_parser("prepare-instance-links")
    deployment_prepare.add_argument("--install-report", required=True)
    deployment_prepare.add_argument("--agent-instance-id", required=True)
    deployment_prepare.add_argument("--instance-link", required=True)
    deployment_prepare.add_argument("--runtime-link", required=True)
    deployment_prepare.add_argument("--wrapper-link", required=True)
    deployment_prepare.add_argument("--claude-wrapper-link")
    deployment_prepare.add_argument("--output", required=True)
    deployment_switch = deployment_commands.add_parser("switch-instance-release")
    deployment_switch.add_argument("--old-install-report", required=True)
    deployment_switch.add_argument("--new-install-report", required=True)
    deployment_switch.add_argument("--agent-instance-id", required=True)
    deployment_switch.add_argument("--instance-link", required=True)
    deployment_switch.add_argument("--runtime-link", required=True)
    deployment_switch.add_argument("--wrapper-link", required=True)
    deployment_switch.add_argument("--intent", required=True)
    deployment_switch.add_argument("--result", required=True)
    deployment_audit = deployment_commands.add_parser("audit")
    deployment_audit.add_argument("--manifest", required=True)
    deployment_audit.add_argument("--output")
    namespace = parser.parse_args()
    try:
        # Artifact-only commands intentionally work on an isolated staging host
        # without a live Runtime configuration or daemon.
        if namespace.command == "qualification":
            if namespace.qualification_command == "plan":
                raise SystemExit(
                    _qualification_plan(
                        report_id=namespace.report_id,
                        release_tuple=namespace.release_tuple,
                        runtime_install_report=namespace.runtime_install_report,
                        upstream_checkout=namespace.upstream_checkout,
                        evidence_directory=namespace.evidence_directory,
                        manifest_output=namespace.manifest_output,
                        report_output=namespace.report_output,
                        output=namespace.output,
                    )
                )
            if namespace.qualification_command == "manifest":
                raise SystemExit(
                    _qualification_manifest(
                        report_id=namespace.report_id,
                        release_tuple=namespace.release_tuple,
                        evidence=namespace.evidence,
                        output=namespace.output,
                    )
                )
            if namespace.qualification_command == "verify":
                raise SystemExit(
                    _qualification_verify(
                        manifest=namespace.manifest,
                        output=namespace.output,
                    )
                )
        if namespace.command == "storage" and namespace.storage_command == "manifest":
            raise SystemExit(
                _storage_manifest(
                    profiles=namespace.profile,
                    shared_root=namespace.shared_root,
                    includes=namespace.include_relative,
                    output=namespace.output,
                )
            )
        if namespace.command == "storage" and namespace.storage_command == "stage":
            raise SystemExit(
                _storage_stage(
                    manifest=namespace.manifest,
                    output=namespace.output,
                )
            )
        if namespace.command == "rollback":
            if namespace.rollback_command == "manifest":
                raise SystemExit(
                    _rollback_manifest(
                        legacy_repo=namespace.legacy_repo,
                        legacy_service=namespace.legacy_service,
                        legacy_codex_bin=namespace.legacy_codex_bin,
                        artifacts=namespace.artifact,
                        control_period_end=namespace.control_period_end,
                        output=namespace.output,
                    )
                )
            if namespace.rollback_command == "verify":
                raise SystemExit(
                    _rollback_verify(
                        manifest=namespace.manifest,
                        output=namespace.output,
                    )
                )
        if namespace.command == "deployment":
            if namespace.deployment_command == "manifest":
                raise SystemExit(
                    _deployment_manifest(
                        target=namespace.target,
                        runtime_install_report=namespace.runtime_install_report,
                        qualification_manifest=namespace.qualification_manifest,
                        rollback_manifest=namespace.rollback_manifest,
                        instances=namespace.instance,
                        output=namespace.output,
                    )
                )
            if namespace.deployment_command == "install-release":
                raise SystemExit(
                    _deployment_install_release(
                        wheel=namespace.wheel,
                        wheel_sha256=namespace.wheel_sha256,
                        releases_root=namespace.releases_root,
                        release_id=namespace.release_id,
                        python=namespace.python,
                        output=namespace.output,
                    )
                )
            if namespace.deployment_command == "verify-install":
                raise SystemExit(
                    _deployment_verify_install(
                        report=namespace.report,
                        output=namespace.output,
                    )
                )
            if namespace.deployment_command == "prepare-instance-links":
                raise SystemExit(
                    _deployment_prepare_instance(
                        install_report=namespace.install_report,
                        agent_instance_id=namespace.agent_instance_id,
                        instance_link=namespace.instance_link,
                        runtime_link=namespace.runtime_link,
                        wrapper_link=namespace.wrapper_link,
                        claude_wrapper_link=namespace.claude_wrapper_link,
                        output=namespace.output,
                    )
                )
            if namespace.deployment_command == "switch-instance-release":
                raise SystemExit(
                    _deployment_switch_instance(
                        old_install_report=namespace.old_install_report,
                        new_install_report=namespace.new_install_report,
                        agent_instance_id=namespace.agent_instance_id,
                        instance_link=namespace.instance_link,
                        runtime_link=namespace.runtime_link,
                        wrapper_link=namespace.wrapper_link,
                        intent=namespace.intent,
                        result=namespace.result,
                    )
                )
            if namespace.deployment_command == "audit":
                raise SystemExit(
                    _deployment_audit(
                        manifest=namespace.manifest,
                        output=namespace.output,
                    )
                )
        if namespace.command == "inventory":
            if namespace.inventory_command == "compare":
                raise SystemExit(
                    _inventory_compare(
                        baseline=namespace.baseline,
                        candidate=namespace.candidate,
                        binding_plan=namespace.binding_plan,
                        output=namespace.output,
                    )
                )
            if namespace.inventory_command == "plan-bindings":
                raise SystemExit(
                    _inventory_plan_bindings(
                        inventory=namespace.inventory,
                        generation_id=namespace.generation_id,
                        output=namespace.output,
                        decisions=namespace.decisions,
                    )
                )
            if namespace.inventory_command == "stage-topic-config":
                raise SystemExit(
                    _inventory_stage_topic_config(
                        baseline=namespace.baseline,
                        binding_plan=namespace.binding_plan,
                        decisions=namespace.decisions,
                        target=namespace.target,
                        output=namespace.output,
                    )
                )
            if namespace.inventory_command == "plan-session-provenance":
                raise SystemExit(
                    _inventory_plan_session_provenance(
                        inventory=namespace.inventory,
                        binding_plan=namespace.binding_plan,
                        output=namespace.output,
                    )
                )
        config = _load_config(namespace.config)
        if namespace.command == "serve":
            asyncio.run(RuntimeServer(config).serve())
            return
        if namespace.command == "status":
            raise SystemExit(asyncio.run(_status(config)))
        if namespace.command == "local-summary":
            raise SystemExit(_local_summary(config))
        if namespace.command == "doctor":
            raise SystemExit(asyncio.run(_doctor(config)))
        if namespace.command == "provenance" and namespace.provenance_command == "import":
            raise SystemExit(asyncio.run(_provenance_import(config, plan_path=namespace.plan)))
        if namespace.command == "inventory" and namespace.inventory_command == "export":
            raise SystemExit(
                _inventory_export(
                    config,
                    output=namespace.output,
                    cus_control_store=namespace.cus_control_store,
                    legacy_project_settings=namespace.legacy_project_settings,
                    skip_live_tmux=namespace.skip_live_tmux,
                )
            )
        if (
            namespace.command == "inventory"
            and namespace.inventory_command == "fresh-context-preflight"
        ):
            raise SystemExit(
                _inventory_fresh_context_preflight(
                    config,
                    baseline=namespace.baseline,
                    binding_plan=namespace.binding_plan,
                    output=namespace.output,
                )
            )
    except (InventoryError, OSError, ValueError) as exc:
        print(f"ai-agent-runtime: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()
