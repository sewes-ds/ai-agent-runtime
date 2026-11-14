"""Prepare absent instance-scoped entrypoint links without starting services."""

from __future__ import annotations

import os
import re
import stat
import uuid
from pathlib import Path
from typing import Final

from ai_agent_runtime.deployment.install import verify_runtime_install_report
from ai_agent_runtime.migration.inventory import InventoryError, _digest

INSTANCE_PREPARATION_SCHEMA: Final = "ai-agent-runtime.instance-preparation.v1"
_INSTANCE_ID_RE: Final = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z")


def _safe_parent(path: Path) -> Path:
    parent = path.parent
    if not path.is_absolute() or ".." in path.parts or parent == Path("/"):
        raise InventoryError("instance entrypoint path is unsafe")
    if parent.is_symlink():
        raise InventoryError("instance entrypoint parent must not be a symlink")
    parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    metadata = parent.stat()
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or metadata.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
    ):
        raise InventoryError("instance entrypoint parent ownership/type/mode is unsafe")
    return parent.resolve(strict=True)


def _create_symlink(link: Path, target: Path) -> None:
    temporary = link.parent / f".{link.name}.{uuid.uuid4().hex}.tmp"
    try:
        temporary.symlink_to(target)
        os.rename(temporary, link)
    finally:
        temporary.unlink(missing_ok=True)


def prepare_instance_links(
    *,
    install_report: Path,
    agent_instance_id: str,
    instance_link: Path,
    runtime_link: Path,
    wrapper_link: Path,
    claude_wrapper_link: Path | None = None,
) -> dict[str, object]:
    """Create a new, unstarted instance link set; existing paths fail closed."""

    if not _INSTANCE_ID_RE.fullmatch(agent_instance_id):
        raise InventoryError("AgentInstance id is invalid")
    instance_link = instance_link.expanduser()
    runtime_link = runtime_link.expanduser()
    wrapper_link = wrapper_link.expanduser()
    if claude_wrapper_link is not None:
        claude_wrapper_link = claude_wrapper_link.expanduser()
    verification = verify_runtime_install_report(install_report)
    if verification.get("accepted") is not True:
        raise InventoryError("Runtime install report is not verified")
    links = (instance_link, runtime_link, wrapper_link) + (
        (claude_wrapper_link,) if claude_wrapper_link is not None else ()
    )
    if len(set(links)) != len(links):
        raise InventoryError("instance entrypoint paths must be distinct")
    if instance_link.name != agent_instance_id:
        raise InventoryError("instance link name must equal AgentInstance id")
    if runtime_link.name != f"ai-agent-runtime@{agent_instance_id}":
        raise InventoryError("Runtime entrypoint name does not match AgentInstance")
    if wrapper_link.name != f"codex-wrapper@{agent_instance_id}":
        raise InventoryError("wrapper entrypoint name does not match AgentInstance")
    if (
        claude_wrapper_link is not None
        and claude_wrapper_link.name != f"claude-wrapper@{agent_instance_id}"
    ):
        raise InventoryError("Claude wrapper entrypoint name does not match AgentInstance")
    for link in links:
        _safe_parent(link)
        if link.exists() or link.is_symlink():
            raise InventoryError(f"refusing to replace existing instance path: {link}")
    release_path = Path(str(verification["release_path"])).resolve(strict=True)
    runtime_target = instance_link / "bin" / "ai-agent-runtime"
    wrapper_target = instance_link / "bin" / "codex-wrapper"
    claude_wrapper_target = instance_link / "bin" / "claude-wrapper"
    created: list[Path] = []
    try:
        _create_symlink(instance_link, release_path)
        created.append(instance_link)
        _create_symlink(runtime_link, runtime_target)
        created.append(runtime_link)
        _create_symlink(wrapper_link, wrapper_target)
        created.append(wrapper_link)
        if claude_wrapper_link is not None:
            _create_symlink(claude_wrapper_link, claude_wrapper_target)
            created.append(claude_wrapper_link)
        if runtime_link.resolve(strict=True) != Path(
            str(verification["runtime_executable"])
        ).resolve(strict=True):
            raise InventoryError(
                "prepared Runtime entrypoint does not resolve to installed release"
            )
        if wrapper_link.resolve(strict=True) != Path(
            str(verification["wrapper_executable"])
        ).resolve(strict=True):
            raise InventoryError("prepared wrapper does not resolve to installed release")
        if claude_wrapper_link is not None and claude_wrapper_link.resolve(strict=True) != (
            release_path / "bin" / "claude-wrapper"
        ):
            raise InventoryError("prepared Claude wrapper does not resolve to installed release")
        for parent in {path.parent for path in links}:
            directory_fd = os.open(parent, os.O_RDONLY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
    except Exception:
        for path in reversed(created):
            if path.is_symlink():
                path.unlink()
        raise
    report: dict[str, object] = {
        "schema": INSTANCE_PREPARATION_SCHEMA,
        "status": "PREPARED_NOT_STARTED",
        "agent_instance_id": agent_instance_id,
        "install_report": str(install_report.expanduser().resolve(strict=True)),
        "release_id": verification["release_id"],
        "instance_link": str(instance_link),
        "runtime_link": str(runtime_link),
        "wrapper_link": str(wrapper_link),
        "claude_wrapper_link": (
            str(claude_wrapper_link) if claude_wrapper_link is not None else None
        ),
        "services_started": False,
        "services_restarted": False,
        "production_cutover_authorized": False,
    }
    report["report_digest"] = _digest(report)
    return report
