"""Read-only, evidence-oriented operator diagnostics."""

from __future__ import annotations

import asyncio
import json
import os
import sqlite3
import stat
import subprocess
import uuid
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from ai_agent_runtime.config import RuntimeConfig
from ai_agent_runtime.ipc.client import request
from ai_agent_runtime.ipc.protocol import versioned
from ai_agent_runtime.process_safety import (
    file_sha256,
    profile_fingerprint,
    secret_file_fingerprint,
    validate_owned_executable,
    validate_profile_path,
    validate_secret_file,
)
from ai_agent_runtime.telegram_upstream.tmux_state import read_tmux_states
from ai_agent_runtime.telegram_upstream.topic_config import read_topic_config

StatusRequester = Callable[[Path, dict[str, Any]], Awaitable[dict[str, Any]]]


def _detail(value: object) -> str:
    if isinstance(value, Path):
        return str(value)
    return str(value)


def _record(
    checks: dict[str, dict[str, object]],
    name: str,
    callback: Callable[[], object],
) -> object | None:
    try:
        value = callback()
    except Exception as exc:
        checks[name] = {
            "ok": False,
            "detail": f"{type(exc).__name__}: {exc}",
        }
        return None
    checks[name] = {"ok": True, "detail": _detail(value)}
    return value


def _check_socket(path: Path) -> str:
    if path.is_symlink():
        raise ValueError("Runtime socket must not be a symlink")
    metadata = path.stat()
    if not stat.S_ISSOCK(metadata.st_mode):
        raise ValueError("Runtime socket path is not a Unix socket")
    if metadata.st_uid != os.getuid():
        raise ValueError("Runtime socket owner differs from the current UID")
    mode = stat.S_IMODE(metadata.st_mode)
    if mode != 0o600:
        raise ValueError(f"Runtime socket mode is {mode:04o}, expected 0600")
    return f"{path} mode=0600 uid={metadata.st_uid}"


def _open_database_read_only(path: Path) -> sqlite3.Connection:
    if path.is_symlink() or not path.is_file():
        raise ValueError("Runtime database is not a regular non-symlink file")
    metadata = path.stat()
    if metadata.st_uid != os.getuid():
        raise ValueError("Runtime database owner differs from the current UID")
    if metadata.st_mode & (stat.S_IRWXG | stat.S_IRWXO):
        raise ValueError("Runtime database grants group/world permissions")
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def _check_database(path: Path) -> str:
    connection = _open_database_read_only(path)
    try:
        result = connection.execute("PRAGMA quick_check").fetchone()
        if result is None or result[0] != "ok":
            raise ValueError("SQLite quick_check did not return ok")
        tables = {
            str(row[0])
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        required = {"bindings", "effective_specs", "instances", "outbox"}
        missing = required - tables
        if missing:
            raise ValueError(f"Runtime database is missing tables: {sorted(missing)}")
        return f"quick_check=ok tables={len(tables)}"
    finally:
        connection.close()


def _check_real_codex(config: RuntimeConfig) -> str:
    binary = validate_owned_executable(config.real_codex)
    observed_digest = file_sha256(binary)
    if observed_digest != config.real_codex_sha256:
        raise ValueError("real Codex digest differs from the configured release tuple")
    completed = subprocess.run(
        (str(binary), "--version"),
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    version_output = f"{completed.stdout}\n{completed.stderr}".strip()
    if completed.returncode != 0:
        raise ValueError("real Codex --version failed")
    if config.real_codex_version not in version_output:
        raise ValueError("real Codex version differs from the configured release tuple")
    return f"path={binary} version={config.real_codex_version} digest={observed_digest}"


def _check_real_claude(config: RuntimeConfig) -> str:
    if not config.real_claude_version or not config.real_claude_sha256:
        return "not configured"
    binary = validate_owned_executable(config.real_claude)
    observed_digest = file_sha256(binary)
    if observed_digest != config.real_claude_sha256:
        raise ValueError("real Claude digest differs from the configured release tuple")
    completed = subprocess.run(
        (str(binary), "--version"),
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    output = f"{completed.stdout}\n{completed.stderr}".strip()
    if completed.returncode != 0 or config.real_claude_version not in output:
        raise ValueError("real Claude version differs from the configured release tuple")
    return f"path={binary} version={config.real_claude_version} digest={observed_digest}"


def _check_profiles(config: RuntimeConfig) -> str:
    root = validate_profile_path(config.profiles_root, root=config.profiles_root)
    connection = _open_database_read_only(config.database_path)
    try:
        rows = connection.execute(
            """
            SELECT payload_json, codex_home, profile_fingerprint
            FROM effective_specs ORDER BY binding_id
            """
        ).fetchall()
    finally:
        connection.close()
    for row in rows:
        payload = json.loads(str(row["payload_json"]))
        if payload.get("provider") == "claude_code":
            profile = validate_secret_file(Path(str(row["codex_home"])))
            fingerprint = secret_file_fingerprint(profile)
        else:
            profile = validate_profile_path(Path(str(row["codex_home"])), root=root)
            fingerprint = profile_fingerprint(profile)
        if fingerprint != row["profile_fingerprint"]:
            raise ValueError(f"profile metadata changed after verification: {profile.name}")
    return f"root={root} verified_effective_profiles={len(rows)}"


def _check_session_view(config: RuntimeConfig, status: dict[str, Any]) -> str:
    root = config.sessions_view_root
    if root is None:
        return "not configured"
    metadata = root.lstat()
    if not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.getuid():
        raise ValueError("transcript view root has unsafe type or owner")
    if metadata.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        raise ValueError("transcript view root is group/world writable")
    report = status.get("status")
    if not isinstance(report, dict):
        raise ValueError("Runtime status has no session-view report")
    view = report.get("session_view")
    if not isinstance(view, dict) or view.get("root") != str(root):
        raise ValueError("Runtime session-view configuration differs from doctor")
    if view.get("error") is not None:
        raise ValueError(f"Runtime transcript projection failed: {view['error']}")
    return f"root={root} projection=healthy"


def _canonical_existing(path: str) -> str:
    return str(Path(path).resolve(strict=True))


def _check_correlation(config: RuntimeConfig) -> str:
    topics = read_topic_config(config.topic_config_path)
    states = read_tmux_states(config.tmux_state_path)
    connection = _open_database_read_only(config.database_path)
    try:
        bindings = connection.execute(
            """
            SELECT binding_id, chat_id, thread_id, canonical_cwd,
                   expected_tmux_session, configured, management_mode
            FROM bindings WHERE agent_instance_id=? ORDER BY binding_id
            """,
            (config.agent_instance_id,),
        ).fetchall()
        active_instances = connection.execute(
            """
            SELECT binding_id, tmux_session, lifecycle, launch_spec_json
            FROM instances
            WHERE lifecycle NOT IN ('EXITED', 'RETIRED_FOR_RESTART')
            """
        ).fetchall()
    finally:
        connection.close()

    by_binding: dict[str, list[sqlite3.Row]] = {}
    for instance in active_instances:
        by_binding.setdefault(str(instance["binding_id"]), []).append(instance)

    checked_active = 0
    for binding in bindings:
        if int(binding["chat_id"]) != config.forum_chat_id:
            raise ValueError(f"binding forum mismatch: {binding['binding_id']}")
        topic = topics.topic(int(binding["thread_id"]))
        if topic is None:
            raise ValueError(f"binding topic is absent: {binding['binding_id']}")
        if topic.cwd is None or _canonical_existing(topic.cwd) != str(binding["canonical_cwd"]):
            raise ValueError(f"binding cwd drift: {binding['binding_id']}")
        if (
            int(binding["configured"]) == 1
            and binding["management_mode"] == "MANAGED"
            and not topic.is_managed_candidate
        ):
            raise ValueError(f"managed binding config drift: {binding['binding_id']}")
        for instance in by_binding.get(str(binding["binding_id"]), []):
            raw_launch = instance["launch_spec_json"]
            launch = json.loads(raw_launch) if isinstance(raw_launch, str) else {}
            desired = launch.get("desired", {}) if isinstance(launch, dict) else {}
            expected_provider = "claude" if desired.get("provider") == "claude_code" else "codex"
            matches = [
                state
                for state in states
                if state.chat_id == config.forum_chat_id
                and state.thread_id == int(binding["thread_id"])
                and state.session_name == instance["tmux_session"]
                and state.provider == expected_provider
                and _canonical_existing(state.cwd) == str(binding["canonical_cwd"])
            ]
            if len(matches) != 1:
                raise ValueError(
                    f"active instance correlation is not exact: {binding['binding_id']}"
                )
            checked_active += 1
    return f"bindings={len(bindings)} active_instances={checked_active} config={topics.digest}"


async def collect_doctor_report(
    config: RuntimeConfig,
    *,
    status_requester: StatusRequester = request,
) -> dict[str, object]:
    """Collect read-only diagnostics without creating or repairing state."""

    checks: dict[str, dict[str, object]] = {}
    _record(checks, "runtime_socket", lambda: _check_socket(config.socket_path))
    try:
        status = await asyncio.wait_for(
            status_requester(
                config.socket_path,
                versioned("status", str(uuid.uuid4())),
            ),
            timeout=5,
        )
        if status.get("ok") is not True:
            raise ValueError("Runtime status request was rejected")
        checks["runtime_status"] = {
            "ok": True,
            "detail": f"daemon_boot_id={status.get('daemon_boot_id', 'missing')}",
        }
        if config.sessions_view_root is not None:
            _record(checks, "session_view", lambda: _check_session_view(config, status))
    except Exception as exc:
        checks["runtime_status"] = {
            "ok": False,
            "detail": f"{type(exc).__name__}: {exc}",
        }
    _record(checks, "database", lambda: _check_database(config.database_path))
    _record(checks, "real_codex", lambda: _check_real_codex(config))
    _record(checks, "real_claude", lambda: _check_real_claude(config))
    _record(checks, "profiles", lambda: _check_profiles(config))
    _record(
        checks,
        "topic_config",
        lambda: f"digest={read_topic_config(config.topic_config_path).digest}",
    )
    _record(
        checks,
        "tmux_state",
        lambda: f"entries={len(read_tmux_states(config.tmux_state_path))}",
    )
    _record(checks, "binding_correlation", lambda: _check_correlation(config))
    healthy = all(check["ok"] is True for check in checks.values())
    return {
        "agent_instance_id": config.agent_instance_id,
        "healthy": healthy,
        "checks": checks,
        "mutations_performed": False,
    }
