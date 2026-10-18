"""SQLite-backed local authority for bindings and verified effective specs."""

from __future__ import annotations

import json
import os
import sqlite3
import stat
import time
from pathlib import Path
from typing import Any, cast

from ai_agent_runtime.domain import (
    DesiredSpec,
    DispatchState,
    ManagementMode,
    ObservedSpecProposal,
    PendingEvidenceInstance,
    RegisteredInstance,
    RetirementCandidate,
    RolloutObservationCursor,
    RolloutObservationTarget,
    RuntimeBinding,
    SyncState,
    UserNotificationCode,
    sha256_digest,
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS schema_meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS bindings (
    binding_id TEXT PRIMARY KEY,
    agent_instance_id TEXT NOT NULL,
    chat_id INTEGER NOT NULL,
    thread_id INTEGER NOT NULL,
    project_id TEXT NOT NULL,
    canonical_cwd TEXT NOT NULL,
    expected_tmux_session TEXT NOT NULL,
    configured INTEGER NOT NULL CHECK (configured IN (0, 1)),
    config_digest TEXT,
    management_mode TEXT NOT NULL DEFAULT 'MANAGED',
    UNIQUE (agent_instance_id, chat_id, thread_id),
    UNIQUE (agent_instance_id, expected_tmux_session)
);
CREATE TABLE IF NOT EXISTS effective_specs (
    binding_id TEXT PRIMARY KEY REFERENCES bindings(binding_id) ON DELETE CASCADE,
    generation_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    spec_digest TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    codex_home TEXT NOT NULL,
    profile_fingerprint TEXT NOT NULL,
    verified_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS desired_namespaces (
    agent_instance_id TEXT PRIMARY KEY,
    generation_id TEXT NOT NULL,
    snapshot_digest TEXT,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS project_relocations (
    relocation_id TEXT PRIMARY KEY,
    agent_instance_id TEXT NOT NULL,
    project_id TEXT NOT NULL,
    old_canonical_cwd TEXT NOT NULL,
    new_canonical_cwd TEXT NOT NULL,
    binding_ids_json TEXT NOT NULL,
    applied_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS control_plane_sync (
    agent_instance_id TEXT PRIMARY KEY,
    last_seen_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS session_provenance (
    session_id TEXT PRIMARY KEY,
    binding_id TEXT NOT NULL REFERENCES bindings(binding_id),
    account_profile TEXT NOT NULL,
    rollout_path TEXT NOT NULL,
    rollout_device INTEGER NOT NULL,
    rollout_inode INTEGER NOT NULL,
    observed_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS instances (
    instance_id TEXT PRIMARY KEY,
    binding_id TEXT NOT NULL REFERENCES bindings(binding_id),
    wrapper_pid INTEGER NOT NULL,
    wrapper_starttime INTEGER NOT NULL,
    child_pid INTEGER,
    child_starttime INTEGER,
    tmux_session TEXT NOT NULL,
    tmux_pane TEXT NOT NULL,
    lifecycle TEXT NOT NULL,
    applied_digest TEXT,
    launch_spec_json TEXT,
    daemon_boot_id TEXT,
    connection_generation INTEGER,
    resync_from_boot_id TEXT,
    last_heartbeat_at REAL,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS outbox (
    event_id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    binding_id TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_digest TEXT NOT NULL,
    ordering_sequence INTEGER,
    attempts INTEGER NOT NULL DEFAULT 0,
    next_retry_at REAL NOT NULL,
    ack_state TEXT NOT NULL DEFAULT 'PENDING'
);
CREATE TABLE IF NOT EXISTS request_results (
    request_id TEXT PRIMARY KEY,
    request_digest TEXT NOT NULL,
    response_json TEXT NOT NULL,
    created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS applied_specs (
    instance_id TEXT PRIMARY KEY REFERENCES instances(instance_id) ON DELETE CASCADE,
    binding_id TEXT NOT NULL REFERENCES bindings(binding_id),
    generation_id TEXT NOT NULL,
    revision INTEGER NOT NULL,
    spec_digest TEXT NOT NULL,
    evidence_json TEXT NOT NULL,
    verified_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS codex_rollout_observers (
    instance_id TEXT PRIMARY KEY REFERENCES instances(instance_id) ON DELETE CASCADE,
    rollout_device INTEGER NOT NULL,
    rollout_inode INTEGER NOT NULL,
    offset INTEGER NOT NULL,
    observation_sequence INTEGER NOT NULL,
    last_model TEXT,
    last_reasoning_effort TEXT,
    active_turn_id TEXT,
    pending_model TEXT,
    pending_reasoning_effort TEXT,
    pending_evidence_digest TEXT,
    pending_turn_id TEXT,
    updated_at REAL NOT NULL
);
"""

_INSTANCE_HEARTBEAT_STALE_SECONDS = 10.0

_PUBLIC_APPLIED_EVIDENCE_FIELDS = frozenset(
    {
        "provider",
        "account_profile",
        "actual_codex_home",
        "actual_model",
        "actual_reasoning_effort",
        "profile_fingerprint",
        "real_codex_path",
        "real_codex_device",
        "real_codex_inode",
        "real_codex_sha256",
        "real_codex_version",
        "child_pid",
        "child_starttime",
        "session_id",
        "rollout_path",
        "rollout_device",
        "rollout_inode",
        "wrapper_pid",
        "wrapper_starttime",
        "wrapper_identity",
        "daemon_boot_id",
        "connection_generation",
        "tmux_session",
        "tmux_pane",
        "observed_event_id",
        "observed_evidence_digest",
        "observation_sequence",
    }
)


def _applied_status(row: sqlite3.Row) -> dict[str, object] | None:
    if row["verified_applied_digest"] is None:
        return None
    raw_evidence = row["applied_evidence_json"]
    evidence: dict[str, Any] = {}
    if isinstance(raw_evidence, str):
        decoded = json.loads(raw_evidence)
        if isinstance(decoded, dict):
            evidence = {
                key: value
                for key, value in decoded.items()
                if key in _PUBLIC_APPLIED_EVIDENCE_FIELDS
            }
    if "provider" not in evidence and isinstance(row["launch_spec_json"], str):
        launch = json.loads(row["launch_spec_json"])
        desired = launch.get("desired") if isinstance(launch, dict) else None
        provider = desired.get("provider") if isinstance(desired, dict) else None
        if isinstance(provider, str) and provider:
            evidence["provider"] = provider
    return {
        "generation_id": row["applied_generation"],
        "revision": row["applied_revision"],
        "spec_digest": row["verified_applied_digest"],
        "effective": evidence,
    }


class RuntimeStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        parent = self.path.parent
        parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if parent.is_symlink():
            raise ValueError("Runtime database directory must not be a symlink")
        parent_metadata = parent.stat()
        if not stat.S_ISDIR(parent_metadata.st_mode) or parent_metadata.st_uid != os.getuid():
            raise ValueError("Runtime database directory ownership/type is unsafe")
        if parent_metadata.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
            raise ValueError("Runtime database directory is group/world writable")
        os.chmod(parent, 0o700)
        if self.path.is_symlink():
            raise ValueError("Runtime database must not be a symlink")
        if self.path.exists():
            database_metadata = self.path.stat()
            if (
                not stat.S_ISREG(database_metadata.st_mode)
                or database_metadata.st_uid != os.getuid()
                or database_metadata.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
            ):
                raise ValueError("Runtime database ownership/type/mode is unsafe")
        self._connection = sqlite3.connect(path)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA foreign_keys=ON")
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=FULL")
        self._connection.executescript(_SCHEMA)
        self._ensure_column("bindings", "config_digest", "TEXT")
        self._ensure_column(
            "bindings",
            "management_mode",
            "TEXT NOT NULL DEFAULT 'MANAGED'",
        )
        self._ensure_column("instances", "launch_spec_json", "TEXT")
        self._ensure_column("instances", "daemon_boot_id", "TEXT")
        self._ensure_column(
            "instances",
            "connection_generation",
            "INTEGER",
        )
        self._ensure_column("instances", "resync_from_boot_id", "TEXT")
        self._ensure_column("instances", "last_heartbeat_at", "REAL")
        self._ensure_column("outbox", "ordering_sequence", "INTEGER")
        self._backfill_outbox_ordering_sequences()
        self._ensure_column("codex_rollout_observers", "active_turn_id", "TEXT")
        self._ensure_column("codex_rollout_observers", "pending_model", "TEXT")
        self._ensure_column("codex_rollout_observers", "pending_reasoning_effort", "TEXT")
        self._ensure_column("codex_rollout_observers", "pending_evidence_digest", "TEXT")
        self._ensure_column("codex_rollout_observers", "pending_turn_id", "TEXT")
        self._connection.execute(
            "INSERT OR REPLACE INTO schema_meta(key, value) VALUES('schema_version', '2')"
        )
        self._connection.commit()
        os.chmod(path, 0o600)

    def _ensure_column(self, table: str, column: str, definition: str) -> None:
        columns = {
            str(row["name"])
            for row in self._connection.execute(f"PRAGMA table_info({table})").fetchall()
        }
        if column not in columns:
            self._connection.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    def _backfill_outbox_ordering_sequences(self) -> None:
        """Upgrade valid pre-ordering observed events without trusting malformed rows."""

        rows = self._connection.execute(
            """
            SELECT event_id, payload_json, payload_digest FROM outbox
            WHERE kind='OBSERVED_SPEC_PROPOSAL' AND ordering_sequence IS NULL
            """
        ).fetchall()
        for row in rows:
            try:
                payload = json.loads(row["payload_json"])
            except (TypeError, json.JSONDecodeError):
                continue
            if not isinstance(payload, dict) or sha256_digest(payload) != row["payload_digest"]:
                continue
            sequence = payload.get("observation_sequence")
            if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 0:
                continue
            self._connection.execute(
                "UPDATE outbox SET ordering_sequence=? WHERE event_id=?",
                (sequence, row["event_id"]),
            )

    def close(self) -> None:
        self._connection.close()

    def upsert_binding(self, binding: RuntimeBinding) -> None:
        with self._connection:
            self._connection.execute(
                """
                INSERT INTO bindings(
                    binding_id, agent_instance_id, chat_id, thread_id, project_id,
                    canonical_cwd, expected_tmux_session, configured,
                    config_digest, management_mode
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(binding_id) DO UPDATE SET
                    agent_instance_id=excluded.agent_instance_id,
                    chat_id=excluded.chat_id,
                    thread_id=excluded.thread_id,
                    project_id=excluded.project_id,
                    canonical_cwd=excluded.canonical_cwd,
                    expected_tmux_session=excluded.expected_tmux_session,
                    configured=excluded.configured,
                    config_digest=excluded.config_digest,
                    management_mode=excluded.management_mode
                """,
                (
                    binding.binding_id,
                    binding.agent_instance_id,
                    binding.chat_id,
                    binding.thread_id,
                    binding.project_id,
                    binding.canonical_cwd,
                    binding.expected_tmux_session,
                    int(binding.configured),
                    binding.config_digest,
                    binding.management_mode,
                ),
            )

    def list_bindings(self, agent_instance_id: str) -> tuple[RuntimeBinding, ...]:
        rows = self._connection.execute(
            "SELECT * FROM bindings WHERE agent_instance_id=? ORDER BY binding_id",
            (agent_instance_id,),
        ).fetchall()
        return tuple(
            RuntimeBinding(
                binding_id=row["binding_id"],
                agent_instance_id=row["agent_instance_id"],
                chat_id=row["chat_id"],
                thread_id=row["thread_id"],
                project_id=row["project_id"],
                canonical_cwd=row["canonical_cwd"],
                expected_tmux_session=row["expected_tmux_session"],
                configured=bool(row["configured"]),
                config_digest=row["config_digest"],
                management_mode=ManagementMode(row["management_mode"]),
            )
            for row in rows
        )

    def get_binding(self, binding_id: str) -> RuntimeBinding | None:
        row = self._connection.execute(
            "SELECT * FROM bindings WHERE binding_id=?",
            (binding_id,),
        ).fetchone()
        if row is None:
            return None
        return RuntimeBinding(
            binding_id=row["binding_id"],
            agent_instance_id=row["agent_instance_id"],
            chat_id=row["chat_id"],
            thread_id=row["thread_id"],
            project_id=row["project_id"],
            canonical_cwd=row["canonical_cwd"],
            expected_tmux_session=row["expected_tmux_session"],
            configured=bool(row["configured"]),
            config_digest=row["config_digest"],
            management_mode=ManagementMode(row["management_mode"]),
        )

    def dispatch_state(
        self,
        *,
        agent_instance_id: str,
        chat_id: int,
        thread_id: int,
    ) -> DispatchState | None:
        binding_row = self._connection.execute(
            """
            SELECT * FROM bindings
            WHERE agent_instance_id=? AND chat_id=? AND thread_id=?
            """,
            (agent_instance_id, chat_id, thread_id),
        ).fetchone()
        if binding_row is None:
            return None
        binding = RuntimeBinding(
            binding_id=binding_row["binding_id"],
            agent_instance_id=binding_row["agent_instance_id"],
            chat_id=binding_row["chat_id"],
            thread_id=binding_row["thread_id"],
            project_id=binding_row["project_id"],
            canonical_cwd=binding_row["canonical_cwd"],
            expected_tmux_session=binding_row["expected_tmux_session"],
            configured=bool(binding_row["configured"]),
            config_digest=binding_row["config_digest"],
            management_mode=ManagementMode(binding_row["management_mode"]),
        )
        desired_row = self._connection.execute(
            "SELECT spec_digest, payload_json FROM effective_specs WHERE binding_id=?",
            (binding.binding_id,),
        ).fetchone()
        desired_payload = (
            json.loads(desired_row["payload_json"]) if desired_row is not None else None
        )
        if desired_payload is not None and not isinstance(desired_payload, dict):
            raise ValueError("persisted effective spec payload is invalid")
        live_rows = self._connection.execute(
            """
            SELECT i.lifecycle, i.launch_spec_json,
                   a.spec_digest AS applied_spec_digest
            FROM instances AS i
            LEFT JOIN applied_specs AS a ON a.instance_id=i.instance_id
            WHERE i.binding_id=?
              AND i.lifecycle NOT IN ('EXITED', 'RETIRED_FOR_RESTART', 'START_FAILED')
            ORDER BY i.updated_at DESC, i.instance_id
            """,
            (binding.binding_id,),
        ).fetchall()
        sole = live_rows[0] if len(live_rows) == 1 else None
        launch_spec_digest: str | None = None
        if sole is not None and sole["launch_spec_json"] is not None:
            launch_spec = json.loads(sole["launch_spec_json"])
            desired = launch_spec.get("desired") if isinstance(launch_spec, dict) else None
            if isinstance(desired, dict) and isinstance(desired.get("spec_digest"), str):
                launch_spec_digest = str(desired["spec_digest"])
        return DispatchState(
            binding=binding,
            desired_spec_digest=(
                str(desired_row["spec_digest"]) if desired_row is not None else None
            ),
            desired_provider=(
                str(desired_payload["provider"])
                if desired_payload is not None
                and isinstance(desired_payload.get("provider"), str)
                else None
            ),
            desired_model=(
                str(desired_payload["model"])
                if desired_payload is not None
                and isinstance(desired_payload.get("model"), str)
                else None
            ),
            live_instance_count=len(live_rows),
            lifecycle=str(sole["lifecycle"]) if sole is not None else None,
            applied_spec_digest=(
                str(sole["applied_spec_digest"])
                if sole is not None and sole["applied_spec_digest"] is not None
                else None
            ),
            launch_spec_digest=launch_spec_digest,
        )

    def live_binding_ids(self, binding_ids: frozenset[str]) -> frozenset[str]:
        if not binding_ids:
            return frozenset()
        placeholders = ",".join("?" for _ in binding_ids)
        rows = self._connection.execute(
            f"""
            SELECT DISTINCT binding_id FROM instances
            WHERE binding_id IN ({placeholders})
              AND lifecycle NOT IN ('EXITED', 'RETIRED_FOR_RESTART')
            """,
            tuple(sorted(binding_ids)),
        ).fetchall()
        return frozenset(str(row["binding_id"]) for row in rows)

    def get_project_relocation(self, relocation_id: str) -> dict[str, object] | None:
        row = self._connection.execute(
            "SELECT * FROM project_relocations WHERE relocation_id=?",
            (relocation_id,),
        ).fetchone()
        if row is None:
            return None
        binding_ids = json.loads(row["binding_ids_json"])
        if not isinstance(binding_ids, list) or any(
            not isinstance(binding_id, str) for binding_id in binding_ids
        ):
            raise ValueError("persisted project relocation binding ids are invalid")
        return {
            "relocation_id": str(row["relocation_id"]),
            "agent_instance_id": str(row["agent_instance_id"]),
            "project_id": str(row["project_id"]),
            "old_canonical_cwd": str(row["old_canonical_cwd"]),
            "new_canonical_cwd": str(row["new_canonical_cwd"]),
            "binding_ids": sorted(binding_ids),
        }

    def update_binding_observation(
        self,
        binding_id: str,
        *,
        config_digest: str,
        management_mode: ManagementMode,
    ) -> None:
        with self._connection:
            cursor = self._connection.execute(
                """
                UPDATE bindings
                SET config_digest=?, management_mode=?
                WHERE binding_id=?
                """,
                (config_digest, management_mode, binding_id),
            )
            if cursor.rowcount != 1:
                raise KeyError(f"unknown binding: {binding_id}")

    def put_effective_spec(
        self,
        spec: DesiredSpec,
        *,
        codex_home: Path,
        profile_fingerprint: str,
    ) -> None:
        self.validate_effective_spec_update(spec)
        payload = self._encode_spec(spec)
        with self._connection:
            self._connection.execute(
                """
                INSERT INTO desired_namespaces(
                    agent_instance_id, generation_id, snapshot_digest, updated_at
                ) VALUES(?, ?, NULL, ?)
                ON CONFLICT(agent_instance_id) DO UPDATE SET updated_at=excluded.updated_at
                """,
                (spec.agent_instance_id, spec.generation_id, time.time()),
            )
            self._connection.execute(
                """
                INSERT INTO effective_specs(
                    binding_id, generation_id, revision, spec_digest, payload_json,
                    codex_home, profile_fingerprint, verified_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(binding_id) DO UPDATE SET
                    generation_id=excluded.generation_id,
                    revision=excluded.revision,
                    spec_digest=excluded.spec_digest,
                    payload_json=excluded.payload_json,
                    codex_home=excluded.codex_home,
                    profile_fingerprint=excluded.profile_fingerprint,
                    verified_at=excluded.verified_at
                """,
                (
                    spec.binding_id,
                    spec.generation_id,
                    spec.revision,
                    spec.spec_digest,
                    payload,
                    str(codex_home),
                    profile_fingerprint,
                    time.time(),
                ),
            )

    def validate_effective_spec_update(self, spec: DesiredSpec) -> None:
        if not spec.verify_digest():
            raise ValueError("desired spec digest mismatch")
        namespace = self._connection.execute(
            "SELECT generation_id FROM desired_namespaces WHERE agent_instance_id=?",
            (spec.agent_instance_id,),
        ).fetchone()
        if namespace is not None and namespace["generation_id"] != spec.generation_id:
            raise ValueError("new generation requires authoritative snapshot reconcile")
        existing = self._connection.execute(
            "SELECT generation_id, revision, spec_digest FROM effective_specs WHERE binding_id=?",
            (spec.binding_id,),
        ).fetchone()
        if existing is not None and existing["generation_id"] != spec.generation_id:
            raise ValueError("new generation requires authoritative snapshot reconcile")
        if existing is not None:
            if spec.revision < existing["revision"]:
                raise ValueError("revision rollback within generation")
            if (
                spec.revision == existing["revision"]
                and spec.spec_digest != existing["spec_digest"]
            ):
                raise ValueError("same revision has a different digest")

    def namespace_generation(self, agent_instance_id: str) -> str | None:
        row = self._connection.execute(
            "SELECT generation_id FROM desired_namespaces WHERE agent_instance_id=?",
            (agent_instance_id,),
        ).fetchone()
        return None if row is None else str(row["generation_id"])

    @staticmethod
    def _encode_spec(spec: DesiredSpec) -> str:
        return json.dumps(
            {
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
            },
            sort_keys=True,
            separators=(",", ":"),
        )

    def apply_authoritative_snapshot(
        self,
        *,
        agent_instance_id: str,
        generation_id: str,
        snapshot_digest: str,
        updates: tuple[tuple[RuntimeBinding, DesiredSpec, Path, str], ...],
        retired_binding_ids: frozenset[str],
        project_relocations: tuple[dict[str, object], ...] = (),
    ) -> None:
        """Atomically replace desired state without deleting runtime history."""

        if not updates and not retired_binding_ids:
            raise ValueError("authoritative snapshot cannot be empty")
        if not snapshot_digest.startswith("sha256:"):
            raise ValueError("snapshot digest must be sha256-prefixed")
        active_ids = {binding.binding_id for binding, _spec, _home, _fingerprint in updates}
        if len(active_ids) != len(updates):
            raise ValueError("snapshot contains duplicate binding ids")
        if active_ids & retired_binding_ids:
            raise ValueError("binding cannot be active and retired in the same snapshot")

        known = {binding.binding_id for binding in self.list_bindings(agent_instance_id)}
        if retired_binding_ids - known:
            raise ValueError("snapshot retires an unknown binding")
        omitted = known - active_ids - retired_binding_ids
        if omitted:
            raise ValueError(
                "snapshot omits existing bindings without explicit retirement: "
                + ", ".join(sorted(omitted))
            )

        topics: set[tuple[int, int]] = set()
        sessions: set[str] = set()
        current_namespace = self._connection.execute(
            "SELECT generation_id FROM desired_namespaces WHERE agent_instance_id=?",
            (agent_instance_id,),
        ).fetchone()
        same_generation = (
            current_namespace is not None and current_namespace["generation_id"] == generation_id
        )
        relocations_by_project = {
            str(relocation["project_id"]): relocation for relocation in project_relocations
        }
        if len(relocations_by_project) != len(project_relocations):
            raise ValueError("snapshot contains duplicate project relocation")
        for binding, spec, _codex_home, _fingerprint in updates:
            if binding.agent_instance_id != agent_instance_id:
                raise ValueError("snapshot binding AgentInstance mismatch")
            if spec.agent_instance_id != agent_instance_id or spec.generation_id != generation_id:
                raise ValueError("snapshot desired namespace mismatch")
            if binding.binding_id != spec.binding_id:
                raise ValueError("snapshot binding/spec identity mismatch")
            if not spec.verify_digest():
                raise ValueError("desired spec digest mismatch")
            topic = (binding.chat_id, binding.thread_id)
            if topic in topics or binding.expected_tmux_session in sessions:
                raise ValueError("snapshot contains duplicate topic or tmux identity")
            topics.add(topic)
            sessions.add(binding.expected_tmux_session)
            existing = self.get_binding(binding.binding_id)
            if existing is not None and (
                existing.agent_instance_id != binding.agent_instance_id
                or existing.chat_id != binding.chat_id
                or existing.thread_id != binding.thread_id
                or existing.project_id != binding.project_id
            ):
                raise ValueError("binding identity is immutable")
            if existing is not None and existing.canonical_cwd != binding.canonical_cwd:
                relocation = relocations_by_project.get(binding.project_id)
                relocation_binding_ids = (
                    cast(list[str], relocation["binding_ids"]) if relocation is not None else []
                )
                if (
                    relocation is None
                    or relocation["old_canonical_cwd"] != existing.canonical_cwd
                    or relocation["new_canonical_cwd"] != binding.canonical_cwd
                    or binding.binding_id not in relocation_binding_ids
                ):
                    raise ValueError("project path change requires explicit relocation")
            if same_generation:
                self.validate_effective_spec_update(spec)

        now = time.time()
        with self._connection:
            self._connection.execute(
                """
                INSERT INTO desired_namespaces(
                    agent_instance_id, generation_id, snapshot_digest, updated_at
                ) VALUES(?, ?, ?, ?)
                ON CONFLICT(agent_instance_id) DO UPDATE SET
                    generation_id=excluded.generation_id,
                    snapshot_digest=excluded.snapshot_digest,
                    updated_at=excluded.updated_at
                """,
                (agent_instance_id, generation_id, snapshot_digest, now),
            )
            for binding, spec, codex_home, fingerprint in updates:
                self._connection.execute(
                    """
                    INSERT INTO bindings(
                        binding_id, agent_instance_id, chat_id, thread_id, project_id,
                        canonical_cwd, expected_tmux_session, configured,
                        config_digest, management_mode
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(binding_id) DO UPDATE SET
                        canonical_cwd=excluded.canonical_cwd,
                        configured=excluded.configured,
                        config_digest=excluded.config_digest,
                        management_mode=excluded.management_mode
                    """,
                    (
                        binding.binding_id,
                        binding.agent_instance_id,
                        binding.chat_id,
                        binding.thread_id,
                        binding.project_id,
                        binding.canonical_cwd,
                        binding.expected_tmux_session,
                        int(binding.configured),
                        binding.config_digest,
                        binding.management_mode,
                    ),
                )
                self._connection.execute(
                    """
                    INSERT INTO effective_specs(
                        binding_id, generation_id, revision, spec_digest, payload_json,
                        codex_home, profile_fingerprint, verified_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(binding_id) DO UPDATE SET
                        generation_id=excluded.generation_id,
                        revision=excluded.revision,
                        spec_digest=excluded.spec_digest,
                        payload_json=excluded.payload_json,
                        codex_home=excluded.codex_home,
                        profile_fingerprint=excluded.profile_fingerprint,
                        verified_at=excluded.verified_at
                    """,
                    (
                        spec.binding_id,
                        spec.generation_id,
                        spec.revision,
                        spec.spec_digest,
                        self._encode_spec(spec),
                        str(codex_home),
                        fingerprint,
                        now,
                    ),
                )
            for relocation in project_relocations:
                relocation_binding_ids = cast(list[str], relocation["binding_ids"])
                encoded_binding_ids = json.dumps(
                    sorted(relocation_binding_ids),
                    separators=(",", ":"),
                )
                existing_relocation = self.get_project_relocation(str(relocation["relocation_id"]))
                normalized = {
                    "relocation_id": str(relocation["relocation_id"]),
                    "agent_instance_id": str(relocation["agent_instance_id"]),
                    "project_id": str(relocation["project_id"]),
                    "old_canonical_cwd": str(relocation["old_canonical_cwd"]),
                    "new_canonical_cwd": str(relocation["new_canonical_cwd"]),
                    "binding_ids": sorted(relocation_binding_ids),
                }
                if existing_relocation is not None and existing_relocation != normalized:
                    raise ValueError("project relocation id was reused with different data")
                self._connection.execute(
                    """
                    INSERT OR IGNORE INTO project_relocations(
                        relocation_id, agent_instance_id, project_id,
                        old_canonical_cwd, new_canonical_cwd,
                        binding_ids_json, applied_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        normalized["relocation_id"],
                        normalized["agent_instance_id"],
                        normalized["project_id"],
                        normalized["old_canonical_cwd"],
                        normalized["new_canonical_cwd"],
                        encoded_binding_ids,
                        now,
                    ),
                )
            for binding_id in retired_binding_ids:
                self._connection.execute(
                    "UPDATE bindings SET configured=0 WHERE binding_id=?",
                    (binding_id,),
                )
                self._connection.execute(
                    "DELETE FROM effective_specs WHERE binding_id=?",
                    (binding_id,),
                )

    def get_effective_spec(self, binding_id: str) -> tuple[DesiredSpec, Path, str] | None:
        row = self._connection.execute(
            """
            SELECT payload_json, codex_home, profile_fingerprint
            FROM effective_specs WHERE binding_id=?
            """,
            (binding_id,),
        ).fetchone()
        if row is None:
            return None
        payload = json.loads(row["payload_json"])
        spec = DesiredSpec(**payload)
        if not spec.verify_digest():
            raise ValueError("persisted effective spec digest mismatch")
        return spec, Path(row["codex_home"]), row["profile_fingerprint"]

    def effective_profile_attestations(self) -> tuple[tuple[str, str, Path, str], ...]:
        """Return local profile attestations that may need a format migration."""

        rows = self._connection.execute(
            """
            SELECT binding_id, spec_digest, codex_home, profile_fingerprint
            FROM effective_specs
            ORDER BY binding_id
            """
        ).fetchall()
        return tuple(
            (
                str(row["binding_id"]),
                str(row["spec_digest"]),
                Path(row["codex_home"]),
                str(row["profile_fingerprint"]),
            )
            for row in rows
        )

    def refresh_effective_profile_fingerprint(
        self,
        *,
        binding_id: str,
        spec_digest: str,
        codex_home: Path,
        previous_fingerprint: str,
        profile_fingerprint: str,
    ) -> bool:
        """Compare-and-set one effective profile attestation without changing desire."""

        with self._connection:
            cursor = self._connection.execute(
                """
                UPDATE effective_specs
                SET profile_fingerprint=?, verified_at=?
                WHERE binding_id=? AND spec_digest=? AND codex_home=?
                  AND profile_fingerprint=?
                """,
                (
                    profile_fingerprint,
                    time.time(),
                    binding_id,
                    spec_digest,
                    str(codex_home),
                    previous_fingerprint,
                ),
            )
        return cursor.rowcount == 1

    def get_launch_spec(self, binding_id: str) -> tuple[DesiredSpec, Path, str] | None:
        """Return authoritative desired plus the latest valid offline observed overlay."""

        effective = self.get_effective_spec(binding_id)
        if effective is None:
            return None
        spec, codex_home, profile_fingerprint_value = effective
        rows = self._connection.execute(
            """
            SELECT event_id, payload_json
            FROM outbox
            WHERE binding_id=?
              AND kind='OBSERVED_SPEC_PROPOSAL'
              AND ack_state='PENDING'
            """,
            (binding_id,),
        ).fetchall()
        proposals: list[tuple[int, str, dict[str, object]]] = []
        for row in rows:
            decoded = json.loads(row["payload_json"])
            if not isinstance(decoded, dict):
                continue
            sequence = decoded.get("observation_sequence")
            if not isinstance(sequence, int) or isinstance(sequence, bool) or sequence < 0:
                continue
            proposals.append((sequence, str(row["event_id"]), decoded))
        for _sequence, event_id, proposal in sorted(proposals, reverse=True):
            if (
                proposal.get("base_generation_id") != spec.generation_id
                or proposal.get("base_revision") != spec.revision
                or proposal.get("base_spec_digest") != spec.spec_digest
            ):
                continue
            model = proposal.get("model")
            effort = proposal.get("reasoning_effort")
            if not isinstance(model, str) or not model or not isinstance(effort, str) or not effort:
                continue
            overlay = self._observed_overlay_spec(spec, event_id=event_id, proposal=proposal)
            return overlay, codex_home, profile_fingerprint_value
        return effective

    @staticmethod
    def _observed_overlay_spec(
        spec: DesiredSpec,
        *,
        event_id: str,
        proposal: dict[str, object],
    ) -> DesiredSpec:
        model = proposal.get("model")
        effort = proposal.get("reasoning_effort")
        if not isinstance(model, str) or not model or not isinstance(effort, str) or not effort:
            raise ValueError("observed proposal model/reasoning is invalid")
        return DesiredSpec.build(
            binding_id=spec.binding_id,
            agent_instance_id=spec.agent_instance_id,
            project_id=spec.project_id,
            canonical_cwd=spec.canonical_cwd,
            provider=spec.provider,
            account_profile=spec.account_profile,
            model=model,
            reasoning_effort=effort,
            generation_id=spec.generation_id,
            revision=spec.revision + 1,
            source_kind="observed_runtime_pending",
            source_ref=event_id,
        )

    def record_instance(
        self,
        *,
        instance_id: str,
        binding_id: str,
        wrapper_pid: int,
        wrapper_starttime: int,
        child_pid: int | None,
        child_starttime: int | None,
        tmux_session: str,
        tmux_pane: str,
        lifecycle: str,
        applied_digest: str | None,
        launch_spec: dict[str, object] | None = None,
        daemon_boot_id: str | None = None,
        connection_generation: int | None = None,
        heartbeat_at: float | None = None,
    ) -> None:
        encoded_launch = (
            json.dumps(launch_spec, sort_keys=True, separators=(",", ":"))
            if launch_spec is not None
            else None
        )
        with self._connection:
            self._connection.execute(
                """
                INSERT INTO instances(
                    instance_id, binding_id, wrapper_pid, wrapper_starttime, child_pid,
                    child_starttime, tmux_session, tmux_pane, lifecycle, applied_digest,
                    launch_spec_json, daemon_boot_id, connection_generation,
                    last_heartbeat_at, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(instance_id) DO UPDATE SET
                    child_pid=excluded.child_pid,
                    child_starttime=excluded.child_starttime,
                    lifecycle=excluded.lifecycle,
                    applied_digest=excluded.applied_digest,
                    launch_spec_json=COALESCE(
                        excluded.launch_spec_json,
                        instances.launch_spec_json
                    ),
                    daemon_boot_id=COALESCE(
                        excluded.daemon_boot_id,
                        instances.daemon_boot_id
                    ),
                    connection_generation=COALESCE(
                        excluded.connection_generation,
                        instances.connection_generation
                    ),
                    last_heartbeat_at=COALESCE(
                        excluded.last_heartbeat_at,
                        instances.last_heartbeat_at
                    ),
                    updated_at=excluded.updated_at
                """,
                (
                    instance_id,
                    binding_id,
                    wrapper_pid,
                    wrapper_starttime,
                    child_pid,
                    child_starttime,
                    tmux_session,
                    tmux_pane,
                    lifecycle,
                    applied_digest,
                    encoded_launch,
                    daemon_boot_id,
                    connection_generation,
                    heartbeat_at,
                    time.time(),
                ),
            )

    def heartbeat_instance(
        self,
        *,
        instance_id: str,
        binding_id: str,
        wrapper_pid: int,
        wrapper_starttime: int,
        child_pid: int,
        child_starttime: int,
        tmux_session: str,
        tmux_pane: str,
        daemon_boot_id: str,
        previous_daemon_boot_id: str | None,
        connection_generation: int,
    ) -> tuple[str, float, bool]:
        heartbeat_at = time.time()
        with self._connection:
            row = self._connection.execute(
                """
                SELECT lifecycle, daemon_boot_id, connection_generation,
                       resync_from_boot_id FROM instances
                WHERE instance_id=? AND binding_id=?
                  AND wrapper_pid=? AND wrapper_starttime=?
                  AND child_pid=? AND child_starttime=?
                  AND tmux_session=? AND tmux_pane=?
                  AND lifecycle NOT IN ('EXITED', 'RETIRED_FOR_RESTART')
                """,
                (
                    instance_id,
                    binding_id,
                    wrapper_pid,
                    wrapper_starttime,
                    child_pid,
                    child_starttime,
                    tmux_session,
                    tmux_pane,
                ),
            ).fetchone()
            if row is None:
                raise KeyError("heartbeat identity does not match a live instance")
            stored_boot_id = row["daemon_boot_id"]
            raw_stored_generation = row["connection_generation"]
            if not isinstance(stored_boot_id, str) or not stored_boot_id:
                raise KeyError("heartbeat instance has no daemon boot identity")
            if not isinstance(raw_stored_generation, int):
                raise KeyError("heartbeat instance has no connection generation")
            stored_generation = raw_stored_generation
            resync_from_boot_id = row["resync_from_boot_id"]
            resynchronized = stored_boot_id != daemon_boot_id
            if resynchronized:
                if previous_daemon_boot_id not in {
                    stored_boot_id, resync_from_boot_id
                }:
                    raise KeyError("heartbeat previous daemon boot identity mismatch")
                # A launch is stored at generation 0; the wrapper's *first*
                # heartbeat uses generation 1. A daemon may restart between
                # verify_started and that heartbeat, so permit exactly this
                # initial 0 -> 1 transition during resync as well. After a
                # successful resync ACK the wrapper increments its generation;
                # a second daemon restart can occur before that next heartbeat.
                expected_generation = 1 if stored_generation == 0 else stored_generation
                allowed_generations = {expected_generation}
                if stored_generation > 0:
                    allowed_generations.add(stored_generation + 1)
                if connection_generation not in allowed_generations:
                    raise KeyError("heartbeat generation changed before daemon resync ACK")
                next_resync_from_boot_id = (
                    resync_from_boot_id
                    if previous_daemon_boot_id == resync_from_boot_id
                    else stored_boot_id
                )
            else:
                replayed_resync = (
                    isinstance(resync_from_boot_id, str)
                    and previous_daemon_boot_id == resync_from_boot_id
                    and connection_generation == stored_generation
                )
                if previous_daemon_boot_id != daemon_boot_id and not replayed_resync:
                    raise KeyError("heartbeat daemon boot identity mismatch")
                if connection_generation < stored_generation:
                    raise KeyError("heartbeat connection generation regressed")
                resynchronized = replayed_resync
                next_resync_from_boot_id = (
                    None
                    if previous_daemon_boot_id == daemon_boot_id
                    and connection_generation > stored_generation
                    else resync_from_boot_id
                )
            self._connection.execute(
                """
                UPDATE instances
                SET daemon_boot_id=?, connection_generation=?, resync_from_boot_id=?,
                    last_heartbeat_at=?, updated_at=?
                WHERE instance_id=?
                """,
                (
                    daemon_boot_id,
                    connection_generation,
                    next_resync_from_boot_id,
                    heartbeat_at,
                    heartbeat_at,
                    instance_id,
                ),
            )
        return str(row["lifecycle"]), heartbeat_at, resynchronized

    def get_registered_instance(self, instance_id: str) -> RegisteredInstance | None:
        row = self._connection.execute(
            """
            SELECT instance_id, binding_id, wrapper_pid, wrapper_starttime,
                   tmux_session, tmux_pane, daemon_boot_id,
                   connection_generation, launch_spec_json
            FROM instances WHERE instance_id=?
            """,
            (instance_id,),
        ).fetchone()
        if row is None or row["launch_spec_json"] is None:
            return None
        launch_spec = json.loads(row["launch_spec_json"])
        if not isinstance(launch_spec, dict):
            raise ValueError("persisted instance launch spec is invalid")
        return RegisteredInstance(
            instance_id=row["instance_id"],
            binding_id=row["binding_id"],
            wrapper_pid=row["wrapper_pid"],
            wrapper_starttime=row["wrapper_starttime"],
            tmux_session=row["tmux_session"],
            tmux_pane=row["tmux_pane"],
            daemon_boot_id=str(row["daemon_boot_id"] or ""),
            connection_generation=int(row["connection_generation"] or 0),
            launch_spec=launch_spec,
        )

    def active_wrapper_identities(self) -> tuple[tuple[str, int, int, int, str], ...]:
        rows = self._connection.execute(
            """
            SELECT instance_id, wrapper_pid, wrapper_starttime, lifecycle
            FROM instances
            WHERE lifecycle IN ('PROVISIONAL_STARTING', 'IDLE', 'BUSY', 'PENDING_CHANGE')
            ORDER BY updated_at, instance_id
            """
        ).fetchall()
        return tuple(
            (
                str(row["instance_id"]),
                int(row["wrapper_pid"]),
                int(row["wrapper_starttime"]),
                int(os.getuid()),
                str(row["lifecycle"]),
            )
            for row in rows
        )

    def mark_instance_wrapper_absent(
        self,
        *,
        instance_id: str,
        wrapper_pid: int,
        wrapper_starttime: int,
    ) -> bool:
        with self._connection:
            updated = self._connection.execute(
                """
                UPDATE instances
                SET lifecycle=CASE
                        WHEN lifecycle='PROVISIONAL_STARTING' THEN 'START_FAILED'
                        ELSE 'EXITED'
                    END,
                    updated_at=?
                WHERE instance_id=? AND wrapper_pid=? AND wrapper_starttime=?
                  AND lifecycle IN ('PROVISIONAL_STARTING', 'IDLE', 'BUSY', 'PENDING_CHANGE')
                """,
                (time.time(), instance_id, wrapper_pid, wrapper_starttime),
            )
        return updated.rowcount == 1

    def mark_instance_exited(
        self,
        *,
        instance_id: str,
        binding_id: str,
        wrapper_pid: int,
        wrapper_starttime: int,
        child_pid: int,
        child_starttime: int,
    ) -> None:
        with self._connection:
            updated = self._connection.execute(
                """
                UPDATE instances
                SET lifecycle='EXITED', updated_at=?
                WHERE instance_id=? AND binding_id=?
                  AND wrapper_pid=? AND wrapper_starttime=?
                  AND child_pid=? AND child_starttime=?
                  AND lifecycle != 'RETIRED_FOR_RESTART'
                """,
                (
                    time.time(),
                    instance_id,
                    binding_id,
                    wrapper_pid,
                    wrapper_starttime,
                    child_pid,
                    child_starttime,
                ),
            )
            if updated.rowcount != 1:
                retired = self._connection.execute(
                    """
                    SELECT 1 FROM instances
                    WHERE instance_id=? AND binding_id=?
                      AND wrapper_pid=? AND wrapper_starttime=?
                      AND child_pid=? AND child_starttime=?
                      AND lifecycle='RETIRED_FOR_RESTART'
                    """,
                    (
                        instance_id,
                        binding_id,
                        wrapper_pid,
                        wrapper_starttime,
                        child_pid,
                        child_starttime,
                    ),
                ).fetchone()
                if retired is None:
                    raise KeyError("exited child does not match registered instance")

    def retirement_candidates(self) -> tuple[RetirementCandidate, ...]:
        rows = self._connection.execute(
            """
            SELECT i.instance_id, i.binding_id, i.wrapper_pid, i.wrapper_starttime,
                   i.tmux_session, i.tmux_pane, b.canonical_cwd,
                   a.spec_digest AS applied_spec_digest,
                   e.spec_digest AS desired_spec_digest
            FROM instances AS i
            JOIN bindings AS b ON b.binding_id=i.binding_id
            JOIN applied_specs AS a ON a.instance_id=i.instance_id
            JOIN effective_specs AS e ON e.binding_id=i.binding_id
            WHERE i.lifecycle='IDLE'
              AND b.configured=1
              AND b.management_mode='MANAGED'
              AND a.spec_digest != e.spec_digest
              AND NOT EXISTS (
                  SELECT 1 FROM outbox AS pending_observed
                  WHERE pending_observed.binding_id=i.binding_id
                    AND pending_observed.kind='OBSERVED_SPEC_PROPOSAL'
                    AND pending_observed.ack_state='PENDING'
              )
              AND (
                  SELECT count(*) FROM instances AS live
                  WHERE live.binding_id=i.binding_id
                    AND live.lifecycle NOT IN ('EXITED', 'RETIRED_FOR_RESTART')
              ) = 1
            ORDER BY i.updated_at, i.instance_id
            """
        ).fetchall()
        return tuple(
            RetirementCandidate(
                instance_id=str(row["instance_id"]),
                binding_id=str(row["binding_id"]),
                wrapper_pid=int(row["wrapper_pid"]),
                wrapper_starttime=int(row["wrapper_starttime"]),
                tmux_session=str(row["tmux_session"]),
                tmux_pane=str(row["tmux_pane"]),
                canonical_cwd=str(row["canonical_cwd"]),
                applied_spec_digest=str(row["applied_spec_digest"]),
                desired_spec_digest=str(row["desired_spec_digest"]),
            )
            for row in rows
        )

    def mark_instance_retired(self, candidate: RetirementCandidate) -> None:
        with self._connection:
            updated = self._connection.execute(
                """
                UPDATE instances
                SET lifecycle='RETIRED_FOR_RESTART', updated_at=?
                WHERE instance_id=? AND binding_id=? AND lifecycle IN ('IDLE', 'EXITED')
                  AND wrapper_pid=? AND wrapper_starttime=?
                  AND applied_digest=?
                """,
                (
                    time.time(),
                    candidate.instance_id,
                    candidate.binding_id,
                    candidate.wrapper_pid,
                    candidate.wrapper_starttime,
                    candidate.applied_spec_digest,
                ),
            )
            if updated.rowcount != 1:
                raise ValueError("retirement candidate changed before commit")

    def pending_evidence_instances(self) -> tuple[PendingEvidenceInstance, ...]:
        rows = self._connection.execute(
            """
            SELECT instance_id, binding_id, wrapper_pid, wrapper_starttime,
                   child_pid, child_starttime, tmux_session, tmux_pane,
                   daemon_boot_id, connection_generation, launch_spec_json
            FROM instances
            WHERE child_pid IS NOT NULL
              AND child_starttime IS NOT NULL
              AND applied_digest IS NULL
              AND lifecycle IN ('IDLE', 'BUSY')
              AND launch_spec_json IS NOT NULL
            ORDER BY updated_at
            """
        ).fetchall()
        return tuple(
            PendingEvidenceInstance(
                instance_id=row["instance_id"],
                binding_id=row["binding_id"],
                wrapper_pid=row["wrapper_pid"],
                wrapper_starttime=row["wrapper_starttime"],
                child_pid=row["child_pid"],
                child_starttime=row["child_starttime"],
                tmux_session=row["tmux_session"],
                tmux_pane=row["tmux_pane"],
                daemon_boot_id=str(row["daemon_boot_id"] or ""),
                connection_generation=int(row["connection_generation"] or 0),
                launch_spec=json.loads(row["launch_spec_json"]),
            )
            for row in rows
        )

    def record_applied_spec(
        self,
        *,
        instance_id: str,
        binding_id: str,
        spec: DesiredSpec,
        evidence: dict[str, object],
        session_id: str,
        account_profile: str,
        rollout_path: str,
        rollout_device: int,
        rollout_inode: int,
        profile_reattestation: tuple[str, str] | None = None,
    ) -> None:
        normalized_evidence = dict(evidence)
        normalized_evidence.setdefault("spec_digest", spec.spec_digest)
        normalized_evidence.setdefault("provider", spec.provider)
        normalized_evidence.setdefault("account_profile", spec.account_profile)
        normalized_evidence.setdefault("actual_model", spec.model)
        normalized_evidence.setdefault("actual_reasoning_effort", spec.reasoning_effort)
        encoded = json.dumps(normalized_evidence, sort_keys=True, separators=(",", ":"))
        with self._connection:
            if profile_reattestation is not None:
                previous_fingerprint, current_fingerprint = profile_reattestation
                row = self._connection.execute(
                    """
                    SELECT launch_spec_json FROM instances
                    WHERE instance_id=? AND binding_id=?
                    """,
                    (instance_id, binding_id),
                ).fetchone()
                if row is None or row["launch_spec_json"] is None:
                    raise KeyError(f"unknown attested instance: {instance_id}")
                launch_spec = json.loads(row["launch_spec_json"])
                if not isinstance(launch_spec, dict):
                    raise ValueError("persisted instance launch spec is invalid")
                if launch_spec.get("profile_fingerprint") != previous_fingerprint:
                    raise ValueError("instance profile attestation changed concurrently")
                raw_desired = launch_spec.get("desired")
                if not isinstance(raw_desired, dict) or raw_desired.get("spec_digest") != (
                    spec.spec_digest
                ):
                    raise ValueError("instance launch desire changed concurrently")
                launch_spec["profile_fingerprint"] = current_fingerprint
                self._connection.execute(
                    """
                    UPDATE instances SET launch_spec_json=?, updated_at=?
                    WHERE instance_id=? AND binding_id=?
                    """,
                    (
                        json.dumps(launch_spec, sort_keys=True, separators=(",", ":")),
                        time.time(),
                        instance_id,
                        binding_id,
                    ),
                )
            existing = self.session_owner(session_id)
            expected_owner = (binding_id, account_profile)
            if existing is not None and existing != expected_owner:
                raise ValueError("session provenance is immutable")
            self._connection.execute(
                """
                INSERT INTO session_provenance(
                    session_id, binding_id, account_profile, rollout_path,
                    rollout_device, rollout_inode, observed_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_id) DO UPDATE SET
                    rollout_path=excluded.rollout_path,
                    rollout_device=excluded.rollout_device,
                    rollout_inode=excluded.rollout_inode,
                    observed_at=excluded.observed_at
                """,
                (
                    session_id,
                    binding_id,
                    account_profile,
                    rollout_path,
                    rollout_device,
                    rollout_inode,
                    time.time(),
                ),
            )
            self._connection.execute(
                """
                INSERT INTO applied_specs(
                    instance_id, binding_id, generation_id, revision,
                    spec_digest, evidence_json, verified_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(instance_id) DO UPDATE SET
                    binding_id=excluded.binding_id,
                    generation_id=excluded.generation_id,
                    revision=excluded.revision,
                    spec_digest=excluded.spec_digest,
                    evidence_json=excluded.evidence_json,
                    verified_at=excluded.verified_at
                """,
                (
                    instance_id,
                    binding_id,
                    spec.generation_id,
                    spec.revision,
                    spec.spec_digest,
                    encoded,
                    time.time(),
                ),
            )
            cursor = self._connection.execute(
                """
                UPDATE instances SET applied_digest=?, updated_at=?
                WHERE instance_id=? AND binding_id=?
                """,
                (spec.spec_digest, time.time(), instance_id, binding_id),
            )
            if cursor.rowcount != 1:
                raise KeyError(f"unknown instance: {instance_id}")

    def get_applied_evidence(self, instance_id: str) -> dict[str, object] | None:
        row = self._connection.execute(
            "SELECT evidence_json FROM applied_specs WHERE instance_id=?",
            (instance_id,),
        ).fetchone()
        if row is None:
            return None
        decoded = json.loads(row["evidence_json"])
        if not isinstance(decoded, dict):
            raise ValueError("persisted AppliedSpec evidence is invalid")
        return decoded

    def revoke_stale_applied_spec(
        self,
        *,
        instance_id: str,
        session_id: str,
        rollout_path: str,
    ) -> bool:
        """Withdraw evidence no longer corroborated by upstream state.

        Session provenance is historical and remains immutable; only the live
        process's AppliedSpec is withdrawn so a new rollout can be collected.
        """

        with self._connection:
            row = self._connection.execute(
                "SELECT evidence_json FROM applied_specs WHERE instance_id=?",
                (instance_id,),
            ).fetchone()
            if row is None:
                return False
            evidence = json.loads(row["evidence_json"])
            if (
                not isinstance(evidence, dict)
                or evidence.get("session_id") != session_id
                or evidence.get("rollout_path") != rollout_path
            ):
                return False
            self._connection.execute(
                "DELETE FROM applied_specs WHERE instance_id=?", (instance_id,)
            )
            self._connection.execute(
                "UPDATE instances SET applied_digest=NULL, updated_at=? WHERE instance_id=?",
                (time.time(), instance_id),
            )
            return True

    def rollout_observation_targets(self) -> tuple[RolloutObservationTarget, ...]:
        rows = self._connection.execute(
            """
            SELECT i.instance_id, i.binding_id, i.lifecycle, i.tmux_session,
                   a.evidence_json
            FROM instances AS i
            JOIN applied_specs AS a ON a.instance_id=i.instance_id
            WHERE i.lifecycle IN ('IDLE', 'BUSY')
            ORDER BY i.updated_at, i.instance_id
            """
        ).fetchall()
        targets: list[RolloutObservationTarget] = []
        for row in rows:
            evidence = json.loads(row["evidence_json"])
            if not isinstance(evidence, dict):
                raise ValueError("persisted AppliedSpec evidence is invalid")
            targets.append(
                RolloutObservationTarget(
                    instance_id=str(row["instance_id"]),
                    binding_id=str(row["binding_id"]),
                    lifecycle=str(row["lifecycle"]),
                    tmux_session=str(row["tmux_session"]),
                    rollout_path=str(evidence["rollout_path"]),
                    rollout_device=int(evidence["rollout_device"]),
                    rollout_inode=int(evidence["rollout_inode"]),
                    session_id=str(evidence["session_id"]),
                    codex_version=str(evidence["real_codex_version"]),
                    applied_model=str(evidence["actual_model"]),
                    applied_reasoning_effort=str(evidence["actual_reasoning_effort"]),
                )
            )
        return tuple(targets)

    def rollout_observation_cursor(
        self,
        *,
        instance_id: str,
        rollout_device: int,
        rollout_inode: int,
    ) -> RolloutObservationCursor:
        row = self._connection.execute(
            "SELECT * FROM codex_rollout_observers WHERE instance_id=?",
            (instance_id,),
        ).fetchone()
        if row is None:
            return RolloutObservationCursor(0, 0, None, None, None, None, None, None, None)
        if row["rollout_device"] != rollout_device or row["rollout_inode"] != rollout_inode:
            applied = self.get_applied_evidence(instance_id)
            if (
                applied is None
                or applied.get("rollout_device") != rollout_device
                or applied.get("rollout_inode") != rollout_inode
            ):
                raise ValueError("observed rollout identity changed for live instance")
            # An explicitly re-verified rollout replaced stale AppliedSpec for
            # this process. Scan its events from the start, but do not reuse an
            # observation sequence that may already be in the durable outbox.
            return RolloutObservationCursor(
                0, int(row["observation_sequence"]), None, None, None, None, None, None, None
            )
        return RolloutObservationCursor(
            offset=int(row["offset"]),
            observation_sequence=int(row["observation_sequence"]),
            last_model=row["last_model"],
            last_reasoning_effort=row["last_reasoning_effort"],
            active_turn_id=row["active_turn_id"],
            pending_model=row["pending_model"],
            pending_reasoning_effort=row["pending_reasoning_effort"],
            pending_evidence_digest=row["pending_evidence_digest"],
            pending_turn_id=row["pending_turn_id"],
        )

    def save_rollout_observation_cursor(
        self,
        *,
        instance_id: str,
        rollout_device: int,
        rollout_inode: int,
        cursor: RolloutObservationCursor,
        lifecycle: str,
    ) -> None:
        if cursor.offset < 0 or cursor.observation_sequence < 0:
            raise ValueError("invalid rollout observation cursor")
        with self._connection:
            self._connection.execute(
                """
                INSERT INTO codex_rollout_observers(
                    instance_id, rollout_device, rollout_inode, offset,
                    observation_sequence, last_model, last_reasoning_effort,
                    active_turn_id, pending_model, pending_reasoning_effort,
                    pending_evidence_digest, pending_turn_id, updated_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(instance_id) DO UPDATE SET
                    rollout_device=excluded.rollout_device,
                    rollout_inode=excluded.rollout_inode,
                    offset=excluded.offset,
                    observation_sequence=excluded.observation_sequence,
                    last_model=excluded.last_model,
                    last_reasoning_effort=excluded.last_reasoning_effort,
                    active_turn_id=excluded.active_turn_id,
                    pending_model=excluded.pending_model,
                    pending_reasoning_effort=excluded.pending_reasoning_effort,
                    pending_evidence_digest=excluded.pending_evidence_digest,
                    pending_turn_id=excluded.pending_turn_id,
                    updated_at=excluded.updated_at
                """,
                (
                    instance_id,
                    rollout_device,
                    rollout_inode,
                    cursor.offset,
                    cursor.observation_sequence,
                    cursor.last_model,
                    cursor.last_reasoning_effort,
                    cursor.active_turn_id,
                    cursor.pending_model,
                    cursor.pending_reasoning_effort,
                    cursor.pending_evidence_digest,
                    cursor.pending_turn_id,
                    time.time(),
                ),
            )
            updated = self._connection.execute(
                "UPDATE instances SET lifecycle=?, updated_at=? WHERE instance_id=?",
                (lifecycle, time.time(), instance_id),
            )
            if updated.rowcount != 1:
                raise KeyError(f"unknown instance: {instance_id}")

    def session_owner(self, session_id: str) -> tuple[str, str] | None:
        row = self._connection.execute(
            "SELECT binding_id, account_profile FROM session_provenance WHERE session_id=?",
            (session_id,),
        ).fetchone()
        if row is None:
            return None
        return str(row["binding_id"]), str(row["account_profile"])

    def record_session_provenance(
        self,
        *,
        session_id: str,
        binding_id: str,
        account_profile: str,
        rollout_path: str = "",
        rollout_device: int = 0,
        rollout_inode: int = 0,
    ) -> None:
        existing = self.session_owner(session_id)
        expected = (binding_id, account_profile)
        if existing is not None and existing != expected:
            raise ValueError("session provenance is immutable")
        with self._connection:
            self._connection.execute(
                """
                INSERT INTO session_provenance(
                    session_id, binding_id, account_profile, rollout_path,
                    rollout_device, rollout_inode, observed_at
                ) VALUES(?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(session_id) DO UPDATE SET
                    rollout_path=excluded.rollout_path,
                    rollout_device=excluded.rollout_device,
                    rollout_inode=excluded.rollout_inode,
                    observed_at=excluded.observed_at
                """,
                (
                    session_id,
                    binding_id,
                    account_profile,
                    rollout_path,
                    rollout_device,
                    rollout_inode,
                    time.time(),
                ),
            )

    def import_session_provenance(
        self,
        records: tuple[dict[str, object], ...],
    ) -> int:
        """Atomically import pre-cutover ownership after external evidence validation."""

        imported = 0
        with self._connection:
            for record in records:
                session_id = str(record["session_id"])
                binding_id = str(record["binding_id"])
                account_profile = str(record["account_profile"])
                canonical_cwd = str(record["canonical_cwd"])
                rollout_path = str(record["rollout_path"])
                raw_rollout_device = record["rollout_device"]
                raw_rollout_inode = record["rollout_inode"]
                if (
                    not isinstance(raw_rollout_device, int)
                    or isinstance(raw_rollout_device, bool)
                    or not isinstance(raw_rollout_inode, int)
                    or isinstance(raw_rollout_inode, bool)
                ):
                    raise ValueError("session provenance rollout identity is invalid")
                rollout_device = raw_rollout_device
                rollout_inode = raw_rollout_inode
                binding = self._connection.execute(
                    """
                    SELECT b.canonical_cwd, b.configured, e.payload_json
                    FROM bindings AS b
                    LEFT JOIN effective_specs AS e ON e.binding_id=b.binding_id
                    WHERE b.binding_id=?
                    """,
                    (binding_id,),
                ).fetchone()
                if binding is None or not bool(binding["configured"]):
                    raise ValueError("session provenance binding is not configured")
                if binding["canonical_cwd"] != canonical_cwd or binding["payload_json"] is None:
                    raise ValueError("session provenance binding cwd/spec mismatch")
                desired = json.loads(binding["payload_json"])
                if (
                    not isinstance(desired, dict)
                    or desired.get("account_profile") != account_profile
                ):
                    raise ValueError("session provenance account does not match effective spec")
                existing = self._connection.execute(
                    """
                    SELECT binding_id, account_profile, rollout_path,
                           rollout_device, rollout_inode
                    FROM session_provenance WHERE session_id=?
                    """,
                    (session_id,),
                ).fetchone()
                expected = (
                    binding_id,
                    account_profile,
                    rollout_path,
                    rollout_device,
                    rollout_inode,
                )
                if existing is not None:
                    observed = (
                        str(existing["binding_id"]),
                        str(existing["account_profile"]),
                        str(existing["rollout_path"]),
                        int(existing["rollout_device"]),
                        int(existing["rollout_inode"]),
                    )
                    if observed != expected:
                        raise ValueError("session provenance is immutable")
                    continue
                self._connection.execute(
                    """
                    INSERT INTO session_provenance(
                        session_id, binding_id, account_profile, rollout_path,
                        rollout_device, rollout_inode, observed_at
                    ) VALUES(?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        session_id,
                        binding_id,
                        account_profile,
                        rollout_path,
                        rollout_device,
                        rollout_inode,
                        time.time(),
                    ),
                )
                imported += 1
        return imported

    def summary(self) -> dict[str, int]:
        return {
            "bindings": self._connection.execute("SELECT count(*) FROM bindings").fetchone()[0],
            "specs": self._connection.execute("SELECT count(*) FROM effective_specs").fetchone()[0],
            "instances": self._connection.execute("SELECT count(*) FROM instances").fetchone()[0],
            "outbox_pending": self._connection.execute(
                "SELECT count(*) FROM outbox WHERE ack_state='PENDING'"
            ).fetchone()[0],
        }

    def mark_control_plane_sync(
        self,
        agent_instance_id: str,
        *,
        observed_at: float | None = None,
    ) -> float:
        timestamp = time.time() if observed_at is None else observed_at
        with self._connection:
            self._connection.execute(
                """
                INSERT INTO control_plane_sync(agent_instance_id, last_seen_at)
                VALUES(?, ?)
                ON CONFLICT(agent_instance_id) DO UPDATE SET
                    last_seen_at=excluded.last_seen_at
                """,
                (agent_instance_id, timestamp),
            )
        return timestamp

    def control_plane_last_seen(self, agent_instance_id: str) -> float | None:
        row = self._connection.execute(
            "SELECT last_seen_at FROM control_plane_sync WHERE agent_instance_id=?",
            (agent_instance_id,),
        ).fetchone()
        return None if row is None else float(row["last_seen_at"])

    def status_report(self, agent_instance_id: str) -> dict[str, object]:
        observed_at = time.time()
        reports: list[dict[str, object]] = []
        for binding in self.list_bindings(agent_instance_id):
            effective = self.get_effective_spec(binding.binding_id)
            desired: dict[str, object] | None = None
            if effective is not None:
                spec, _codex_home, profile = effective
                desired = {
                    "generation_id": spec.generation_id,
                    "revision": spec.revision,
                    "spec_digest": spec.spec_digest,
                    "account_profile": spec.account_profile,
                    "model": spec.model,
                    "reasoning_effort": spec.reasoning_effort,
                    "profile_fingerprint": profile,
                }
            rows = self._connection.execute(
                """
                SELECT i.*, a.generation_id AS applied_generation,
                       a.revision AS applied_revision,
                       a.spec_digest AS verified_applied_digest,
                       a.evidence_json AS applied_evidence_json
                FROM instances AS i
                LEFT JOIN applied_specs AS a ON a.instance_id=i.instance_id
                WHERE i.binding_id=?
                ORDER BY i.updated_at DESC, i.instance_id
                """,
                (binding.binding_id,),
            ).fetchall()
            instances = [
                {
                    "instance_id": row["instance_id"],
                    "lifecycle": row["lifecycle"],
                    "tmux_session": row["tmux_session"],
                    "tmux_pane": row["tmux_pane"],
                    "wrapper_pid": row["wrapper_pid"],
                    "wrapper_starttime": row["wrapper_starttime"],
                    "child_pid": row["child_pid"],
                    "child_starttime": row["child_starttime"],
                    "daemon_boot_id": row["daemon_boot_id"],
                    "connection_generation": row["connection_generation"],
                    "last_heartbeat_at": row["last_heartbeat_at"],
                    "heartbeat_stale": (
                        row["lifecycle"] in {"PROVISIONAL_STARTING", "IDLE", "BUSY"}
                        and (
                            row["last_heartbeat_at"] is None
                            or observed_at - float(row["last_heartbeat_at"])
                            > _INSTANCE_HEARTBEAT_STALE_SECONDS
                        )
                    ),
                    "applied": _applied_status(row),
                    "updated_at": row["updated_at"],
                }
                for row in rows
            ]
            live = [
                item
                for item in instances
                if item["lifecycle"] in {"PROVISIONAL_STARTING", "IDLE", "BUSY", "PENDING_CHANGE"}
            ]
            retired = [item for item in instances if item["lifecycle"] == "RETIRED_FOR_RESTART"]
            active_applied_digests = frozenset(
                str(applied["spec_digest"])
                for item in live
                if isinstance((applied := item.get("applied")), dict)
                and isinstance(applied.get("spec_digest"), str)
            )
            observed_sync = self.observed_sync_state(
                binding.binding_id,
                active_applied_digests=active_applied_digests,
            )
            if not binding.configured or desired is None:
                health = "OUT_OF_SYNC"
            elif binding.management_mode != ManagementMode.MANAGED:
                health = binding.management_mode.value
            elif any(item["heartbeat_stale"] is True for item in live) or any(
                item["applied"] is None for item in live
            ):
                health = "OUT_OF_SYNC"
            elif any(
                isinstance(item["applied"], dict)
                and item["applied"]["spec_digest"] != desired["spec_digest"]
                for item in live
            ) or (
                not live
                and any(
                    isinstance(item["applied"], dict)
                    and item["applied"]["spec_digest"] != desired["spec_digest"]
                    for item in retired
                )
            ):
                health = "PENDING_CHANGE"
            else:
                health = "RUNTIME_HEALTHY"
            if observed_sync is not None and health in {"RUNTIME_HEALTHY", "PENDING_CHANGE"}:
                health = observed_sync.value
            reports.append(
                {
                    "binding_id": binding.binding_id,
                    "source_ref": {
                        "agent_instance_id": binding.agent_instance_id,
                        "chat_id": binding.chat_id,
                        "thread_id": binding.thread_id,
                    },
                    "project_id": binding.project_id,
                    "canonical_cwd": binding.canonical_cwd,
                    "configured": binding.configured,
                    "management_mode": binding.management_mode.value,
                    "health": health,
                    "desired": desired,
                    "instances": instances,
                }
            )
        return {
            "agent_instance_id": agent_instance_id,
            "bindings": reports,
            "summary": self.summary(),
        }

    def enqueue_observed_proposal(self, proposal: ObservedSpecProposal) -> str:
        self.enqueue_outbox(
            event_id=proposal.event_id,
            kind="OBSERVED_SPEC_PROPOSAL",
            binding_id=proposal.binding_id,
            payload=proposal.payload(),
            ordering_sequence=proposal.observation_sequence,
        )
        return proposal.event_id

    def record_observed_proposal(self, proposal: ObservedSpecProposal) -> DesiredSpec:
        """Durably record observed state as both outbox proposal and local LKG overlay."""

        effective = self.get_effective_spec(proposal.binding_id)
        if effective is None:
            raise ValueError("observed binding has no authoritative local spec")
        base, _codex_home, _profile_fingerprint = effective
        if (
            proposal.base_generation_id != base.generation_id
            or proposal.base_revision != base.revision
            or proposal.base_spec_digest != base.spec_digest
        ):
            raise ValueError("observed proposal base differs from authoritative local spec")
        payload = proposal.payload()
        overlay = self._observed_overlay_spec(
            base,
            event_id=proposal.event_id,
            proposal=payload,
        )
        row = self._connection.execute(
            """
            SELECT a.evidence_json, i.lifecycle
            FROM applied_specs AS a
            JOIN instances AS i ON i.instance_id=a.instance_id
            WHERE a.instance_id=? AND a.binding_id=? AND i.binding_id=?
              AND i.lifecycle IN ('IDLE', 'BUSY')
            """,
            (proposal.instance_id, proposal.binding_id, proposal.binding_id),
        ).fetchone()
        if row is None:
            raise ValueError("observed proposal has no matching live AppliedSpec")
        evidence = json.loads(row["evidence_json"])
        if not isinstance(evidence, dict):
            raise ValueError("observed AppliedSpec evidence is invalid")
        evidence.update(
            {
                "spec_digest": overlay.spec_digest,
                "actual_model": overlay.model,
                "actual_reasoning_effort": overlay.reasoning_effort,
                "observed_event_id": proposal.event_id,
                "observed_evidence_digest": proposal.evidence_digest,
                "observation_sequence": proposal.observation_sequence,
            }
        )
        encoded_evidence = json.dumps(evidence, sort_keys=True, separators=(",", ":"))
        encoded_payload = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        payload_digest = sha256_digest(payload)
        existing = self._connection.execute(
            "SELECT payload_digest, ordering_sequence FROM outbox WHERE event_id=?",
            (proposal.event_id,),
        ).fetchone()
        if existing is not None:
            if existing["payload_digest"] != payload_digest:
                raise ValueError("outbox event_id reused with different payload")
            if existing["ordering_sequence"] != proposal.observation_sequence:
                raise ValueError("outbox event_id reused with different ordering sequence")
        now = time.time()
        with self._connection:
            self._connection.execute(
                """
                INSERT INTO outbox(
                    event_id, kind, binding_id, payload_json, payload_digest,
                    ordering_sequence, attempts, next_retry_at, ack_state
                ) VALUES(?, 'OBSERVED_SPEC_PROPOSAL', ?, ?, ?, ?, 0, 0, 'PENDING')
                ON CONFLICT(event_id) DO NOTHING
                """,
                (
                    proposal.event_id,
                    proposal.binding_id,
                    encoded_payload,
                    payload_digest,
                    proposal.observation_sequence,
                ),
            )
            applied = self._connection.execute(
                """
                UPDATE applied_specs
                SET generation_id=?, revision=?, spec_digest=?, evidence_json=?, verified_at=?
                WHERE instance_id=? AND binding_id=?
                """,
                (
                    overlay.generation_id,
                    overlay.revision,
                    overlay.spec_digest,
                    encoded_evidence,
                    now,
                    proposal.instance_id,
                    proposal.binding_id,
                ),
            )
            instance = self._connection.execute(
                """
                UPDATE instances SET applied_digest=?, updated_at=?
                WHERE instance_id=? AND binding_id=? AND lifecycle IN ('IDLE', 'BUSY')
                """,
                (
                    overlay.spec_digest,
                    now,
                    proposal.instance_id,
                    proposal.binding_id,
                ),
            )
            if applied.rowcount != 1 or instance.rowcount != 1:
                raise ValueError("observed proposal target changed before commit")
        return overlay

    def observed_sync_state(
        self,
        binding_id: str,
        *,
        active_applied_digests: frozenset[str],
    ) -> SyncState | None:
        """Derive user-visible observed synchronization state from durable evidence."""

        pending = self._connection.execute(
            """
            SELECT 1 FROM outbox
            WHERE binding_id=? AND kind='OBSERVED_SPEC_PROPOSAL' AND ack_state='PENDING'
            LIMIT 1
            """,
            (binding_id,),
        ).fetchone()
        if pending is not None:
            return SyncState.OBSERVED_PENDING_SYNC
        if not active_applied_digests:
            return None
        effective = self.get_effective_spec(binding_id)
        if effective is None:
            return None
        base, _codex_home, _profile_fingerprint = effective
        rejected = self._connection.execute(
            """
            SELECT event_id, payload_json FROM outbox
            WHERE binding_id=? AND kind='OBSERVED_SPEC_PROPOSAL'
              AND ack_state='POLICY_REJECTED'
            """,
            (binding_id,),
        ).fetchall()
        for row in rejected:
            proposal = json.loads(row["payload_json"])
            if not isinstance(proposal, dict):
                continue
            if (
                proposal.get("base_generation_id") != base.generation_id
                or proposal.get("base_revision") != base.revision
                or proposal.get("base_spec_digest") != base.spec_digest
            ):
                continue
            overlay = self._observed_overlay_spec(
                base,
                event_id=str(row["event_id"]),
                proposal=proposal,
            )
            if overlay.spec_digest in active_applied_digests:
                return SyncState.OBSERVED_POLICY_REJECTED
        return None

    def enqueue_user_notification(
        self,
        *,
        code: UserNotificationCode,
        binding: RuntimeBinding,
        event_ref: str,
    ) -> str:
        """Persist a sanitized, binding-scoped CUS Bot notification request."""

        identity: dict[str, object] = {
            "code": code.value,
            "binding_id": binding.binding_id,
            "event_ref": event_ref,
        }
        event_id = f"notice:{sha256_digest(identity)[7:]}"
        self.enqueue_outbox(
            event_id=event_id,
            kind="USER_NOTIFICATION_REQUIRED",
            binding_id=binding.binding_id,
            payload={
                "schema": "ai-agent-runtime.user-notification.v1",
                "notification_code": code.value,
                "agent_instance_id": binding.agent_instance_id,
                "binding_id": binding.binding_id,
                "chat_id": binding.chat_id,
                "thread_id": binding.thread_id,
            },
        )
        return event_id

    def enqueue_outbox(
        self,
        *,
        event_id: str,
        kind: str,
        binding_id: str,
        payload: dict[str, object],
        ordering_sequence: int | None = None,
    ) -> None:
        if ordering_sequence is not None and ordering_sequence < 0:
            raise ValueError("outbox ordering sequence must be non-negative")
        payload_digest = sha256_digest(payload)
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        existing = self._connection.execute(
            "SELECT payload_digest, ordering_sequence FROM outbox WHERE event_id=?",
            (event_id,),
        ).fetchone()
        if existing is not None:
            if existing["payload_digest"] != payload_digest:
                raise ValueError("outbox event_id reused with different payload")
            if existing["ordering_sequence"] != ordering_sequence:
                raise ValueError("outbox event_id reused with different ordering sequence")
        with self._connection:
            self._connection.execute(
                """
                INSERT INTO outbox(
                    event_id, kind, binding_id, payload_json, payload_digest,
                    ordering_sequence, attempts, next_retry_at, ack_state
                ) VALUES(?, ?, ?, ?, ?, ?, 0, 0, 'PENDING')
                ON CONFLICT(event_id) DO NOTHING
                """,
                (
                    event_id,
                    kind,
                    binding_id,
                    encoded,
                    payload_digest,
                    ordering_sequence,
                ),
            )

    def claim_outbox(
        self,
        *,
        limit: int = 50,
        now: float | None = None,
    ) -> tuple[dict[str, object], ...]:
        if limit < 1 or limit > 100:
            raise ValueError("outbox claim limit must be between 1 and 100")
        current = time.time() if now is None else now
        with self._connection:
            rows = self._connection.execute(
                """
                SELECT pending.* FROM outbox AS pending
                WHERE pending.ack_state='PENDING' AND pending.next_retry_at <= ?
                  AND (
                    pending.kind != 'OBSERVED_SPEC_PROPOSAL'
                    OR NOT EXISTS (
                      SELECT 1 FROM outbox AS newer
                      WHERE newer.binding_id=pending.binding_id
                        AND newer.kind='OBSERVED_SPEC_PROPOSAL'
                        AND newer.ack_state='PENDING'
                        AND COALESCE(newer.ordering_sequence, -1)
                            > COALESCE(pending.ordering_sequence, -1)
                    )
                  )
                ORDER BY pending.next_retry_at,
                         COALESCE(pending.ordering_sequence, -1) DESC,
                         pending.event_id
                LIMIT ?
                """,
                (current, limit),
            ).fetchall()
            events: list[dict[str, object]] = []
            for row in rows:
                attempt = int(row["attempts"]) + 1
                retry_delay = min(2 ** min(attempt, 8), 300)
                self._connection.execute(
                    """
                    UPDATE outbox
                    SET attempts=?, next_retry_at=?
                    WHERE event_id=? AND ack_state='PENDING'
                    """,
                    (attempt, current + retry_delay, row["event_id"]),
                )
                payload = json.loads(row["payload_json"])
                events.append(
                    {
                        "event_id": row["event_id"],
                        "kind": row["kind"],
                        "binding_id": row["binding_id"],
                        "payload": payload,
                        "payload_digest": row["payload_digest"],
                        "attempt": attempt,
                    }
                )
        return tuple(events)

    def acknowledge_outbox(self, event_id: str, ack_state: str) -> None:
        allowed = {"ACKED", "STALE_OBSERVED", "POLICY_REJECTED"}
        if ack_state not in allowed:
            raise ValueError("invalid outbox acknowledgement state")
        row = self._connection.execute(
            "SELECT ack_state FROM outbox WHERE event_id=?",
            (event_id,),
        ).fetchone()
        if row is None:
            raise KeyError(f"unknown outbox event: {event_id}")
        current = str(row["ack_state"])
        if current != "PENDING" and current != ack_state:
            raise ValueError("outbox event already has a different final acknowledgement")
        with self._connection:
            self._connection.execute(
                "UPDATE outbox SET ack_state=? WHERE event_id=?",
                (ack_state, event_id),
            )

    def get_request_result(self, request_id: str, request_digest: str) -> dict[str, object] | None:
        row = self._connection.execute(
            "SELECT request_digest, response_json FROM request_results WHERE request_id=?",
            (request_id,),
        ).fetchone()
        if row is None:
            return None
        if row["request_digest"] != request_digest:
            raise ValueError("request_id replayed with different payload")
        decoded = json.loads(row["response_json"])
        if not isinstance(decoded, dict):
            raise ValueError("persisted request result is invalid")
        return decoded

    def put_request_result(
        self,
        request_id: str,
        request_digest: str,
        response: dict[str, object],
    ) -> None:
        encoded = json.dumps(response, sort_keys=True, separators=(",", ":"))
        with self._connection:
            self._connection.execute(
                """
                INSERT INTO request_results(
                    request_id, request_digest, response_json, created_at
                ) VALUES(?, ?, ?, ?)
                ON CONFLICT(request_id) DO NOTHING
                """,
                (request_id, request_digest, encoded, time.time()),
            )
