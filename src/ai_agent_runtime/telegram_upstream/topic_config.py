"""Fresh-read, eventual-reconciliation adapter for upstream topic_config.json."""

from __future__ import annotations

import ctypes
import hashlib
import json
import os
import stat
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_AT_FDCWD = -100
_RENAME_EXCHANGE = 2
_MAX_CONCURRENT_WRITES = 4


class TopicConfigManagedConflictError(RuntimeError):
    """An upstream managed-field change must not be retried as a CUS write."""


class TopicConfigUnresolvedConflictError(RuntimeError):
    """A bounded merge stopped while an upstream version still needs recovery."""


def _exchange_paths(left: Path, right: Path) -> None:
    """Publish while retaining the exact file displaced by a concurrent writer.

    A plain ``os.replace`` discards a topic created after our read. Linux is
    mandatory in V1; fail closed if the filesystem lacks atomic exchange.
    """

    libc = ctypes.CDLL(None, use_errno=True)
    renameat2 = libc.renameat2
    renameat2.argtypes = [
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_uint,
    ]
    renameat2.restype = ctypes.c_int
    if (
        renameat2(
            _AT_FDCWD,
            os.fsencode(left),
            _AT_FDCWD,
            os.fsencode(right),
            _RENAME_EXCHANGE,
        )
        != 0
    ):
        error = ctypes.get_errno()
        raise OSError(error, os.strerror(error), str(right))


def _digest_bytes(raw: bytes) -> str:
    return f"sha256:{hashlib.sha256(raw).hexdigest()}"


@dataclass(frozen=True, slots=True)
class TopicConfigEntry:
    thread_id: int
    cwd: str | None
    engine: str
    exec_mode: str
    raw: dict[str, Any]

    @property
    def is_managed_candidate(self) -> bool:
        return self.engine in {"codex", "claude"} and self.exec_mode == "tmux"


@dataclass(frozen=True, slots=True)
class TopicConfigSnapshot:
    path: Path
    digest: str
    raw: dict[str, Any]

    def topic(self, thread_id: int) -> TopicConfigEntry | None:
        topics = self.raw.get("topics")
        if not isinstance(topics, dict):
            return None
        value = topics.get(str(thread_id))
        if not isinstance(value, dict):
            return None
        cwd_value = value.get("cwd")
        return TopicConfigEntry(
            thread_id=thread_id,
            cwd=str(cwd_value) if isinstance(cwd_value, str) else None,
            engine=str(value.get("engine", "claude")),
            exec_mode=str(value.get("exec_mode", "subprocess")),
            raw=dict(value),
        )

    def unbound_named_topics(self, bound_thread_ids: set[int]) -> list[dict[str, object]]:
        """Expose only upstream-created forum topics needing CUS onboarding.

        This is a read-only discovery surface. The CUS connector still validates
        the AgentInstance and performs the notification; Runtime never sends it.
        """

        topics = self.raw["topics"]
        assert isinstance(topics, dict)
        candidates: list[tuple[int, str]] = []
        for raw_thread_id, raw_entry in topics.items():
            if (
                not isinstance(raw_thread_id, str)
                or len(raw_thread_id) > 19
                or not raw_thread_id.isdecimal()
            ):
                continue
            thread_id = int(raw_thread_id)
            if str(thread_id) != raw_thread_id or thread_id <= 1 or thread_id in bound_thread_ids:
                continue
            if not isinstance(raw_entry, dict):
                continue
            raw_name = raw_entry.get("name")
            if not isinstance(raw_name, str):
                continue
            name = raw_name.strip()
            if not name or len(name) > 256:
                continue
            candidates.append((thread_id, name))
        return [
            {"thread_id": thread_id, "topic_name": name} for thread_id, name in sorted(candidates)
        ]


def read_topic_config(path: Path) -> TopicConfigSnapshot:
    raw_bytes = path.read_bytes()
    parsed = json.loads(raw_bytes)
    if not isinstance(parsed, dict) or not isinstance(parsed.get("topics"), dict):
        raise ValueError("topic_config must contain an object-valued topics field")
    return TopicConfigSnapshot(path=path, digest=_digest_bytes(raw_bytes), raw=parsed)


def _validate_exchange_paths(path: Path, backup_dir: Path) -> None:
    config_stat = path.lstat()
    backup_stat = backup_dir.lstat()
    if not stat.S_ISREG(config_stat.st_mode) or config_stat.st_uid != os.getuid():
        raise ValueError("topic_config has unsafe type or owner")
    if (
        not stat.S_ISDIR(backup_stat.st_mode)
        or backup_stat.st_uid != os.getuid()
        or backup_stat.st_mode & (stat.S_IWGRP | stat.S_IWOTH)
    ):
        raise ValueError("topic_config backup directory has unsafe type, owner, or mode")
    if config_stat.st_dev != backup_stat.st_dev:
        raise ValueError("topic_config and backup directory must share one filesystem")


def reconcile_managed_topic(
    path: Path,
    *,
    thread_id: int,
    cwd: str,
    expected_digest: str | None,
    backup_dir: Path,
    provider: str = "codex",
) -> TopicConfigSnapshot:
    """Patch allowlisted fields without claiming cross-process serializability.

    The upstream-created thread key must already exist. A caller should retain
    the returned digest and keep watching for a later lost update.
    """

    return reconcile_managed_topics(
        path,
        managed_topics={thread_id: cwd},
        expected_digest=expected_digest,
        backup_dir=backup_dir,
        providers={thread_id: provider},
    )


def reconcile_managed_topics(
    path: Path,
    *,
    managed_topics: dict[int, str],
    expected_digest: str | None,
    backup_dir: Path,
    providers: dict[int, str] | None = None,
) -> TopicConfigSnapshot:
    """Patch multiple upstream-created topics in one atomic file replacement."""

    if not managed_topics:
        raise ValueError("at least one managed topic is required")
    if any(thread_id <= 0 or not cwd for thread_id, cwd in managed_topics.items()):
        raise ValueError("managed topics require positive thread ids and non-empty cwd")

    before = read_topic_config(path)
    if expected_digest is not None and before.digest != expected_digest:
        raise RuntimeError("topic_config changed before reconciliation")
    initial_topics = before.raw["topics"]
    assert isinstance(initial_topics, dict)
    for thread_id in managed_topics:
        if not isinstance(initial_topics.get(str(thread_id)), dict):
            raise KeyError(f"upstream topic key does not exist: {thread_id}")
    backup_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    _validate_exchange_paths(path, backup_dir)
    added_topics: dict[str, Any] = {}
    base = before
    conflict = False
    expected_displaced_digest = before.digest
    last_displaced_backup: Path | None = None
    for attempt in range(_MAX_CONCURRENT_WRITES):
        base_topics = base.raw["topics"]
        assert isinstance(base_topics, dict)
        updated_root = dict(base.raw)
        updated_topics = dict(base_topics)
        # A stale upstream write can omit a topic found in an earlier displaced
        # version. Keep every newly observed topic until this reconcile ends.
        for key, value in added_topics.items():
            updated_topics.setdefault(key, value)
        for thread_id, cwd in managed_topics.items():
            key = str(thread_id)
            current = updated_topics.get(key)
            if not isinstance(current, dict):
                # A concurrent upstream delete must be restored to the file,
                # never overwritten by a stale managed snapshot.
                conflict = True
                continue
            initial = initial_topics.get(key)
            if not isinstance(initial, dict):
                raise KeyError(f"upstream topic key does not exist: {thread_id}")
            provider = (providers or {}).get(thread_id, "codex")
            if provider not in {"codex", "claude_code"}:
                raise ValueError(f"unsupported managed provider: {provider}")
            engine = "claude" if provider == "claude_code" else "codex"
            for field, desired in (("cwd", cwd), ("engine", engine), ("exec_mode", "tmux")):
                observed = current.get(field)
                if observed not in (initial.get(field), desired):
                    conflict = True
            if not conflict:
                entry = dict(current)
                entry.update({"cwd": cwd, "engine": engine, "exec_mode": "tmux"})
                updated_topics[key] = entry
        updated_root["topics"] = updated_topics

        encoded = (json.dumps(updated_root, ensure_ascii=False, indent=2) + "\n").encode()
        fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
        tmp_path = Path(tmp_name)
        exchanged = False
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
            os.chmod(tmp_path, 0o600)
            _exchange_paths(tmp_path, path)
            exchanged = True
            # The old path content is still available at tmp_path. Archive it
            # before another pass; even a crash cannot silently discard it.
            displaced = read_topic_config(tmp_path)
            displaced_backup = (
                backup_dir / f"topic-config-displaced-{time.time_ns()}-{attempt}.json"
            )
            os.replace(tmp_path, displaced_backup)
            last_displaced_backup = displaced_backup
            os.chmod(displaced_backup, 0o600)
            dir_fd = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        finally:
            if not exchanged:
                tmp_path.unlink(missing_ok=True)

        if displaced.digest == expected_displaced_digest:
            if conflict:
                raise TopicConfigManagedConflictError(
                    "topic_config managed fields changed during reconciliation"
                )
            break
        expected_displaced_digest = _digest_bytes(encoded)
        displaced_topics = displaced.raw["topics"]
        assert isinstance(displaced_topics, dict)
        for key, value in displaced_topics.items():
            if key not in initial_topics:
                added_topics[key] = value
        base = displaced
    else:
        # The last displaced upstream file may contain a topic absent from the
        # published candidate. Never let an outer CUS retry claim success from
        # that candidate; the archived version requires explicit reconciliation.
        raise TopicConfigUnresolvedConflictError(
            "topic_config concurrent writes exceeded merge limit; "
            f"recover upstream changes from {last_displaced_backup}"
        )

    after = read_topic_config(path)
    for thread_id, cwd in managed_topics.items():
        actual = after.topic(thread_id)
        expected_engine = "claude" if (providers or {}).get(thread_id) == "claude_code" else "codex"
        if (
            actual is None
            or actual.cwd != cwd
            or actual.engine != expected_engine
            or not actual.is_managed_candidate
        ):
            raise RuntimeError("topic_config post-write verification failed")
    return after
