"""Linux process and filesystem checks used at the execution boundary."""

from __future__ import annotations

import hashlib
import os
import stat
import subprocess
from dataclasses import dataclass
from pathlib import Path

from ai_agent_runtime.domain import WrapperIdentity
from ai_agent_runtime.errors import ErrorCode, RuntimeFailureError


def proc_starttime(pid: int) -> int:
    """Return Linux /proc starttime without misparsing spaces in comm."""

    raw = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8")
    closing = raw.rfind(")")
    if closing < 0:
        raise ValueError("malformed /proc stat")
    fields_after_comm = raw[closing + 2 :].split()
    return int(fields_after_comm[19])


def proc_environment(pid: int) -> dict[str, str]:
    raw = Path(f"/proc/{pid}/environ").read_bytes()
    environment: dict[str, str] = {}
    for item in raw.split(b"\0"):
        key, separator, value = item.partition(b"=")
        if separator:
            environment[key.decode(errors="strict")] = value.decode(errors="strict")
    return environment


def proc_uid(pid: int) -> int:
    return Path(f"/proc/{pid}").stat().st_uid


def proc_parent_pid(pid: int) -> int:
    for line in Path(f"/proc/{pid}/status").read_text(encoding="utf-8").splitlines():
        if line.startswith("PPid:"):
            return int(line.partition(":")[2].strip())
    raise ValueError("process status has no PPid")


def proc_cmdline(pid: int) -> tuple[str, ...]:
    return tuple(
        item.decode(errors="strict")
        for item in Path(f"/proc/{pid}/cmdline").read_bytes().split(b"\0")
        if item
    )


@dataclass(frozen=True, slots=True)
class TmuxPaneProcess:
    pane_id: str
    session_name: str
    pane_pid: int
    session_id: str = ""


def _validate_tmux_socket(socket_path: Path) -> Path:
    if not socket_path.is_absolute() or ".." in socket_path.parts:
        raise RuntimeFailureError(ErrorCode.AMBIGUOUS, "unsafe tmux socket path")
    metadata = socket_path.stat()
    parent = socket_path.parent.stat()
    if not stat.S_ISSOCK(metadata.st_mode):
        raise RuntimeFailureError(ErrorCode.AMBIGUOUS, "TMUX path is not a socket")
    if metadata.st_uid != os.getuid() or parent.st_uid != os.getuid():
        raise RuntimeFailureError(ErrorCode.AMBIGUOUS, "tmux socket ownership mismatch")
    if parent.st_mode & stat.S_IWOTH:
        raise RuntimeFailureError(ErrorCode.AMBIGUOUS, "tmux socket directory is world writable")
    return socket_path


def _tmux_socket_from_environment(value: str) -> Path:
    socket_value, separator, _remainder = value.partition(",")
    if not separator or not socket_value:
        raise RuntimeFailureError(ErrorCode.AMBIGUOUS, "malformed TMUX environment")
    return _validate_tmux_socket(Path(socket_value))


def _tmux_socket_for_wrapper(environment: dict[str, str], *, uid: int) -> Path:
    tmux_value = environment.get("TMUX")
    if tmux_value:
        return _tmux_socket_from_environment(tmux_value)
    tmux_tmpdir = environment.get("TMUX_TMPDIR")
    if not tmux_tmpdir:
        raise RuntimeFailureError(
            ErrorCode.AMBIGUOUS,
            "wrapper has neither TMUX nor qualified TMUX_TMPDIR environment",
        )
    base = Path(tmux_tmpdir)
    if not base.is_absolute() or ".." in base.parts:
        raise RuntimeFailureError(ErrorCode.AMBIGUOUS, "unsafe TMUX_TMPDIR")
    base_metadata = base.stat()
    if base_metadata.st_uid != uid or base_metadata.st_mode & stat.S_IWOTH:
        raise RuntimeFailureError(ErrorCode.AMBIGUOUS, "unsafe TMUX_TMPDIR ownership/mode")
    return _validate_tmux_socket(base / f"tmux-{uid}" / "default")


def inspect_tmux_pane(socket_path: Path, pane_id: str) -> TmuxPaneProcess:
    completed = subprocess.run(
        (
            "tmux",
            "-S",
            str(socket_path),
            "list-panes",
            "-a",
            "-F",
            "#{pane_id}\t#{session_name}\t#{pane_pid}\t#{session_id}",
        ),
        check=False,
        capture_output=True,
        text=True,
        timeout=5,
    )
    if completed.returncode != 0:
        raise RuntimeFailureError(ErrorCode.AMBIGUOUS, "cannot inspect tmux panes")
    matches: list[TmuxPaneProcess] = []
    for line in completed.stdout.splitlines():
        raw_pane, separator, remainder = line.partition("\t")
        session_name, second_separator, pid_and_session = remainder.partition("\t")
        raw_pid, third_separator, session_id = pid_and_session.partition("\t")
        if (
            not separator
            or not second_separator
            or not third_separator
            or raw_pane != pane_id
            or not session_id
        ):
            continue
        try:
            pane_pid = int(raw_pid)
        except ValueError:
            continue
        matches.append(TmuxPaneProcess(raw_pane, session_name, pane_pid, session_id))
    if len(matches) != 1:
        raise RuntimeFailureError(
            ErrorCode.AMBIGUOUS,
            f"expected one matching tmux pane, found {len(matches)}",
        )
    return matches[0]


def verify_wrapper_identity(identity: WrapperIdentity) -> None:
    if proc_uid(identity.pid) != identity.uid or identity.uid != os.getuid():
        raise RuntimeFailureError(ErrorCode.AMBIGUOUS, "wrapper process UID mismatch")
    if proc_starttime(identity.pid) != identity.starttime:
        raise RuntimeFailureError(ErrorCode.AMBIGUOUS, "wrapper process starttime mismatch")
    actual_cwd = Path(f"/proc/{identity.pid}/cwd").resolve(strict=True)
    if actual_cwd != Path(identity.cwd).resolve(strict=True):
        raise RuntimeFailureError(ErrorCode.AMBIGUOUS, "wrapper process cwd mismatch")
    environment = proc_environment(identity.pid)
    reported_pane = environment.get("TMUX_PANE")
    if reported_pane is not None and reported_pane != identity.tmux_pane:
        raise RuntimeFailureError(ErrorCode.AMBIGUOUS, "wrapper TMUX_PANE mismatch")
    pane = inspect_tmux_pane(
        _tmux_socket_for_wrapper(environment, uid=identity.uid), identity.tmux_pane
    )
    if pane.session_name != identity.tmux_session or pane.pane_pid != identity.pid:
        raise RuntimeFailureError(
            ErrorCode.AMBIGUOUS,
            "tmux pane/session/process identity mismatch",
        )


def retire_tmux_session(identity: WrapperIdentity) -> None:
    """Kill only the exact tmux session proven to contain this wrapper."""

    verify_wrapper_identity(identity)
    environment = proc_environment(identity.pid)
    socket_path = _tmux_socket_for_wrapper(environment, uid=identity.uid)
    pane = inspect_tmux_pane(socket_path, identity.tmux_pane)
    if (
        pane.pane_id != identity.tmux_pane
        or pane.session_name != identity.tmux_session
        or pane.pane_pid != identity.pid
        or not pane.session_id
    ):
        raise RuntimeFailureError(
            ErrorCode.AMBIGUOUS,
            "tmux retirement target changed after identity verification",
        )
    completed = subprocess.run(
        ("tmux", "-S", str(socket_path), "kill-session", "-t", pane.session_id),
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )
    if completed.returncode != 0:
        raise RuntimeFailureError(ErrorCode.AMBIGUOUS, "exact tmux retirement failed")


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return f"sha256:{digest.hexdigest()}"


def process_executable_evidence(pid: int) -> dict[str, object]:
    """Capture immutable executable identity for one live Linux process."""

    executable = Path(f"/proc/{pid}/exe").resolve(strict=True)
    validated = validate_owned_executable(executable, uid=proc_uid(pid))
    metadata = validated.stat()
    return {
        "path": str(validated),
        "device": metadata.st_dev,
        "inode": metadata.st_ino,
        "size": metadata.st_size,
        "mtime_ns": metadata.st_mtime_ns,
        "sha256": file_sha256(validated),
    }


def verify_process_executable_evidence(
    pid: int,
    expected: object,
    *,
    verify_digest: bool,
) -> None:
    """Fail closed if a live process no longer matches captured executable evidence."""

    if not isinstance(expected, dict):
        raise RuntimeFailureError(
            ErrorCode.CORRELATION_MISMATCH,
            "wrapper executable evidence is absent",
        )
    executable = Path(f"/proc/{pid}/exe").resolve(strict=True)
    validated = validate_owned_executable(executable, uid=proc_uid(pid))
    metadata = validated.stat()
    observed = {
        "path": str(validated),
        "device": metadata.st_dev,
        "inode": metadata.st_ino,
        "size": metadata.st_size,
        "mtime_ns": metadata.st_mtime_ns,
    }
    if any(expected.get(key) != value for key, value in observed.items()):
        raise RuntimeFailureError(
            ErrorCode.CORRELATION_MISMATCH,
            "wrapper executable identity changed",
        )
    if verify_digest and expected.get("sha256") != file_sha256(validated):
        raise RuntimeFailureError(
            ErrorCode.CORRELATION_MISMATCH,
            "wrapper executable digest changed",
        )


def validate_owned_executable(path: Path, *, uid: int | None = None) -> Path:
    expected_uid = os.getuid() if uid is None else uid
    if not path.is_absolute():
        raise RuntimeFailureError(ErrorCode.UNSAFE_BINARY, "real Codex path must be absolute")
    resolved = path.resolve(strict=True)
    metadata = resolved.stat()
    if not stat.S_ISREG(metadata.st_mode):
        raise RuntimeFailureError(ErrorCode.UNSAFE_BINARY, "real Codex is not a regular file")
    if metadata.st_uid not in {0, expected_uid}:
        raise RuntimeFailureError(ErrorCode.UNSAFE_BINARY, "real Codex has an unexpected owner")
    if metadata.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        raise RuntimeFailureError(ErrorCode.UNSAFE_BINARY, "real Codex is group/world writable")
    if not metadata.st_mode & stat.S_IXUSR:
        raise RuntimeFailureError(ErrorCode.UNSAFE_BINARY, "real Codex is not owner-executable")
    return resolved


def validate_profile_path(path: Path, *, root: Path, uid: int | None = None) -> Path:
    expected_uid = os.getuid() if uid is None else uid
    resolved_root = root.resolve(strict=True)
    resolved = path.resolve(strict=True)
    if resolved != resolved_root and resolved_root not in resolved.parents:
        raise RuntimeFailureError(ErrorCode.UNSAFE_PROFILE, "CODEX_HOME escapes profiles root")
    metadata = resolved.stat()
    if not stat.S_ISDIR(metadata.st_mode):
        raise RuntimeFailureError(ErrorCode.UNSAFE_PROFILE, "CODEX_HOME is not a directory")
    if metadata.st_uid != expected_uid:
        raise RuntimeFailureError(ErrorCode.UNSAFE_PROFILE, "CODEX_HOME has an unexpected owner")
    if metadata.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        raise RuntimeFailureError(ErrorCode.UNSAFE_PROFILE, "CODEX_HOME is group/world writable")
    return resolved


def validate_project_path(path: Path, *, allowed_roots: tuple[Path, ...]) -> Path:
    resolved = path.resolve(strict=True)
    if not resolved.is_dir():
        raise ValueError("project cwd is not a directory")
    roots = tuple(root.resolve(strict=True) for root in allowed_roots)
    if not any(resolved == root or root in resolved.parents for root in roots):
        raise ValueError("project cwd is outside allowed project roots")
    return resolved


def profile_fingerprint(path: Path) -> str:
    """Fingerprint the stable, safe identity of an account-profile directory.

    Credential files are mutable by design: an authentication refresh may replace
    ``auth.json`` or change its size and mtime without changing the configured
    account-profile alias. The fingerprint therefore records only filesystem
    identity and security properties, and never credential content or rotation
    metadata.
    """

    resolved = path.resolve(strict=True)
    profile_stat = resolved.stat()
    auth_path = resolved / "auth.json"
    auth_metadata: tuple[int, int] | None = None
    try:
        auth_stat = auth_path.stat(follow_symlinks=False)
    except FileNotFoundError:
        auth_stat = None
    if auth_stat is not None:
        if stat.S_ISLNK(auth_stat.st_mode):
            raise RuntimeFailureError(ErrorCode.UNSAFE_PROFILE, "auth.json must not be a symlink")
        if not stat.S_ISREG(auth_stat.st_mode):
            raise RuntimeFailureError(ErrorCode.UNSAFE_PROFILE, "auth.json must be a regular file")
        if auth_stat.st_uid != profile_stat.st_uid:
            raise RuntimeFailureError(ErrorCode.UNSAFE_PROFILE, "auth.json has an unexpected owner")
        if auth_stat.st_mode & (stat.S_IRWXG | stat.S_IRWXO):
            raise RuntimeFailureError(
                ErrorCode.UNSAFE_PROFILE, "auth.json must not be accessible by group/other"
            )
        auth_metadata = (
            auth_stat.st_uid,
            stat.S_IMODE(auth_stat.st_mode),
        )
    material = repr(
        (
            str(resolved),
            profile_stat.st_dev,
            profile_stat.st_ino,
            profile_stat.st_uid,
            stat.S_IMODE(profile_stat.st_mode),
            auth_metadata,
        )
    ).encode()
    return f"profile-v2:sha256:{hashlib.sha256(material).hexdigest()}"


def validate_secret_file(path: Path, *, uid: int | None = None) -> Path:
    expected_uid = os.getuid() if uid is None else uid
    if not path.is_absolute() or path == Path("/") or ".." in path.parts:
        raise RuntimeFailureError(ErrorCode.UNSAFE_PROFILE, "credential path is unsafe")
    metadata = path.lstat()
    if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISREG(metadata.st_mode):
        raise RuntimeFailureError(ErrorCode.UNSAFE_PROFILE, "credential must be a regular file")
    if metadata.st_uid != expected_uid or metadata.st_mode & (stat.S_IRWXG | stat.S_IRWXO):
        raise RuntimeFailureError(ErrorCode.UNSAFE_PROFILE, "credential must be owner-only")
    if metadata.st_size <= 0 or metadata.st_size > 4096:
        raise RuntimeFailureError(ErrorCode.UNSAFE_PROFILE, "credential size is invalid")
    return path.resolve(strict=True)


def secret_file_fingerprint(path: Path) -> str:
    resolved = validate_secret_file(path)
    metadata = resolved.stat()
    material = repr(
        (
            str(resolved),
            metadata.st_dev,
            metadata.st_ino,
            metadata.st_uid,
            stat.S_IMODE(metadata.st_mode),
        )
    ).encode()
    return f"secret-v1:sha256:{hashlib.sha256(material).hexdigest()}"


def verify_claude_child(
    *,
    child_pid: int,
    child_starttime: int,
    wrapper_pid: int,
    credential_path: Path,
    config_dir: Path,
    base_url: str,
    model: str,
    reasoning_effort: str,
    real_claude: Path,
) -> dict[str, object]:
    if proc_uid(child_pid) != os.getuid() or proc_starttime(child_pid) != child_starttime:
        raise RuntimeFailureError(ErrorCode.CORRELATION_MISMATCH, "Claude child identity mismatch")
    if proc_parent_pid(child_pid) != wrapper_pid:
        raise RuntimeFailureError(ErrorCode.CORRELATION_MISMATCH, "Claude child parent mismatch")
    environment = proc_environment(child_pid)
    expected = {
        "ANTHROPIC_BASE_URL": base_url,
        "ANTHROPIC_MODEL": model,
        "CLAUDE_CODE_EFFORT_LEVEL": reasoning_effort,
        "CLAUDE_CONFIG_DIR": str(config_dir),
    }
    if any(environment.get(key) != value for key, value in expected.items()):
        raise RuntimeFailureError(
            ErrorCode.CORRELATION_MISMATCH, "Claude child environment mismatch"
        )
    if not environment.get("ANTHROPIC_AUTH_TOKEN"):
        raise RuntimeFailureError(ErrorCode.CORRELATION_MISMATCH, "Claude auth token is absent")
    argv = proc_cmdline(child_pid)
    if str(real_claude) not in argv:
        raise RuntimeFailureError(
            ErrorCode.CORRELATION_MISMATCH, "real Claude path absent from argv"
        )
    try:
        actual_model = argv[argv.index("--model") + 1]
    except (ValueError, IndexError) as exc:
        raise RuntimeFailureError(ErrorCode.CORRELATION_MISMATCH, "Claude model is absent") from exc
    if actual_model != model:
        raise RuntimeFailureError(ErrorCode.CORRELATION_MISMATCH, "Claude model mismatch")
    binary_stat = real_claude.stat()
    return {
        "provider": "claude_code",
        "credential_fingerprint": secret_file_fingerprint(credential_path),
        "actual_model": actual_model,
        "actual_reasoning_effort": reasoning_effort,
        "real_claude_path": str(real_claude),
        "real_claude_device": binary_stat.st_dev,
        "real_claude_inode": binary_stat.st_ino,
        "real_claude_sha256": file_sha256(real_claude),
        "child_pid": child_pid,
        "child_starttime": child_starttime,
    }


def verify_codex_child(
    *,
    child_pid: int,
    child_starttime: int,
    wrapper_pid: int,
    codex_home: Path,
    model: str,
    reasoning_effort: str,
    real_codex: Path,
) -> dict[str, object]:
    if proc_uid(child_pid) != os.getuid():
        raise RuntimeFailureError(ErrorCode.CORRELATION_MISMATCH, "child UID mismatch")
    if proc_starttime(child_pid) != child_starttime:
        raise RuntimeFailureError(ErrorCode.CORRELATION_MISMATCH, "child starttime mismatch")
    if proc_parent_pid(child_pid) != wrapper_pid:
        raise RuntimeFailureError(ErrorCode.CORRELATION_MISMATCH, "child parent mismatch")
    environment = proc_environment(child_pid)
    if environment.get("CODEX_HOME") != str(codex_home):
        raise RuntimeFailureError(ErrorCode.CORRELATION_MISMATCH, "child CODEX_HOME mismatch")
    argv = proc_cmdline(child_pid)
    if str(real_codex) not in argv:
        raise RuntimeFailureError(
            ErrorCode.CORRELATION_MISMATCH, "real Codex path absent from child argv"
        )
    try:
        model_index = argv.index("--model")
        actual_model = argv[model_index + 1]
    except (ValueError, IndexError) as exc:
        raise RuntimeFailureError(
            ErrorCode.CORRELATION_MISMATCH, "child model argument is absent"
        ) from exc
    if actual_model != model:
        raise RuntimeFailureError(ErrorCode.CORRELATION_MISMATCH, "child model mismatch")
    expected_reasoning = f"model_reasoning_effort={reasoning_effort}"
    if not any(
        argv[index] == "-c" and index + 1 < len(argv) and argv[index + 1] == expected_reasoning
        for index in range(len(argv))
    ):
        raise RuntimeFailureError(
            ErrorCode.CORRELATION_MISMATCH, "child reasoning argument mismatch"
        )
    binary_stat = real_codex.stat()
    return {
        "actual_codex_home": str(codex_home),
        "actual_model": actual_model,
        "actual_reasoning_effort": reasoning_effort,
        "real_codex_path": str(real_codex),
        "real_codex_device": binary_stat.st_dev,
        "real_codex_inode": binary_stat.st_ino,
        "real_codex_sha256": file_sha256(real_codex),
        "child_pid": child_pid,
        "child_starttime": child_starttime,
    }
