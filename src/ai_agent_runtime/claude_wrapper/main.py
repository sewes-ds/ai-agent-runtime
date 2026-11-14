"""Transparent Claude Code wrapper used as TELEGRAM_CLAUDE_BIN."""

from __future__ import annotations

import json
import os
import re
import sys
import tempfile
import uuid
from pathlib import Path

from ai_agent_runtime import __version__
from ai_agent_runtime.config import RuntimeConfig
from ai_agent_runtime.errors import ErrorCode
from ai_agent_runtime.ipc.protocol import versioned
from ai_agent_runtime.process_safety import validate_secret_file
from ai_agent_runtime.wrapper.main import (
    _call_runtime,
    _identity_payload,
    _restore_tmux_selector,
    _run_supervised,
    _tmux_identity,
    _tmux_identity_from_controlling_tty,
)

_INSTANCE_EXECUTABLE = re.compile(r"^claude-wrapper@([A-Za-z0-9][A-Za-z0-9_.-]{0,63})$")
_SESSION_ID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}")
_TMUX_SESSION = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}")
_PROVIDER_RUNTIME_METADATA = "provider.runtime.json"


def _config_path(executable: str | None = None) -> Path:
    configured = os.environ.get("AI_AGENT_RUNTIME_CONFIG")
    if configured:
        return Path(configured).expanduser()
    executable_path = Path(executable or sys.argv[0])
    matched = _INSTANCE_EXECUTABLE.fullmatch(executable_path.name)
    if matched:
        return (
            Path.home() / ".config" / "ai-agent-runtime" / "instances" / f"{matched.group(1)}.toml"
        )
    if (
        executable_path.name == "claude-wrapper"
        and executable_path.parent.name == "bin"
        and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}", executable_path.parent.parent.name)
    ):
        return (
            Path.home()
            / ".config"
            / "ai-agent-runtime"
            / "instances"
            / f"{executable_path.parent.parent.name}.toml"
        )
    return Path.home() / ".config" / "ai-agent-runtime" / "config.toml"


def _resume_session_id(argv: list[str]) -> str | None:
    for index, token in enumerate(argv):
        if token == "--resume" and index + 1 < len(argv) and _SESSION_ID.fullmatch(argv[index + 1]):
            return argv[index + 1]
    return None


def _tui_session_id(argv: list[str]) -> str | None:
    for index, token in enumerate(argv):
        if (
            token in {"--session-id", "--resume"}
            and index + 1 < len(argv)
            and _SESSION_ID.fullmatch(argv[index + 1])
        ):
            return argv[index + 1]
    return None


def _write_provider_runtime_metadata(
    *,
    tmux_state_path: Path,
    tmux_session: str,
    config_dir: Path,
    session_id: str,
) -> Path:
    """Publish the non-secret Claude transcript root to the Telegram adapter."""
    if not _TMUX_SESSION.fullmatch(tmux_session):
        raise ValueError("tmux session name is invalid")
    if not _SESSION_ID.fullmatch(session_id):
        raise ValueError("Claude session id is invalid")
    if not config_dir.is_absolute() or config_dir == Path("/") or ".." in config_dir.parts:
        raise ValueError("Claude config directory is invalid")

    sessions_root = tmux_state_path.parent.resolve(strict=True)
    lexical_session_dir = sessions_root / tmux_session
    if lexical_session_dir.is_symlink():
        raise ValueError("tmux session directory must not be a symlink")
    session_dir = lexical_session_dir.resolve(strict=True)
    if session_dir.parent != sessions_root or not session_dir.is_dir():
        raise ValueError("tmux session directory is outside the configured sessions root")

    payload = {
        "schema_version": 1,
        "provider": "claude_code",
        "session_id": session_id,
        "config_dir": str(config_dir),
    }
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True) + "\n"
    target = session_dir / _PROVIDER_RUNTIME_METADATA
    fd, temporary_name = tempfile.mkstemp(prefix=f".{target.name}.", dir=session_dir)
    temporary = Path(temporary_name)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, target)
        os.chmod(target, 0o600)
        directory_fd = os.open(session_dir, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        temporary.unlink(missing_ok=True)
    return target


def _rewrite_model(argv: list[str], model: str) -> list[str]:
    rewritten: list[str] = []
    index = 0
    while index < len(argv):
        token = argv[index]
        if token == "--model":
            if index + 1 >= len(argv):
                raise ValueError("--model requires a value")
            index += 2
            continue
        if token.startswith("--model="):
            index += 1
            continue
        rewritten.append(token)
        index += 1
    return [*rewritten, "--model", model]


def _read_token(path: Path) -> str:
    safe = validate_secret_file(path)
    token = safe.read_text(encoding="utf-8").strip()
    if not token or len(token) > 4096 or any(character.isspace() for character in token):
        raise ValueError("DeepSeek credential format is invalid")
    return token


def run(argv: list[str]) -> int:
    if argv and argv[0] in {"--version", "-v", "-V"}:
        print(f"claude-wrapper {__version__}")
        return 0
    if argv and argv[0] in {"login", "logout", "update"}:
        raise SystemExit(70)
    config = RuntimeConfig.load(_config_path())
    config.validate()
    _restore_tmux_selector(config)
    pane = os.environ.get("TMUX_PANE")
    if pane:
        session = _tmux_identity(pane)
    else:
        pane, session = _tmux_identity_from_controlling_tty()
    response = _call_runtime(
        config,
        versioned(
            "register",
            str(uuid.uuid4()),
            agent_instance_id=config.agent_instance_id,
            provider_hint="claude_code",
            resume_session_id=_resume_session_id(argv),
            **_identity_payload(pane=pane, session=session),
        ),
    )
    spec = response.get("spec")
    if not isinstance(spec, dict):
        raise RuntimeError("Runtime response has no launch spec")
    credential_path = Path(str(spec["credential_path"]))
    config_dir = Path(str(spec["claude_config_dir"]))
    config_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(config_dir, 0o700)
    model = str(spec["model"])
    session_id = _tui_session_id(argv)
    if session_id is None:
        raise ValueError("Claude TUI launch requires --session-id or --resume")
    _write_provider_runtime_metadata(
        tmux_state_path=config.tmux_state_path,
        tmux_session=session,
        config_dir=config_dir,
        session_id=session_id,
    )
    environment = dict(os.environ)
    environment.update(
        {
            "ANTHROPIC_BASE_URL": str(spec["deepseek_api_base_url"]),
            "ANTHROPIC_AUTH_TOKEN": _read_token(credential_path),
            "ANTHROPIC_MODEL": model,
            "ANTHROPIC_DEFAULT_OPUS_MODEL": model,
            "ANTHROPIC_DEFAULT_SONNET_MODEL": model,
            "ANTHROPIC_DEFAULT_HAIKU_MODEL": "deepseek-flash",
            "CLAUDE_CODE_SUBAGENT_MODEL": "deepseek-flash",
            "CLAUDE_CODE_EFFORT_LEVEL": str(spec["reasoning_effort"]),
            "CLAUDE_CODE_AUTO_COMPACT_WINDOW": "786432",
            "CLAUDE_CONFIG_DIR": str(config_dir),
            "API_TIMEOUT_MS": "600000",
            "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
            "DISABLE_AUTOUPDATER": "1",
        }
    )
    response["tmux_pane"] = pane
    response["tmux_session"] = session
    return _run_supervised(
        str(spec["real_claude"]),
        _rewrite_model(argv, model),
        environment=environment,
        registration=response,
        config=config,
    )


def main() -> None:
    try:
        raise SystemExit(run(sys.argv[1:]))
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"claude-wrapper: {ErrorCode.RUNTIME_UNAVAILABLE.value}: {exc}", file=sys.stderr)
        raise SystemExit(76) from exc


if __name__ == "__main__":
    main()
