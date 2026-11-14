"""Runtime configuration loaded from a non-secret TOML file."""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def _xdg_state_home() -> Path:
    configured = os.environ.get("XDG_STATE_HOME")
    return Path(configured) if configured else Path.home() / ".local" / "state"


def _xdg_runtime_dir() -> Path:
    configured = os.environ.get("XDG_RUNTIME_DIR")
    if configured:
        return Path(configured)
    return Path("/run/user") / str(os.getuid())


@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    agent_instance_id: str
    forum_chat_id: int
    state_dir: Path
    socket_path: Path
    database_path: Path
    real_codex: Path
    real_codex_version: str
    real_codex_sha256: str
    profiles_root: Path
    topic_config_path: Path
    tmux_state_path: Path
    upstream_sha: str
    real_claude: Path = Path.home() / ".local" / "bin" / "claude-real"
    real_claude_version: str = ""
    real_claude_sha256: str = ""
    deepseek_api_key_path: Path = (
        Path.home()
        / ".config"
        / "ai-session-control-center"
        / "credentials"
        / "deepseek"
        / "default.token"
    )
    deepseek_api_base_url: str = "https://api.deepseek.com/anthropic"
    claude_config_root: Path = Path.home() / ".local" / "share" / "ai-agent-runtime" / "claude"
    tmux_session_prefix: str = "cc-"
    tmux_tmpdir: Path | None = None
    sessions_view_root: Path | None = None
    allowed_project_roots: tuple[Path, ...] = ()
    retirement_enabled: bool = False
    retirement_qualification_report_id: str = ""
    control_plane_stale_after_seconds: float = 60.0
    qualification_disable_codex_hooks: bool = False

    @classmethod
    def defaults(cls) -> RuntimeConfig:
        state_dir = _xdg_state_home() / "ai-agent-runtime"
        return cls(
            agent_instance_id="",
            forum_chat_id=0,
            state_dir=state_dir,
            socket_path=_xdg_runtime_dir() / "ai-agent-runtime" / "runtime.sock",
            database_path=state_dir / "runtime.sqlite",
            real_codex=Path.home() / ".local" / "bin" / "codex-real",
            real_codex_version="",
            real_codex_sha256="",
            profiles_root=Path.home() / ".local" / "share" / "ai-agent-runtime" / "profiles",
            topic_config_path=Path.home() / "apps" / "telegram-ai-agent" / "topic_config.json",
            tmux_state_path=Path.home()
            / "apps"
            / "telegram-ai-agent"
            / "tmux_sessions"
            / "state.json",
            upstream_sha="",
            allowed_project_roots=(),
            retirement_enabled=False,
            retirement_qualification_report_id="",
            control_plane_stale_after_seconds=60.0,
            qualification_disable_codex_hooks=False,
        )

    @classmethod
    def load(cls, path: Path) -> RuntimeConfig:
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
        defaults = cls.defaults()
        runtime = _table(raw, "runtime")
        telegram = _table(raw, "telegram_agent")
        codex = _table(raw, "codex")
        claude = _table(raw, "claude_code")
        policy = _table(raw, "policy")
        state_dir = Path(runtime.get("state_dir", defaults.state_dir)).expanduser()
        raw_roots = policy.get("allowed_project_roots", [])
        if not isinstance(raw_roots, list) or not all(isinstance(item, str) for item in raw_roots):
            raise ValueError("policy.allowed_project_roots must be a list of paths")
        retirement_enabled = policy.get("retirement_enabled", False)
        if not isinstance(retirement_enabled, bool):
            raise ValueError("policy.retirement_enabled must be a boolean")
        qualification_disable_codex_hooks = policy.get("qualification_disable_codex_hooks", False)
        if not isinstance(qualification_disable_codex_hooks, bool):
            raise ValueError("policy.qualification_disable_codex_hooks must be a boolean")
        return cls(
            agent_instance_id=str(runtime.get("agent_instance_id", "")),
            forum_chat_id=int(runtime.get("forum_chat_id", 0)),
            state_dir=state_dir,
            socket_path=Path(runtime.get("socket", defaults.socket_path)).expanduser(),
            database_path=Path(runtime.get("database", state_dir / "runtime.sqlite")).expanduser(),
            real_codex=Path(codex.get("real_binary", defaults.real_codex)).expanduser(),
            real_codex_version=str(codex.get("version", "")),
            real_codex_sha256=str(codex.get("sha256", "")),
            profiles_root=Path(codex.get("profiles_root", defaults.profiles_root)).expanduser(),
            topic_config_path=Path(
                telegram.get("topic_config", defaults.topic_config_path)
            ).expanduser(),
            tmux_state_path=Path(telegram.get("tmux_state", defaults.tmux_state_path)).expanduser(),
            upstream_sha=str(telegram.get("upstream_sha", "")),
            real_claude=Path(claude.get("real_binary", defaults.real_claude)).expanduser(),
            real_claude_version=str(claude.get("version", "")),
            real_claude_sha256=str(claude.get("sha256", "")),
            deepseek_api_key_path=Path(
                claude.get("deepseek_api_key_path", defaults.deepseek_api_key_path)
            ).expanduser(),
            deepseek_api_base_url=str(
                claude.get("deepseek_api_base_url", defaults.deepseek_api_base_url)
            ).rstrip("/"),
            claude_config_root=Path(
                claude.get("config_root", defaults.claude_config_root)
            ).expanduser(),
            tmux_session_prefix=str(telegram.get("tmux_session_prefix", "cc-")),
            tmux_tmpdir=(
                Path(str(telegram["tmux_tmpdir"])).expanduser()
                if "tmux_tmpdir" in telegram
                else None
            ),
            sessions_view_root=(
                Path(str(telegram["sessions_view_root"])).expanduser()
                if "sessions_view_root" in telegram
                else None
            ),
            allowed_project_roots=tuple(Path(item).expanduser() for item in raw_roots),
            retirement_enabled=retirement_enabled,
            retirement_qualification_report_id=str(
                policy.get("retirement_qualification_report_id", "")
            ),
            control_plane_stale_after_seconds=float(
                policy.get("control_plane_stale_after_seconds", 60.0)
            ),
            qualification_disable_codex_hooks=qualification_disable_codex_hooks,
        )

    def validate(self) -> None:
        if not self.agent_instance_id:
            raise ValueError("runtime.agent_instance_id is required")
        if self.forum_chat_id >= 0:
            raise ValueError("runtime.forum_chat_id must be a Telegram supergroup id")
        if len(self.upstream_sha) != 40 or any(
            ch not in "0123456789abcdef" for ch in self.upstream_sha
        ):
            raise ValueError("telegram_agent.upstream_sha must be a lowercase 40-char commit SHA")
        for name, value in {
            "state_dir": self.state_dir,
            "socket_path": self.socket_path,
            "database_path": self.database_path,
            "real_codex": self.real_codex,
            "profiles_root": self.profiles_root,
            "topic_config_path": self.topic_config_path,
            "tmux_state_path": self.tmux_state_path,
            "real_claude": self.real_claude,
            "deepseek_api_key_path": self.deepseek_api_key_path,
            "claude_config_root": self.claude_config_root,
        }.items():
            if not value.is_absolute():
                raise ValueError(f"{name} must be absolute")
            if value == Path("/") or ".." in value.parts:
                raise ValueError(f"{name} must be a non-root traversal-free path")
        lexical_state = Path(os.path.abspath(self.state_dir))
        lexical_database = Path(os.path.abspath(self.database_path))
        if lexical_database != lexical_state and lexical_state not in lexical_database.parents:
            raise ValueError("database_path must be contained by state_dir")
        if self.socket_path.parent == Path("/"):
            raise ValueError("socket_path must use a private non-root directory")
        tmux_tmpdir = self.effective_tmux_tmpdir
        if not tmux_tmpdir.is_absolute() or tmux_tmpdir == Path("/") or ".." in tmux_tmpdir.parts:
            raise ValueError("tmux_tmpdir must be an absolute non-root traversal-free path")
        if self.sessions_view_root is not None:
            view_root = self.sessions_view_root
            if not view_root.is_absolute() or view_root == Path("/") or ".." in view_root.parts:
                raise ValueError(
                    "sessions_view_root must be an absolute non-root traversal-free path"
                )
            lexical_view = Path(os.path.abspath(view_root))
            lexical_profiles = Path(os.path.abspath(self.profiles_root))
            if lexical_view == lexical_profiles or lexical_profiles in lexical_view.parents:
                raise ValueError("sessions_view_root must not be inside profiles_root")
        if not self.allowed_project_roots:
            raise ValueError("policy.allowed_project_roots must not be empty")
        if any(not root.is_absolute() for root in self.allowed_project_roots):
            raise ValueError("every allowed project root must be absolute")
        if not self.real_codex_version or len(self.real_codex_version) > 100:
            raise ValueError("codex.version is required")
        if (
            not self.real_codex_sha256.startswith("sha256:")
            or len(self.real_codex_sha256) != 71
            or any(ch not in "0123456789abcdef" for ch in self.real_codex_sha256[7:])
        ):
            raise ValueError("codex.sha256 must be a sha256-prefixed lowercase digest")
        if self.real_claude_version or self.real_claude_sha256:
            if not self.real_claude_version or len(self.real_claude_version) > 100:
                raise ValueError("claude_code.version is invalid")
            if (
                not self.real_claude_sha256.startswith("sha256:")
                or len(self.real_claude_sha256) != 71
                or any(ch not in "0123456789abcdef" for ch in self.real_claude_sha256[7:])
            ):
                raise ValueError("claude_code.sha256 must be a sha256-prefixed lowercase digest")
        if self.deepseek_api_base_url != "https://api.deepseek.com/anthropic":
            raise ValueError("claude_code.deepseek_api_base_url is not an approved endpoint")
        if self.retirement_enabled and not self.retirement_qualification_report_id:
            raise ValueError(
                "policy.retirement_qualification_report_id is required when retirement is enabled"
            )
        if not 5.0 <= self.control_plane_stale_after_seconds <= 3600.0:
            raise ValueError("policy.control_plane_stale_after_seconds must be between 5 and 3600")

    @property
    def effective_tmux_tmpdir(self) -> Path:
        """Return the qualified per-AgentInstance tmux selector directory.

        Pinned upstream starts the configured Codex binary through ``env -i``
        and therefore may remove ``TMUX_TMPDIR``.  Runtime socket and tmux
        directories share the same private AgentInstance root by default; an
        explicit value supports deployments with a different layout.
        """

        return self.tmux_tmpdir or self.socket_path.parent / "tmux"


def _table(raw: dict[str, Any], key: str) -> dict[str, Any]:
    value = raw.get(key, {})
    if not isinstance(value, dict):
        raise ValueError(f"[{key}] must be a TOML table")
    return value
