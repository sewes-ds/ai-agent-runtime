"""Two-phase binding correlation."""

from __future__ import annotations

from pathlib import Path

from ai_agent_runtime.domain import RuntimeBinding, WrapperIdentity
from ai_agent_runtime.errors import ErrorCode, RuntimeFailureError

from .adapter import matches_managed_tmux_name
from .tmux_state import UpstreamTmuxState
from .topic_config import TopicConfigSnapshot


def replaced_start_state(
    *,
    binding: RuntimeBinding,
    identity: WrapperIdentity,
    states: tuple[UpstreamTmuxState, ...],
    provider: str = "codex",
) -> UpstreamTmuxState | None:
    """Find the previous durable state for a fresh launch under the same tmux name.

    Pinned upstream `/new` and `/clear` respawn tmux under its existing name,
    including a `-switch-<hex>` name after `/resume`. A prior session ID must
    be remembered for both forms: otherwise Phase B and AppliedSpec could
    mistake the still-durable old rollout for the new Codex process.
    """

    if not matches_managed_tmux_name(
        expected=binding.expected_tmux_session,
        observed=identity.tmux_session,
        resume_requested=True,
    ):
        return None
    matches = tuple(
        state
        for state in states
        if state.chat_id == binding.chat_id
        and state.thread_id == binding.thread_id
        and state.session_name == identity.tmux_session
        and state.provider == ("claude" if provider == "claude_code" else provider)
        and state.session_id
        and Path(state.cwd).resolve() == Path(binding.canonical_cwd).resolve()
    )
    return matches[0] if len(matches) == 1 else None


def phase_a_identity_candidates(
    *,
    identity: WrapperIdentity,
    bindings: tuple[RuntimeBinding, ...],
    resume_session_id: str | None = None,
    states: tuple[UpstreamTmuxState, ...] = (),
    provider: str = "codex",
) -> tuple[RuntimeBinding, ...]:
    return tuple(
        binding
        for binding in bindings
        if (
            matches_managed_tmux_name(
                expected=binding.expected_tmux_session,
                observed=identity.tmux_session,
                resume_requested=resume_session_id is not None,
            )
            or (
                resume_session_id is None
                and replaced_start_state(
                    binding=binding, identity=identity, states=states, provider=provider
                )
                is not None
            )
        )
        and Path(binding.canonical_cwd).resolve() == Path(identity.cwd).resolve()
    )


def phase_a_correlate(
    *,
    identity: WrapperIdentity,
    bindings: tuple[RuntimeBinding, ...],
    topic_config: TopicConfigSnapshot,
    resume_session_id: str | None = None,
    states: tuple[UpstreamTmuxState, ...] = (),
    provider: str = "codex",
) -> RuntimeBinding:
    candidates = phase_a_identity_candidates(
        identity=identity,
        bindings=bindings,
        resume_session_id=resume_session_id,
        states=states,
        provider=provider,
    )
    if not candidates:
        raise RuntimeFailureError(
            ErrorCode.UNKNOWN_BINDING, "no binding matches tmux identity and cwd"
        )
    if len(candidates) != 1:
        raise RuntimeFailureError(
            ErrorCode.AMBIGUOUS, "multiple bindings match tmux identity and cwd"
        )
    binding = candidates[0]
    if binding.management_mode.value == "CONFIG_DRIFT":
        raise RuntimeFailureError(ErrorCode.CONFIG_DRIFT, "binding config drift is unresolved")
    if binding.management_mode.value != "MANAGED":
        raise RuntimeFailureError(ErrorCode.UNKNOWN_BINDING, "binding is currently unmanaged")
    if binding.config_digest != topic_config.digest:
        raise RuntimeFailureError(
            ErrorCode.CONFIG_DRIFT,
            "fresh topic_config digest has not been reconciled",
        )
    entry = topic_config.topic(binding.thread_id)
    if entry is None:
        raise RuntimeFailureError(ErrorCode.CONFIG_DRIFT, "upstream topic key is absent")
    expected_engine = "claude" if provider == "claude_code" else provider
    if entry.engine != expected_engine:
        raise RuntimeFailureError(ErrorCode.UNKNOWN_BINDING, "binding is currently unmanaged")
    if entry.exec_mode != "tmux":
        raise RuntimeFailureError(
            ErrorCode.UNSUPPORTED_CODEX_SUBPROCESS,
            "Codex subprocess is unsupported in Runtime V1",
        )
    if entry.cwd is None or Path(entry.cwd).resolve() != Path(binding.canonical_cwd).resolve():
        raise RuntimeFailureError(ErrorCode.CONFIG_DRIFT, "topic cwd differs from Runtime Binding")
    return binding


def phase_b_verify(
    *,
    binding: RuntimeBinding,
    identity: WrapperIdentity,
    states: tuple[UpstreamTmuxState, ...],
    topic_config: TopicConfigSnapshot,
    replaced_session_id: str | None = None,
    provider: str = "codex",
) -> UpstreamTmuxState:
    if binding.config_digest != topic_config.digest:
        raise RuntimeFailureError(
            ErrorCode.CONFIG_DRIFT,
            "fresh topic_config digest has not been reconciled during Phase B",
        )
    candidates = tuple(
        state
        for state in states
        if state.chat_id == binding.chat_id
        and state.thread_id == binding.thread_id
        and state.session_name == identity.tmux_session
        and state.provider == ("claude" if provider == "claude_code" else provider)
        and Path(state.cwd).resolve() == Path(binding.canonical_cwd).resolve()
    )
    if len(candidates) != 1:
        raise RuntimeFailureError(
            ErrorCode.CORRELATION_MISMATCH,
            f"expected one upstream state entry, found {len(candidates)}",
        )
    if replaced_session_id is not None and candidates[0].session_id == replaced_session_id:
        raise RuntimeFailureError(
            ErrorCode.CORRELATION_MISMATCH,
            "upstream state still describes the session replaced by /new",
        )
    entry = topic_config.topic(binding.thread_id)
    expected_engine = "claude" if provider == "claude_code" else provider
    if entry is None or not entry.is_managed_candidate or entry.engine != expected_engine:
        raise RuntimeFailureError(ErrorCode.CONFIG_DRIFT, "topic stopped being provider+tmux")
    if entry.cwd is None or Path(entry.cwd).resolve() != Path(binding.canonical_cwd).resolve():
        raise RuntimeFailureError(ErrorCode.CONFIG_DRIFT, "topic cwd changed during startup")
    return candidates[0]
