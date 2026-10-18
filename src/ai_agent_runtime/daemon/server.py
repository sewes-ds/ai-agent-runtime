"""Local Runtime daemon and fail-closed launch authorization."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import socket
import stat
import struct
import subprocess
import time
import uuid
from collections import OrderedDict
from collections.abc import Callable
from pathlib import Path
from typing import Any

from ai_agent_runtime.config import RuntimeConfig
from ai_agent_runtime.domain import (
    DesiredSpec,
    LifecycleState,
    ManagementMode,
    ObservedSpecProposal,
    RolloutObservationCursor,
    RolloutObservationTarget,
    RuntimeBinding,
    UserNotificationCode,
    WrapperIdentity,
    sha256_digest,
)
from ai_agent_runtime.errors import ErrorCode, RuntimeFailureError
from ai_agent_runtime.ipc.protocol import read_frame, validate_version, versioned, write_frame
from ai_agent_runtime.migration.session_provenance import (
    inspect_rollout_session,
    validate_session_provenance_plan,
)
from ai_agent_runtime.process_safety import (
    file_sha256,
    proc_environment,
    proc_starttime,
    proc_uid,
    process_executable_evidence,
    profile_fingerprint,
    retire_tmux_session,
    validate_owned_executable,
    validate_profile_path,
    validate_project_path,
    verify_codex_child,
    verify_process_executable_evidence,
    verify_wrapper_identity,
)
from ai_agent_runtime.providers.codex.rollout import (
    RolloutContractError,
    RolloutSignalKind,
    scan_rollout,
)
from ai_agent_runtime.state.store import RuntimeStore
from ai_agent_runtime.systemd_notify import notify, watchdog_loop
from ai_agent_runtime.telegram_upstream.adapter import TelegramUpstreamAdapter
from ai_agent_runtime.telegram_upstream.correlation import (
    phase_a_correlate,
    phase_a_identity_candidates,
    phase_b_verify,
    replaced_start_state,
)
from ai_agent_runtime.telegram_upstream.session_view import (
    SessionViewError,
    project_session_transcripts,
)
from ai_agent_runtime.telegram_upstream.tmux_state import UpstreamTmuxState, read_tmux_states
from ai_agent_runtime.telegram_upstream.topic_config import (
    TopicConfigManagedConflictError,
    TopicConfigUnresolvedConflictError,
    read_topic_config,
    reconcile_managed_topic,
    reconcile_managed_topics,
)


def _verify_legacy_process_evidence(
    config: RuntimeConfig,
    live_process: dict[str, object],
    *,
    canonical_cwd: str,
    account_profile: str,
) -> None:
    """Revalidate the exact legacy Codex process immediately before import."""

    live_pid = _required_int(live_process, "pid")
    live_starttime = _required_int(live_process, "starttime")
    live_uid = _required_int(live_process, "uid")
    live_codex_home = _required_str(live_process, "codex_home")
    live_account = _required_str(live_process, "account_profile")
    if (
        isinstance(live_pid, bool)
        or isinstance(live_starttime, bool)
        or isinstance(live_uid, bool)
        or live_pid <= 0
        or live_starttime <= 0
        or live_uid != os.getuid()
        or live_account != account_profile
        or len(live_codex_home) > 4096
        or len(live_account) > 128
        or any(character in live_codex_home for character in ("\x00", "\n", "\r"))
        or any(character in live_account for character in ("\x00", "\n", "\r"))
    ):
        raise RuntimeFailureError(
            ErrorCode.INVALID_REQUEST,
            "session provenance live process identity is invalid",
        )
    try:
        if proc_uid(live_pid) != live_uid:
            raise ValueError("UID mismatch")
        if proc_starttime(live_pid) != live_starttime:
            raise ValueError("starttime mismatch")
        if Path(f"/proc/{live_pid}/cwd").resolve(strict=True) != Path(canonical_cwd).resolve(
            strict=True
        ):
            raise ValueError("cwd mismatch")
        profile = validate_profile_path(
            Path(live_codex_home),
            root=config.profiles_root,
            uid=live_uid,
        )
        if (
            profile.parent != config.profiles_root.resolve(strict=True)
            or profile.name != account_profile
            or proc_environment(live_pid).get("CODEX_HOME") != str(profile)
        ):
            raise ValueError("profile mismatch")
        verify_process_executable_evidence(
            live_pid,
            live_process.get("executable"),
            verify_digest=True,
        )
        if proc_starttime(live_pid) != live_starttime:
            raise ValueError("process identity changed during verification")
    except (OSError, RuntimeFailureError, UnicodeError, ValueError) as exc:
        raise RuntimeFailureError(
            ErrorCode.INVALID_REQUEST,
            "session provenance live account evidence changed after planning",
        ) from exc


_PEERCRED = struct.Struct("3i")
logger = logging.getLogger(__name__)


def _prepare_runtime_socket(path: Path) -> None:
    """Create a private socket directory and remove only an owned stale socket."""

    parent = path.parent
    parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if parent.is_symlink():
        raise ValueError("Runtime socket directory must not be a symlink")
    parent_metadata = parent.stat()
    if not stat.S_ISDIR(parent_metadata.st_mode) or parent_metadata.st_uid != os.getuid():
        raise ValueError("Runtime socket directory ownership/type is unsafe")
    if parent_metadata.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        raise ValueError("Runtime socket directory is group/world writable")
    os.chmod(parent, 0o700)
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return
    if stat.S_ISLNK(metadata.st_mode):
        raise ValueError("Runtime socket path must not be a symlink")
    if not stat.S_ISSOCK(metadata.st_mode) or metadata.st_uid != os.getuid():
        raise ValueError("refusing to replace non-socket or foreign Runtime socket path")
    path.unlink()


def _socket_identity(path: Path) -> tuple[int, int]:
    metadata = path.lstat()
    if not stat.S_ISSOCK(metadata.st_mode) or metadata.st_uid != os.getuid():
        raise ValueError("created Runtime socket ownership/type is unsafe")
    return metadata.st_dev, metadata.st_ino


def _unlink_runtime_socket(path: Path, identity: tuple[int, int]) -> None:
    """Remove the daemon's own socket only if the path still names that inode."""

    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return
    if (
        stat.S_ISSOCK(metadata.st_mode)
        and metadata.st_uid == os.getuid()
        and (metadata.st_dev, metadata.st_ino) == identity
    ):
        path.unlink()


class RuntimeServer:
    def __init__(
        self,
        config: RuntimeConfig,
        *,
        phase_b_timeout: float = 45.0,
        identity_verifier: Callable[[WrapperIdentity], None] = verify_wrapper_identity,
        child_verifier: Callable[..., dict[str, object]] = verify_codex_child,
        retirement_executor: Callable[[WrapperIdentity], None] = retire_tmux_session,
        evidence_interval: float = 1.0,
    ) -> None:
        self.config = config
        self.phase_b_timeout = phase_b_timeout
        self.store = RuntimeStore(config.database_path)
        self.boot_id = str(uuid.uuid4())
        self.identity_verifier = identity_verifier
        self.child_verifier = child_verifier
        self.retirement_executor = retirement_executor
        self.evidence_interval = evidence_interval
        self._results: OrderedDict[str, tuple[str, dict[str, Any]]] = OrderedDict()
        self._session_view_error: str | None = None
        self._rollout_observer_faults: set[str] = set()

    async def serve(self) -> None:
        self._refresh_effective_profile_attestations_once()
        self._project_session_view_once()
        _prepare_runtime_socket(self.config.socket_path)
        server = await asyncio.start_unix_server(self._handle, path=self.config.socket_path)
        os.chmod(self.config.socket_path, 0o600)
        socket_identity = _socket_identity(self.config.socket_path)
        watchdog = asyncio.create_task(watchdog_loop())
        reconciler = asyncio.create_task(self._reconcile_topic_config_loop())
        wrapper_reaper = asyncio.create_task(self._reap_absent_wrappers_loop())
        evidence_collector = asyncio.create_task(self._collect_applied_evidence_loop())
        rollout_observer = asyncio.create_task(self._observe_codex_rollouts_loop())
        session_view = asyncio.create_task(self._session_view_loop())
        retirement = (
            asyncio.create_task(self._retire_pending_loop())
            if self.config.retirement_enabled
            else None
        )
        notify("READY=1")
        try:
            async with server:
                await server.serve_forever()
        finally:
            notify("STOPPING=1")
            watchdog.cancel()
            reconciler.cancel()
            wrapper_reaper.cancel()
            evidence_collector.cancel()
            rollout_observer.cancel()
            session_view.cancel()
            if retirement is not None:
                retirement.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await watchdog
            with contextlib.suppress(asyncio.CancelledError):
                await reconciler
            with contextlib.suppress(asyncio.CancelledError):
                await wrapper_reaper
            with contextlib.suppress(asyncio.CancelledError):
                await evidence_collector
            with contextlib.suppress(asyncio.CancelledError):
                await rollout_observer
            with contextlib.suppress(asyncio.CancelledError):
                await session_view
            if retirement is not None:
                with contextlib.suppress(asyncio.CancelledError):
                    await retirement
            _unlink_runtime_socket(self.config.socket_path, socket_identity)
            self.store.close()

    def _refresh_effective_profile_attestations_once(self) -> None:
        """Migrate verified local profile identities to the stable v2 format."""

        for (
            binding_id,
            spec_digest,
            codex_home,
            previous,
        ) in self.store.effective_profile_attestations():
            safe_home = validate_profile_path(codex_home, root=self.config.profiles_root)
            current = profile_fingerprint(safe_home)
            if current == previous:
                continue
            if not previous.startswith("sha256:"):
                raise RuntimeFailureError(
                    ErrorCode.UNSAFE_PROFILE,
                    f"unsupported profile attestation for binding {binding_id}",
                )
            if not self.store.refresh_effective_profile_fingerprint(
                binding_id=binding_id,
                spec_digest=spec_digest,
                codex_home=safe_home,
                previous_fingerprint=previous,
                profile_fingerprint=current,
            ):
                raise RuntimeFailureError(
                    ErrorCode.AMBIGUOUS,
                    f"profile attestation changed concurrently for binding {binding_id}",
                )

    async def _handle(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
        request_id = "unknown"
        try:
            peer_pid, peer_uid = self._validate_peer(writer)
            payload = await read_frame(reader)
            validate_version(payload)
            request_id = _required_str(payload, "request_id")
            request_digest = sha256_digest(payload)
            cached_memory = self._results.get(request_id)
            if cached_memory is not None:
                cached_digest, cached_response = cached_memory
                if cached_digest != request_digest:
                    raise RuntimeFailureError(
                        ErrorCode.INVALID_REQUEST,
                        "request_id replayed with different payload",
                    )
                await write_frame(writer, cached_response)
                return
            cached_durable = self.store.get_request_result(request_id, request_digest)
            if cached_durable is not None:
                cached_response = dict(cached_durable)
                cached_response["daemon_boot_id"] = self.boot_id
                await write_frame(writer, cached_response)
                return
            action = _required_str(payload, "action")
            if action == "register":
                response = self._register(request_id, payload, peer_pid, peer_uid)
            elif action == "verify_started":
                response = await self._verify_started(request_id, payload, peer_pid, peer_uid)
            elif action == "child_exited":
                response = self._child_exited(request_id, payload, peer_pid, peer_uid)
            elif action == "heartbeat":
                response = self._heartbeat(request_id, payload, peer_pid, peer_uid)
            elif action == "control_plane_heartbeat":
                response = self._control_plane_heartbeat(request_id, payload)
            elif action == "status":
                response = self._status(request_id)
            elif action == "prepare_dispatch":
                response = self._prepare_dispatch(request_id, payload)
            elif action == "apply_binding":
                response = self._apply_binding(request_id, payload)
            elif action == "apply_snapshot":
                response = self._apply_snapshot(request_id, payload)
            elif action == "import_session_provenance":
                response = self._import_session_provenance(request_id, payload)
            elif action == "outbox_pull":
                limit = payload.get("limit", 50)
                if not isinstance(limit, int):
                    raise RuntimeFailureError(
                        ErrorCode.INVALID_REQUEST, "outbox limit must be an integer"
                    )
                response = self._ok(
                    request_id,
                    events=self.store.claim_outbox(limit=limit),
                )
            elif action == "outbox_ack":
                self.store.acknowledge_outbox(
                    _required_str(payload, "event_id"),
                    _required_str(payload, "ack_state"),
                )
                response = self._ok(request_id, acknowledged=True)
            else:
                raise RuntimeFailureError(ErrorCode.INVALID_REQUEST, f"unknown action: {action}")
            self._cache(
                request_id,
                request_digest,
                response,
                durable=action
                not in {
                    "heartbeat",
                    "control_plane_heartbeat",
                    "status",
                    "prepare_dispatch",
                    "outbox_pull",
                },
            )
            await write_frame(writer, response)
        except (ConnectionError, TimeoutError, asyncio.IncompleteReadError):
            # The peer may have timed out or disconnected while a response was
            # being prepared. There is no recipient for an error frame; trying
            # to write one only creates another broken-pipe exception.
            pass
        except RuntimeFailureError as exc:
            with contextlib.suppress(ConnectionError, TimeoutError):
                await write_frame(writer, self._error(request_id, exc.code, exc.detail))
        except (ValueError, KeyError, OSError) as exc:
            with contextlib.suppress(ConnectionError, TimeoutError):
                await write_frame(
                    writer,
                    self._error(request_id, ErrorCode.INVALID_REQUEST, str(exc)),
                )
        finally:
            writer.close()
            with contextlib.suppress(ConnectionError, OSError):
                await writer.wait_closed()

    def _validate_peer(self, writer: asyncio.StreamWriter) -> tuple[int, int]:
        transport_socket = writer.get_extra_info("socket")
        if transport_socket is None:
            raise RuntimeFailureError(ErrorCode.INVALID_REQUEST, "missing peer socket")
        raw = transport_socket.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, _PEERCRED.size)
        _pid, uid, _gid = _PEERCRED.unpack(raw)
        if uid != os.getuid():
            raise RuntimeFailureError(ErrorCode.INVALID_REQUEST, "Unix peer UID mismatch")
        return _pid, uid

    def _register(
        self,
        request_id: str,
        payload: dict[str, Any],
        peer_pid: int,
        peer_uid: int,
    ) -> dict[str, Any]:
        if payload.get("agent_instance_id") != self.config.agent_instance_id:
            raise RuntimeFailureError(ErrorCode.UNKNOWN_BINDING, "AgentInstance namespace mismatch")
        identity = _identity(payload)
        _verify_claimed_peer(identity, peer_pid=peer_pid, peer_uid=peer_uid)
        self.identity_verifier(identity)
        wrapper_executable = process_executable_evidence(identity.pid)
        config_snapshot = read_topic_config(self.config.topic_config_path)
        state_snapshot = read_tmux_states(self.config.tmux_state_path)
        bindings = self.store.list_bindings(self.config.agent_instance_id)
        resume_session_id = payload.get("resume_session_id")
        if resume_session_id is not None and (
            not isinstance(resume_session_id, str) or not resume_session_id
        ):
            raise RuntimeFailureError(ErrorCode.INVALID_REQUEST, "invalid resume session id")
        try:
            binding = phase_a_correlate(
                identity=identity,
                bindings=bindings,
                topic_config=config_snapshot,
                resume_session_id=resume_session_id,
                states=state_snapshot,
            )
        except RuntimeFailureError as exc:
            candidates = phase_a_identity_candidates(
                identity=identity,
                bindings=bindings,
                resume_session_id=resume_session_id,
                states=state_snapshot,
            )
            if exc.code is ErrorCode.UNSUPPORTED_CODEX_SUBPROCESS and len(candidates) == 1:
                self.store.enqueue_user_notification(
                    code=UserNotificationCode.UNSUPPORTED_CODEX_SUBPROCESS,
                    binding=candidates[0],
                    event_ref=config_snapshot.digest,
                )
            raise
        if not binding.configured:
            self.store.enqueue_user_notification(
                code=UserNotificationCode.ONBOARDING_REQUIRED,
                binding=binding,
                event_ref=config_snapshot.digest,
            )
            raise RuntimeFailureError(ErrorCode.UNKNOWN_BINDING, "binding is not configured")
        effective = self.store.get_launch_spec(binding.binding_id)
        if effective is None:
            raise RuntimeFailureError(
                ErrorCode.UNKNOWN_BINDING, "binding has no verified local spec"
            )
        spec, codex_home, profile_fingerprint = effective
        if spec.agent_instance_id != self.config.agent_instance_id:
            raise RuntimeFailureError(
                ErrorCode.UNKNOWN_BINDING, "effective spec namespace mismatch"
            )
        if resume_session_id is not None:
            owner = self.store.session_owner(resume_session_id)
            if owner != (binding.binding_id, spec.account_profile):
                self.store.enqueue_user_notification(
                    code=UserNotificationCode.RESUME_BINDING_MISMATCH,
                    binding=binding,
                    event_ref=resume_session_id,
                )
                raise RuntimeFailureError(
                    ErrorCode.RESUME_BINDING_MISMATCH,
                    "session does not belong to this binding and account",
                )
        real_codex = validate_owned_executable(self.config.real_codex)
        real_digest = file_sha256(real_codex)
        if real_digest != self.config.real_codex_sha256:
            raise RuntimeFailureError(
                ErrorCode.UNSAFE_BINARY,
                "real Codex digest differs from qualified release tuple",
            )
        safe_home = validate_profile_path(codex_home, root=self.config.profiles_root)
        try:
            self._project_session_view_once()
        except (OSError, ValueError, SessionViewError) as exc:
            self._session_view_error = f"{type(exc).__name__}: {exc}"
            raise RuntimeFailureError(
                ErrorCode.SESSION_VIEW_UNAVAILABLE,
                f"qualified upstream transcript view is unavailable: {exc}",
            ) from exc
        instance_id = str(uuid.uuid4())
        launch_context: dict[str, object] = {
            "wrapper_identity": {
                "pid": identity.pid,
                "starttime": identity.starttime,
                "uid": identity.uid,
                "executable": wrapper_executable,
            },
            "desired": {
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
            },
            "codex_home": str(safe_home),
            "profile_fingerprint": profile_fingerprint,
            "real_codex": str(real_codex),
            "real_codex_version": self.config.real_codex_version,
            "real_codex_sha256": real_digest,
        }
        if resume_session_id is None:
            previous = replaced_start_state(
                binding=binding,
                identity=identity,
                states=state_snapshot,
            )
            if previous is not None:
                launch_context["replaced_session_id"] = previous.session_id
        self.store.record_instance(
            instance_id=instance_id,
            binding_id=binding.binding_id,
            wrapper_pid=identity.pid,
            wrapper_starttime=identity.starttime,
            child_pid=None,
            child_starttime=None,
            tmux_session=identity.tmux_session,
            tmux_pane=identity.tmux_pane,
            lifecycle=LifecycleState.PROVISIONAL_STARTING,
            applied_digest=None,
            launch_spec=launch_context,
            daemon_boot_id=self.boot_id,
            connection_generation=0,
        )
        return self._ok(
            request_id,
            instance_id=instance_id,
            binding_id=binding.binding_id,
            spec={
                "generation_id": spec.generation_id,
                "revision": spec.revision,
                "spec_digest": spec.spec_digest,
                "account_profile": spec.account_profile,
                "model": spec.model,
                "reasoning_effort": spec.reasoning_effort,
                "codex_home": str(safe_home),
                "profile_fingerprint": profile_fingerprint,
                "real_codex": str(real_codex),
                "real_codex_version": self.config.real_codex_version,
                "real_codex_sha256": real_digest,
            },
        )

    def _apply_binding(self, request_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        raw_binding = payload.get("binding")
        raw_desired = payload.get("desired")
        if not isinstance(raw_binding, dict) or not isinstance(raw_desired, dict):
            raise RuntimeFailureError(
                ErrorCode.INVALID_REQUEST,
                "binding and desired objects are required",
            )
        agent_instance_id = _required_str(raw_binding, "agent_instance_id")
        if agent_instance_id != self.config.agent_instance_id:
            raise RuntimeFailureError(ErrorCode.UNKNOWN_BINDING, "AgentInstance mismatch")
        chat_id = _required_int(raw_binding, "chat_id")
        if chat_id != self.config.forum_chat_id:
            raise RuntimeFailureError(ErrorCode.UNKNOWN_BINDING, "forum chat mismatch")
        thread_id = _required_int(raw_binding, "thread_id")
        if thread_id <= 0:
            raise RuntimeFailureError(ErrorCode.INVALID_REQUEST, "thread_id must be positive")
        binding_id = _required_str(raw_binding, "binding_id")
        project_id = _required_str(raw_binding, "project_id")
        requested_cwd = validate_project_path(
            Path(_required_str(raw_binding, "canonical_cwd")),
            allowed_roots=self.config.allowed_project_roots,
        )
        if raw_desired.get("binding_id") != binding_id:
            raise RuntimeFailureError(ErrorCode.INVALID_REQUEST, "desired binding_id mismatch")
        spec = _desired_spec(raw_desired)
        if spec.agent_instance_id != agent_instance_id:
            raise RuntimeFailureError(ErrorCode.INVALID_REQUEST, "desired AgentInstance mismatch")
        if spec.project_id != project_id or Path(spec.canonical_cwd) != requested_cwd:
            raise RuntimeFailureError(
                ErrorCode.INVALID_REQUEST, "desired project identity mismatch"
            )
        if spec.provider != "codex":
            raise RuntimeFailureError(
                ErrorCode.INVALID_REQUEST, "V1 managed provider must be codex"
            )
        existing_binding = self.store.get_binding(binding_id)
        if existing_binding is not None:
            if (
                existing_binding.agent_instance_id != agent_instance_id
                or existing_binding.chat_id != chat_id
                or existing_binding.thread_id != thread_id
                or existing_binding.project_id != project_id
            ):
                raise RuntimeFailureError(
                    ErrorCode.INVALID_REQUEST,
                    "binding identity is immutable",
                )
            if Path(existing_binding.canonical_cwd) != requested_cwd:
                raise RuntimeFailureError(
                    ErrorCode.PROJECT_RELOCATION_REQUIRED,
                    "project path change requires explicit relocation",
                )
        for known in self.store.list_bindings(agent_instance_id):
            if (
                known.chat_id == chat_id
                and known.thread_id == thread_id
                and known.binding_id != binding_id
            ):
                raise RuntimeFailureError(
                    ErrorCode.INVALID_REQUEST,
                    "Telegram topic is already assigned to another binding",
                )
        try:
            self.store.validate_effective_spec_update(spec)
        except ValueError as exc:
            code = (
                ErrorCode.GENERATION_RECONCILE_REQUIRED
                if "generation" in str(exc)
                else ErrorCode.INVALID_REQUEST
            )
            raise RuntimeFailureError(code, str(exc)) from exc
        profile_name = spec.account_profile
        if (
            not profile_name
            or profile_name in {".", ".."}
            or Path(profile_name).name != profile_name
        ):
            raise RuntimeFailureError(ErrorCode.UNSAFE_PROFILE, "invalid account profile name")
        codex_home = validate_profile_path(
            self.config.profiles_root / profile_name,
            root=self.config.profiles_root,
        )
        fingerprint = profile_fingerprint(codex_home)
        adapter = TelegramUpstreamAdapter(
            self.config.upstream_sha,
            self.config.tmux_session_prefix,
        )
        expected_session = adapter.expected_session_name(
            chat_id=chat_id,
            thread_id=thread_id,
        )
        snapshot = None
        for _ in range(3):
            before = read_topic_config(self.config.topic_config_path)
            current_entry = before.topic(thread_id)
            if existing_binding is not None and (
                current_entry is None or not current_entry.is_managed_candidate
            ):
                raise RuntimeFailureError(
                    ErrorCode.CONFIG_DRIFT,
                    "existing binding left codex+tmux; CUS will not restore it implicitly",
                )
            try:
                snapshot = reconcile_managed_topic(
                    self.config.topic_config_path,
                    thread_id=thread_id,
                    cwd=str(requested_cwd),
                    expected_digest=before.digest,
                    backup_dir=self.config.state_dir / "topic-config-backups",
                )
                break
            except KeyError as exc:
                raise RuntimeFailureError(ErrorCode.CONFIG_DRIFT, str(exc)) from exc
            except (TopicConfigManagedConflictError, TopicConfigUnresolvedConflictError) as exc:
                raise RuntimeFailureError(ErrorCode.CONFIG_DRIFT, str(exc)) from exc
            except RuntimeError:
                continue
        if snapshot is None:
            raise RuntimeFailureError(
                ErrorCode.CONFIG_DRIFT,
                "topic_config changed during all reconciliation attempts",
            )
        binding = RuntimeBinding(
            binding_id=binding_id,
            agent_instance_id=agent_instance_id,
            chat_id=chat_id,
            thread_id=thread_id,
            project_id=project_id,
            canonical_cwd=str(requested_cwd),
            expected_tmux_session=expected_session,
            configured=True,
            config_digest=snapshot.digest,
            management_mode=ManagementMode.MANAGED,
        )
        self.store.upsert_binding(binding)
        self.store.put_effective_spec(
            spec,
            codex_home=codex_home,
            profile_fingerprint=fingerprint,
        )
        self.store.mark_control_plane_sync(agent_instance_id)
        return self._ok(
            request_id,
            binding_id=binding_id,
            config_digest=snapshot.digest,
            local_effective={
                "generation_id": spec.generation_id,
                "revision": spec.revision,
                "spec_digest": spec.spec_digest,
                "profile_fingerprint": fingerprint,
            },
        )

    def _apply_snapshot(self, request_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        agent_instance_id = _required_str(payload, "agent_instance_id")
        if agent_instance_id != self.config.agent_instance_id:
            raise RuntimeFailureError(ErrorCode.UNKNOWN_BINDING, "AgentInstance mismatch")
        generation_id = _required_str(payload, "generation_id")
        snapshot_digest = _required_str(payload, "snapshot_digest")
        raw_items = payload.get("bindings")
        unmanaged_present = "unmanaged_bindings" in payload
        raw_unmanaged = payload.get("unmanaged_bindings", [])
        raw_retired = payload.get("retired_binding_ids", [])
        relocations_present = "project_relocations" in payload
        raw_relocations = payload.get("project_relocations", [])
        if (
            not isinstance(raw_items, list)
            or not isinstance(raw_unmanaged, list)
            or not isinstance(raw_retired, list)
            or not isinstance(raw_relocations, list)
        ):
            raise RuntimeFailureError(
                ErrorCode.INVALID_REQUEST,
                "snapshot binding, retirement and relocation fields must be arrays",
            )
        if (
            len(raw_items) + len(raw_unmanaged) > 1000
            or len(raw_retired) > 1000
            or len(raw_relocations) > 1000
        ):
            raise RuntimeFailureError(ErrorCode.INVALID_REQUEST, "snapshot is too large")
        if any(not isinstance(value, str) or not value for value in raw_retired):
            raise RuntimeFailureError(ErrorCode.INVALID_REQUEST, "invalid retired binding id")
        retired = frozenset(raw_retired)
        if len(retired) != len(raw_retired):
            raise RuntimeFailureError(ErrorCode.INVALID_REQUEST, "duplicate retired binding id")
        relocations = self._validate_project_relocations(raw_relocations)
        relocations_by_project = {
            str(relocation["project_id"]): relocation for relocation in relocations
        }
        normalized_items: list[dict[str, Any]] = []
        normalized_unmanaged: list[dict[str, Any]] = []
        prepared: list[tuple[RuntimeBinding, DesiredSpec, Path, str]] = []
        managed_topics: dict[int, str] = {}
        binding_ids: set[str] = set()
        tmux_sessions: set[str] = set()
        for raw_item in raw_items:
            if not isinstance(raw_item, dict):
                raise RuntimeFailureError(ErrorCode.INVALID_REQUEST, "snapshot item must be object")
            raw_binding = raw_item.get("binding")
            raw_desired = raw_item.get("desired")
            if not isinstance(raw_binding, dict) or not isinstance(raw_desired, dict):
                raise RuntimeFailureError(
                    ErrorCode.INVALID_REQUEST,
                    "snapshot item requires binding and desired objects",
                )
            binding, spec, codex_home, fingerprint = self._validate_snapshot_item(
                raw_binding,
                raw_desired,
                generation_id=generation_id,
                relocations_by_project=relocations_by_project,
            )
            if binding.thread_id in managed_topics:
                raise RuntimeFailureError(
                    ErrorCode.INVALID_REQUEST,
                    "snapshot contains duplicate Telegram topic",
                )
            if binding.binding_id in binding_ids or binding.expected_tmux_session in tmux_sessions:
                raise RuntimeFailureError(
                    ErrorCode.INVALID_REQUEST,
                    "snapshot contains duplicate binding or tmux identity",
                )
            binding_ids.add(binding.binding_id)
            tmux_sessions.add(binding.expected_tmux_session)
            managed_topics[binding.thread_id] = binding.canonical_cwd
            prepared.append((binding, spec, codex_home, fingerprint))
            normalized_items.append({"binding": raw_binding, "desired": raw_desired})

        for raw_item in raw_unmanaged:
            if not isinstance(raw_item, dict):
                raise RuntimeFailureError(
                    ErrorCode.INVALID_REQUEST,
                    "unmanaged snapshot item must be object",
                )
            raw_binding = raw_item.get("binding")
            raw_desired = raw_item.get("desired")
            if not isinstance(raw_binding, dict) or not isinstance(raw_desired, dict):
                raise RuntimeFailureError(
                    ErrorCode.INVALID_REQUEST,
                    "unmanaged snapshot item requires binding and desired objects",
                )
            binding, spec, codex_home, fingerprint = self._validate_snapshot_item(
                raw_binding,
                raw_desired,
                generation_id=generation_id,
                relocations_by_project=relocations_by_project,
            )
            if binding.binding_id in binding_ids or binding.expected_tmux_session in tmux_sessions:
                raise RuntimeFailureError(
                    ErrorCode.INVALID_REQUEST,
                    "snapshot contains duplicate binding or tmux identity",
                )
            if binding.thread_id in managed_topics:
                raise RuntimeFailureError(
                    ErrorCode.INVALID_REQUEST,
                    "snapshot contains duplicate Telegram topic",
                )
            binding_ids.add(binding.binding_id)
            tmux_sessions.add(binding.expected_tmux_session)
            prepared.append(
                (
                    RuntimeBinding(
                        binding_id=binding.binding_id,
                        agent_instance_id=binding.agent_instance_id,
                        chat_id=binding.chat_id,
                        thread_id=binding.thread_id,
                        project_id=binding.project_id,
                        canonical_cwd=binding.canonical_cwd,
                        expected_tmux_session=binding.expected_tmux_session,
                        configured=True,
                        management_mode=ManagementMode.UNMANAGED,
                    ),
                    spec,
                    codex_home,
                    fingerprint,
                )
            )
            normalized_unmanaged.append({"binding": raw_binding, "desired": raw_desired})

        prepared_by_id = {
            binding.binding_id: (binding, spec) for binding, spec, _home, _fingerprint in prepared
        }
        for relocation in relocations:
            relocation_binding_ids = relocation["binding_ids"]
            assert isinstance(relocation_binding_ids, list)
            new_cwd = str(relocation["new_canonical_cwd"])
            for binding_id in relocation_binding_ids:
                relocated = prepared_by_id.get(binding_id)
                if relocated is None or (
                    relocated[0].canonical_cwd != new_cwd or relocated[1].canonical_cwd != new_cwd
                ):
                    raise RuntimeFailureError(
                        ErrorCode.INVALID_REQUEST,
                        "project relocation snapshot does not apply the new path",
                    )

        digest_material = {
            "agent_instance_id": agent_instance_id,
            "generation_id": generation_id,
            "bindings": sorted(
                normalized_items,
                key=lambda item: str(item["binding"].get("binding_id", "")),
            ),
            "retired_binding_ids": sorted(retired),
        }
        if unmanaged_present:
            digest_material["unmanaged_bindings"] = sorted(
                normalized_unmanaged,
                key=lambda item: str(item["binding"].get("binding_id", "")),
            )
        if relocations_present:
            digest_material["project_relocations"] = sorted(
                relocations,
                key=lambda item: str(item["relocation_id"]),
            )
        if snapshot_digest != sha256_digest(digest_material):
            raise RuntimeFailureError(ErrorCode.INVALID_REQUEST, "snapshot digest mismatch")

        known_ids = {
            binding.binding_id
            for binding in self.store.list_bindings(self.config.agent_instance_id)
        }
        active_ids = {binding.binding_id for binding, _spec, _home, _fp in prepared}
        if retired - known_ids:
            raise RuntimeFailureError(
                ErrorCode.INVALID_REQUEST,
                "snapshot retires an unknown binding",
            )
        if active_ids & retired:
            raise RuntimeFailureError(
                ErrorCode.INVALID_REQUEST,
                "binding cannot be active and retired in the same snapshot",
            )
        if known_ids - active_ids - retired:
            raise RuntimeFailureError(
                ErrorCode.INVALID_REQUEST,
                "snapshot omits existing bindings without explicit retirement",
            )
        for known in self.store.list_bindings(self.config.agent_instance_id):
            collision = next(
                (
                    binding
                    for binding, _spec, _home, _fp in prepared
                    if binding.binding_id != known.binding_id
                    and (
                        binding.thread_id == known.thread_id
                        or binding.expected_tmux_session == known.expected_tmux_session
                    )
                ),
                None,
            )
            if collision is not None:
                raise RuntimeFailureError(
                    ErrorCode.INVALID_REQUEST,
                    "snapshot reuses an existing topic or tmux identity",
                )
        if self.store.namespace_generation(agent_instance_id) == generation_id:
            try:
                for _binding, spec, _home, _fingerprint in prepared:
                    self.store.validate_effective_spec_update(spec)
            except ValueError as exc:
                raise RuntimeFailureError(ErrorCode.INVALID_REQUEST, str(exc)) from exc

        snapshot = read_topic_config(self.config.topic_config_path)
        for binding, _spec, _home, _fingerprint in prepared:
            if binding.management_mode != ManagementMode.UNMANAGED:
                continue
            entry = snapshot.topic(binding.thread_id)
            if entry is None:
                raise RuntimeFailureError(
                    ErrorCode.CONFIG_DRIFT,
                    "unmanaged binding topic is absent",
                )
            if entry.engine == "codex":
                raise RuntimeFailureError(
                    ErrorCode.INVALID_REQUEST,
                    "codex topic cannot be imported as unmanaged",
                )
            if (
                entry.cwd is None
                or Path(entry.cwd).resolve() != Path(binding.canonical_cwd).resolve()
            ):
                raise RuntimeFailureError(
                    ErrorCode.CONFIG_DRIFT,
                    "unmanaged binding cwd differs from topic config",
                )
        if managed_topics:
            for _ in range(3):
                for binding, _spec, _home, _fingerprint in prepared:
                    existing = self.store.get_binding(binding.binding_id)
                    if existing is None or binding.management_mode == ManagementMode.UNMANAGED:
                        continue
                    entry = snapshot.topic(binding.thread_id)
                    if entry is None or not entry.is_managed_candidate:
                        raise RuntimeFailureError(
                            ErrorCode.CONFIG_DRIFT,
                            "existing binding left codex+tmux; "
                            "CUS snapshot will not restore it implicitly",
                        )
                try:
                    snapshot = reconcile_managed_topics(
                        self.config.topic_config_path,
                        managed_topics=managed_topics,
                        expected_digest=snapshot.digest,
                        backup_dir=self.config.state_dir / "topic-config-backups",
                    )
                    break
                except KeyError as exc:
                    raise RuntimeFailureError(ErrorCode.CONFIG_DRIFT, str(exc)) from exc
                except (TopicConfigManagedConflictError, TopicConfigUnresolvedConflictError) as exc:
                    raise RuntimeFailureError(ErrorCode.CONFIG_DRIFT, str(exc)) from exc
                except RuntimeError:
                    snapshot = read_topic_config(self.config.topic_config_path)
            else:
                raise RuntimeFailureError(
                    ErrorCode.CONFIG_DRIFT,
                    "topic_config changed during all snapshot reconciliation attempts",
                )
        finalized = tuple(
            (
                RuntimeBinding(
                    binding_id=binding.binding_id,
                    agent_instance_id=binding.agent_instance_id,
                    chat_id=binding.chat_id,
                    thread_id=binding.thread_id,
                    project_id=binding.project_id,
                    canonical_cwd=binding.canonical_cwd,
                    expected_tmux_session=binding.expected_tmux_session,
                    configured=True,
                    config_digest=snapshot.digest,
                    management_mode=binding.management_mode,
                ),
                spec,
                codex_home,
                fingerprint,
            )
            for binding, spec, codex_home, fingerprint in prepared
        )
        try:
            self.store.apply_authoritative_snapshot(
                agent_instance_id=agent_instance_id,
                generation_id=generation_id,
                snapshot_digest=snapshot_digest,
                updates=finalized,
                retired_binding_ids=retired,
                project_relocations=tuple(relocations),
            )
        except ValueError as exc:
            raise RuntimeFailureError(ErrorCode.INVALID_REQUEST, str(exc)) from exc
        self.store.mark_control_plane_sync(agent_instance_id)
        return self._ok(
            request_id,
            agent_instance_id=agent_instance_id,
            generation_id=generation_id,
            snapshot_digest=snapshot_digest,
            applied_bindings=sorted(
                binding.binding_id
                for binding, _spec, _home, _fingerprint in finalized
                if binding.management_mode == ManagementMode.MANAGED
            ),
            unmanaged_bindings=sorted(
                binding.binding_id
                for binding, _spec, _home, _fingerprint in finalized
                if binding.management_mode == ManagementMode.UNMANAGED
            ),
            retired_bindings=sorted(retired),
            config_digest=snapshot.digest,
            project_relocations=sorted(
                str(relocation["relocation_id"]) for relocation in relocations
            ),
        )

    def _import_session_provenance(
        self,
        request_id: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        plan = validate_session_provenance_plan(payload.get("plan"))
        agent_instance_id = _required_str(plan, "agent_instance_id")
        if agent_instance_id != self.config.agent_instance_id:
            raise RuntimeFailureError(ErrorCode.UNKNOWN_BINDING, "AgentInstance mismatch")
        raw_records = plan.get("records")
        if not isinstance(raw_records, list) or len(raw_records) > 1000:
            raise RuntimeFailureError(
                ErrorCode.INVALID_REQUEST,
                "session provenance records must be a bounded array",
            )

        normalized: list[dict[str, object]] = []
        session_ids: set[str] = set()
        for raw_record in raw_records:
            if not isinstance(raw_record, dict):
                raise RuntimeFailureError(
                    ErrorCode.INVALID_REQUEST,
                    "session provenance record must be an object",
                )
            if set(raw_record) != {
                "agent_instance_id",
                "source_ref",
                "binding_id",
                "account_profile",
                "canonical_cwd",
                "session_id",
                "live_process",
                "rollout_path",
                "rollout_device",
                "rollout_inode",
                "rollout_size",
            }:
                raise RuntimeFailureError(
                    ErrorCode.INVALID_REQUEST,
                    "session provenance record fields mismatch",
                )
            record = dict(raw_record)
            record_agent = _required_str(record, "agent_instance_id")
            binding_id = _required_str(record, "binding_id")
            source_ref = _required_str(record, "source_ref")
            account_profile = _required_str(record, "account_profile")
            canonical_cwd = _required_str(record, "canonical_cwd")
            session_id = _required_str(record, "session_id")
            rollout_path = _required_str(record, "rollout_path")
            live_process = record.get("live_process")
            if not isinstance(live_process, dict) or set(live_process) != {
                "pid",
                "starttime",
                "uid",
                "codex_home",
                "account_profile",
                "executable",
            }:
                raise RuntimeFailureError(
                    ErrorCode.INVALID_REQUEST,
                    "session provenance live process evidence is invalid",
                )
            if any(
                len(value) > maximum or "\x00" in value or "\n" in value or "\r" in value
                for value, maximum in (
                    (record_agent, 64),
                    (source_ref, 1024),
                    (binding_id, 128),
                    (account_profile, 128),
                    (canonical_cwd, 4096),
                    (session_id, 512),
                    (rollout_path, 4096),
                )
            ):
                raise RuntimeFailureError(
                    ErrorCode.INVALID_REQUEST,
                    "session provenance record string is invalid",
                )
            if any(
                isinstance(record[field], bool)
                for field in ("rollout_device", "rollout_inode", "rollout_size")
            ):
                raise RuntimeFailureError(
                    ErrorCode.INVALID_REQUEST,
                    "session provenance rollout identity is invalid",
                )
            rollout_device = _required_int(record, "rollout_device")
            rollout_inode = _required_int(record, "rollout_inode")
            rollout_size = _required_int(record, "rollout_size")
            if (
                record_agent != agent_instance_id
                or rollout_device < 0
                or rollout_inode <= 0
                or rollout_size < 0
                or session_id in session_ids
            ):
                raise RuntimeFailureError(
                    ErrorCode.INVALID_REQUEST,
                    "session provenance record identity is invalid",
                )
            session_ids.add(session_id)
            binding = self.store.get_binding(binding_id)
            if binding is None or not binding.configured:
                raise RuntimeFailureError(
                    ErrorCode.UNKNOWN_BINDING,
                    "session provenance binding is not configured",
                )
            expected_source_ref = (
                f"{binding.agent_instance_id}/{binding.chat_id}/{binding.thread_id}"
            )
            if (
                binding.agent_instance_id != agent_instance_id
                or source_ref != expected_source_ref
                or canonical_cwd != binding.canonical_cwd
            ):
                raise RuntimeFailureError(
                    ErrorCode.INVALID_REQUEST,
                    "session provenance binding identity mismatch",
                )
            effective = self.store.get_effective_spec(binding_id)
            if effective is None or effective[0].account_profile != account_profile:
                raise RuntimeFailureError(
                    ErrorCode.INVALID_REQUEST,
                    "session provenance account does not match effective spec",
                )
            _verify_legacy_process_evidence(
                self.config,
                live_process,
                canonical_cwd=canonical_cwd,
                account_profile=account_profile,
            )
            observed = inspect_rollout_session(
                Path(rollout_path),
                expected_session_id=session_id,
                expected_cwd=canonical_cwd,
            )
            if observed != {
                "rollout_path": rollout_path,
                "rollout_device": rollout_device,
                "rollout_inode": rollout_inode,
                "rollout_size": rollout_size,
            }:
                raise RuntimeFailureError(
                    ErrorCode.INVALID_REQUEST,
                    "session provenance rollout evidence changed after planning",
                )
            normalized.append(record)

        imported = self.store.import_session_provenance(tuple(normalized))
        return self._ok(
            request_id,
            agent_instance_id=agent_instance_id,
            plan_digest=plan["plan_digest"],
            imported=imported,
            total=len(normalized),
        )

    def _validate_project_relocations(
        self,
        raw_relocations: list[Any],
    ) -> list[dict[str, object]]:
        known_bindings = self.store.list_bindings(self.config.agent_instance_id)
        relocations: list[dict[str, object]] = []
        relocation_ids: set[str] = set()
        project_ids: set[str] = set()
        for raw in raw_relocations:
            if not isinstance(raw, dict):
                raise RuntimeFailureError(
                    ErrorCode.INVALID_REQUEST,
                    "project relocation must be an object",
                )
            if set(raw) != {
                "relocation_id",
                "agent_instance_id",
                "project_id",
                "old_canonical_cwd",
                "new_canonical_cwd",
                "binding_ids",
            }:
                raise RuntimeFailureError(
                    ErrorCode.INVALID_REQUEST,
                    "project relocation fields mismatch",
                )
            relocation_id = _required_str(raw, "relocation_id")
            agent_instance_id = _required_str(raw, "agent_instance_id")
            project_id = _required_str(raw, "project_id")
            old_cwd = _required_str(raw, "old_canonical_cwd")
            old_path = Path(old_cwd)
            if not old_path.is_absolute() or ".." in old_path.parts or old_path == Path("/"):
                raise RuntimeFailureError(
                    ErrorCode.INVALID_REQUEST,
                    "project relocation old path is unsafe",
                )
            new_path = validate_project_path(
                Path(_required_str(raw, "new_canonical_cwd")),
                allowed_roots=self.config.allowed_project_roots,
            )
            if agent_instance_id != self.config.agent_instance_id:
                raise RuntimeFailureError(ErrorCode.UNKNOWN_BINDING, "AgentInstance mismatch")
            if str(new_path) == old_cwd:
                raise RuntimeFailureError(
                    ErrorCode.INVALID_REQUEST,
                    "project relocation must change the canonical path",
                )
            raw_binding_ids = raw.get("binding_ids")
            if (
                not isinstance(raw_binding_ids, list)
                or not raw_binding_ids
                or any(not isinstance(value, str) or not value for value in raw_binding_ids)
            ):
                raise RuntimeFailureError(
                    ErrorCode.INVALID_REQUEST,
                    "project relocation requires binding ids",
                )
            binding_ids = frozenset(raw_binding_ids)
            if len(binding_ids) != len(raw_binding_ids):
                raise RuntimeFailureError(
                    ErrorCode.INVALID_REQUEST,
                    "project relocation contains duplicate binding ids",
                )
            if relocation_id in relocation_ids or project_id in project_ids:
                raise RuntimeFailureError(
                    ErrorCode.INVALID_REQUEST,
                    "snapshot contains duplicate project relocation",
                )
            relocation_ids.add(relocation_id)
            project_ids.add(project_id)
            project_bindings = tuple(
                binding
                for binding in known_bindings
                if binding.project_id == project_id and binding.configured
            )
            expected_ids = frozenset(binding.binding_id for binding in project_bindings)
            if not project_bindings or binding_ids != expected_ids:
                raise RuntimeFailureError(
                    ErrorCode.INVALID_REQUEST,
                    "project relocation must include every configured project binding",
                )
            if any(
                binding.management_mode != ManagementMode.MANAGED for binding in project_bindings
            ):
                raise RuntimeFailureError(
                    ErrorCode.INVALID_REQUEST,
                    "project relocation requires all bindings to be managed",
                )
            normalized: dict[str, object] = {
                "relocation_id": relocation_id,
                "agent_instance_id": agent_instance_id,
                "project_id": project_id,
                "old_canonical_cwd": old_cwd,
                "new_canonical_cwd": str(new_path),
                "binding_ids": sorted(binding_ids),
            }
            existing_relocation = self.store.get_project_relocation(relocation_id)
            current_paths = {binding.canonical_cwd for binding in project_bindings}
            if existing_relocation is not None:
                if existing_relocation != normalized or current_paths != {str(new_path)}:
                    raise RuntimeFailureError(
                        ErrorCode.INVALID_REQUEST,
                        "project relocation id was reused with different state",
                    )
            else:
                if current_paths != {old_cwd}:
                    raise RuntimeFailureError(
                        ErrorCode.PROJECT_RELOCATION_REQUIRED,
                        "project relocation old path does not match every binding",
                    )
                live = self.store.live_binding_ids(binding_ids)
                if live:
                    raise RuntimeFailureError(
                        ErrorCode.PROJECT_RELOCATION_REQUIRED,
                        "project relocation requires all bindings to be stopped",
                    )
            if any(
                binding.configured
                and binding.project_id != project_id
                and binding.canonical_cwd == str(new_path)
                for binding in known_bindings
            ):
                raise RuntimeFailureError(
                    ErrorCode.INVALID_REQUEST,
                    "project relocation target belongs to another project",
                )
            relocations.append(normalized)
        return relocations

    def _validate_snapshot_item(
        self,
        raw_binding: dict[str, Any],
        raw_desired: dict[str, Any],
        *,
        generation_id: str,
        relocations_by_project: dict[str, dict[str, object]],
    ) -> tuple[RuntimeBinding, DesiredSpec, Path, str]:
        agent_instance_id = _required_str(raw_binding, "agent_instance_id")
        if agent_instance_id != self.config.agent_instance_id:
            raise RuntimeFailureError(ErrorCode.UNKNOWN_BINDING, "AgentInstance mismatch")
        chat_id = _required_int(raw_binding, "chat_id")
        if chat_id != self.config.forum_chat_id:
            raise RuntimeFailureError(ErrorCode.UNKNOWN_BINDING, "forum chat mismatch")
        thread_id = _required_int(raw_binding, "thread_id")
        if thread_id <= 0:
            raise RuntimeFailureError(ErrorCode.INVALID_REQUEST, "thread_id must be positive")
        binding_id = _required_str(raw_binding, "binding_id")
        project_id = _required_str(raw_binding, "project_id")
        requested_cwd = validate_project_path(
            Path(_required_str(raw_binding, "canonical_cwd")),
            allowed_roots=self.config.allowed_project_roots,
        )
        spec = _desired_spec(raw_desired)
        if (
            spec.binding_id != binding_id
            or spec.agent_instance_id != agent_instance_id
            or spec.project_id != project_id
            or Path(spec.canonical_cwd) != requested_cwd
        ):
            raise RuntimeFailureError(ErrorCode.INVALID_REQUEST, "desired identity mismatch")
        if spec.generation_id != generation_id:
            raise RuntimeFailureError(ErrorCode.INVALID_REQUEST, "desired generation mismatch")
        if spec.provider != "codex":
            raise RuntimeFailureError(
                ErrorCode.INVALID_REQUEST,
                "V1 managed provider must be codex",
            )
        existing = self.store.get_binding(binding_id)
        if existing is not None and (
            existing.agent_instance_id != agent_instance_id
            or existing.chat_id != chat_id
            or existing.thread_id != thread_id
            or existing.project_id != project_id
        ):
            raise RuntimeFailureError(ErrorCode.INVALID_REQUEST, "binding identity is immutable")
        if existing is not None and Path(existing.canonical_cwd) != requested_cwd:
            relocation = relocations_by_project.get(project_id)
            binding_ids = relocation.get("binding_ids") if relocation is not None else None
            if (
                relocation is None
                or relocation.get("old_canonical_cwd") != existing.canonical_cwd
                or relocation.get("new_canonical_cwd") != str(requested_cwd)
                or not isinstance(binding_ids, list)
                or binding_id not in binding_ids
            ):
                raise RuntimeFailureError(
                    ErrorCode.PROJECT_RELOCATION_REQUIRED,
                    "project path change requires explicit relocation",
                )
        profile_name = spec.account_profile
        if (
            not profile_name
            or profile_name in {".", ".."}
            or Path(profile_name).name != profile_name
        ):
            raise RuntimeFailureError(ErrorCode.UNSAFE_PROFILE, "invalid account profile name")
        codex_home = validate_profile_path(
            self.config.profiles_root / profile_name,
            root=self.config.profiles_root,
        )
        adapter = TelegramUpstreamAdapter(
            self.config.upstream_sha,
            self.config.tmux_session_prefix,
        )
        return (
            RuntimeBinding(
                binding_id=binding_id,
                agent_instance_id=agent_instance_id,
                chat_id=chat_id,
                thread_id=thread_id,
                project_id=project_id,
                canonical_cwd=str(requested_cwd),
                expected_tmux_session=adapter.expected_session_name(
                    chat_id=chat_id,
                    thread_id=thread_id,
                ),
            ),
            spec,
            codex_home,
            profile_fingerprint(codex_home),
        )

    async def _verify_started(
        self,
        request_id: str,
        payload: dict[str, Any],
        peer_pid: int,
        peer_uid: int,
    ) -> dict[str, Any]:
        instance_id = _required_str(payload, "instance_id")
        binding_id = _required_str(payload, "binding_id")
        binding = self.store.get_binding(binding_id)
        if binding is None:
            raise RuntimeFailureError(ErrorCode.UNKNOWN_BINDING, "binding disappeared")
        identity = _identity(payload)
        _verify_claimed_peer(identity, peer_pid=peer_pid, peer_uid=peer_uid)
        self.identity_verifier(identity)
        child_pid = _required_int(payload, "child_pid")
        child_starttime = _required_int(payload, "child_starttime")
        registered = self.store.get_registered_instance(instance_id)
        if registered is None:
            raise RuntimeFailureError(ErrorCode.UNKNOWN_BINDING, "instance disappeared")
        if (
            registered.binding_id != binding_id
            or registered.wrapper_pid != identity.pid
            or registered.wrapper_starttime != identity.starttime
            or registered.tmux_session != identity.tmux_session
            or registered.tmux_pane != identity.tmux_pane
        ):
            raise RuntimeFailureError(
                ErrorCode.CORRELATION_MISMATCH,
                "started child does not match registered provisional instance",
            )
        launch = registered.launch_spec
        verify_process_executable_evidence(
            identity.pid,
            _wrapper_executable_from_launch(launch),
            verify_digest=True,
        )
        raw_desired = launch.get("desired")
        if not isinstance(raw_desired, dict):
            raise RuntimeFailureError(ErrorCode.INVALID_REQUEST, "instance desired spec is invalid")
        spec = _desired_spec(raw_desired)
        replaced_session_id = launch.get("replaced_session_id")
        if replaced_session_id is not None and not isinstance(replaced_session_id, str):
            raise RuntimeFailureError(ErrorCode.INVALID_REQUEST, "replaced session id is invalid")
        codex_home = Path(_required_str(launch, "codex_home"))
        real_codex = Path(_required_str(launch, "real_codex"))
        self.child_verifier(
            child_pid=child_pid,
            child_starttime=child_starttime,
            wrapper_pid=identity.pid,
            codex_home=codex_home,
            model=spec.model,
            reasoning_effort=spec.reasoning_effort,
            real_codex=real_codex,
        )
        deadline = asyncio.get_running_loop().time() + self.phase_b_timeout
        last_failure: RuntimeFailureError | None = None
        while asyncio.get_running_loop().time() < deadline:
            try:
                state = phase_b_verify(
                    binding=binding,
                    identity=identity,
                    states=read_tmux_states(self.config.tmux_state_path),
                    topic_config=read_topic_config(self.config.topic_config_path),
                    replaced_session_id=replaced_session_id,
                )
                break
            except RuntimeFailureError as exc:
                if exc.code is ErrorCode.CONFIG_DRIFT:
                    raise
                last_failure = exc
                await asyncio.sleep(0.2)
        else:
            detail = str(last_failure) if last_failure else "Phase B timed out"
            raise RuntimeFailureError(ErrorCode.CORRELATION_MISMATCH, detail)
        self.store.record_instance(
            instance_id=instance_id,
            binding_id=binding_id,
            wrapper_pid=identity.pid,
            wrapper_starttime=identity.starttime,
            child_pid=child_pid,
            child_starttime=child_starttime,
            tmux_session=identity.tmux_session,
            tmux_pane=identity.tmux_pane,
            lifecycle=LifecycleState.IDLE,
            # Phase B proves binding/process correlation, but a new Codex
            # session's rollout identity is not necessarily durable yet.
            # AppliedSpec remains unset until rollout evidence is collected.
            applied_digest=None,
            daemon_boot_id=self.boot_id,
            connection_generation=registered.connection_generation,
            heartbeat_at=time.time(),
        )
        return self._ok(
            request_id,
            verified=True,
            applied=False,
            session_id=state.session_id,
            expected_spec_digest=spec.spec_digest,
        )

    def _heartbeat(
        self,
        request_id: str,
        payload: dict[str, Any],
        peer_pid: int,
        peer_uid: int,
    ) -> dict[str, Any]:
        if payload.get("agent_instance_id") != self.config.agent_instance_id:
            raise RuntimeFailureError(ErrorCode.UNKNOWN_BINDING, "AgentInstance namespace mismatch")
        connection_generation = _required_int(payload, "connection_generation")
        if connection_generation < 1:
            raise RuntimeFailureError(
                ErrorCode.INVALID_REQUEST,
                "connection_generation must be positive",
            )
        previous_boot_id = payload.get("previous_daemon_boot_id")
        if previous_boot_id is not None and (
            not isinstance(previous_boot_id, str) or not previous_boot_id
        ):
            raise RuntimeFailureError(
                ErrorCode.INVALID_REQUEST,
                "previous_daemon_boot_id must be null or non-empty string",
            )
        identity = _identity(payload)
        _verify_claimed_peer(identity, peer_pid=peer_pid, peer_uid=peer_uid)
        self.identity_verifier(identity)
        registered = self.store.get_registered_instance(_required_str(payload, "instance_id"))
        if registered is None:
            raise RuntimeFailureError(
                ErrorCode.CORRELATION_MISMATCH,
                "heartbeat instance is not registered",
            )
        verify_process_executable_evidence(
            identity.pid,
            _wrapper_executable_from_launch(registered.launch_spec),
            verify_digest=registered.daemon_boot_id != self.boot_id,
        )
        try:
            lifecycle, heartbeat_at, resynchronized = self.store.heartbeat_instance(
                instance_id=registered.instance_id,
                binding_id=_required_str(payload, "binding_id"),
                wrapper_pid=identity.pid,
                wrapper_starttime=identity.starttime,
                child_pid=_required_int(payload, "child_pid"),
                child_starttime=_required_int(payload, "child_starttime"),
                tmux_session=identity.tmux_session,
                tmux_pane=identity.tmux_pane,
                daemon_boot_id=self.boot_id,
                previous_daemon_boot_id=previous_boot_id,
                connection_generation=connection_generation,
            )
        except KeyError as exc:
            raise RuntimeFailureError(
                ErrorCode.CORRELATION_MISMATCH,
                "heartbeat does not match a registered live instance",
            ) from exc
        return self._ok(
            request_id,
            heartbeat_at=heartbeat_at,
            lifecycle=lifecycle,
            connection_generation=connection_generation,
            resynchronized=resynchronized,
        )

    def _control_plane_heartbeat(
        self,
        request_id: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        agent_instance_id = _required_str(payload, "agent_instance_id")
        if agent_instance_id != self.config.agent_instance_id:
            raise RuntimeFailureError(ErrorCode.UNKNOWN_BINDING, "AgentInstance mismatch")
        last_seen_at = self.store.mark_control_plane_sync(agent_instance_id)
        return self._ok(
            request_id,
            agent_instance_id=agent_instance_id,
            last_seen_at=last_seen_at,
            sync_state="IN_SYNC",
        )

    def _status(self, request_id: str) -> dict[str, Any]:
        report = self.store.status_report(self.config.agent_instance_id)
        try:
            topic_snapshot = read_topic_config(self.config.topic_config_path)
            bound_thread_ids = {
                binding.thread_id
                for binding in self.store.list_bindings(self.config.agent_instance_id)
            }
            report["topic_discovery"] = {
                "status": "OK",
                "config_digest": topic_snapshot.digest,
                "unbound_topics": topic_snapshot.unbound_named_topics(bound_thread_ids),
            }
        except (OSError, ValueError, KeyError, TypeError):
            report["topic_discovery"] = {"status": "ERROR", "unbound_topics": []}
        last_seen_at = self.store.control_plane_last_seen(self.config.agent_instance_id)
        now = time.time()
        age_seconds = None if last_seen_at is None else max(0.0, now - last_seen_at)
        in_sync = (
            age_seconds is not None and age_seconds <= self.config.control_plane_stale_after_seconds
        )
        raw_bindings = report.get("bindings")
        if isinstance(raw_bindings, list) and self._session_view_error is not None:
            for binding in raw_bindings:
                if isinstance(binding, dict) and binding.get("health") == "RUNTIME_HEALTHY":
                    binding["health"] = "ERROR"
        if isinstance(raw_bindings, list) and not in_sync:
            for binding in raw_bindings:
                if isinstance(binding, dict) and binding.get("health") == "RUNTIME_HEALTHY":
                    binding["health"] = "DEGRADED_CUS_SYNC"
        report["control_plane_sync"] = {
            "state": "IN_SYNC" if in_sync else "DEGRADED_CUS_SYNC",
            "last_seen_at": last_seen_at,
            "age_seconds": age_seconds,
            "stale_after_seconds": self.config.control_plane_stale_after_seconds,
        }
        report["session_view"] = {
            "configured": self.config.sessions_view_root is not None,
            "root": str(self.config.sessions_view_root) if self.config.sessions_view_root else None,
            "error": self._session_view_error,
        }
        report["qualification_only"] = {
            "codex_hooks_disabled": self.config.qualification_disable_codex_hooks,
        }
        return self._ok(request_id, status=report)

    def _prepare_dispatch(
        self,
        request_id: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Gate one topic prompt against authoritative desired/applied state."""

        agent_instance_id = _required_str(payload, "agent_instance_id")
        if agent_instance_id != self.config.agent_instance_id:
            raise RuntimeFailureError(ErrorCode.UNKNOWN_BINDING, "AgentInstance mismatch")
        chat_id = _required_int(payload, "chat_id")
        thread_id = _required_int(payload, "thread_id")
        if chat_id != self.config.forum_chat_id:
            raise RuntimeFailureError(ErrorCode.UNKNOWN_BINDING, "forum chat mismatch")
        state = self.store.dispatch_state(
            agent_instance_id=agent_instance_id,
            chat_id=chat_id,
            thread_id=thread_id,
        )
        if state is None or not state.binding.configured:
            raise RuntimeFailureError(ErrorCode.UNKNOWN_BINDING, "topic binding is not configured")
        if state.binding.management_mode is not ManagementMode.MANAGED:
            raise RuntimeFailureError(
                ErrorCode.CONFIG_DRIFT,
                f"topic binding is not managed: {state.binding.management_mode.value}",
            )
        if state.desired_spec_digest is None:
            raise RuntimeFailureError(ErrorCode.UNKNOWN_BINDING, "binding has no desired spec")
        if state.desired_provider is None or state.desired_model is None:
            raise RuntimeFailureError(ErrorCode.CONFIG_DRIFT, "desired spec identity is incomplete")
        desired_identity = {
            "provider": state.desired_provider,
            "model": state.desired_model,
        }
        if state.live_instance_count == 0:
            return self._ok(
                request_id,
                decision="START_REQUIRED",
                binding_id=state.binding.binding_id,
                desired_spec_digest=state.desired_spec_digest,
                desired=desired_identity,
            )
        if state.live_instance_count != 1:
            return self._ok(
                request_id,
                decision="DENY",
                reason="AMBIGUOUS_LIVE_INSTANCES",
                binding_id=state.binding.binding_id,
            )
        effective_digest = state.applied_spec_digest or state.launch_spec_digest
        if effective_digest == state.desired_spec_digest:
            return self._ok(
                request_id,
                decision="ALLOW",
                binding_id=state.binding.binding_id,
                lifecycle=state.lifecycle,
                desired_spec_digest=state.desired_spec_digest,
                desired=desired_identity,
            )
        if state.lifecycle == LifecycleState.BUSY:
            return self._ok(
                request_id,
                decision="WAIT",
                reason="ACTIVE_TURN_USES_PREVIOUS_SPEC",
                retry_after_ms=500,
                binding_id=state.binding.binding_id,
                desired=desired_identity,
            )
        if state.lifecycle == LifecycleState.IDLE:
            return self._ok(
                request_id,
                decision="RECYCLE_REQUIRED",
                reason="IDLE_INSTANCE_USES_PREVIOUS_SPEC",
                binding_id=state.binding.binding_id,
                desired=desired_identity,
            )
        if state.lifecycle == LifecycleState.PROVISIONAL_STARTING:
            return self._ok(
                request_id,
                decision="WAIT",
                reason="INSTANCE_STARTING",
                retry_after_ms=250,
                binding_id=state.binding.binding_id,
                desired=desired_identity,
            )
        return self._ok(
            request_id,
            decision="DENY",
            reason="UNSAFE_INSTANCE_STATE",
            binding_id=state.binding.binding_id,
            lifecycle=state.lifecycle,
        )

    def _project_session_view_once(self) -> None:
        view_root = self.config.sessions_view_root
        if view_root is None:
            return
        profiles: dict[str, Path] = {}
        for binding in self.store.list_bindings(self.config.agent_instance_id):
            if not binding.configured:
                continue
            effective = self.store.get_effective_spec(binding.binding_id)
            if effective is None:
                continue
            spec, raw_home, _fingerprint = effective
            home = validate_profile_path(raw_home, root=self.config.profiles_root)
            if home.name != spec.account_profile:
                raise SessionViewError("effective account profile identity mismatch")
            profiles[spec.account_profile] = home
        if profiles:
            result = project_session_transcripts(profiles=profiles, view_root=view_root)
            if result.removed:
                logger.info("Pruned %d archived transcript links from session view", result.removed)
        self._session_view_error = None

    async def _session_view_loop(self) -> None:
        while True:
            try:
                self._project_session_view_once()
            except (OSError, ValueError, SessionViewError) as exc:
                detail = f"{type(exc).__name__}: {exc}"
                if detail != self._session_view_error:
                    logger.error("Transcript projection failed: %s", detail)
                self._session_view_error = detail
            await asyncio.sleep(1.0)

    def _child_exited(
        self,
        request_id: str,
        payload: dict[str, Any],
        peer_pid: int,
        peer_uid: int,
    ) -> dict[str, Any]:
        identity = _identity(payload)
        _verify_claimed_peer(identity, peer_pid=peer_pid, peer_uid=peer_uid)
        self.identity_verifier(identity)
        instance_id = _required_str(payload, "instance_id")
        binding_id = _required_str(payload, "binding_id")
        child_pid = _required_int(payload, "child_pid")
        child_starttime = _required_int(payload, "child_starttime")
        exit_code = _required_int(payload, "exit_code")
        registered = self.store.get_registered_instance(instance_id)
        if registered is None or (
            registered.binding_id != binding_id
            or registered.wrapper_pid != identity.pid
            or registered.wrapper_starttime != identity.starttime
            or registered.tmux_session != identity.tmux_session
            or registered.tmux_pane != identity.tmux_pane
        ):
            raise RuntimeFailureError(
                ErrorCode.CORRELATION_MISMATCH,
                "exited child does not match registered wrapper instance",
            )
        verify_process_executable_evidence(
            identity.pid,
            _wrapper_executable_from_launch(registered.launch_spec),
            verify_digest=False,
        )
        self.store.mark_instance_exited(
            instance_id=instance_id,
            binding_id=binding_id,
            wrapper_pid=identity.pid,
            wrapper_starttime=identity.starttime,
            child_pid=child_pid,
            child_starttime=child_starttime,
        )
        return self._ok(request_id, instance_id=instance_id, exit_code=exit_code)

    async def _reconcile_topic_config_loop(self) -> None:
        while True:
            with contextlib.suppress(OSError, ValueError):
                self._reconcile_topic_config_once()
            await asyncio.sleep(1.0)

    async def _reap_absent_wrappers_loop(self) -> None:
        while True:
            self._reap_absent_wrappers_once()
            await asyncio.sleep(self.evidence_interval)

    def _reap_absent_wrappers_once(self) -> None:
        for instance_id, pid, starttime, uid, _lifecycle in self.store.active_wrapper_identities():
            try:
                process_matches = proc_uid(pid) == uid and proc_starttime(pid) == starttime
            except (OSError, ValueError):
                process_matches = False
            if not process_matches:
                self.store.mark_instance_wrapper_absent(
                    instance_id=instance_id,
                    wrapper_pid=pid,
                    wrapper_starttime=starttime,
                )

    def _reconcile_topic_config_once(self) -> None:
        snapshot = read_topic_config(self.config.topic_config_path)
        for binding in self.store.list_bindings(self.config.agent_instance_id):
            entry = snapshot.topic(binding.thread_id)
            if entry is None:
                mode = ManagementMode.CONFIG_DRIFT
            elif entry.engine != "codex":
                mode = ManagementMode.UNMANAGED
            elif (
                entry.cwd is not None
                and Path(entry.cwd).resolve() == Path(binding.canonical_cwd).resolve()
            ):
                mode = (
                    ManagementMode.MANAGED
                    if entry.exec_mode == "tmux"
                    else ManagementMode.UNSUPPORTED_CODEX_SUBPROCESS
                )
            else:
                mode = ManagementMode.CONFIG_DRIFT
            self.store.update_binding_observation(
                binding.binding_id,
                config_digest=snapshot.digest,
                management_mode=mode,
            )
            if mode == ManagementMode.UNSUPPORTED_CODEX_SUBPROCESS:
                self.store.enqueue_user_notification(
                    code=UserNotificationCode.UNSUPPORTED_CODEX_SUBPROCESS,
                    binding=binding,
                    event_ref=snapshot.digest,
                )

    async def _collect_applied_evidence_loop(self) -> None:
        while True:
            with contextlib.suppress(OSError, ValueError, RuntimeFailureError):
                self._collect_applied_evidence_once()
            await asyncio.sleep(self.evidence_interval)

    def _collect_applied_evidence_once(self) -> None:
        states = read_tmux_states(self.config.tmux_state_path)
        self._revoke_stale_applied_evidence(states)
        for instance in self.store.pending_evidence_instances():
            binding = self.store.get_binding(instance.binding_id)
            if binding is None:
                continue
            matches = tuple(
                state
                for state in states
                if state.chat_id == binding.chat_id
                and state.thread_id == binding.thread_id
                and state.session_name == instance.tmux_session
                and state.provider == "codex"
                and Path(state.cwd).resolve() == Path(binding.canonical_cwd).resolve()
            )
            if len(matches) != 1:
                continue
            state = matches[0]
            if not state.session_id or not state.transcript_path:
                continue
            launch = instance.launch_spec
            raw_desired = launch.get("desired")
            if not isinstance(raw_desired, dict):
                continue
            spec = _desired_spec(raw_desired)
            codex_home = Path(_required_str(launch, "codex_home"))
            expected_fingerprint = _required_str(launch, "profile_fingerprint")
            current_fingerprint = profile_fingerprint(codex_home)
            profile_reattestation: tuple[str, str] | None = None
            if current_fingerprint != expected_fingerprint:
                if not expected_fingerprint.startswith("sha256:"):
                    continue
                profile_reattestation = (expected_fingerprint, current_fingerprint)
                expected_fingerprint = current_fingerprint
            if not expected_fingerprint.startswith("profile-v2:sha256:"):
                continue
            real_codex = Path(_required_str(launch, "real_codex"))
            expected_binary_digest = _required_str(launch, "real_codex_sha256")
            if file_sha256(real_codex) != expected_binary_digest:
                continue
            rollout_path = Path(state.transcript_path).resolve(strict=True)
            rollout_stat = rollout_path.stat()
            if not stat.S_ISREG(rollout_stat.st_mode):
                continue
            if rollout_stat.st_uid != os.getuid():
                continue
            process_evidence = self.child_verifier(
                child_pid=instance.child_pid,
                child_starttime=instance.child_starttime,
                wrapper_pid=instance.wrapper_pid,
                codex_home=codex_home,
                model=spec.model,
                reasoning_effort=spec.reasoning_effort,
                real_codex=real_codex,
            )
            evidence: dict[str, object] = {
                **process_evidence,
                "spec_digest": spec.spec_digest,
                "account_profile": spec.account_profile,
                "profile_fingerprint": expected_fingerprint,
                "real_codex_version": _required_str(launch, "real_codex_version"),
                "real_codex_sha256": expected_binary_digest,
                "session_id": state.session_id,
                "rollout_path": str(rollout_path),
                "rollout_device": rollout_stat.st_dev,
                "rollout_inode": rollout_stat.st_ino,
                "wrapper_pid": instance.wrapper_pid,
                "wrapper_starttime": instance.wrapper_starttime,
                "wrapper_identity": instance.launch_spec.get("wrapper_identity"),
                "daemon_boot_id": instance.daemon_boot_id,
                "connection_generation": instance.connection_generation,
                "tmux_session": instance.tmux_session,
                "tmux_pane": instance.tmux_pane,
            }
            self.store.record_applied_spec(
                instance_id=instance.instance_id,
                binding_id=instance.binding_id,
                spec=spec,
                evidence=evidence,
                session_id=state.session_id,
                account_profile=spec.account_profile,
                rollout_path=str(rollout_path),
                rollout_device=rollout_stat.st_dev,
                rollout_inode=rollout_stat.st_ino,
                profile_reattestation=profile_reattestation,
            )

    def _revoke_stale_applied_evidence(self, states: tuple[UpstreamTmuxState, ...]) -> None:
        """Do not keep a previous rollout attached to a newly respawned child."""

        for target in self.store.rollout_observation_targets():
            binding = self.store.get_binding(target.binding_id)
            if binding is None:
                continue
            matching = tuple(
                state
                for state in states
                if state.chat_id == binding.chat_id
                and state.thread_id == binding.thread_id
                and state.session_name == target.tmux_session
                and state.provider == "codex"
                and Path(state.cwd).resolve() == Path(binding.canonical_cwd).resolve()
            )
            current_path: Path | None = None
            if len(matching) == 1 and matching[0].transcript_path:
                with contextlib.suppress(OSError, RuntimeError):
                    current_path = Path(matching[0].transcript_path).resolve(strict=True)
            if (
                len(matching) != 1
                or matching[0].session_id != target.session_id
                or current_path != Path(target.rollout_path)
            ):
                self.store.revoke_stale_applied_spec(
                    instance_id=target.instance_id,
                    session_id=target.session_id,
                    rollout_path=target.rollout_path,
                )

    async def _observe_codex_rollouts_loop(self) -> None:
        while True:
            for target in self.store.rollout_observation_targets():
                try:
                    self._observe_codex_rollout(target)
                    self._rollout_observer_faults.discard(target.instance_id)
                except (OSError, ValueError, RolloutContractError) as exc:
                    if target.instance_id not in self._rollout_observer_faults:
                        logger.error(
                            "Codex rollout observer blocked for instance %s: %s",
                            target.instance_id,
                            exc,
                        )
                        self._rollout_observer_faults.add(target.instance_id)
            await asyncio.sleep(self.evidence_interval)

    async def _retire_pending_loop(self) -> None:
        while True:
            await self._retire_pending_once()
            await asyncio.sleep(self.evidence_interval)

    async def _retire_pending_once(self) -> None:
        for candidate in self.store.retirement_candidates():
            binding = self.store.get_binding(candidate.binding_id)
            if binding is None:
                continue
            try:
                snapshot = read_topic_config(self.config.topic_config_path)
                entry = snapshot.topic(binding.thread_id)
                config_is_current = (
                    entry is not None
                    and entry.engine == "codex"
                    and entry.exec_mode == "tmux"
                    and entry.cwd is not None
                    and Path(entry.cwd).resolve() == Path(binding.canonical_cwd).resolve()
                    and binding.management_mode == ManagementMode.MANAGED
                    and binding.config_digest == snapshot.digest
                )
            except (OSError, ValueError):
                config_is_current = False
            if not config_is_current:
                continue
            identity = WrapperIdentity(
                pid=candidate.wrapper_pid,
                starttime=candidate.wrapper_starttime,
                uid=os.getuid(),
                tmux_session=candidate.tmux_session,
                tmux_pane=candidate.tmux_pane,
                cwd=candidate.canonical_cwd,
            )
            try:
                await asyncio.to_thread(self.retirement_executor, identity)
                self.store.mark_instance_retired(candidate)
            except (OSError, ValueError, RuntimeFailureError, subprocess.SubprocessError):
                continue

    def _observe_codex_rollout(self, target: RolloutObservationTarget) -> None:
        cursor = self.store.rollout_observation_cursor(
            instance_id=target.instance_id,
            rollout_device=target.rollout_device,
            rollout_inode=target.rollout_inode,
        )
        scanned = scan_rollout(
            Path(target.rollout_path),
            codex_version=target.codex_version,
            start_offset=cursor.offset,
            expected_device=target.rollout_device,
            expected_inode=target.rollout_inode,
        )
        if scanned.skipped_oversized_records:
            logger.warning(
                "Skipped %d oversized non-lifecycle Codex rollout record(s) for instance %s",
                scanned.skipped_oversized_records,
                target.instance_id,
            )
        lifecycle = target.lifecycle
        active_turn_id = cursor.active_turn_id
        pending_model = cursor.pending_model
        pending_effort = cursor.pending_reasoning_effort
        pending_evidence = cursor.pending_evidence_digest
        pending_turn_id = cursor.pending_turn_id
        for signal in scanned.signals:
            if signal.kind == RolloutSignalKind.TURN_STARTED and signal.turn_id is not None:
                lifecycle = LifecycleState.BUSY
                active_turn_id = signal.turn_id
            elif (
                signal.kind == RolloutSignalKind.TURN_CONTEXT
                and lifecycle == LifecycleState.BUSY
                and signal.turn_id == active_turn_id
            ):
                pending_model = signal.model
                pending_effort = signal.reasoning_effort
                pending_evidence = signal.evidence_digest
                pending_turn_id = signal.turn_id
            elif (
                signal.kind == RolloutSignalKind.TURN_COMPLETED
                and signal.turn_id is not None
                and signal.turn_id == active_turn_id
            ):
                lifecycle = LifecycleState.IDLE
                active_turn_id = None

        sequence = cursor.observation_sequence
        last_model = cursor.last_model or target.applied_model
        last_effort = cursor.last_reasoning_effort or target.applied_reasoning_effort
        if (
            scanned.at_eof
            and pending_model is not None
            and pending_effort is not None
            and pending_evidence is not None
            and pending_turn_id is not None
        ):
            effective = self.store.get_effective_spec(target.binding_id)
            if effective is None:
                raise ValueError("observed binding has no local effective spec")
            spec, _codex_home, _profile_fingerprint = effective
            observed = (pending_model, pending_effort)
            if observed != (last_model, last_effort) and observed != (
                spec.model,
                spec.reasoning_effort,
            ):
                sequence += 1
                evidence_digest = sha256_digest(
                    {
                        "instance_id": target.instance_id,
                        "rollout_device": target.rollout_device,
                        "rollout_inode": target.rollout_inode,
                        "session_id": target.session_id,
                        "turn_id": pending_turn_id,
                        "provider_evidence_digest": pending_evidence,
                    }
                )
                self.store.record_observed_proposal(
                    ObservedSpecProposal(
                        binding_id=target.binding_id,
                        base_generation_id=spec.generation_id,
                        base_revision=spec.revision,
                        base_spec_digest=spec.spec_digest,
                        model=pending_model,
                        reasoning_effort=pending_effort,
                        instance_id=target.instance_id,
                        observation_sequence=sequence,
                        evidence_digest=evidence_digest,
                    )
                )
            last_model = pending_model
            last_effort = pending_effort
            pending_model = None
            pending_effort = None
            pending_evidence = None
            pending_turn_id = None
        self.store.save_rollout_observation_cursor(
            instance_id=target.instance_id,
            rollout_device=target.rollout_device,
            rollout_inode=target.rollout_inode,
            cursor=RolloutObservationCursor(
                offset=scanned.next_offset,
                observation_sequence=sequence,
                last_model=last_model,
                last_reasoning_effort=last_effort,
                active_turn_id=active_turn_id,
                pending_model=pending_model,
                pending_reasoning_effort=pending_effort,
                pending_evidence_digest=pending_evidence,
                pending_turn_id=pending_turn_id,
            ),
            lifecycle=str(lifecycle),
        )

    def _cache(
        self,
        request_id: str,
        request_digest: str,
        result: dict[str, Any],
        *,
        durable: bool,
    ) -> None:
        self._results[request_id] = (request_digest, result)
        self._results.move_to_end(request_id)
        if durable:
            self.store.put_request_result(request_id, request_digest, result)
        while len(self._results) > 1024:
            self._results.popitem(last=False)

    def _ok(self, request_id: str, **payload: Any) -> dict[str, Any]:
        return versioned(
            "response",
            request_id,
            ok=True,
            daemon_boot_id=self.boot_id,
            **payload,
        )

    def _error(self, request_id: str, code: ErrorCode, detail: str) -> dict[str, Any]:
        return versioned(
            "response",
            request_id,
            ok=False,
            daemon_boot_id=self.boot_id,
            error={"code": code, "detail": detail},
        )


def _wrapper_executable_from_launch(launch: dict[str, Any]) -> object:
    wrapper_identity = launch.get("wrapper_identity")
    if not isinstance(wrapper_identity, dict):
        raise RuntimeFailureError(
            ErrorCode.CORRELATION_MISMATCH,
            "registered wrapper process identity is absent",
        )
    return wrapper_identity.get("executable")


def _identity(payload: dict[str, Any]) -> WrapperIdentity:
    wrapper = payload.get("wrapper")
    if not isinstance(wrapper, dict):
        raise RuntimeFailureError(ErrorCode.INVALID_REQUEST, "wrapper identity is required")
    return WrapperIdentity(
        pid=_required_int(wrapper, "pid"),
        starttime=_required_int(wrapper, "starttime"),
        uid=_required_int(wrapper, "uid"),
        tmux_session=_required_str(payload, "tmux_session"),
        tmux_pane=_required_str(payload, "tmux_pane"),
        cwd=_required_str(payload, "cwd"),
    )


def _required_str(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str) or not value:
        raise RuntimeFailureError(ErrorCode.INVALID_REQUEST, f"{key} must be a non-empty string")
    return value


def _required_int(payload: dict[str, Any], key: str) -> int:
    value = payload.get(key)
    if not isinstance(value, int):
        raise RuntimeFailureError(ErrorCode.INVALID_REQUEST, f"{key} must be an integer")
    return value


def _verify_claimed_peer(
    identity: WrapperIdentity,
    *,
    peer_pid: int,
    peer_uid: int,
) -> None:
    if identity.pid != peer_pid or identity.uid != peer_uid:
        raise RuntimeFailureError(
            ErrorCode.INVALID_REQUEST,
            "claimed wrapper PID/UID does not match Unix peer credentials",
        )


def _desired_spec(payload: dict[str, Any]) -> DesiredSpec:
    try:
        spec = DesiredSpec(
            binding_id=_required_str(payload, "binding_id"),
            agent_instance_id=_required_str(payload, "agent_instance_id"),
            project_id=_required_str(payload, "project_id"),
            canonical_cwd=_required_str(payload, "canonical_cwd"),
            provider=_required_str(payload, "provider"),
            account_profile=_required_str(payload, "account_profile"),
            model=_required_str(payload, "model"),
            reasoning_effort=_required_str(payload, "reasoning_effort"),
            generation_id=_required_str(payload, "generation_id"),
            revision=_required_int(payload, "revision"),
            spec_digest=_required_str(payload, "spec_digest"),
            source_kind=_optional_str(payload, "source_kind", max_length=64),
            source_ref=_optional_str(payload, "source_ref", max_length=256),
        )
    except TypeError as exc:
        raise RuntimeFailureError(ErrorCode.INVALID_REQUEST, str(exc)) from exc
    if not spec.verify_digest():
        raise RuntimeFailureError(ErrorCode.INVALID_REQUEST, "desired spec digest mismatch")
    return spec


def _optional_str(
    payload: dict[str, Any],
    key: str,
    *,
    max_length: int,
) -> str | None:
    value = payload.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value or len(value) > max_length:
        raise RuntimeFailureError(ErrorCode.INVALID_REQUEST, f"invalid {key}")
    return value
