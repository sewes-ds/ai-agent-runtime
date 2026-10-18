# Architecture and trust boundaries

The Runtime is a local execution layer. It does not own Telegram updates, topic
routing, work-message queues, prompt injection, rollout delivery, or CUS product
state.

## Launch authorization

1. The unchanged Telegram Agent creates a tmux session and invokes
   `TELEGRAM_CODEX_BIN`.
2. `codex-wrapper` proves its pane/session identity and connects to the local Unix
   socket. It never contacts CUS.
3. Runtime performs Phase A against a fresh `topic_config.json` read, an exact
   version-qualified tmux session name, canonical cwd, and the AgentInstance
   namespace. A live `/resume` may use the pinned upstream's temporary
   `-switch-<hex time_ns>` suffix, but only when the wrapper supplies a resume
   session ID and Runtime proves that session belongs to the same binding and
   account.
4. Runtime returns only a previously verified local effective spec. Unknown,
   ambiguous, unmanaged, drifted, and Codex-subprocess launches are denied.
5. Wrapper starts the real Codex as a supervised process group with an isolated
   `CODEX_HOME`.
6. Runtime performs Phase B against upstream `tmux_sessions/state.json` and another
   fresh topic-config read. Failure terminates the child group and does not create
   AppliedSpec.

Both phases are required. Cwd alone is never an identity.

The pinned upstream deliberately launches agents through `env -i`; arbitrary service
environment variables are not inherited. Multi-AgentInstance deployments therefore
use an instance-specific safe executable name such as
`codex-wrapper@agent_01`. The wrapper derives
`~/.config/ai-agent-runtime/instances/agent_01.toml` from that name. The generic
`codex-wrapper` uses the single-instance `config.toml`. An environment override is a
development/operator convenience, not the production namespace mechanism.

## State ownership

- CUS owns authoritative desired generation/revision/digest.
- Runtime owns verified local effective specs, process evidence, session provenance,
  explicit Project relocation history, the durable outbox, and a bounded
  last-control-plane-contact lease.
- Telegram Agent owns its topic configuration and tmux state. Runtime is an
  independent file writer, so updates use eventual reconciliation rather than a
  false cross-process transaction guarantee.

Runtime's Linux topic-config adapter atomically exchanges its candidate with the
current file and retains each displaced version. This allows a bounded merge of
upstream topic creates/renames observed during a Runtime write without modifying
upstream source. A concurrent engine/mode/cwd change is not silently overwritten;
managed launch remains blocked until the observed state is reconciled. The adapter
does not claim strict serialization with upstream's separate in-process locks.

CUS availability is not in the wrapper startup path. CUS sends a namespaced local
heartbeat during periodic reconciliation, and every successful desired-state apply
also refreshes the same durable lease. Lease expiry changes observability to
`DEGRADED_CUS_SYNC` but deliberately leaves verified LKG execution available.

## Codex rollout observation

The pinned upstream discovers TUI transcripts under its process
`HOME/.codex/sessions`, whereas each real Codex child writes under its private
`CODEX_HOME/sessions`. An AgentInstance can use a private Telegram Agent `HOME`
and configure `telegram_agent.sessions_view_root` in Runtime. The Codex adapter
projects only owned `sessions/YYYY/MM/DD/rollout-*.jsonl` files from configured
account profiles into that view as symlinks. Runtime adds new rollouts while
running; conflicting names, unsafe source paths, or an unsafe view deny new
managed launches and surface an error. The original profiles, credentials,
indexes, SQLite state, and archives are not merged or modified. A complete
multi-account storage and resume qualification remains required before
production activation.

The Codex adapter is version-qualified. For `codex-cli 0.156.1`, Runtime treats only
matching `task_started`, `turn_context`, and `task_complete` records as lifecycle and
configuration evidence. A `turn_context` must belong to the active turn and explicitly
contain model and effort. Runtime scans owned rollout files by device/inode and durable
byte offset, retains metadata only, and writes an idempotent observed proposal to the
outbox when the proven settings differ from local desired state. Ordinary transcript
text, Telegram commands, picker screens, and output silence never become evidence.
Oversized non-lifecycle records are classified from a bounded version-qualified
prefix and skipped to their newline with bounded memory; the durable cursor must
advance past them before later lifecycle evidence is interpreted. Oversized or
unclassifiable lifecycle records fail closed.

The same durable transaction records a binding-scoped local LKG overlay for the
proven model/reasoning. While its proposal awaits CUS, new launches of that binding
use the overlay without making CUS synchronous startup infrastructure, health is
`OBSERVED_PENDING_SYNC`, and automatic retirement is suppressed. CUS transports
desired provenance (`source_kind`/`source_ref`) outside the desired digest. An
accepted observed revision therefore becomes authoritative with the same digest as
the already proven process and needs no redundant restart. A stale or policy-rejected
proposal stops being a launch overlay; if the live process still differs from the
authoritative desired spec, the normal safe-IDLE retirement flow restores it.

Fail-closed launch paths that require a user explanation enqueue a sanitized,
binding-scoped notification event. Runtime never receives a Telegram token and never
sends that message itself; the CUS connector and CUS Bot remain the delivery adapter.

## Local filesystem boundary

The Runtime database must remain under its configured private state directory. The
daemon refuses symlinked, foreign-owned, or group/world-writable database and socket
locations. Startup removes only a stale Unix socket owned by the Runtime user; it
never unlinks a regular file or symlink at the configured path. Shutdown records the
created socket inode and removes it only if the path still identifies that exact
socket, preventing a replacement path from being deleted.

## Implemented Runtime V1 surface

The executable launch skeleton, durable/idempotent local protocol, incremental and
authoritative snapshot apply, durable retrying outbox, SQLite state, fresh-read
adapters, two-phase correlation, process-group supervision, live-wrapper
resynchronization, qualified Codex rollout observation, exact-target retirement,
read-only operator diagnostics, and fail-closed policies are implemented and tested.
Retirement remains disabled by default and requires an exact qualification report
identifier. The pinned-upstream qualification tools emit release-tuple-bound
evidence, and rollback inputs can be pinned and reverified without authorizing
cleanup. Every deployment must qualify its own Telegram Agent, Codex binary,
Runtime wheel, CUS connector, filesystem layout, and rollback tuple; repository test
results are not deployment evidence.
