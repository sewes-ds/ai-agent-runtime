# Local Runtime protocol v1

Transport is a same-user Unix stream socket with length-delimited JSON frames. Each
request carries `protocol_major`, `protocol_minor`, `action`, and a globally unique
`request_id`. Runtime verifies `SO_PEERCRED`; wrapper registration additionally
requires its claimed PID and UID to match the socket peer.

Every frame read/write has a deadline. Ordinary wrapper commands use a 10-second
response deadline. `verify_started` uses 60 seconds because the qualified Phase B
state correlation may wait up to 45 seconds; the client deadline must always exceed
the daemon's bounded verification window. Heartbeats are best effort and have their
own 2-second outer deadline.

Successful mutating responses are stored durably with a digest of the request. A
retry using the same ID and payload returns the original outcome, including after a
daemon restart. Reusing an ID with a different payload is rejected.
`heartbeat`, `control_plane_heartbeat`, `status`, `prepare_dispatch`, and `outbox_pull` use only
the daemon's bounded in-memory cache; their responses are not appended to the
durable idempotency table on every poll. After a daemon restart, replaying one
may refresh the lease, return a newer status snapshot, or re-read an outbox
event on its next retry. Outbox event IDs and CUS receipts provide durable
delivery idempotency; an unacknowledged event remains in the outbox.

## Wrapper actions

- `register`: Phase A launch authorization. Inputs include AgentInstance, wrapper
  PID/starttime/UID, cwd, tmux session/pane, and optional resume session ID.
  Runtime independently captures the live `/proc/<pid>/exe` path, device, inode,
  size, mtime and SHA-256; wrapper-supplied data is not trusted for executable
  identity.
- `verify_started`: Phase B verification after the real child starts. Inputs add
  instance/binding IDs and child PID/starttime.
- `child_exited`: terminal wrapper notification with the same composite process and
  tmux identity plus exit code. A missing/mismatched instance is rejected; daemon
  outage cannot change the already determined child exit code.

Unknown binding, unverified resume provenance, config drift, ambiguous identity, and
Codex subprocess mode fail closed.

## Control-plane action

`prepare_dispatch` is the prompt-free, same-user pre-dispatch barrier used by
the qualified Telegram adapter. It accepts only AgentInstance/chat/thread
identity and returns `ALLOW`, `START_REQUIRED`, `WAIT`, or
`RECYCLE_REQUIRED`. A live process whose applied or launch-authorized digest
differs from current desired state can never receive a new prompt: BUSY waits,
IDLE recycles, and ambiguous or unmanaged state fails closed. Prompt content is
not part of this protocol. Successful decisions include the provider/model
identity the adapter must use when it starts or recycles the local session.

`apply_binding` accepts a binding identity and complete DesiredSpec. Before touching
Telegram Agent configuration, Runtime verifies:

- exact AgentInstance and forum chat;
- canonical cwd containment in the configured project roots;
- immutable binding/topic identity and no implicit project relocation;
- provider/account profile safety;
- canonical DesiredSpec digest;
- monotonic revision within the existing generation.

The upstream-created topic key must already exist. Runtime performs a fresh-read
allowlisted patch and publishes it with Linux `renameat2(RENAME_EXCHANGE)`, retaining
the exact displaced file as a private backup. If an independent upstream writer
changed the file between read and exchange, Runtime performs a bounded merge of
new topic keys and unmanaged fields before post-write verification and storing the
local effective spec. A concurrent managed-field change is restored and rejected
as `CONFIG_DRIFT`; it is not retried as an implicit return to Codex. If atomic
exchange is unavailable, the write fails closed. This is still eventual
reconciliation, not a cross-process transaction: sustained contention can exhaust
the bounded attempts. In that case `TopicConfigUnresolvedConflictError` names the
last retained upstream backup, the binding apply fails as `CONFIG_DRIFT`, and CUS
must not retry it as a successful apply from the published candidate; an operator
must inspect the backup and reconcile the upstream topic inventory. A
different generation is rejected from this incremental action and requires
`apply_snapshot`.

The continuous fresh-read watcher records a missing topic key or a changed managed
cwd as `CONFIG_DRIFT`; it never silently recreates the key and managed launch remains
blocked until a successful reconciliation. A non-Codex engine is instead
`UNMANAGED`, while `codex+subprocess` with the correct managed cwd is the distinct
fail-closed `UNSUPPORTED_CODEX_SUBPROCESS` state.

`apply_snapshot` is the only action allowed to introduce a new generation. It
contains the complete AgentInstance desired inventory, one common generation, a
canonical snapshot digest, explicit `unmanaged_bindings`, and explicit
`retired_binding_ids`. Runtime validates all identities, profiles, digests, topic
keys, and inventory coverage before one batched topic-config replacement and one
SQLite transaction. Existing bindings may not be silently omitted. An unmanaged
binding retains its identity and verified local desired profile, but Runtime does not
rewrite its non-Codex engine or authorize a Codex launch. A later observed return to
`codex+tmux` can restore managed operation through fresh Phase A/Phase B verification.
Retirement disables future managed launches and removes the local effective spec,
but preserves binding, instance, and session-provenance history.

An explicit Project path change is carried inside that full snapshot as
`project_relocations`. Each record has a unique relocation ID, immutable Project ID,
old/new canonical paths, and the complete set of configured bindings for that
Project. Runtime accepts it only when every affected binding is stopped and managed,
the target is an allowed existing directory not owned by another Project, and every
binding plus DesiredSpec in the same snapshot adopts the new path. The topic-config
change, effective specs, binding paths, and durable alias/history record commit as
one operation. Omitting this record, relocating only part of a Project, or reusing an
ID with different data fails closed.

`import_session_provenance` accepts only an already-reviewed
`ai-agent-runtime.session-provenance-plan.v1` artifact after the authoritative
binding snapshot has been applied. The plan is self-digested and bound to exact
inventory and binding-plan digests. Runtime independently checks AgentInstance,
source ref, configured binding, canonical cwd and effective account. Planning also
requires sanitized live-process evidence binding the sole tmux pane to the qualified
real-Codex digest and a safe `CODEX_HOME` profile matching that account. Runtime
revalidates that same live process immediately before import, then re-opens each
rollout with no-follow semantics and revalidates its first `session_meta` plus
path/device/inode/size. All ownership rows are inserted in one transaction. Existing
identical rows are no-ops; changing an existing session owner or rollout identity is
rejected. The CLI derives a stable request ID from the plan digest, so lost-ACK retry
uses the normal durable request-result cache.

This protocol is local execution control, not a public CUS backend API. The CUS
backend remains provider-neutral and talks through its Runtime connector.

`control_plane_heartbeat` carries the exact `agent_instance_id` and refreshes a
durable local sync lease. Successful `apply_binding` and `apply_snapshot` do the
same. If no control-plane contact occurs within the configured bounded window,
`status.control_plane_sync.state` becomes `DEGRADED_CUS_SYNC`; otherwise it is
`IN_SYNC`. The degraded state never blocks a verified LKG launch and never changes
desired state—it prevents a stale control plane from being reported as healthy.

`status` returns sanitized binding source identity, project/cwd, management mode,
desired profile/model/reasoning and revision, exact wrapper/child/tmux identities,
verified applied revision plus allowlisted factual account/CODEX_HOME,
model/reasoning, binary, session and rollout evidence, lifecycle, and derived health.
It never returns credential content, raw prompts, or transcript text. A binding that
would otherwise be `RUNTIME_HEALTHY` is
reported as `DEGRADED_CUS_SYNC` while the control-plane lease is stale; stronger
binding errors and pending states retain precedence. A nominally live instance whose
wrapper heartbeat is absent or older than the bounded ten-second liveness window is
`OUT_OF_SYNC`; stale durable process state can never remain `RUNTIME_HEALTHY`.
`status.topic_discovery` is a read-only, fresh topic-config view containing only
named upstream-created thread IDs that have no Runtime binding, plus the config
digest. CUS uses it to recover an onboarding notice when the independent Bot API
topic-created webhook is absent. A config read error is reported as
`topic_discovery.status=ERROR`, never as an empty successful inventory. Runtime
does not create topic keys or send Telegram notices.

## Durable outbox

`outbox_pull` claims up to 100 due events and advances a bounded exponential retry
lease. `outbox_ack` finalizes an event as `ACKED`, `STALE_OBSERVED`, or
`POLICY_REJECTED`; replaying the same ACK is idempotent, while changing a final ACK
is rejected. Observed proposals carry their base generation/revision/digest so CUS
can reject delayed state rather than overwrite a newer user choice.
For multiple offline observations of one binding, Runtime releases the highest
`observation_sequence` first and keeps older proposals behind its retry lease. This
prevents an older queued observation from overtaking the locally latest proven state;
after the latest event is finalized, older proposals may be delivered only to receive
the normal stale-base acknowledgement. Existing V1 databases backfill this ordering
field only from digest-valid proposal payloads.

Pending proposals also back the Runtime's local launch overlay and are visible as
`OBSERVED_PENDING_SYNC`. `POLICY_REJECTED` remains visible as
`OBSERVED_POLICY_REJECTED` while the rejected proven process state is still live;
once that state is retired and the authoritative desired spec is relaunched, the
status clears. Desired IPC payloads preserve `source_kind` and `source_ref` so an
accepted observed revision cannot feed back into a second proposal or cause an
unnecessary retirement.

Runtime also emits sanitized `USER_NOTIFICATION_REQUIRED` events for
`ONBOARDING_REQUIRED`, `RESUME_BINDING_MISMATCH`, and
`UNSUPPORTED_CODEX_SUBPROCESS`. Their immutable identity is binding-scoped and
derived from a stable local event reference, so wrapper retries and daemon restarts
cannot create duplicate notification requests. Payloads contain only the exact
AgentInstance/topic identity and a fixed notification code; CUS owns the user-facing
message text and Telegram delivery.

## Live wrapper resynchronization

After Phase B succeeds, wrapper sends a bounded best-effort heartbeat while the
Codex child is alive. Every heartbeat repeats the exact wrapper/child/tmux identity,
`connection_generation`, and the last observed `daemon_boot_id`. Runtime accepts it
only for the matching non-exited instance, matching captured wrapper executable,
non-regressing connection generation, and exact previously acknowledged daemon boot
ID. On a real daemon restart Runtime re-hashes the wrapper executable before ACK,
persists the new boot epoch, and causes wrapper to advance its connection generation
on the next heartbeat. A stale or fabricated boot handshake fails closed. Socket
outages never terminate an already-running child; startup and Phase B remain fail
closed.
