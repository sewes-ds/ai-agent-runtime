# Deployment layout

V1 uses one upstream Telegram Agent, one forum chat, and one Runtime namespace per
AgentInstance. A host may run several independent instances under the same Unix user.

For an AgentInstance named `agent_01`:

- Runtime config:
  `~/.config/ai-agent-runtime/instances/agent_01.toml`;
- Runtime service: `ai-agent-runtime@agent_01.service`;
- Runtime executable name: `~/.local/bin/ai-agent-runtime@agent_01`;
- socket: `/run/user/$UID/ai-agent-runtime/agent_01/runtime.sock`;
- wrapper executable name: `~/.local/bin/codex-wrapper@agent_01`;
- Telegram Agent `TELEGRAM_CODEX_BIN` points to that instance-specific executable;
- Telegram Agent config/state, bot token, tmux prefix/namespace, and systemd service
  are also instance-specific.

The instance-specific wrapper may be a symlink to the installed `codex-wrapper`
entrypoint, provided the target satisfies upstream ownership and mode validation.
Do not use `AI_AGENT_RUNTIME_CONFIG` as the production selector: the qualified
upstream strips it with `env -i`.

The supplied units are templates/examples, not authorization to deploy. Before an
actual service is enabled, its exact paths, user ownership, forum-chat uniqueness,
upstream release tuple, staging evidence, and rollback manifest must be validated.
The production Telegram Agent drop-in must retain `KillMode=process` so its restart
does not recursively destroy qualified tmux/wrapper/Codex processes.

Keep the Telegram bot token and instance-specific dispatch configuration in a
service-owned file outside every source checkout, for example
`~/.config/ai-agent-runtime/secrets/agent_01-telegram.env`, with directory mode
`0700` and file mode `0600`. The Runtime dispatch guard is enabled only when the
file supplies all three identity values:

```dotenv
AI_AGENT_RUNTIME_SOCKET=/run/user/1000/ai-agent-runtime/agent_01/runtime.sock
AI_AGENT_RUNTIME_AGENT_INSTANCE_ID=agent_01
AI_AGENT_RUNTIME_CHAT_ID=-1000000000000
AI_AGENT_RUNTIME_DISPATCH_WAIT_SEC=21600
```

The first three values are an all-or-nothing contract; the wait value is optional.
Add the bot's normal environment to the same private file, point the Telegram
service's `EnvironmentFile=` at it, run `systemctl --user daemon-reload`, and restart
only that Telegram service after changing the file. Never copy the real file into a
release or repository. The checked-in drop-in uses neutral example identifiers and
must be rendered for the target AgentInstance before deployment.

For the optional transcript projection, create an owned private directory for
`[telegram_agent] sessions_view_root` and give that AgentInstance's upstream
service a separate `HOME` whose `.codex/sessions` resolves to this view. Keep
the instance-named wrapper config reachable under the new `HOME` because pinned
upstream strips arbitrary environment variables from provider launches. The
Runtime daemon projects only rollout-file links from verified account profiles;
it never merges profile homes or changes the user's existing `~/.codex` tree.
When Codex archives a rollout, reconciliation removes only the now-broken
generated view link after verifying its target is under a configured profile's
`sessions` tree. Foreign or still-live links fail closed and are never deleted.
The service's `HOME`, view link, profile set, fresh rollout discovery, resume,
archive lifecycle, and rollback topology must be qualified together. A single
successful Telegram response is not a full storage-lifecycle qualification.

CI builds the wheel from the checked-out source and publishes it as the
`ai-agent-runtime-wheel` workflow artifact. Record that exact file's SHA-256 in the
release record, and install it into a
new side-by-side directory. The installer refuses symlinked or tampered wheels,
unexpected digests, unsafe release roots, and any existing release ID. It never
changes an active symlink, restarts a service, or removes an older release:

```bash
ai-agent-runtime deployment install-release \
  --wheel /srv/releases/ai_agent_runtime-0.1.0-py3-none-any.whl \
  --wheel-sha256 sha256:<exact-wheel-digest> \
  --releases-root /srv/ai-agent-runtime/releases \
  --release-id runtime-0.1.0-<git-sha> \
  --python /usr/bin/python3.12 \
  --output /srv/qualification/runtime-install-report.json

ai-agent-runtime deployment verify-install \
  --report /srv/qualification/runtime-install-report.json \
  --output /srv/qualification/runtime-install-verification.json

ai-agent-runtime deployment prepare-instance-links \
  --install-report /srv/qualification/runtime-install-report.json \
  --agent-instance-id agent_01 \
  --instance-link /srv/ai-agent-runtime/instances/agent_01 \
  --runtime-link /usr/local/bin/ai-agent-runtime@agent_01 \
  --wrapper-link /usr/local/bin/codex-wrapper@agent_01 \
  --output /srv/qualification/agent_01-link-preparation.json
```

An upgrade repeats this command with a new release ID. The result is
`INSTALLED_NOT_ACTIVATED`; activation remains a separate audited deployment step, so
rollback can continue to reference the prior directory. The installer creates the
venv at its final unique release path because Python entrypoint shebangs are not
relocatable. A private `.installing` marker exists only while that new directory is
being populated; failure removes that new directory, while success fsyncs it and
removes the marker. Merely installing a directory never activates it.

Install report schema v2 also records a sorted SHA-256 manifest for every Runtime
distribution file named by the wheel's installed `RECORD`, including the daemon,
wrapper and both qualification entrypoints. `verify-install`, instance-link
preparation and deployment audit re-hash that manifest; retaining the original wheel
while modifying installed Python code or a harness therefore blocks activation.

`prepare-instance-links` is intentionally limited to an absent instance. It creates
one instance link to the verified release and two stable, instance-named entrypoints
through that link; it refuses to replace or upgrade any existing path. Its result is
`PREPARED_NOT_STARTED`: no service is started/restarted and no production cutover is
authorized. For an existing instance, install and verify the new release first, then
switch only its stable release symlink with a separate, audited command:

```bash
ai-agent-runtime deployment switch-instance-release \
  --old-install-report /srv/qualification/runtime-install-old.json \
  --new-install-report /srv/qualification/runtime-install-new.json \
  --agent-instance-id agent_01 \
  --instance-link /srv/ai-agent-runtime/instances/agent_01 \
  --runtime-link /usr/local/bin/ai-agent-runtime@agent_01 \
  --wrapper-link /usr/local/bin/codex-wrapper@agent_01 \
  --intent /srv/qualification/agent_01-switch-intent.json \
  --result /srv/qualification/agent_01-switch-result.json
```

Both install reports must still verify against their wheels and installed files. The
current instance link must point exactly at the old release; stable entrypoints must
route through it. The command writes an immutable intent before an atomic symlink
replacement, then verifies both entrypoints and writes an immutable result. A failed
post-check restores the old link. It never restarts services, touches live tmux/child
processes, or authorizes production cutover. Existing processes keep their executable;
the operator must separately qualify and restart the daemon/Agent as appropriate.
If the process is interrupted between intent and result, inspect the link and service
state before any retry. To reverse a completed switch, run the same command with
old/new install reports exchanged and fresh intent/result paths. Keep both release
directories and reports until the rollback window closes.

Capture the effective, secret-free deployment values in a digest-protected
`ai-agent-runtime.deployment-manifest.v1` artifact. Each `--instance` file is a
secret-free JSON object containing one `agent_instances` entry described below.
Build and audit the artifact before staging:

```bash
ai-agent-runtime deployment manifest \
  --target staging \
  --runtime-install-report /srv/qualification/runtime-install-report.json \
  --qualification-manifest /srv/qualification/qualification-manifest.json \
  --rollback-manifest /srv/qualification/rollback-manifest.json \
  --instance /srv/qualification/instances/agent_01.json \
  --output /srv/qualification/deployment-manifest.json

ai-agent-runtime deployment audit \
  --manifest /srv/qualification/deployment-manifest.json \
  --output /srv/qualification/deployment-audit.json
```

The deployment manifest has an explicit `target` (`staging` or `production`) and
pins the verified wheel/install report, `qualification_manifest`, and
`rollback_manifest`. Each AgentInstance descriptor includes the exact
`runtime_executable`; its captured Runtime service must expose `exec_start` as the
four-item argv list `[runtime_executable, "--config", config_path, "serve"]`. The
auditor resolves both instance-named Runtime/wrapper paths back to the executables in
that install report. The qualification tuple's `runtime_release` and
`runtime_wheel_sha256` must also equal the install report's exact `release_id` and
wheel digest, so evidence cannot be reused for a different wheel built with the same
semantic version or release label. For `staging`, the
qualification manifest must be structurally valid and bound to the exact tuple, but
its only permitted errors are missing evidence gates: those are the tests staging is
being created to run. Existing invalid/tampered evidence still blocks deployment.
For `production`, every gate must already be `QUALIFIED`. Both targets require the
rollback verifier to confirm that the exact legacy inputs are unchanged and their
declared control period is active. This separation prevents a cyclic requirement to
produce real staging evidence before a staging instance can exist.

Each `agent_instances` entry identifies its Runtime config, exact Telegram checkout,
instance-named wrapper, forum chat and one-way working-bot token fingerprint. It also
contains captured effective Runtime service properties (`Type=notify`,
`Restart=always`, 2-second restart, 30-second watchdog, `PrivateTmp=false`,
`Accept=false`). `PrivateTmp=false` is an execution invariant rather than a relaxed
default: Runtime verifies the exact wrapper cwd and executable through `/proc`, and
the target kernel denies those cross-namespace symlinks from a private `/tmp` mount.
Each AgentInstance instead uses a mode-0700 `TMUX_TMPDIR` below its user runtime
directory, so tmux socket isolation does not depend on a private `/tmp` namespace.
The Runtime service unit must set `RuntimeDirectoryPreserve=yes`, because a
full `systemctl stop` must not remove `TMUX_TMPDIR` while an upstream-owned tmux
server and Codex child remain alive. `restart` alone is insufficient: systemd
preserves the directory for restarts but removes it on an ordinary stop.
The manifest also captures Telegram service properties (`KillMode=process`,
exact wrapper path, Codex autoupdate disabled). The audit re-hashes the real Codex binary, reads the pinned Git
HEAD, verifies the complete qualification manifest, checks namespace/path/forum/bot
uniqueness, and performs no mutations. `READY_FOR_STAGING` explicitly does not
authorize production cutover.

After starting a staging instance, run the read-only live diagnostic before any
Telegram prompt test:

```bash
ai-agent-runtime --config ~/.config/ai-agent-runtime/instances/agent_01.toml doctor
```

`doctor` fails unless it can verify the mode/owner of the local socket, obtain a
versioned daemon status response, open the SQLite store read-only and pass
`quick_check`, match the real Codex digest and version, revalidate every effective
profile fingerprint, parse both upstream config/state files, and prove exact
binding/topic/active-tmux correlation. Its report includes
`mutations_performed=false`; it neither repairs drift nor creates missing state.

Profile fingerprints identify the configured profile directory and the safe
owner/type/mode of `auth.json`; they deliberately do not include credential
content, size, inode, or mtime. Normal credential rotation therefore keeps the
same profile identity. On the first daemon start after upgrading from the legacy
metadata fingerprint, Runtime compare-and-set migrates effective profile
attestations. A live instance's launch attestation is migrated only while the
ordinary applied-evidence collector can again prove its exact process, tmux,
model, reasoning, binary and rollout identity; no wrapper or Codex process is
signalled or replaced.

# Migration M0 inventory

Before switching `TELEGRAM_CODEX_BIN`, create a read-only baseline artifact for
each AgentInstance. The command records topic routing, safe effective-profile
metadata, upstream tmux state and non-content live pane identities; it never
copies bot tokens, Codex credentials, prompts or transcript contents.
When `tmux_tmpdir` is explicit in the Runtime config, inventory queries only that
AgentInstance's validated tmux socket; an absent or unsafe isolated socket never
falls back to another bot's default tmux server. A legacy shadow config without
an explicit selector continues to observe the legacy default server.

```bash
ai-agent-runtime --config /etc/ai-agent-runtime/prod.toml inventory export \
  --cus-control-store /var/lib/cus/runtime-control-v1.json \
  --output /var/lib/ai-agent-runtime/migration/pre-cutover.json
```

For the first migration from the legacy project-scoped CUS store, use its
settings file instead of `--cus-control-store`:

```bash
ai-agent-runtime --config /etc/ai-agent-runtime/prod.toml inventory export \
  --legacy-project-settings \
    /var/lib/ai-session-control-center/project_settings_v2.json \
  --output /var/lib/ai-agent-runtime/migration/pre-cutover-from-legacy.json
```

The two source options are mutually exclusive. The legacy reader copies only
the project id/path and account-profile slug, provider, model and reasoning;
tokens, credentials and unrelated legacy fields are omitted. Because the old
store is project-scoped rather than topic-scoped, any legacy record whose cwd
is shared by multiple topics is marked `LEGACY_PROJECT_SCOPE_AMBIGUOUS` and
cannot be imported until the operator assigns a profile to each topic.

The destination is created atomically with mode `0600` and is never
overwritten. Store it in the change record and apply the operator's detached
signature outside the repository. After cutover, export a new artifact and
compare the stable routing fields:

```bash
ai-agent-runtime --config /etc/ai-agent-runtime/prod.toml inventory compare \
  --baseline /var/lib/ai-agent-runtime/migration/pre-cutover.json \
  --candidate /var/lib/ai-agent-runtime/migration/post-cutover.json \
  --binding-plan /var/lib/ai-agent-runtime/migration/binding-plan-resolved.json \
  --output /var/lib/ai-agent-runtime/migration/acceptance.json
```

Exit status `1` blocks acceptance when a topic is lost, added, rebound, renamed,
duplicated, malformed, or leaves orphan upstream tmux state. Multiple topics
sharing one canonical cwd are preserved as separate source refs and reported
explicitly; they are never merged automatically.

With `--binding-plan`, the read-only comparison also requires each `READY`
binding's exact ID, Project ID, canonical cwd and account/model/reasoning profile
in both the post-cutover CUS desired snapshot and Runtime local effective spec.
The plan must be accepted, digest-valid and tied to the baseline inventory;
missing, ambiguous or drifted records block acceptance without printing secrets.
Before the reviewed binding plan exists, omit this option for the preliminary
name/cwd/routing comparison; that preliminary result is not final cutover evidence.

Generate a non-mutating Runtime Binding plan only after the baseline artifact
has been reviewed. A managed topic is marked `READY` only when one exact CUS
binding supplies its account, model and reasoning; incomplete data remains
`BLOCKED` and no default profile is invented. Non-Codex topics remain
`UNMANAGED`.

```bash
ai-agent-runtime --config /etc/ai-agent-runtime/prod.toml inventory plan-bindings \
  --inventory /var/lib/ai-agent-runtime/migration/pre-cutover.json \
  --generation-id migration-2026-10-03 \
  --output /var/lib/ai-agent-runtime/migration/binding-plan.json
```

Topics that have already been deleted from the Telegram forum are not silently
dropped. First retain the blocked base plan for review. An owner may then provide a
digest-protected `ai-agent-runtime.binding-migration-decisions.v1` artifact bound to
that exact inventory and base-plan digest. Each decision must identify the exact
source ref, binding ID, thread ID and topic name, and use
`EXCLUDE_DELETED_TOPIC` with a recorded reason and approval timestamp. Rebuild the
plan with:

```bash
ai-agent-runtime --config /etc/ai-agent-runtime/prod.toml inventory plan-bindings \
  --inventory /var/lib/ai-agent-runtime/migration/pre-cutover.json \
  --generation-id migration-2026-10-03 \
  --decisions /var/lib/ai-agent-runtime/migration/owner-decisions.json \
  --output /var/lib/ai-agent-runtime/migration/binding-plan-resolved.json
```

The resolved rows are marked `EXCLUDED`, carry the decision digest, and do not
create Runtime Bindings. This is a migration exclusion, not destructive cleanup of
legacy files. Legacy `topic_config.json` and CUS artifacts remain available for
rollback until the control period ends.

Stage the **existing** topic configuration in the new AgentInstance's separate
private directory before starting its upstream service. This creates only a new
`0600` file; it never edits or replaces the legacy bot's live config. The source
file must still match the baseline digest, and the resolved plan is replayed
against the exact Owner decision artifact before an excluded topic is omitted:

```bash
ai-agent-runtime inventory stage-topic-config \
  --baseline /var/lib/ai-agent-runtime/migration/pre-cutover.json \
  --binding-plan /var/lib/ai-agent-runtime/migration/binding-plan-resolved.json \
  --decisions /var/lib/ai-agent-runtime/migration/owner-decisions.json \
  --target /srv/telegram-ai-agent-new/topic_config.json \
  --output /var/lib/ai-agent-runtime/migration/topic-config-stage.json
```

The target and report parents must already be owned by the service user with
mode `0700`, and neither target nor report may exist. The command preserves all unknown
fields and every retained topic's name, cwd, engine and exec mode. Only exact
`EXCLUDED` thread keys leave the staged copy; the source remains untouched.
Its result is `STAGED_NOT_ACTIVATED`, not authorization to start production.
If the source changes during staging, leave that candidate inactive, refresh M0
and the Owner-bound plan, then stage to a new target path. Do not overwrite a
candidate in place or delete it without a separate reversible cleanup manifest.

The plan is evidence, not an apply command. Import it into CUS with the separately
versioned `cus-runtime-migration-import` tool. Run that tool without `--commit` first;
only a reviewed, digest-valid, unblocked plan can be committed. CUS then sends one
authoritative Runtime snapshot, keeping non-Codex entries explicitly `UNMANAGED`
instead of rewriting their Telegram Agent engine.

When a deployment explicitly chooses a fresh-context cutover, importing pre-cutover
Codex sessions is not required. Keep the legacy tmux state and session files as
rollback inputs, but do not carry old tmux state into the new AgentInstance or
reattach a legacy pane. At the maintenance window, first establish that no topic
is BUSY, then verify the new namespace has no live legacy tmux session name or
state entry that could auto-resume an old session. Do not terminate active work or
delete state/files to achieve this; a collision blocks cutover until a safe,
reviewed migration step resolves it. The first managed message in each retained
topic must create a new Codex session. Compare the post-cutover inventory to prove
topic names, canonical cwd and routing have not drifted. Use the
`--binding-plan` comparison to verify CUS desired and Runtime effective profile
values, then separately inspect process evidence before calling a live instance
Applied; a local spec alone never proves actual Codex parameters.

Use the read-only fresh-context guard against the **target** AgentInstance config
after staging its separate config/state namespace, immediately before cutover:

```bash
ai-agent-runtime --config /etc/ai-agent-runtime/prod.toml inventory fresh-context-preflight \
  --baseline /var/lib/ai-agent-runtime/migration/pre-cutover.json \
  --binding-plan /var/lib/ai-agent-runtime/migration/binding-plan-resolved.json \
  --output /var/lib/ai-agent-runtime/migration/fresh-context-preflight.json
```

It requires an explicit isolated `tmux_tmpdir`, target topic config and tmux
state paths distinct from the preserved legacy files, exactly the retained plan's
topic keys and names/cwd/engine/mode in the target config, an empty target state,
an absent or empty non-symlink session view when configured, no live pane
in the target tmux server, and a digest-valid plan for the same AgentInstance/forum.
A missing isolated socket is acceptable before first launch;
an unreadable or ambiguous socket is not. This command never clears state or kills
tmux, and its `production_cutover_authorized=false` result is not a substitute for
the full qualification report. Repeat it if any config/state/tmux input changes.

The following provenance plan/import remains available **only** for a future
explicitly approved legacy-continuation migration, after its own storage and
rollout qualification. It is not part of the current production checklist:

```bash
ai-agent-runtime inventory plan-session-provenance \
  --inventory /var/lib/ai-agent-runtime/migration/pre-cutover.json \
  --binding-plan /var/lib/ai-agent-runtime/migration/binding-plan.json \
  --output /var/lib/ai-agent-runtime/migration/session-provenance-plan.json

ai-agent-runtime --config /etc/ai-agent-runtime/prod.toml provenance import \
  --plan /var/lib/ai-agent-runtime/migration/session-provenance-plan.json
```

Planning is read-only. It accepts a session only when the inventory and binding-plan
digests match, the binding is `READY`, upstream state provides one session ID and
rollout path, and the sole exact live tmux pane proves its current Codex account.
That account proof binds pane PID, Linux process starttime/UID, canonical process cwd,
qualified real-Codex binary digest, and a safe direct-child `CODEX_HOME` beneath the
configured profiles root. The observed account must equal the reviewed binding-plan
account. The owned rollout's first `session_meta` record must also prove the exact
session ID and canonical cwd. The artifact records only sanitized process identity
and path/device/inode/size metadata, never arbitrary environment, credentials, prompt,
or transcript content. Import revalidates the live process identity, executable,
cwd and `CODEX_HOME`, then re-opens every rollout and requires unchanged filesystem
identity before atomically inserting immutable binding/account ownership. If the
legacy process exits or changes between planning and import, the import fails closed;
repeating the same digest-bound import while evidence remains valid is idempotent.

Even without an import, older picker entries may remain visible. Unknown legacy
sessions are preserved on disk but are never assigned a binding from cwd or
account; `/resume` must fail closed before real Codex starts. New sessions created
after cutover acquire normal binding/account provenance. If an optional legacy
continuation migration is later approved, regenerate its inventory and plan
whenever upstream state or storage topology changes before importing.

The same inventory command is the M1 shadow-mode observer when it runs before the
`TELEGRAM_CODEX_BIN` switch. Every topic receives one conservative confidence:

- `OBSERVED_VERIFIED` requires one exact upstream state entry and one live,
  non-dead tmux pane with the expected session name and canonical cwd;
- `OBSERVED_PARTIAL` means topic configuration is valid but complete live identity
  is not present (including a stopped topic);
- `UNKNOWN` means identity, cwd, state, or pane evidence is malformed, conflicting,
  or ambiguous.

`OBSERVED_VERIFIED` by itself does not prove account, `CODEX_HOME`, model, reasoning,
or applied revision. A separate `live_codex_process` inventory field may prove the
legacy account for provenance planning; this does not upgrade shadow confidence or
claim model/reasoning/revision. Full applied facts require Runtime process evidence after
cutover; shadow observation never promotes cwd or process name into AppliedSpec.

Session-storage candidates follow a separate manifest-and-stage workflow documented
in `qualification.md`. Staging is copy-only and does not activate a topology; no
deployment script may replace a non-empty profile directory with a symlink or delete
the original data.

## Preserve the legacy rollback path

Before production cutover, pin the exact clean legacy checkout, the legacy wrapper
binary, and every non-secret file needed to restore the old system. The command below
only hashes and records existing inputs. It does not copy secrets, stop services,
restore files, or authorize cleanup:

```bash
ai-agent-runtime rollback manifest \
  --legacy-repo /srv/rollback/telegram-ai-agent-private \
  --legacy-service telegram-ai-agent-private.service \
  --legacy-codex-bin /srv/rollback/bin/legacy-codex-wrapper \
  --artifact service-unit=/srv/rollback/systemd/telegram-ai-agent-private.service \
  --artifact overlay-pth=/srv/rollback/python/00-cus-telegram-overlay.pth \
  --artifact pre-cutover-inventory=/srv/qualification/pre-cutover.json \
  --control-period-end 2026-11-04T00:00:00Z \
  --output /srv/qualification/rollback-manifest.json

ai-agent-runtime rollback verify \
  --manifest /srv/qualification/rollback-manifest.json \
  --output /srv/qualification/rollback-verification.json
```

The Git origin is stored only as a fingerprint because remote URLs can contain
credentials. Secret-shaped paths, symlinks, dirty tracked worktrees, tampered files,
and changed Git commits fail closed. `VERIFIED_NOT_DRILLED` means the inputs still
exist unchanged; it is not rollback-drill evidence and does not authorize a restore.
The reserved `legacy_codex_bin` input may be the actual launcher symlink: its link
inode/target and the resolved regular binary inode/digest are both pinned. Symlinks
remain forbidden for every operator-supplied additional artifact.
An immutable release checkout may be owned by root; Git inspection uses a command-local
`safe.directory` value for that one already validated absolute worktree path and never
modifies global or repository Git configuration.

The private GitHub repository, local legacy checkout, overlay, old virtual
environment, and server files remain protected rollback inputs until all of the
following are true: the staging rollback drill passed, production completed its
declared control period, the new session-storage topology no longer needs rollback,
and the Owner made a separate cleanup decision. The manifest intentionally reports
`cleanup_authorized=false`; deletion is never an automatic deployment step.
