### 11.4 Method translation table

The table below maps each removed Harness-era method to the new session-first
equivalent. These mappings are rewrite guidance only.

These mappings are not implementation aliases. Old run-scoped Harness methods,
run-topic APIs, process-local abort/follow-up/pending resolvers, run-level stream
events, `runId`-only resume calls, thread-selection helpers, and thread locks do
not back v1 by themselves. Removed APIs are removed; v1 behavior is provided only
by the session, signal, lease, receipt, and channel bridge contracts described in
§3, §4.2, §5.7, §5.8, and §14. Opening an existing thread enters the §5.3
session resolver, including resource-mismatch masking, closing-row rejection,
closed-session reopen, deterministic `sessionId` conflict checks, and direct-ID
current-owner corruption checks.

**`harness.sendMessage(...)`**

v1 `Harness` + `Session` : `session.signal({ type: 'user-message', ... })` by
default; use `session.queue(...)` only when the caller needs a sequential
standalone turn. See §3 and §4.2.

**`harness.sendSignal(...)`**

v1 `Harness` + `Session` : `session.signal(...)`. Current Mastra already has
agent/thread-scoped signals through `Harness.sendSignal(...)` and
`Agent.sendSignal(...)`; v1 moves that admission boundary onto the resolved
Session so resource/session ownership, operation receipts, and result lookup are
owned by the same runtime object.

**`harness.getCurrentThreadId()`**

v1 `Harness` + `Session` : `session.threadId`

**`harness.createThread(...)`**

v1 `Harness` + `Session` :
`harness.session({ threadId: { fresh: true }, resourceId })` for product
lifecycle. Current core also exposes a top-level `createThread()` method, and
MastraCode uses it for pending/new-thread flows; do not migrate only
`harness.memory.createThread(...)` call sites. V1 creation enters the §5.3
session resolver and creates the backing thread as part of session creation,
with resource/session ownership and close/reopen semantics owned by
`SessionRecord`.

**`harness.switchThread({ threadId })`**

v1 `Harness` + `Session` :
`harness.session({ threadId, resourceId })` through the §5.3 resolver, not by
assigning a process-local current thread.

**`harness.getState()`**

v1 `Harness` + `Session` : `session.getState()` for
caller intent only. This is not an implementation alias: legacy reads return a
process-local Harness snapshot, while v1 reads a detached snapshot of the latest
committed `SessionRecord.state` (§4.2/§5.1/§6.2).

**`harness.setState(updates)`**

v1 `Harness` + `Session` :
`session.setState(updates)` for object-form caller intent. This is not an
implementation alias: legacy writes shallow-merge into process-local Harness
state and emit immediately, while v1 writes validate JSON/lossless state, commit
through `SessionRecord.state` under the lease/version boundary, emit
`state_changed` only after durable success, and also support local/tool
functional `setState(prev => next)` (§5.1/§5.8/§10.2).

**`harness.switchMode({ modeId })`**

v1 `Harness` + `Session` :
`session.switchMode({ mode })`

**`harness.switchModel({ modelId })`**

v1 `Harness` + `Session` : `session.switchModel({ model })`.

**`harness.switchModel({ modelId, scope: 'global' })`**

v1 `Harness` + `Session` : _removed_ — use `session.switchModel(...)` for
session-level changes. Future session bootstrap defaults come from
`HarnessConfig.defaultModelId`; see §9.

**`harness.setSubagentModelId({ agentType, modelId })`**

v1 `Harness` + `Session` : `session.setSubagentModel(...)`. Current MastraCode
stores thread-scoped subagent model choices through Harness helpers and global
defaults through settings. V1 must persist thread/session-scoped overrides in
`SessionRecord`-owned configuration, apply them when the built-in subagent tool
creates child sessions, and keep global MastraCode settings as bootstrap
defaults only.

**`harness.getSubagentModelId({ agentType })`**

v1 `Harness` + `Session` : `session.getSubagentModel(...)`. It resolves the
effective subagent model from session override, configured/global default, and
mode mapping without reading raw legacy thread metadata.

**`harness.subscribe(listener)`**

v1 `Harness` + `Session` :
`session.subscribe(listener)` (or `harness.subscribe` for cross-session) for
caller intent only. This is not an implementation alias: legacy listeners
receive the legacy `HarnessEvent` union, including legacy-only display
notifications, while v1 subscriptions receive only the projected §10
`HarnessEventV1` surface with the §10.1 base envelope, the §10.2 display-event
exclusion, and the §10.5 ring-buffer / `Last-Event-ID` replay rules for session
SSE.

**`harness.getDisplayState()`**

v1 `Harness` + `Session` :
`session.getDisplayState()`

**`harness.abort()`**

v1 `Harness` + `Session` : _removed_ — cancellation
is not a session concern; see §3.

**`harness.steer(...)`**

v1 `Harness` + `Session` : _removed_ — use
`session.signal(...)` for new content. Abort-first behavior remains an
agent-layer concern; see §3.

**`harness.followUp(...)`**

v1 `Harness` + `Session` : `session.signal(...)` by
default, or `session.queue(...)` for sequential turns; see §3 and §4.2.

**MastraCode local queued follow-up FIFO**

v1 `Harness` + `Session` : `session.queue(...)` for the Ctrl+F / hold-until-idle
pattern. Current MastraCode has two local queues to audit: Harness follow-up
buffering and TUI queued actions/slash commands that drain around agent
lifecycle. V1 stores only resolved operation intent in `SessionRecord.pendingQueue`
and admits the drained item through the same session-owned signal boundary as
other work. Raw local slash commands and UI actions are not automatically durable
Harness queue rows; the MastraCode controller must first resolve them to stable
operation intent, JSON-safe args, persisted attachments, and an explicit target
operation kind.

**`harness.isRunning()`**

v1 `Harness` + `Session` : `session.isBusy()` for
caller intent only. This is not an implementation alias: legacy checks a
process-local `AbortController`, while v1 reads the owning session's
live/reconciled idle boundary from `currentRun`, canonical pending items, and
`SessionRecord.pendingQueue` (§4.2).

**`harness.getFollowUpCount()`**

v1 `Harness` + `Session` : `session.getQueueDepth()`
only when the caller meant durable queued standalone turns. Legacy follow-up
count reads an in-memory retry/follow-up buffer; v1 queue depth is
`SessionRecord.pendingQueue.length` under the active session owner (§4.2/§5.1).

**`harness.getCurrentRunId()`**

v1 `Harness` + `Session` :
`session.getCurrentRunId()` for caller intent only. V1 reconciles live agent
state with `SessionRecord.currentRun` after hydration and returns `null` when no
pending item or agent-layer liveness proves a still-live run (§4.2/§5.7).

**`harness.getCurrentTraceId()`**

v1 `Harness` + `Session` :
`session.getCurrentTraceId()` for caller intent only. V1 follows the same
live-or-reconciled `SessionRecord.currentRun` projection as `getCurrentRunId()`
(§4.2/§5.7).

**`harness.getTokenUsage()`**

v1 `Harness` + `Session` : `session.getTokenUsage()`
for caller intent only. Legacy returns a process-local accumulator best-effort
mirrored to thread metadata; v1 returns the session-owned token-usage projection
hydrated from `SessionRecord.tokenUsage`, with old `thread.metadata.tokenUsage`
only as §11.2 bootstrap/import input.

**`harness.getWorkspace()`**

v1 `Harness` + `Session` : _removed_ from the public Harness class. Runtime
callers use `session.getWorkspace()` / `session.resolveWorkspace()` through the
owning session resolver (§2.7/§4.2). Out-of-session scripts that need a shared
workspace use a deployment-owned admin/runtime API, not a product Harness
method.

**`harness.resolveWorkspace()`**

v1 `Harness` + `Session` : _removed_ from the public Harness class.
`per-resource` and `per-session` workspaces are resolved from a `Session` or
`HarnessRequestContext`, not from a global harness cache. Shared workspace
materialization outside a session is deployment/admin infrastructure, not a
portable Harness lifecycle method.

**`harness.destroyWorkspace()`**

v1 `Harness` + `Session` : _removed_ from the public Harness class. Shared
workspace teardown is part of `harness.shutdown()`, per-session workspace
teardown follows `session.close()` / the explicit close helper, and
per-resource teardown belongs to an explicit workspace-admin/operator boundary
with the persisted active-session guard from §2.7.

**`harness.destroy()`**

v1 `Harness`: `harness.shutdown()`. Current MastraCode and docs may still call
`destroy()` for legacy heartbeat/workspace cleanup. V1 shutdown is the
Harness-level lifecycle boundary that stops live hooks, drains/release-ready
session runtime state, and coordinates with server shutdown where §13.6 owns the
deployment lifecycle. It is not a session close/delete operation and must not
emit `session_closed` for retained active records.

**`harness.memory.createThread(...)`**

v1 `Harness` + `Session`: `harness.session({ threadId: { fresh: true },
resourceId })`. There is no in-process thread-create method and no resource-only
"latest or create" resolver on the v1 Harness surface (§4.1). Product
controllers that want continue/latest behavior choose a concrete session or
thread before calling Harness. Bare thread row creation for import tooling uses
the optional operator wire route `POST /operator/threads` (§13.2) only, not
product lifecycle.

**`harness.cloneThread(...)`**

v1 `Harness` + `Session`: `session.clone(...)`.
Lower-level thread clone can be used by that implementation to copy committed
history, but it is not the app-facing clone lifecycle operation. Partial-history
fork is deferred from v1 rather than exposed as an alias over thread clone.
Current `Memory.cloneThread(...)` / `Harness.cloneThread(...)` can copy working
memory and observational-memory records; v1 `session.clone(...)` must not copy
OM, active goals, permissions, queues, pending inbox, leases, channel rows, or
runtime state unless a future clone option explicitly owns that behavior.
Cloning a selected non-current thread/session is a product-controller or
operator-history resolution step before `session.clone(...)`; v1 does not expose
`harness.cloneThread({ sourceThreadId })` as a normal lifecycle API.

**`harness.memory.deleteThread(...)`**

v1 `Harness` + `Session`: `session.delete(...)` for normal app lifecycle. A
lower-level thread-delete route is operator/history cleanup only and must run the
§5.5 session-first delete cascade before any physical message/vector cleanup.

**`harness.listThreads(...)`**

v1 `Harness` + `Session`: `harness.listSessions(...)` for lifecycle/navigation,
or lower-level history reads only when the product specifically needs a thread
index.

**`harness.renameThread({ title })`**

v1 `Harness` + `Session`: `session.rename({ title })`.

**`harness.setThreadSetting({ key, value })`**

v1 `Harness` + `Session`: `session.setThreadSetting({ key, value })`.
This is a semantic migration, not a direct metadata write alias. Current Harness
writes arbitrary top-level thread metadata keys; v1 validates `key`/`value`,
writes only `thread.metadata.app[key]`, and keeps canonical Harness-owned fields
such as goals, mode/model selection, OM settings, sandbox/workspace state, and
subagent model overrides on their owning session/storage fields.

**`harness.grantSessionCategory(...)`**

v1 `Harness` + `Session` :
`session.permissions.grantCategory(...)`

**`harness.grantSessionTool(...)`**

v1 `Harness` + `Session` :
`session.permissions.grantTool(...)`

**`harness.setPermissionForCategory(...)`**

v1 `Harness` + `Session` :
`session.permissions.setPolicy({ category, policy })`

**`harness.setPermissionForTool(...)`**

v1 `Harness` + `Session` :
`session.permissions.setPolicy({ toolName, policy })`

**`harness.getObservationalMemoryRecord()`**

v1 `Harness` + `Session` : `session.om.getRecord()`
for caller intent only. This is not an implementation alias: legacy/current
reads may return the raw `ObservationalMemoryRecord`, while v1 returns only the
§4.8 `ObservationalMemorySnapshot` after session/resource/scope verification and
redaction.

**`harness.switchObserverModel(...)`**

v1 `Harness` + `Session` :
`session.om.switchObserverModel({ model })` for caller intent only. This is not
an implementation alias: legacy/current switches may use `{ modelId }`,
process-local state, top-level thread metadata, or pre-commit events; v1 commits
`SessionRecord.observationalMemory` under the session
lease/version/authorization boundary and emits only after durable success.

**`harness.switchReflectorModel(...)`**

v1 `Harness` + `Session` :
`session.om.switchReflectorModel({ model })` with the same caller-intent-only
boundary as observer model switching.

**`harness.registerHeartbeat(...)`**

v1 `Harness` + `Session` : same identifier `harness.registerHeartbeat(...)`;
v1 returns an async unsubscribe function from this call. See the semantic-delta
paragraph below for the v1-tightened validation, lifecycle, and tick rules.

**`harness.removeHeartbeat({ id })`**

v1 `Harness` + `Session` : _removed_ — await the async unsubscribe function
returned by `registerHeartbeat(...)` instead.

**`harness.stopHeartbeats()`**

v1 `Harness` + `Session` : same identifier `harness.stopHeartbeats()`; semantics
tightened per the paragraph below.

**`harness.getModelName()`**

v1 `Harness` + `Session` : _removed_ — no v1
equivalent. `session.getCurrentModelId()` returns an opaque model ID;
applications that need display labels own that catalog and may consult advisory
`harness.listAvailableModels()` when the ID appears there (§4.1, §9).

**`harness.getFullModelId()`**

v1 `Harness` + `Session` : _removed_ — use
`session.getCurrentModelId()` (§4.2).

**`harness.getResolvedObserverModel()`**

v1 `Harness` + `Session` : _removed_ — trivial
composition

**`harness.getSession()`**

v1 `Harness` + `Session` : _removed_ — name collides
with new `Session`

**`harness.selectOrCreateThread()`**

v1 `Harness` + `Session` : _removed_. Core Harness does not pick the latest
thread or session from a `resourceId` alone. Product controllers implement
continue/latest selection by reading session list models, then opening the chosen
`sessionId` or `(threadId, resourceId)` through `harness.session(...)`.

**`harness.setResourceId(...)`**

v1 `Harness` + `Session` : _removed_ — set at session
creation

#### MastraCode transition boundaries

The rows above are core API translation rules. MastraCode product-command
semantics are owned by the §13.4j session controller boundary and tested in
§15.2; this section only records the removed core method mappings.

Documentation migration is versioned with the code cutover. Current MastraCode
docs may describe the legacy `Harness` methods while the released package still
uses them, but v1 docs, examples, and generated API pages must not teach
`selectOrCreateThread()`, `sendMessage(...)`, `setResourceId(...)`, or
`cloneThread(...)` as app-facing APIs. The v1 documentation surface should show
controller selection followed by `harness.session(...)`, then
`session.signal(...)`, `session.queue(...)`, and `session.clone(...)` as defined
above.

Cutover invariant: after v1, MastraCode TUI/headless code routes product commands
through a product-owned session controller, not through removed Harness methods.
Normal product code must not call `harness.switchThread(...)`,
`harness.setResourceId(...)`, `harness.sendMessage(...)`,
`harness.sendSignal(...)`, `harness.cloneThread(...)`,
`harness.setThreadSetting(...)`, or `harness.memory.*` for lifecycle,
admission, queue, settings, navigation, or recovery authority. The controller
may hold local UI state such as staged fresh-session intent or optimistic
rendering, but accepted work, queue order, result settlement, pending inbox,
goal state, subagent ownership, model settings, and recovery are proven only by
Session/Storage rows.

Current MastraCode data that was written through old thread metadata or
process-local helpers is import/bootstrap input only. New v1 writes move each
family to its owner: goals to §4.7, model/mode and subagent choices to
session-owned runtime settings, workspace/project/sandbox state to the workspace
and session owners, session-local OM settings to
`SessionRecord.observationalMemory`. Current MastraCode goal judge memory is not
stored inside legacy `GoalState`; it lives in separate MemoryStorage threads
named from the current thread and goal id. A v1 importer must either discover
and link those legacy judge-memory threads into the §4.7 goal/judge-memory owner,
or explicitly mark them as not imported. It must not pretend a
`GoalState.judgeMemory` legacy field exists. Global MastraCode settings remain
bootstrap defaults, and raw OM records remain MemoryStorage-owned. `SignalsPubSub`,
Unix sockets, pending UI maps, current core `HarnessConfig.threadLock`, and
MastraCode filesystem `threadLock` remain old-runtime/live controller aids only;
storage-backed admission/result evidence and the §5.8 lease/claim contract are
the v1 durability and exclusivity boundaries. External
hooks that previously received `session_id = threadId` require a
compatibility/versioning step before that field can mean v1 `sessionId`; new
payloads should expose both `harness_session_id` and `thread_id`. Silent
semantic retargeting of `session_id` is not a valid migration because existing
hooks may use it as the durable transcript/thread key.

The permission rows above are caller-intent mappings, not implementation
aliases. Current grant and policy helpers are implementation input only; they do
not satisfy v1 unless they commit `SessionRecord.sessionGrants` /
`SessionRecord.permissionRules` under the active session lease/version boundary
and remote callers first pass the §13.2 `harness:permission-admin` capability.
MastraCode already has a `SessionGrants` class with mutable in-process sets, and
also re-declares permission policy types that overlap core Harness types. Those
names are migration hazards, not reusable durable shapes: v1 must translate them
into the plain JSON `SessionRecord.sessionGrants` / `permissionRules` contract
instead of storing class instances or preserving duplicate type definitions as a
second permission vocabulary.
Revokes, explicit `ask`, pending-approval separation, subagent
non-inheritance, and deny-before-grant/yolo precedence remain owned by §4.2 and
verified by §15. Runtime gate placement and additive approval-source
composition remain tracked separately by HC-320 and HC-322.

The heartbeat rows above are semantic-delta guidance, not name renames. V1 keeps
the process-local `HeartbeatHandler` type, `heartbeatHandlers` config field, and
`intervalMs` tick-interval field name, but the retained identifiers are an
intentional breaking behavior boundary rather than a compatibility layer. V1
tightens behavior on the same surface: `immediate` defaults to `false`;
duplicate configured or runtime IDs reject
instead of silently skipping or replacing a live handler; invalid IDs,
non-positive `intervalMs`, and non-function handlers reject at init or
`registerHeartbeat(...)`; slow async ticks skip later ticks for the same
heartbeat ID rather than overlapping; `registerHeartbeat(...)` returns an async
unsubscribe function (legacy `removeHeartbeat({ id })` is removed); and the
async unsubscribe, `stopHeartbeats()`, and `harness.shutdown()` all await any
in-flight handler before calling the optional `shutdown` hook. These
process-local heartbeats remain separate from durable recurring work, which
uses `HarnessWakeupItem` rows as described in §11.5, §14.6, and §15.

**Skill-related current surfaces.** The current Harness has no public
`useSkill(...)`, `listSkills()`, `getSkill(...)`, `refreshSkills()`, or
code-registered `HarnessConfig.skills` equivalent. Those APIs are new v1
Session/Harness surfaces owned by §4.6, not method aliases over the legacy
`@mastra/core/harness` export.

**Core `Workspace.skills` / `WorkspaceSkills`**

v1 boundary: Implementation material for the workspace side of §4.6 resolution.
V1 session skill methods delegate to the resolved session workspace's configured
`WorkspaceSkills` source/resolver for workspace-owned skills, after
code-registered skills have taken precedence.

**Model-facing `skill` activation tool**

v1 boundary: model-facing helper. If exposed for a v1 session, activation must
resolve through the same dual-source catalog as `session.useSkill(...)` so host
callers and the model do not see different skill availability. It injects
instructions into the model turn; it is not a replacement for the caller-facing
`session.useSkill(...)` operation.

**Model-facing `skill_search` / `skill_read` tools**

v1 boundary: workspace-owned model helpers. They may continue to
search/read the workspace skill source because code-registered skills do not
have a required `references/`, `scripts/`, or `assets` filesystem layout. They
must omit or gracefully reject code-registered skills rather than implying those
skills are unavailable to `session.useSkill(...)`.

**`SkillsProcessor` / `SkillSearchProcessor`**

v1 boundary: processor-framework implementation input. If a v1 agent keeps them for
eager or on-demand model prompting, any catalog or activation claim must be
aligned with the §4.6 session resolver; otherwise the product must document them
as workspace-only processor behavior outside the public Harness skill API.

**Current processor `listSkills()` server exposure**

v1 boundary: current read-model input only. V1 public skill reads are
`session.listSkills()` / `session.getSkill(...)` for the resolved session
catalog. Code-registered deployment skills are visible through that session
catalog after resolution; there is no `harness.listSkills()` or
`session.skills.*` public surface.
