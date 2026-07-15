### 11.6 Current-vs-v1 status ledger

This ledger classifies every `class`, `interface`, and `type` name declared
across non-example `sections/` files against the pre-v1 Mastra codebase so
implementers and reviewers can tell whether a name is reused, replaced, or new
at the clean Harness v1 cutover. The owning sections remain authoritative; this
is an index, not a redefinition and not a compatibility contract.

Status values:

- `reused-current` — the name exists in pre-v1 Mastra code with the same
  intended shape and may keep the same identifier after the breaking cutover.
- `changed-v1` — the name exists in pre-v1 Mastra code, but v1 semantics differ;
  the new shape replaces the old one at the breaking boundary.
- `old-implementation-input` — pre-v1 plumbing that may inform an implementation
  rewrite but must not remain as a public alias, shim, fallback, or alternate
  runtime path.
- `new-v1` — no current implementation under that name; declared by the v1
  contract or its owning wire/storage section.
- `deferred` — declared in the spec for context but intentionally not
  shipped in v1; the §11.5 and §15.3 deferrals are concept-level and do not
  presently flag any of the 256 declared names by identifier. The status is
  retained so future deferrals can land here without re-categorisation.

Export-path convention: every public runtime name exports from
`@mastra/core/harness` after cutover unless its spec section explicitly assigns
it to a wire-only or storage-only owner. Wire DTOs (§13.3) and storage-only
records (§5.1, §5.2) stay with their owning sections and are not re-exported
through the runtime entry; clients reach them via the §13.3 wire surface or
§5.2 `HarnessStorageDomain` instead.

#### Current main source snapshot

This ledger still classifies spec names against pre-v1 Mastra collision risk. It
is not a compatibility promise and it must not claim that target v1 surfaces are
already shipped on `main`. Entity vocabulary and cutover order:
[§0 Mental model](../../00-mental-model.md). Duplicate traps: §11.6e.

Current-source surfaces present on Mastra `main`:

- Current `Harness` exists at `packages/core/src/harness/harness.ts:211` and is
  a singleton, thread-first runtime. Its live state includes
  `currentThreadId`, `resourceId`, `currentRunId`, display state, pending
  approval/question/plan resolvers, process-local follow-up queues, and a single
  agent-thread subscription. It exposes agent/thread-oriented
  `sendSignal(...)`, `sendMessage(...)`, `followUp(...)`, `steer(...)`, thread
  switching, display state, and process-local helpers. It is not the v1
  `Session` surface.
- Current `Agent.sendSignal(...)` and `AgentThreadStreamRuntime.sendSignal(...)`
  provide the agent-layer foundation for signals: stable `signal.id`, active-run
  queue/drain, idle wake, thread reservation, same-thread PubSub fan-out, and
  optional persisted signal messages for explicit persist paths. Normal active
  delivery and idle wake remain live runtime paths. The active-run maps,
  idle-signal queues, and prepared-run maps are process-local; persisted signal
  messages, when present, are not by themselves v1 operation result evidence.
- Current `AgentChannels` lives under `packages/core/src/channels` as a
  per-agent live Chat SDK pipeline. It maps provider events to Mastra memory
  threads, calls `agent.sendSignal(...)`, resumes tool approvals with
  `agent.approveToolCall(...)` / `agent.declineToolCall(...)`, consumes live
  agent streams, and posts/edits through adapter thread handles. Its
  `MastraStateAdapter` persists subscription metadata in `MemoryStorage`
  thread metadata, but cache, locks, lists, queues, and event dedupe are
  process-local.
- Current `ChannelsStorage` exists as a storage domain for provider
  installation/config rows (`ChannelInstallation`, `ChannelConfig`) with an
  abstract base and current in-memory implementation. It owns
  credentials/provisioning/config persistence for channel providers; it does not
  provide Harness `ChannelBinding`, inbox, outbox, action-token, action-receipt,
  provider callback binding, wakeup, or claim/renew ledgers.
- Current server/client signal routes exist under the agent surface
  (`POST /agents/:agentId/signals` and client SDK agent resources), not under a
  `/harness/:name/sessions/:sessionId/signals` route family.
- Current `HarnessSession` is a snapshot type returned by
  `Harness.getSession()`, not a durable session runtime. Current thread
  lifecycle remains `createThread`, `switchThread`, `cloneThread`,
  `memory.deleteThread`, `listThreads`, and best-effort thread metadata writes.
- Current core `HarnessConfig` already exposes `threadLock` callbacks and
  current `Harness` calls them around thread selection, creation, switching, and
  clone. This is a core API, not only a MastraCode helper. V1 must migrate that
  concurrency concern to storage leases/claims with narrower scope instead of
  treating `threadLock` as a UI-only artifact.
- MastraCode constructs a current `Harness` in `mastracode/src/index.ts:552`,
  passes a filesystem `threadLock` unless Unix-socket PubSub is configured, and
  records observability/session hook identity from thread events. Its request
  context currently carries `harness.threadId`, `harness.resourceId`,
  `harness.modeId`, and `harness.harnessId`, not a v1 `sessionId`.
- MastraCode uses `Harness.sendSignal(...)` for immediate Enter/steer behavior,
  `Harness.sendMessage(...)` for local FIFO drain, `system-reminder` signals for
  goals/plan approval, and `SignalsPubSub` / Unix sockets for same-thread live
  process coordination. Those live fan-out paths are not durable admission,
  result, queue, lease, or recovery evidence.
- MastraCode headless and TUI entry points are thread-first today:
  `--thread`, `--clone-thread`, `/thread`, `/threads`, `/new`, `/clone`,
  `/resource`, optimistic pending signal confirmation keyed primarily by
  accepted signal id with text/echo matching as fallback UI cleanup, and
  resource switching all operate through current Harness thread APIs.

Target v1 surfaces not present on current `main`:

- Harness `Session` class and `RemoteSession` / `RemoteSafeSession` SDK surface.
- Session resolver, `SessionRecord`, session lease/CAS storage primitives,
  close/delete/clone lifecycle, durable session queue, and result
  tombstones.
- `/harness/:name/...` server route family, `SignalAdmissionResponse`,
  `SignalResultResponse`, per-signal result lookup routes, SSE recovery around
  session snapshots, and remote session hooks.
- `AgentSignalBoundary.getSignalResult(...)`, admission dedupe at the session
  boundary, and durable operation receipts that survive restart independently of
  the live stream.
- Harness channel durable inbox/action/outbox rows, wakeup workers tied to
  session ownership, and session-scoped channel diagnostics.

The implementation direction is therefore a clean replacement at the v1 cutover:
current signal mechanics are implementation input for the agent boundary, but the
session-first API, storage, route, SDK, and recovery contracts remain v1 work.

#### Implementation composition checklist

Before adding any v1 Harness primitive, implementation review must prove it
composes with the existing Mastra owner instead of recreating a parallel
subsystem:

- `Mastra` remains the process composition root for agents, workflows, storage,
  memory, pubsub, channels, background tasks, workers, observability, and
  workspaces. Harness registration plugs into that root; it does not introduce a
  second application registry.
- Agent execution and active/idle signal delivery build on current
  `Agent.sendSignal(...)`, `Agent.subscribeToThread(...)`,
  `AgentThreadStreamRuntime`, and `agent/signals.ts`. Harness v1 adds
  session-owned admission/result evidence around that boundary; it does not fork
  another live signal runtime.
- Thread and message history stay in the shared `MemoryStorage` domain. Harness
  storage starts at `SessionRecord`, leases, receipts, tombstones, accepted
  signal evidence, attachment metadata, wakeups, and source-specific domain
  extension rows; it must not mirror thread/message logs, channel provider
  configuration, background-task storage, pubsub replay, or blob bytes.
- Channel v1 work reuses Mastra channel provider registration, verification, and
  formatting where those are already canonical. Durable binding, inbox, action,
  and outbox ledgers add the missing recovery proof; they do not replace provider
  configuration storage or live `AgentChannels` behavior for non-Harness agents.
- Worker and recovery loops compose with existing workflow, scheduler,
  background-task, pubsub, and storage-domain contracts. Any new Harness worker
  must own a source-specific durable row and claim/renew contract rather than a
  generic catch-all work table.
- Observational Memory, workspace state, model/mode selection, permissions,
  token usage, goals, and display snapshots use their canonical v1 fields or
  existing Mastra domain owners. Raw thread metadata is import input only, not a
  fallback source of truth after a `SessionRecord` exists.
- `HarnessDisplayStateSnapshotV1` is a v1 persisted read model, not the current
  in-memory display state type under a familiar name. Migration must project
  current display state into the v1 JSON-safe snapshot shape and must not expose
  legacy `display_state_changed` as durability or keep raw component/runtime
  handles in storage.

#### Migration-sensitive current MastraCode surfaces

The following current MastraCode surfaces need explicit rewrite before the v1
cutover. This subsection owns the migration-sensitive behavior inventory; §15.2
turns these rules into focused tests and should not restate alternate product
semantics. They are listed here because they are easy to miss when reading only
the core Harness files:

- `createMastraCode(...)` must stop treating the constructed Harness as one
  mutable current-thread singleton. Startup must create/open a v1 `Session`, pass
  session identity into hooks, analytics, observability attributes, and request
  context, and leave `threadId` as the durable history identifier.
- Headless `--thread`, `--clone-thread`, `--continue`, `--resource-id`, and
  `--title` behavior must be re-expressed through session resolution,
  `session.clone(...)`, session rename, and session result
  lookup. MastraCode may keep these flag names as product-local commands only:
  they must resolve before calling the v1 Session API and must not expose removed
  Harness methods, old-runtime shims, or alternate public Harness entry
  points after cutover. `--thread <title>` lookup fails loudly when more than one
  matching title exists in the resource scope; v1 does not preserve the pre-v1
  duplicate-title fallback.
- TUI commands for `/thread`, `/threads`, `/new`, `/clone`, `/resource`, and
  `/name` must become session lifecycle/navigation commands. Any command that
  displays thread history should clearly distinguish `sessionId` from
  `threadId`.
- MastraCode README/docs pages that describe `/thread`, `/clone`,
  terminal notifications, configuration, plan persistence, sandbox access,
  yolo, model packs, hooks, or headless flags must be updated as part of the
  v1 cutover. They may keep product command names, but they must describe the
  session-first semantics, accepted-operation/result identity, and explicit
  migration behavior rather than the old thread-first Harness API.
- The local Ctrl+F/follow-up queue and optimistic Enter signal path must become
  session-keyed. Pending UI confirmation must key by accepted operation identity
  (`admissionId`, `signalId`, `queuedItemId`) instead of relying on message-text
  matching; text/echo matching may remain only as a UI fallback for legacy stream
  cleanup during cutover.
- `SignalsPubSub` may remain a live fan-out optimization, but v1 correctness
  must come from storage-backed admission/result evidence and session event
  replay. PubSub loss, process exit, or Unix-socket unavailability must not be a
  data-loss boundary for accepted work.
- Core `HarnessConfig.threadLock` plus MastraCode's filesystem
  `threadLock` / `ThreadLockError` are current thread coordination, not v1
  leases. They must not block multi-client read, subscribe, append, or admitted
  `Session.signal(...)`; v1 exclusivity belongs only to the storage lease/claim
  scopes defined in §5.2 and §5.8.
- `attachOMThreadStatePersistence(...)` and thread metadata based OM restore are
  legacy bootstrap inputs only. V1 commits OM settings through
  `SessionRecord.observationalMemory` and restores them during session
  hydration.

#### 11.6a Names that overlap with or intentionally replace current Mastra code

Most names in this section appear under the exact same identifier in both the v1
spec and current `../packages/**/src`. A small number are deliberate v1 public
replacement names for current concepts with different identifiers; those rows
must be marked `new-v1` or `changed-v1` and must say which current export is only
a precursor. Each row is a hallucination-risk boundary: a worker that assumes
the pre-v1 export already implements the v1 shape may wire the wrong
implementation after cutover.

Each entry below records one declared name with `Owner:` (spec section),
`Status:`, `Current code:`, and an optional `Notes:` line.

`Notes:` blocks are status-rationale pointers, not secondary definitions.
Keep each Note to the shortest text needed to justify the `Status:`
classification and route readers to canonical owners, normally no more
than two short cross-references plus one sentence of rationale. If a
Note needs to enumerate field families, source streams, projection
mappings, slot construction, or any other multi-claim material, that
content belongs in the owning section; the ledger should link to it. A
brief current-code mismatch may remain when the mismatch is only
meaningful as migration triage and does not define v1 behavior. This
rule scopes to §11.6a entries; §11.6b name-collision disambiguation
entries are not Notes blocks and are exempt.

**`AvailableModel`**

- Owner: §4.8a.
- Status: `reused-current`.
- Current code: `../packages/core/src/harness/types.ts:356`.
- Notes: §4.8a declares the shape inline.

**`BackgroundTask`**

- Owner: §4.8c.
- Status: `changed-v1`.
- Current code: `../packages/core/src/background-tasks/types.ts:20`.
- Notes: Spec carries the diagnostic projection (§4.8c); current code
  carries the runtime row. §5.1b.2 / §5.2d own the v1 storage extensions.

**`BackgroundTaskStatus`**

- Owner: §4.8c.
- Status: `changed-v1`.
- Current code: `../packages/core/src/background-tasks/types.ts:11`.
- Notes: v1 declares `BackgroundTaskStatus` as a transparent alias of
  `BackgroundTaskRowStatus` (§5.1b.2); no independent literals live at the
  projection site. Current code declares an independent union with
  `'suspended'` and without the v1-only `'dead'` terminal status. The v1 alias
  preserves existing `suspended` task state and adds the v1-only terminal
  `dead`; projection-only read models may still map suspended task rows to a
  broader `DurableWorkStatus` such as `waiting`, but the stored background-task
  row must not drop `suspendedAt` or `suspendPayload`.

**`Harness`**

- Owner: §4.1.
- Status: `changed-v1`.
- Current code: `../packages/core/src/harness/harness.ts:211`.
- Notes: §11.1 replaces the current class at the public Harness entry point;
  the current and v1 classes are not assignment-compatible.

**`HarnessConfig`**

- Owner: §9.1.
- Status: `changed-v1`.
- Current code: `../packages/core/src/harness/types.ts:158`.
- Notes: Mode/model/observational-memory/request-context fields all
  shift; see the §11.1 narrative for the legacy-vs-v1 split.

**`HarnessEventV1`**

- Owner: §10.1.
- Status: `new-v1`.
- Current code: `../packages/core/src/harness/types.ts:725`.
- Notes: There is no current `HarnessEventV1` export at this anchor; current
  core exports `HarnessEvent`. §11.1 routes that legacy event union as
  old-implementation input through the §10 projector before the v1 closed union
  admits it. The v1 public/wire name is `HarnessEventV1` until the legacy event
  surface is removed.

**`HarnessMessage`**

- Owner: §4.8b.
- Status: `changed-v1`.
- Current code: `../packages/core/src/harness/types.ts:938`.
- Notes: Current message projection uses `Date` timestamps, `attributes`, and
  non-JSON `unknown` payloads; §4.8b replaces it with the JSON-safe public
  projection and epoch-millisecond timestamps.

**`HarnessMessageContent`**

- Owner: §4.8b.
- Status: `changed-v1`.
- Current code: `../packages/core/src/harness/types.ts:948`.
- Notes: Same family as `HarnessMessage`; v1 narrows content payloads to the
  §4.8b JSON-safe union.

**`HarnessMode`**

- Owner: §9.2.
- Status: `changed-v1`.
- Current code: `../packages/core/src/harness/types.ts:44`.
- Notes: Current mode embeds a live `Agent`; v1 carries `agentId`
  (§9.2 / §11.1).

**`HarnessRequestContext`**

- Owner: §6.1.
- Status: `changed-v1`.
- Current code: `../packages/core/src/harness/types.ts:1005`.
- Notes: Required fields, runtime-slot ownership, and
  `emitEvent`/`suspendTool` boundaries all shift. §11.1 forbids reusing
  legacy `requestContext.set('harness', …)` behind the v1 subpath. See
  §6.0 / §6.1 for the slot-overlay pattern over `ToolExecutionContext` /
  `RequestContext` (`context.requestContext.get('harness')`).
  `getActivityTimeline(...)` is a v1-addition read accessor on this changed
  slot surface; it returns the v1-new `SessionActivityTimeline` projection and
  has no current-code `Session.getActivityTimeline` implementation yet.

**`HarnessSubagent`**

- Owner: §9.2.
- Status: `changed-v1`.
- Current code: `../packages/core/src/harness/types.ts:78`.
- Notes: Ownership, depth limit, and resume-metadata semantics shift per
  §8 and §9.2.

**`HarnessThread`**

- Owner: §5.1a.1.
- Status: `changed-v1`.
- Current code: `../packages/core/src/harness/types.ts:411`.
- Notes: Current thread rows lack `harnessName` and use `Date` timestamps; v1
  adds Harness namespace ownership and epoch-millisecond timestamps (§5.1a.1).

**`HeartbeatHandler`**

- Owner: §9.2.
- Status: `changed-v1`.
- Current code: `../packages/core/src/harness/types.ts:23`.
- Notes: v1 keeps the identifier, the `heartbeatHandlers` config field, and the
  `intervalMs` tick-interval field name so MastraCode and other existing callers
  keep working without rename. Semantics tighten per §11.4: `immediate` defaults
  to `false`; duplicate IDs reject instead of silently replacing a live handler;
  `registerHeartbeat(...)` returns an async unsubscribe (legacy
  `removeHeartbeat({ id })` is removed); slow ticks skip rather than overlap;
  unsubscribe, `stopHeartbeats()`, and `harness.shutdown()` await the in-flight
  handler before calling the optional `shutdown` hook. Restart-safe scheduled
  work uses `HarnessWakeupItem` (§5.1e, §14.6), not heartbeat handlers.

**`ModelAuthStatus`**

- Owner: §4.8a.
- Status: `reused-current`.
- Current code: `../packages/core/src/harness/types.ts:348`.
- Notes: Runtime auth status for the selected model (§4.8a).

**`ObservationalMemoryConfig`**

- Owner: §9.2.
- Status: `changed-v1`.
- Current code: `../packages/memory/src/processors/observational-memory/types.ts:827`.
- Notes: Current `HarnessConfig.omConfig` is typed as `HarnessOMConfig`
  (current `../packages/core/src/harness/types.ts:306`);
  v1 reuses the memory-package identifier `ObservationalMemoryConfig`
  and rebuilds the boundary per §11.1.

**`PermissionPolicy`**

- Owner: §4.8a.
- Status: `reused-current`.
- Current code: `../packages/core/src/harness/types.ts:330`.
- Notes: Literal union `'allow' | 'ask' | 'deny'`.

**`PermissionRules`**

- Owner: §5.1f.
- Status: `changed-v1`.
- Current code: `../packages/core/src/harness/types.ts:336`.
- Notes: Approval grants and pending-suspension shapes reshape per
  §5.1f.

**`Session`**

- Owner: §4.2.
- Status: `new-v1`.
- Current code: `../packages/core/src/auth/interfaces/session.ts:9`
  (auth, unrelated). Other unrelated current session concepts appear in
  playground/browser state and voice live-session managers.
- Notes: Name collision only. Current auth/browser/playground/voice `Session`
  concepts are not Harness sessions and must not be imported or referenced as
  v1 runtime rooms. The v1 `Session` class is the Harness session exported from
  `@mastra/core/harness` after cutover; migration docs should qualify Harness
  imports when the surrounding package also has a local session type.

**`ThreadCloneMetadata`**

- Owner: §5.1a.1.
- Status: `reused-current`.
- Current code: `../packages/core/src/storage/types.ts:192`.
- Notes: Storage-owned shape; v1 re-exports through the §5.1a.1 anchor.

**`TokenUsage`**

- Owner: §4.8a.
- Status: `changed-v1`.
- Current code: `../packages/core/src/harness/types.ts:437`.
- Notes: Current `raw` is `unknown`; v1 requires JSON-safe `JsonValue` for the
  public projection (§4.8a).

**`ToolCategory`**

- Owner: §4.8a.
- Status: `reused-current`.
- Current code: `../packages/core/src/harness/types.ts:325`.
- Notes: Literal union `'read' | 'edit' | 'execute' | 'mcp' | 'other'`.

**`ToolExecutionContext`**

- Owner: §6.1.
- Status: `reused-current`.
- Current code: `../packages/core/src/tools/types.ts:412`.
- Notes: Harness v1 attaches its `harness` slot to the existing public
  tool-authoring context. The same-named durable-workflow internal interface is
  tracked separately in §11.6b and must not be re-exported as the public tool
  context.

**`JsonValue`**

- Owner: §6.1.
- Status: `exact-name-collision`.
- Current code: `../packages/cli/src/commands/api/input.ts:4`,
  `../packages/server/src/server/schemas/agents.ts:14`.
- Notes: Existing package-local aliases are implementation inputs only. V1 uses
  the §6.1 canonical JSON value definition for Harness storage/wire contracts.

#### 11.6b Names that current Mastra carries under a different identifier

These are not exact-name overlaps with current code but are easy to confuse
with v1-declared names. Worker triage should treat them as
`old-implementation-input` when the old identifier appears in current code,
and as the v1 name when the spec section is the source of truth.

- Current `HarnessSession` (`../packages/core/src/harness/types.ts:424`) →
  no v1 export under that name. v1 splits this surface into `Session`
  (§4.2), `SessionRecord` (§5.1a.1), and the active-session resolver matrix
  in §5.3.
- Current `AgentSignalInput` (`../packages/core/src/agent/signals.ts:26`) →
  v1 Harness boundary input is `AgentSignalBoundaryInput` (§4.2f). Current
  `AgentSignalInput` remains the agent-layer input shape; the Harness boundary
  adds session/resource/source/admission ownership fields and must not reuse the
  same public name for a different contract.
- Current `HarnessEventListener` (`../packages/core/src/harness/types.ts:907`)
  → renamed to `HarnessListener` (§4.8a). Same callable shape
  `(event: HarnessEventV1) => void | Promise<void>`; only the v1 name is exported
  after cutover.
- Current `HarnessOMConfig` (`../packages/core/src/harness/types.ts:306`)
  → v1 routes observational-memory configuration through
  `ObservationalMemoryConfig` (§9.2 / §11.6a above). The old field name is
  implementation input only; the v1 public Harness surface uses the renamed
  shape.
- Current `ToolExecutionContext`
  (`../packages/core/src/agent/durable/workflows/shared/execute-tool-calls.ts:8`)
  → an internal durable-workflow orchestration interface
  (`toolCalls`, `tools`, `runId`, `agentId`, `messageId`, `state`,
  `onToolStart` / `onToolResult` / `onToolError`); it shares the name with
  the public `ToolExecutionContext` at
  `../packages/core/src/tools/types.ts:412` but is a different shape and
  scope. Worker triage: the v1 tool-authoring surface always anchors to the
  public current `tools/types.ts:412` context (§6.1, §11.6a
  `ToolExecutionContext` note); the
  durable-workflow variant is implementation-internal, never re-exported through
  `@mastra/core/harness`.

#### 11.6d Implementation precursors without exact-name overlap

§11.6a is scoped to _exact-name_ identifier overlap with current Mastra
`../packages/{core,server,memory,mcp,deployer,cli}/src`. The rows below name
current-code surfaces that materially **inform** a v1-declared family but ship
under a different identifier — they are hallucination-risk in the other
direction: a worker that assumes a v1 family must be built from scratch may
miss existing scaffolding and re-implement instead of refactoring. Each row
is implementation guidance only; the owning spec section remains authoritative
for the v1 contract and the entry below does not promise the precursor is
assignment-compatible with its v1 successor.

- **Display state family** (§5.1a.2: `HarnessDisplayStateSnapshotV1`,
  `HarnessDisplayTokenUsageSnapshotV1`, `HarnessDisplayMessageSnapshotV1`,
  `HarnessDisplayToolSnapshotV1`, `HarnessDisplayPendingBaseSnapshotV1`,
  `HarnessDisplayPendingApprovalSnapshotV1`,
  `HarnessDisplayPendingSuspensionSnapshotV1`,
  `HarnessDisplayPendingQuestionSnapshotV1`,
  `HarnessDisplayPendingPlanSnapshotV1`, `HarnessDisplaySubagentSnapshotV1`,
  `HarnessDisplayTaskSnapshotV1`) — current `HarnessDisplayState`
  (`../packages/core/src/harness/types.ts:581`) and `DisplayStateScheduler`
  (`../packages/core/src/harness/display-state-scheduler.ts`) implement the
  process-local projection that v1 commits as session-owned snapshot records.
  Reuse the scheduler/throttle and field layout; persist via §5.1a.2.
- **Channel bridge family** (§9.3: `HarnessChannelBridge`,
  `HarnessChannelConfig`, `HarnessChannelTransportRequest`,
  `HarnessChannelRouteContext`, `HarnessChannelDeliveryContext`,
  `ChannelIngressContext`, `ChannelIngressEnvelope`, `ChannelActionEnvelope`,
  and §14 dispatch types) — current `ChannelAdapterBaseConfig`
  (`../packages/core/src/channels/types.ts`) and `agent-channels.ts`
  infrastructure (`../packages/core/src/channels/agent-channels.ts`) provide
  the in-process channel processor and provider dispatch surface. Reuse
  provider-owned verification/normalization/send capabilities; route durability
  through §14.1/§14.2/§14.4 binding/inbox/outbox rows.
- **Background-task storage primitives** (§5.1b.2:
  `BackgroundTaskRowBase`, `BackgroundTaskStorageRow`,
  `BackgroundTaskDiagnosticRow`, `BackgroundTaskReconstructableRow`,
  `ClaimableBackgroundTaskRow`, `BackgroundTaskOwnerRef`; §5.2d `claim*` /
  `renew*` / `update*` storage methods) — current `BackgroundTasksStorage`
  (`../packages/core/src/storage/domains/background-tasks/base.ts:8`)
  ships the abstract storage domain with `createTask`, `updateTask`,
  `getTask`, `listTasks`, `deleteTask`, `deleteTasks`, `getRunningCount`, and
  `getRunningCountByAgent`. v1 extends the
  row shape (three field families per §5.1b.2) and adds the claim/renew
  primitives §5.2d requires; the existing domain is the migration target,
  not a parallel implementation.
- **Skill plumbing** (§4.6: `HarnessSkill`; flat session-level `useSkill` /
  `listSkills` / `getSkill` / `refreshSkills`) — current `WorkspaceSkills` infrastructure
  (`../packages/core/src/workspace/skills/`) and `SkillsStorage` domain
  (`../packages/core/src/storage/domains/skills/`) provide the workspace-side
  skill source/resolver that §4.6 delegates to after code-registered skills
  take precedence. The §11.4 "Core `Workspace.skills` / `WorkspaceSkills`"
  row owns the migration prose; this entry is the structural pointer for
  workers triaging the §4.6 surface.

#### 11.6c Remaining v1-declared names by owning section

The remaining 235 declared names have no current-code occurrence under the
same identifier. They are `new-v1` unless flagged otherwise. The list is
the index that turns the 256-name surface into a reviewable map; any
future drift against current code becomes either a new row in §11.6a, a new
rename row in §11.6b, or a new precursor row in §11.6d.

MastraCode is outside `../packages/**/src` but still matters for migration:
`mastracode/src/permissions.ts` already declares a `SessionGrants` class with
mutable process-local sets. The §5.1f `SessionGrants` name below is new for the
core durable Harness surface, but it is also a MastraCode name collision. Treat
that class as legacy import input and translate it into the plain JSON
`SessionRecord.sessionGrants` shape; do not reuse the in-memory class as a
durable row.

- §4.2f — `04-public-api/02-session/06-required-agent-signal-boundary.md`:
  `AgentSignalBoundary`, `AgentSignalBoundaryInput`, `AgentSignalAccepted`,
  `AgentSignalResultLookup`, `AgentSignalSubscription`,
  `AgentSignalResultStatus`, `AgentSignalTerminalEvent`.
- §4.2g — `04-public-api/02-session/07-required-agent-resume-boundary.md`:
  `AgentResumeBoundary`, `AgentResumeSupportInput`, `AgentResumeSupport`,
  `AgentResumeInput`, `AgentResumeResultLookup`, `AgentResumeResult`.
- §4.3 — `04-public-api/03-per-turn-overrides.md`: `HarnessOverrides`,
  `PersistedRunOverrides`.
- §4.4a — `04-public-api/04-operation-option-types/01-list-and-signal-options.md`:
  `ListPageOptions`, `ListPage`, `ListMessagesOptions`,
  `ListThreadsOptions`, `ListSessionsOptions`, `SignalOptions`.
- §4.4b — `04-public-api/04-operation-option-types/02-queue-and-skill-options.md`:
  `QueueOptions`, `UseSkillOptions`.
- §4.4c — `04-public-api/04-operation-option-types/03-request-context-options.md`:
  `RequestContextInput`, `TrustedRequestContextInput`.
- §4.4d — `04-public-api/04-operation-option-types/04-inbox-response-options.md`:
  `InboxResponseOptions`, `ToolApprovalResponse`, `ToolSuspensionResponse`,
  `InboxResponseResult`.
- §4.4e — `04-public-api/04-operation-option-types/05-thread-and-file-options.md`:
  `CreateThreadOptions`, `CloneThreadOptions`, `FileAttachment`.
- §4.5a — `04-public-api/05-errors/01-admission-channel-and-inbox-errors.md`:
  `HarnessBusyError`, `HarnessQueueFullError`, `HarnessValidationError`,
  `HarnessOutputGenerationError`, `HarnessForbiddenError`,
  `HarnessOverrideConflictError`, `HarnessAdmissionConflictError`,
  `HarnessAttachmentInUseError`, `HarnessAttachmentUnavailableError`,
  `HarnessChannelActionConflictError`, `HarnessInboxItemNotFoundError`,
  `HarnessInboxResponseConflictError`, `HarnessRecoveryDeferredError`.
- §4.5b — `04-public-api/05-errors/02-session-lifecycle-errors.md`:
  `HarnessSubagentDepthExceededError`, `HarnessLiveSessionLimitError`,
  `HarnessSessionClosedError`, `HarnessSessionClosingError`,
  `HarnessSessionNotFoundError`, `HarnessSessionConflictError`,
  `HarnessSessionDeleteBlockedError`, `HarnessSkillNotFoundError`,
  `HarnessSessionDeletedError`, `HarnessChannelBindingClosedError`,
  `HarnessChannelDeliveryUnavailableError`, `HarnessRuntimeDriftError`.
- §4.5c — `04-public-api/05-errors/03-abort-errors.md`:
  `HarnessAbortReason`, `HarnessAbortedError`.
- §4.5d — `04-public-api/05-errors/04-storage-state-workspace-and-lock-errors.md`:
  `HarnessRowErrorCode`, `HarnessStorageOperation`, `HarnessStorageSubject`,
  `HarnessStorageError`, `HarnessSessionCorruptError`,
  `HarnessStateSerializationError`, `HarnessStateConflictError`,
  `HarnessConfigError`, `HarnessWorkspaceProviderMismatchError`,
  `HarnessWorkspaceLostError`, `HarnessResourceWorkspaceInUseError`,
  `HarnessSessionLockedError`.
- §4.6 — `04-public-api/06-skills.md`: `HarnessSkill`.
- §4.7 — `04-public-api/07-goals.md`: `GoalState`, `GoalJudgeMemoryRef`,
  `GoalJudgedTurn`, `GoalJudgeDecision`, `SetGoalOptions`.
- §4.8a — `04-public-api/08-public-type-surface/01-type-surface-index-and-shared-helpers.md`:
  `Awaitable`, `ReadonlyState`, `HarnessStorage`, `ToolsetInput`,
  `HarnessListener`.
- §4.8b — `04-public-api/08-public-type-surface/02-messages-results-and-streams.md`:
  `AgentResult`, `AgentToolCallSummary`, `AgentStream`.
- §4.8d — `04-public-api/08-public-type-surface/04-remote-safe-supporting-types.md`:
  `RemoteSafeSkillDescriptor`, `RemoteSignalOptions`,
  `RemoteQueueOptions`, `RemoteUseSkillOptions`, `RemoteSafePermissions`,
  `ObservationalMemorySnapshot`, `RemoteSafeObservationalMemory`.
- §4.8e — `04-public-api/08-public-type-surface/05-remote-safe-session.md`:
  `RemoteSafeSession`, `RemoteSession`.
- §5.1a.1 — `05-session-persistence/01-what-gets-persisted/02-thread-and-session-records.md`:
  `ThreadMetadata`, `SessionRecord`.
- §5.1a.2 — `05-session-persistence/01-what-gets-persisted/03-display-records.md`:
  `HarnessDisplayStateSnapshotV1`, `HarnessDisplayTokenUsageSnapshotV1`,
  `HarnessDisplayMessageSnapshotV1`, `HarnessDisplayToolSnapshotV1`,
  `HarnessDisplayPendingBaseSnapshotV1`,
  `HarnessDisplayPendingApprovalSnapshotV1`,
  `HarnessDisplayPendingSuspensionSnapshotV1`,
  `HarnessDisplayPendingQuestionSnapshotV1`,
  `HarnessDisplayPendingPlanSnapshotV1`,
  `HarnessDisplaySubagentSnapshotV1`, `HarnessDisplayTaskSnapshotV1`.
- §5.1b.1 — `05-session-persistence/01-what-gets-persisted/05-session-summary-records.md`:
  `SessionSummary`, `SessionLifecycleStatus`, `PendingInboxKind`,
  `SessionThreadLabel`, `SessionRunProjection`, `SessionGoalSummary`,
  `SessionChannelBindingSummary`, `SessionPendingInboxSummary`.
- §5.1b.2 — `05-session-persistence/01-what-gets-persisted/06-background-task-records.md`:
  `DurableWorkKind`, `DurableWorkStatus`, `DurableWorkProofKind`,
  `BackgroundTaskRowStatus`, `BackgroundTaskOwnerRef`,
  `BackgroundTaskRowBase`, `BackgroundTaskDiagnosticRow`,
  `BackgroundTaskReconstructableRow`, `BackgroundTaskStorageRow`,
  `ClaimableBackgroundTaskRow`.
- §5.1b.3 — `05-session-persistence/01-what-gets-persisted/07-durable-work-summary.md`:
  `DurableWorkSummary`.
- §5.1b.4 — `05-session-persistence/01-what-gets-persisted/08-activity-and-session-list-records.md`:
  `DurableWorkListSummary`, `DurableWorkSnapshotWindow`,
  `SessionMessageCursor`, `SessionMessageWindow`,
  `ActivityTimelineOptions`, `SessionActivityTimeline`,
  `ActivityTimelineEntryKind`, `ActivityTimelineSourceKind`,
  `ActivityTimelineEntry`, `SessionListItem`.
- §5.1c — `05-session-persistence/01-what-gets-persisted/09-session-snapshot.md`:
  `SessionSnapshot`.
- §5.1d — `05-session-persistence/01-what-gets-persisted/10-queue-admission-and-tombstones.md`:
  `QueuedItem`, `QueueAdmissionReceipt`, `OperationAdmissionTombstone`.
- §5.1e — `05-session-persistence/01-what-gets-persisted/11-background-wakeups-runs-and-attachments.md`:
  `HarnessWakeupItem`, `HarnessRunStatus`, `HarnessRunOperationRef`,
  `HarnessRunOperationalState`, `PersistedAttachment`,
  `PersistedRequestContextInput`.
- §5.1f — `05-session-persistence/01-what-gets-persisted/12-permissions-pending-and-inbox.md`:
  `SessionGrants`, `ToolApprovalReasonSource`, `PendingApproval`,
  `PendingToolSuspension`, `PendingQuestion`, `PendingPlanApproval`,
  `InboxResponseReceipt`.
- §5.1h — `05-session-persistence/01-what-gets-persisted/14-channel-records.md`:
  `ChannelBindingMode`, `ChannelBinding`,
  `HarnessProviderCallbackBindingStatus`, `HarnessProviderCallbackBinding`,
  `ChannelInboxItem`, `ChannelProviderDeliveryReceipt`, `ChannelOutboxItem`,
  `ChannelActionToken`, `ChannelActionReceipt`.
- §5.2a — `05-session-persistence/02-storage-shape/01-thread-message-session-methods.md`:
  `HarnessMemoryHistoryProjection`, `HarnessStorageDomain`.
- §6.1 — `06-tool-authoring-contract/01-harnessrequestcontext.md`:
  `SetStateFn`, `JsonValue`, `HarnessCustomEventInput`, `SuspendToolParams`.
- §7 — `07-sandbox-command-registry/00-section.md`: `SandboxConfig`,
  `CommandDefinition`.
- §9.1 — `09-configuration/01-harness-config.md`: `ListLimitConfig`.
- §9.2 — `09-configuration/02-runtime-registrations.md`:
  `BackgroundTaskExecutorRegistration`,
  `BackgroundTaskCompletionPolicyRegistration`.
- §9.3 — `09-configuration/03-channel-configuration.md`:
  `HarnessChannelConfig`, `ChannelIngressPolicy`, `ChannelDeliverySemantics`,
  `ChannelOutboxOperationKind`, `ChannelOutboxDeliveryPlan`,
  `HarnessChannelTransportRequest`, `HarnessChannelBridge`,
  `HarnessChannelRouteContext`, `HarnessChannelDeliveryContext`,
  `ChannelIngressContext`, `ChannelIngressEnvelope`, `ChannelActionEnvelope`.
- §9.4 — `09-configuration/04-workspace-configuration.md`:
  `HarnessWorkspaceConfig`, `WorkspaceProvider`, `WorkspaceStateUpdate`,
  `WorkspaceCreateContext`, `WorkspaceResumeContext`, `WorkspaceFactoryFn`.
- §10.1 — `10-events/01-event-shape.md`: `HarnessEventBase`,
  `HarnessEventV1`.
- §10.2 — `10-events/02-built-in-event-union.md`: `HarnessEventError`,
  `LifecycleEvent`, `StateEvent`, `TurnEvent`, `OperationEvent`,
  `ToolEvent`, `SubagentEvent`, `SuspensionEvent`, `AttachmentEvent`,
  `ChannelEvent`, `GoalEvent`, `StorageErrorEvent`, `CustomEventType`,
  `CustomEvent`.
- §13.1 — `13-mastra-server-integration/01-registration.md`:
  `HarnessChannelProviderSelector`.
- §13.3b — `13-mastra-server-integration/03-wire-protocol-sketch/02-request-payloads.md`:
  `JsonSchema`, `WireSchemaRef`, `WireSchemaDescriptor`, `SignalRequest`,
  `WireAttachment`, `SkillInvocationRequest`, `WireHarnessSkillDescriptor`,
  `WireListPage`.
- §13.3c — `13-mastra-server-integration/03-wire-protocol-sketch/03-conditional-session-version-mutations.md`:
  `ThreadSettingRequest`, `RenameSessionRequest`, `CloneThreadRequest`.
- §13.3e — `13-mastra-server-integration/03-wire-protocol-sketch/05-operation-result-lookups.md`:
  `SignalAdmissionResponse`, `QueueAdmissionResponse`,
  `SignalResultResponse`, `QueueResultResponse`.
- §13.3f — `13-mastra-server-integration/03-wire-protocol-sketch/06-error-envelope.md`:
  `HarnessPublicErrorProjection`, `HarnessErrorResponseBase`,
  `HarnessErrorResponse`.
- §13.4e — `13-mastra-server-integration/04-client-sdk/05-pending-inbox-view-model.md`:
  `PendingInboxItemKind` (transparent alias of `PendingInboxKind` (§5.1b.1);
  the projection re-exports the canonical kind union rather than redeclaring
  it), `PendingInboxCardState`, `PendingInboxItemBase`, `PendingInboxItem`.
- §14 — `14-channels/00-section.md`: `ResolveChannelBindingOptions`,
  `ChannelIngressOptions`, `ChannelIngressResult`, `ChannelActionOptions`,
  `ChannelActionResult`, `ChannelOutboxEnqueueOptions`,
  `ChannelDispatchOptions`, `MastraChannelOperatorDispatchOptions`,
  `ChannelDispatchResult`.
- §14.3 — `14-channels/03-request-context.md`: `ChannelRequestContext`,
  `BaseChannelRequestContext`, `InboundChannelRequestContext`,
  `BindingBackedChannelRequestContext`.
- §14.5 — `14-channels/05-approval-and-inbox-bridge.md`:
  `ChannelActionAudience`.

Maintenance rule: when a new top-level `class`, `interface`, or `type` is
added to a non-example `sections/` file, add it to the appropriate §11.6c
section bucket; when a current-code shape is renamed, removed, or gains a
matching v1 export, update §11.6a or §11.6b; when a v1 family is materially
informed by current-code scaffolding under a different identifier, add or
update a §11.6d precursor row. When referencing a child section, use the
heading-letter form (e.g. `§5.1b.2`, `§13.3d`), not the file-ordinal form
(`§5.1.6`, `§13.3.4`). File ordinals are filesystem state, not section
identity. Every `Current code:` line anchor must be re-verified against
`../packages/**/src` on each spec pass; cross-package anchors
(`../packages/memory`, `../packages/core/src/background-tasks`,
`../packages/core/src/tools`, `../packages/core/src/storage/domains/**`) drift
silently when the spec author edits only the harness file, so a grep pass
against the source HEAD is part of every §11.6 update. Counts are intentionally
not hand-maintained in prose because they drift when section snippets or
package-local type aliases change; rerun the inventory before using this ledger
as a migration checklist. Duplicate-cutover traps: §11.6e.

#### 11.6e Implementation traps by entity

This table indexes cutover mistakes that create **parallel implementations** when
the [§0 Mental model](../../00-mental-model.md) is ignored. It does not add new
semantics; owning sections remain authoritative.

| Entity         | Trap (today on `main`)                                                                                       | v1 owner                                                                                                                            | Canonical pointer         |
| -------------- | ------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------------------------------------- | ------------------------- |
| **Harness**    | Thread-first lifecycle (`createThread`, `switchThread`, `sendMessage`, `followUp`, `steer`) on the singleton | `harness.session()` + `Session.*`                                                                                                   | §11.4, §4.1               |
| **Harness**    | `harness.memory.*` duplicates top-level thread methods                                                       | Remove from product path; session-first lifecycle                                                                                   | §11.4, `harness.ts`       |
| **Harness**    | `harness.threads.*` used as normal app lifecycle                                                             | No `harness.threads` product surface; `/operator/threads*` remains only for import/history tooling outside the public Harness class | §4.1, §13.2, §0           |
| **Storage**    | `admissionId`/`admissionHash` redefined in §3/§13/§15                                                        | Canonical hash: §4.4b; canonical rows: §5.1d                                                                                        | §4.4, §5.1d               |
| **Harness**    | `POST /agents/.../signals` kept as product path alongside §13                                                | `/harness/...` projects `Session.signal` admission                                                                                  | §13, §11.6                |
| **Harness**    | Process-local harness fields as config/runtime state                                                         | `HarnessConfig` = composition; room state in `SessionRecord`                                                                        | §9.1, §5.1a.1             |
| **Session**    | `HarnessSession` snapshot from `getSession()` treated as the room                                            | `Session` class + `SessionRecord`                                                                                                   | §11.6b                    |
| **Session**    | `Harness.sendSignal` owns admission/receipts                                                                 | `Session.signal` + `AgentSignalBoundary` post-accept                                                                                | §4.2f                     |
| **Session**    | Process-local queue and pending resolvers                                                                    | `SessionRecord.pendingQueue`, durable pending inbox                                                                                 | §5.1                      |
| **Session**    | `display_state_changed` as public durability                                                                 | Persisted display snapshots; §10 union excludes legacy display event                                                                | §10.2, §5.1a.2            |
| **Thread**     | `--thread`, `/thread`, `switchThread` as lifecycle                                                           | Product commands resolving to `harness.session(...)` first                                                                          | §11.6 migration-sensitive |
| **Memory**     | OM/thread-metadata bootstrap without `SessionRecord`                                                         | `SessionRecord.observationalMemory`                                                                                                 | §11.6                     |
| **Memory**     | Message append mistaken for signal admission                                                                 | `Session.signal` is not a generic thread write                                                                                      | §4.2, §0                  |
| **Storage**    | `threadLock` blocks read/subscribe/signal                                                                    | Leases for recovery-sensitive work only                                                                                             | §5.8, §11.6               |
| **Storage**    | `SignalsPubSub` loss = data loss                                                                             | Storage admission/result + §10 replay                                                                                               | §11.6                     |
| **Storage**    | Second background-task store                                                                                 | Extend `BackgroundTasksStorage` with §5.2d `claim*`/`renew*`                                                                        | §11.6d                    |
| **Storage**    | `registerHeartbeat` as restart-safe scheduling                                                               | `HarnessWakeupItem` rows                                                                                                            | §11.5                     |
| **Storage**    | `ChannelsStorage` provider install/config rows treated as harness inbox/outbox                               | Keep provider config canonical; add §14 bridge ledgers as channel-domain extensions                                                 | §14.7, §11.6d             |
| **Workers**    | `BackgroundTaskWorker` substituted for harness wakeup recovery                                               | Harness workers claim logbook rows                                                                                                  | §15.2 preamble            |
| **Workers**    | Second worker framework beside `packages/core/src/worker/`                                                   | Claim semantics on Storage rows                                                                                                     | §0 cutover                |
| **Live layer** | `AgentChannels` + harness bridge on same binding                                                             | §14.7 init fence                                                                                                                    | §14.7                     |
| **Live layer** | Stream/pubsub IDs as replay cursors                                                                          | §10.5 session epoch + buffer                                                                                                        | §10.5                     |
| **Live layer** | MCP/HTTP session IDs as recovery keys                                                                        | Process-local diagnostics only                                                                                                      | §11.5                     |
