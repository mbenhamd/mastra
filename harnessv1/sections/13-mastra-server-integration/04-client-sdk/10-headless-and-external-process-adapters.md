### 13.4j Headless and External Process Boundaries

**Headless and external process boundaries.** Headless CLIs, TUIs, connector
apps, one-shot scripts, and child processes participate in Harness v1 by
choosing a role for each integration boundary. The role is decided by which call
path creates or owns the durable Harness state, not by the binary itself. V1
does not add a generic process event union, process ledger,
`IntegrationInbox`, `IntegrationOutbox`, generic `ActionReceipt`, durable PID
handle, or new Session cancellation API.

- A **session host/controller** creates or opens one or more Harness sessions and
  maps its control loop to `Session` when it runs in-process, or to
  `RemoteSafeSession` when it runs remotely. In-process hosts may use the full
  local `Session` surface plus local-only `Harness` powers such as
  `Harness.subscribe(...)` and `Harness.registerHeartbeat(...)`; remote hosts use
  `RemoteSafeSession`, per-session SSE, result lookup by `signalId` /
  `queuedItemId`, and read models. Remote cross-session subscription still does
  not cross the wire (§13.5). After browser reload, controller restart, Harness
  restart, session eviction, auth-token refresh, or SSE `412`, controllers reuse
  the controller recovery rules above. Pending prompts, including subagent
  prompts, are answered through the owning session and `responseId` rules from
  the pending-inbox contract. Existing headless hosts migrate from current
  `Harness.sendMessage(...)` / `Harness.subscribe(...)` through the §11.4 method
  translation table.
- A **session-owned tool/process** is work started by a Harness run, tool,
  background task, wakeup, or source-specific row. Its stdin/stdout/stderr,
  progress, status, and exit code are tool effects: they may appear as tool
  output, tool custom events, model-visible output, display diagnostics, or
  source-specific status projections. The child process handle, PID, stream, and
  exit code are not a `Session` and are not a durable recovery boundary. Durable
  completion remains anchored by the owning run, `QueuedItem` /
  `QueueAdmissionReceipt`, `HarnessWakeupItem`, channel inbox/action/outbox row,
  qualified background-task claim contract, or other source-specific row in
  §5.7.
- A **protocol bridge** fronts an external process or protocol only by translating
  it into existing Harness surfaces. It routes input through
  `session.signal(...)`, `session.queue(...)`, or `session.useSkill(...)` under
  their existing retry and non-retry-safe rules; routes approvals, questions,
  suspensions, and plans through `PendingInboxItem` plus the `respondTo*`
  methods with `responseId`; exposes output/status through Harness events,
  result lookup, snapshots, and source-specific read models; and keeps subagent
  responses addressed to the `owningSessionId`. A private stdin/stdout stream
  protocol, stdout-scraped approval marker, child-process exit code, or custom
  process event cannot be the source of truth for Harness admission, settlement,
  pending-inbox response, or recovery.

**MastraCode session controller.** MastraCode is a first-party session
host/controller. Its TUI and headless entry points may keep product command
names such as `/thread`, `/threads`, `/new`, `/clone`, `/resource`, `/name`,
`--thread`, and `--clone-thread`, but those names are MastraCode UX, not core
Harness aliases. The controller behind them does not expose removed Harness
methods and resolves product intent into v1 Session operations before work is
admitted:

- Startup resolves the selected resource to an existing `Session` when one is
  addressable, or keeps a local staged fresh-session intent when MastraCode has
  no suitable conversation yet. A durable `SessionRecord` is created only by an
  explicit create/open path or by the first real admission that commits that
  staged intent. Once a session exists, both identities stay available:
  `sessionId` is the runtime/admission/recovery identity; `threadId` is the
  durable transcript/history identity. New hook manager state, analytics
  correlation, observability attributes, pending UI, queued actions, OM
  restore, goal state, and subagent settings bind to `sessionId`, but existing
  MastraCode hook integrations used `session_id` to mean the legacy thread ID.
  Migration must keep that field compatible or versioned and add explicit
  `harness_session_id` and `thread_id` fields before changing external hook
  semantics.
- Thread-title selectors are product pre-resolution only. MastraCode may search
  resource-scoped thread/session summaries by title, but duplicate title matches
  fail loudly before calling the Harness resolver. V1 does not preserve the
  pre-v1 duplicate-title fallback. Cross-resource mismatch still fails before
  calling the Harness resolver. The Harness resolver receives `sessionId` or
  `{ threadId, resourceId }`, never a title.
- `/resource`, `--resource-id`, `/new`, and startup "no suitable conversation"
  behavior are controller selection state, not Harness mutable resource state.
  The controller may keep a local current-resource draft, list session summaries
  for that resource, choose a concrete `sessionId` / `(threadId, resourceId)`, or
  stage a fresh-session intent. It must not recreate removed
  `harness.setResourceId(...)` or call a resource-only session resolver.
- `/new` and startup "no suitable conversation" behavior stages a fresh-session
  intent until first admission under the v1 MastraCode product rule. That staged
  intent is local controller state, not a durable Session until a real admission
  or explicit create commits it.
- `/clone` and `--clone-thread` call `session.clone(...)` when cloning the
  current session. MastraCode also has UX that clones a selected non-current or
  lock-prompt thread; that path must first resolve the source to a concrete
  source session/thread summary through a product controller or operator history
  helper, then invoke the same session clone contract. It must not resurrect
  `harness.cloneThread(...)` as an app-facing lifecycle API. Clones copy only the
  durable state allowed by the clone contract; they do not copy live streams,
  pending UI handles, process queues, leases, stale `currentRun`, goals, or
  in-flight tool ownership. Current MastraCode clones can carry goal metadata
  through thread metadata; v1 must make the behavior explicit in UX: either start
  the clone without an active goal and tell the user, or create a fresh goal on
  the clone through `setGoal(...)` after clone creation. Partial-history fork
  remains product-owned/deferred until v1 defines a storage and route contract.
- Enter/steer uses `session.signal(...)`; Ctrl+F and held work use
  `session.queue(...)` or a session-owned queued-action wrapper that ultimately
  drains into `session.signal(...)`, `session.queue(...)`, or
  `session.useSkill(...)` by action kind. Pending UI confirmation keys by
  `admissionId`, `signalId`, or `queuedItemId`; message-text echo matching is
  only a legacy UI fallback during migration, not settlement evidence.
- MastraCode settings that are currently stored in top-level thread metadata
  must move to owning v1 surfaces before raw metadata writes are rejected:
  goals to `GoalState`, mode/model and model-pack choices to session runtime
  settings, subagent model choices to session subagent model overrides,
  project/sandbox paths to workspace/session state, and app-owned display
  metadata only to `thread.metadata.app`. OM migration is per-key: session-local
  observer/reflector/threshold choices move to
  `SessionRecord.observationalMemory`, while global MastraCode settings remain
  bootstrap defaults and raw OM rows remain owned by MemoryStorage.
- Plan/build/fast are MastraCode mode names and model-pack choices, not model
  identity by themselves. Session settings must store the selected mode id and
  resolved model id, and may store the product-local pack id/name as app-owned
  preference metadata; migrations must not infer a provider model only from the
  word `plan` or assume pack labels are stable provider IDs.
- Plan mode is not inherently read-only at the Harness layer. If MastraCode
  wants plan-mode commands or headless `--mode plan` to restrict file writes,
  shell starts, or tools, it must express that through explicit mode policy,
  workspace command/path policy, or session permission rules rather than relying
  on the mode name alone.
- MastraCode permission migration must key grants, denies, categories, and hook
  payloads by the model-visible/Harness-exposed tool name after remapping. Core
  workspace constants such as a provider's internal execute-command id are
  implementation inputs only; using them directly can miss existing MastraCode
  deny filters for exposed names such as `execute_command`.
- MastraCode `/sandbox` and `request_access` path grants are not the v1 sandbox
  command policy. They remain filesystem/workspace access state and must be
  migrated before raw thread metadata writes are rejected. The optional v1
  command-start allowlist is an additional execute-command fence, not a
  replacement for path access approvals.
- MastraCode headless is a session controller, not a thin
  `await session.signal({ sync: true })` wrapper. It must preserve its current
  output modes, prompt/approval/sandbox auto-response behavior, event streaming,
  and wait-for-agent-end semantics by composing `Session.subscribe(...)`, pending
  inbox responders, result lookup, and the existing output-format contract.
- MastraCode workspace resolution may continue to use a composition-root
  workspace registry or an equivalent Harness-owned workspace cache to reuse
  long-lived `ProcessManager` state. The v1 "tools do not receive
  `context.mastra`" rule applies to tool execution authority; it must not be
  over-applied to remove the product workspace resolver before an equivalent
  cache exists.
- Goal judge memory is session-owned runtime support. If implemented as a
  separate history/memory record, it must name the owning `sessionId`, goal id,
  resource, and parent session, and it must not rely on ad hoc
  `forkedSubagent` / `goalJudge` thread metadata as proof of ownership or
  recovery authority.
- `SignalsPubSub`, Unix sockets, local pending UI maps, heartbeat timers, and
  filesystem `threadLock` are live controller aids only. V1 leases replace the
  correctness boundary for durable session ownership, but MastraCode must either
  provide a new local "owned by another process" UX projection or intentionally
  remove the old PID-based prompt; it must not treat filesystem locks as the v1
  session lease. Accepted work, duplicate detection, queue order, result lookup,
  pending-inbox response, clone safety, recovery, and provider-visible delivery
  are proven by storage rows, leases/claims, operation evidence, and workers.

Stop or Cancel, when a headless controller or bridge exposes it, remains an
agent/run-layer or process-local control outside `RemoteSafeSession`. It is not
`session.abort()`, not `session.clearQueue()`, and not session close. If an
agent/run-layer abort interrupts accepted Harness work, unresolved operations
still follow the existing operation-terminality rules (§5.7, §15.1, §15.2).
MastraCode must provide this local run-controller abort path for Ctrl+C/Escape,
approval dismissal, and headless timeout before removing legacy
`harness.abort()`.
Process exit codes do not map to Session lifecycle events and do not settle
`signal(...)` or `queue(...)` by themselves; they may be rendered as tool return
data, `tool_end.isError`, tool custom events, model output, or diagnostics under
an existing source-specific owner. MCP runtime status, MCP resources, and any
future MCP/app callback receipts remain outside this adapter contract unless a
source-specific ledger is specified.
