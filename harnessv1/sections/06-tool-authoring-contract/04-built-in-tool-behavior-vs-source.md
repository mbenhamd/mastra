### 6.4 Built-in tool behavior vs `source`

The built-in tools (`task_write`, `submit_plan`, `ask_user`) read `source` to
keep parent and subagent state isolated. This is a calling-session ownership
rule, not just an event-tagging rule: for every tool invocation, Harness builds
or overlays a fresh `HarnessRequestContext` whose live helper fields are bound
to the owning `SessionRecord` identified by `sessionId`. Implementations must
not satisfy this contract by shallow-copying a parent context and changing only
identity fields; `getState`, `setState`, pending registration, suspension,
approval-driven mode switching, and event emission must all resolve through the
calling session.

For a subagent, the calling session is the child session. Parent-stream events
are attributed projections only; pending records, task state, inbox responses,
and approval-driven mode changes remain owned by the child session, with routing
as defined in §10.6/§13.2.

- **`task_write`** — writes to the calling session's task list. A subagent's
  task list is separate from the parent's; calling `task_write` from a subagent
  never overwrites the parent's tasks. (The mechanism: tasks live in
  `session.state`, and there are two sessions involved.)
- **`submit_plan`** — awaits plan-approval registration against the calling
  session before suspending. When approved by the user, the harness flips the
  calling session's mode (typically plan → build). A subagent's `submit_plan`
  flips the subagent's mode, never the parent's. The user-facing event is tagged
  with `source` so the UI can attribute it ("subagent X submitted a plan").
- **`ask_user`** — awaits pending-question registration against the calling
  session before suspending. The user sees the question with subagent attribution
  if `source === 'subagent'`.

Custom tool authors implementing similar suspension patterns should follow the
same rule: act on the calling session only, and tag user-facing events with
`source` for attribution.

**Plan-task tool surface (TM-3 / TM-4).** The built-in plan-task tools
(`task_add`, `task_decompose`, `task_reparent`, `task_update`, `task_complete`,
and `plan_task_check`) are the ONLY model-facing mutation path for the durable
`HarnessPlanTask` tree (§5.1k). They are registered on the `harness:builtin`
toolset and, like `task_write` / `submit_plan` / `ask_user` above, act on the
calling session only: each tool routes its write through the live `Session`
under its lease so the storage mutators stay session-owner-fenced (§5.6, §5.8).
`task_decompose` and `task_reparent` are transaction-shaped multi-row writes.
The TM-4 hierarchy layer runs over the loaded tree on every mutation: a
`'derived'`-status parent's status rolls up from its children (the ratified
truth-table) and from its own `blockedBy` dependencies, an explicit terminal
status is never overwritten, reparent + `blockedBy` edges are cycle-checked,
and at most one task per root subtree may be `in_progress`. Each mutating tool
emits the `papersflow.plan_task.updated` custom event (§10.3).

**Durable delegation tool (TM-6).** `task_delegate` is added to the same
`harness:builtin` set, registered ONLY when subagent types are configured
(mirroring `spawn_subagent`). It hands a plan task (and, with `includeSubtree`,
its descendants) to a subagent SESSION whose terminal outcome the plan task
tracks across turns and restarts via the durable `delegatedSubagentSessionId`
link — distinct from `spawn_subagent`, which runs an inline child the parent
awaits within the turn. The delegated task stays `in_progress` until the subagent
session terminalizes, then rolls up `completed` / `failed` (cascading through the
truth-table). See §5.6 for the wait-point, completion hook, and
recovery-on-rehydrate.

**Subagent control tool (PF-7473).** `subagent` joins the same `harness:builtin`
set when subagent types are configured and the calling session is below
`subagents.maxDepth`. It acts only on the calling session's own children
(`parentSessionId === session.id`, same resource):

- `status` lists or inspects children from the live active-subagent projection
  plus the durable delegation links. A delegated child carries its settled
  result. An inline `spawn_subagent` result is returned by that tool call only.
- `wait` blocks up to a bounded timeout (default 30 s, maximum 300 s) and
  returns the settled children and those still running. A closing child counts
  as running until `closedAt`.
- `send` interleaves a bounded system reminder into the child's active run
  (`injectSystemReminder` with the internal `requireActiveRun`, dispatched with
  `ifIdle: discard` and `ifActive.expireWithRun`). It never wakes an idle or
  parked child, and a message the run ends without consuming is dropped
  instead of starting a follow-up run.
- `cancel` runs the owned cancellation cascade, which commits `cancelRequest`
  on the child's own record. That cancel is honoured by the delegation driver
  and by reconcile-on-rehydrate. Cancel needs the child's lease to be free or
  held by this process. The tool never writes under another process's
  unexpired lease; it returns the retryable
  `harness.subagent_cancel_unavailable` instead.

Rejections are `isError` payloads. The tool passes through the normal
permission gate.

> **Deferred (TM-5).** The `plan_task_*` event payload polish (TM-5) is not yet
> implemented.
