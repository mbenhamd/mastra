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
`plan_task_check`, and the back-compat `task_write`) are the ONLY model-facing
mutation path for the durable `HarnessPlanTask` tree (§5.1k). They are
registered on the `harness:builtin` toolset and, like `task_write` /
`submit_plan` / `ask_user` above, act on the calling session only: each tool
routes its write through the live `Session` under its lease so the storage
mutators stay session-owner-fenced (§5.6, §5.8). `task_decompose` and
`task_reparent` are transaction-shaped multi-row writes. The TM-4 hierarchy
layer runs over the loaded tree on every mutation: a `'derived'`-status parent's
status rolls up from its children (the ratified truth-table) and from its own
`blockedBy` dependencies, an explicit terminal status is never overwritten,
reparent + `blockedBy` edges are cycle-checked, and at most one task per root
subtree may be `in_progress`. Each mutating tool emits the
`papersflow.plan_task.updated` custom event (§10.3). The `task_write` alias maps
to add (no `taskId`) / update (`taskId`) semantics so an agent trained on the
legacy single-tool name keeps working against the tree.

> **Deferred (TM-5 / TM-6).** The `plan_task_*` event payload polish (TM-5) and
> the `delegatedSubagentSessionId` subagent-delegation surface (TM-6) are not
> yet implemented.
