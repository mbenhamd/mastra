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

> **Plan-task tool surface (stub — TM-3).** The built-in tool that lets the
> model create, decompose, reorder, reparent, and update the `HarnessPlanTask`
> tree (§5.1k) is defined by TM-3, not here. That tool is the only model-facing
> mutation path for plan tasks; it routes every write through the live `Session`
> under its lease so the storage mutators stay session-owner-fenced (§5.6,
> §5.8). Its field semantics, decompose/reparent multi-row behavior, and the
> §5.1k status-rollup interaction (TM-4) are filled in by those lanes. TM-2 ships
> only the durable storage layer.
