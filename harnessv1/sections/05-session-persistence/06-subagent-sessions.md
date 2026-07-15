### 5.6 Subagent sessions

This section is the canonical owner for persisted child-session lifecycle,
durable addressability, parent-bound write ownership, and how parent close /
eviction / shutdown affect child sessions. Close and delete mechanics remain
owned by §5.5. Event shape and parent-stream attribution live in §10.2/§10.6;
wire routing lives in §13.2; depth overflow behavior lives in §8.

A subagent session is a normal `SessionRecord` with `parentSessionId` set and a
child `threadId` distinct from the parent's thread. The §2.2 owner-key
uniqueness rule applies independently to each child
`(harnessName, resourceId, threadId)`.
It persists like any other session. This means:

- Subagent state survives restarts the same way parent state does.
- Walking `parentSessionId` rebuilds the subagent tree without needing in-memory
  state.
- Public `listSessions(...)` stays resource-scoped; storage exposes
  `listChildSessions(...)` so recovery and outbox projection can rebuild subagent
  trees after restart.

**Ownership model.** A subagent session is **independently addressable** by its
own `sessionId`: direct SSE under §10.6 and direct inbox writes under §13.2 are
required surfaces, not optional conveniences. Its **write ownership is parent-bound**:
the child's storage-level lease entry is installed by
`createOrLoadCurrentSessionOwner(...)` with the parent's `ownerId` and a TTL no later
than the parent's current lease expiry (§5.2, §5.8); it is never independently
`acquireSessionLease(...)`-ed and never independently renewed. Parent/root lease
renewal goes through `renewSessionLeaseSubtree(...)` (§5.2), extending every
active descendant's lease entry on the same storage-linearized renewal cycle
(capped at the new parent expiry) or failing the parent/root renewal, so a
long-lived idle subagent does not fence itself prematurely while the parent is
healthy. A child request that lands on an instance which does not own the
parent's lease loads the child record to read `parentSessionId`, walks the chain
to the root if needed (the entire active chain shares one `ownerId`), and
applies the parent/root's `lockMode` (`fail` / `wait` / `steal`) on the
parent/root record — never on the child. Parent eviction (§5.4) and shutdown
(§5.8) release the lease without closing; descendants stay active and are
re-acquirable when a later request hydrates the parent. Parent **close**
cascades terminally to all active descendants per §5.5.

Subagent depth is computed from the persisted `parentSessionId` chain per §2.4;
§8 owns cap enforcement, including the restart-stable overflow behavior.

`HarnessPlanTask` rows (§5.1k) are owned by the session that created them and are
isolated by `(harnessName, sessionId)`: a parent and each subagent child own
disjoint plan trees, and plan-task reads/writes never cross the session boundary.
A child session keeps its own plan tree under its own `sessionId` even though it
shares the parent's lease for _write ownership_ — plan-task mutators are fenced on
**the owning session's** lease/version, and because a child shares the parent's
`ownerId`, the same live owner serializes both trees' writes.

**Durable subtask → subagent delegation (TM-6).** A parent plan node can be
DELEGATED to a subagent session via the built-in `task_delegate` tool (registered
only when subagent types are configured). Unlike `spawn_subagent` — a synchronous
in-turn child the parent awaits inline (§9) — delegation is DURABLE and spans
turns and restarts:

- **Link + status, one transaction.** `task_delegate({ taskId, agentType, task?,
includeSubtree? })` creates a subagent session via the normal subagent-session
  path (`origin: 'subagent-tool'`, `parentSessionId`, depth+1, §8 cap enforced
  before any mutation), then writes `delegatedSubagentSessionId` onto the plan
  task AND drives it `in_progress` in ONE `mutatePlanTasksForSession` write under
  the session-owner fence. The single-`in_progress`-per-root invariant applies.
  `includeSubtree` delegates the task and its descendants as one unit. The PARENT
  turn does **not** block on the subagent.
- **Durable wait-point.** The wait-point is the persisted
  `delegatedSubagentSessionId` link itself, NOT an in-memory await and NOT a
  `pendingResume` suspension (delegation deliberately does not couple to the
  agent loop / `suspendTool`). The delegated task stays `in_progress` until the
  subagent session terminalizes.
- **Rollup from the subagent outcome.** A live COMPLETION HOOK fires when the
  delegated subagent's turn settles: completed → the plan task rolls up
  `completed` (`statusSource: 'explicit'`); a subagent error / abort / cancelled
  session → `failed`. The §5.1k rollup truth-table then cascades the change to
  ancestors. The rollup write is fenced on the parent session's lease/version and
  emits the `papersflow.plan_task.updated` delta BEST-EFFORT (a delegated subagent
  can terminalize outside any parent turn, where the turn-gated custom event is
  skipped; the durable write + display summary remain authoritative).
- **Recovery on rehydrate.** On hydration the owner scans its plan tasks for a
  `delegatedSubagentSessionId` whose task is still non-terminal and RECONCILES
  each against the subagent session's durable state: terminalized while down →
  roll the plan task up from its outcome; still live → re-attach the completion
  hook; subagent session gone/deleted → fail closed (`failed`). The reconcile is
  idempotent (a task already at an explicit terminal status is a no-op) and
  ignores a stale link (the task was re-delegated to a different session), so a
  late terminal callback from an abandoned session cannot clobber the live
  delegation. Plan-task isolation stays strictly per-session: the parent's plan
  node references the child by id; it never reads or writes the child's tree.
