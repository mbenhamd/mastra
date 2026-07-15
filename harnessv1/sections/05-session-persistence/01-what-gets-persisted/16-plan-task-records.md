### 5.1k Plan-task records

`HarnessPlanTask` is the durable, arbitrary-depth, **model-authored** agent
task/todo tree (§2.8). It is a session-owned plan node, not the runtime
work-unit `HarnessTask` and not an admission receipt, event, run projection, or
recovery boundary. The tree is an adjacency list: each row carries an optional
`parentTaskId` and a numeric sibling `order`, so depth is unbounded and a parent
owns its children purely by edge. The plan tree captures what the agent intends
to do and how it has decomposed a goal; it is revised by the model as work
proceeds.

**Record shape.** The persisted columns are the §4.8f `HarnessPlanTask` fields:

- `taskId` — generated stable id (never the model's free-text title). An
  optional `idempotencyKey` lets a retried create resolve to the same row
  instead of forking a duplicate node.
- `harnessName`, `sessionId`, `resourceId`, `threadId` — the owning identity.
  `(harnessName, sessionId)` is the isolation + fence scope (§5.6, §5.8).
- `parentTaskId?` — adjacency-list edge to the parent node; absent for roots.
- `order` — sibling ordering within one parent (and among roots). Listing is
  ordered by `(parentTaskId, order)`.
- `status` — `'pending' | 'in_progress' | 'blocked' | 'completed' |
'cancelled' | 'failed'`.
- `statusSource` — `'explicit'` when a caller/model wrote the status, or
  `'derived'` when the harness computed it from child rollup. **Rollup is
  DEFERRED to TM-4**; until then every write is `'explicit'`.
- `content` (the task title) and `activeForm?` (present-continuous label).
- `priority?`.
- `blockedBy?: string[]` — stored as data in TM-2. **`blockedBy` cycle-checking
  and the rollup that consumes it are DEFERRED to TM-4**; storage only persists
  and returns the column.
- `origin?`.
- `delegatedSubagentSessionId?` — **LIVE (TM-6)**: the subagent SESSION this plan
  task was durably delegated to. Written by `task_delegate` under the
  session-owner fence in the SAME transaction that drives the task `in_progress`,
  so the link and the status can never diverge across a crash. While the subagent
  session has not terminalized the task stays `in_progress (delegated)`; when it
  terminalizes the task rolls up `completed` / `failed` and the §5.1k truth-table
  cascades to ancestors. See §5.6 for the delegation lifecycle, the durable
  wait-point, and recovery-on-rehydrate.
- `metadata?` (`JsonValue`).
- `createdAt`, `updatedAt`, `completedAt?`.
- `version` — per-row optimistic-concurrency token for the field write,
  advanced on each successful `updatePlanTask`, but only ever mutated **under
  the session-owner fence** (§5.6, §5.8).

**Schema.** Adapters persist plan tasks in a dedicated namespace-scoped table
(`mastra_harness_plan_tasks`) keyed by `(harness_name, session_id, task_id)`,
indexed by `(harness_name, session_id, parent_task_id, "order")` for ordered
listing and subtree walks. `blockedBy`, `metadata`, and any structured columns
are JSON. The adjacency-list shape means a bounded subtree read and a
cascade-delete both walk `parent_task_id` via a recursive CTE (SQL adapters) or
a BFS frontier (in-memory); both **defensively guard cycles** with a visited
set / `UNION` dedupe even though full cycle _prevention_ is TM-4.

**Concurrency model.** All plan-task writes go through the live `Session` under
its lease (§5.8): the session is the single serialized writer, so the storage
mutators are **session-owner-fenced** rather than relying on bare per-row OCC.
Every mutator takes `{ harnessName, sessionId, ownerId, ifSessionVersion }` and
verifies, against the owning `SessionRecord`, that `ownerId` still holds the
unexpired lease and that the session's `version` matches `ifSessionVersion`
before any plan-task row changes — mirroring how `saveSession` fences on the
session's owner/version. The per-row `version` is the field-write OCC token
_inside_ that fence (so a stale in-memory plan-task read is still caught), not
an independent cross-process authority. Multi-row operations
(`mutatePlanTasksForSession`, `deletePlanTaskSubtree`) are transaction-shaped:
they either apply every row change under one adapter boundary or reject without
partial application. Plan tasks are `harnessName`-scoped; a cross-harness
mismatch is tenant-safe not-found, never retargeting (§5.2). Reads
(`listPlanTasks`, `loadPlanTaskSubtree`) are paginated / bounded so a large plan
tree never forces an unbounded scan — `loadPlanTaskSubtree` is the
anti-forgetting "next-N under this root" read the agent uses to re-orient.
