### 4.8f Plan-task types

The `HarnessPlanTask` type surface is the durable, arbitrary-depth, model-authored
agent task/todo **tree** (§2.8, §5.1k). It is distinct from the runtime
work-unit `HarnessTask` (which is not part of this spec). Plan tasks are
session-owned: every mutation flows through the live `Session` under its lease,
and the storage mutators are session-owner-fenced (§5.6, §5.8). The detailed
storage record fields and the `HarnessStorage` method shapes live with §5.1k and
§5.2; this declaration prevents the names from becoming implicit imports.

```ts
// §5.1k owns the persisted record field semantics; §5.2 owns the storage
// method shapes. The plan tool surface that mutates these (§6.4) and the
// plan_task_* custom event projection (§10.3) are TM-3 / TM-5 and are NOT yet
// part of the closed public/event surface.

type HarnessPlanTaskStatus =
  | 'pending'
  | 'in_progress'
  | 'blocked'
  | 'completed'
  | 'cancelled'
  | 'failed';

// Whether the current `status` was written by an explicit caller/model action
// or derived by the harness from child rollup (rollup itself is TM-4 — until
// then every write is 'explicit').
type HarnessPlanTaskStatusSource = 'explicit' | 'derived';

interface HarnessPlanTask {
  // Generated stable id; never the model's free-text title. An optional
  // `idempotencyKey` lets a retried create resolve to the same row.
  taskId: string;
  idempotencyKey?: string;

  // Namespace + owning session identity (the fence scope — §5.6 / §5.8).
  harnessName: string;
  sessionId: string;
  resourceId: string;
  threadId: string;

  // Adjacency-list tree edge → arbitrary depth. Root tasks have no parent.
  parentTaskId?: string;
  // Sibling ordering within one parent (and among roots). Stable sort key.
  order: number;

  status: HarnessPlanTaskStatus;
  statusSource: HarnessPlanTaskStatusSource;

  // Model-authored content. `content` is the imperative task title;
  // `activeForm` is the present-continuous label shown while in progress.
  content: string;
  activeForm?: string;

  priority?: number;

  // Stored as data only in TM-2. Cycle-checking and status rollup that consume
  // `blockedBy` are DEFERRED to TM-4.
  blockedBy?: string[];

  origin?: string;
  // RESERVED for TM-6 subagent delegation. Always nullable; TM-2 never writes
  // a non-null value.
  delegatedSubagentSessionId?: string;

  metadata?: JsonValue;

  createdAt: number;
  updatedAt: number;
  completedAt?: number;

  // Per-row optimistic-concurrency token for the field write, mutated only
  // under the session-owner fence (§5.6 / §5.8).
  version: number;
}
```
