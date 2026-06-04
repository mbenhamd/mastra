### 2.8 Terminology aliases

The Harness v1 spec uses a small number of paired terms interchangeably. Both
words refer to the same underlying concept; the variation reflects the audience
of the surrounding paragraph (data shape vs. runtime behavior vs. caller-facing
API) and is not a sign that two distinct constructs exist. If a spec section
introduces a third term that resolves to one of the canonical concepts below,
that section should add it here rather than redefine it.

- **Tombstone ≡ compacted evidence.** Canonical name: `tombstone` (specifically
  `OperationAdmissionTombstone`, §5.1d). "Compacted evidence" appears in §3 and
  §5.7 prose when the focus is the post-compaction retention lifecycle rather
  than the persisted row shape. Both names refer to the same retention-bounded
  index row that survives result/tombstone retention boundaries.
- **Detached context ≡ overlay.** Canonical phrase: "detached context/overlay",
  written as a hyphenated pair in §11.1 and used interchangeably in §6.1. Both
  refer to the per-tool-execution `HarnessRequestContext` projection that does
  not mutate the caller's `RequestContext`. The §6.1 slot-overlay pattern
  (`context.requestContext.get('harness')`) is the same construct as the
  §11.1 "rebuilt per tool execution on a detached context/overlay"
  requirement.
- **Session resolver ≡ session lookup.** Canonical name: `session resolver`
  (§5.3). "Session lookup" appears in §4-level call-signature prose when the
  surrounding paragraph is about caller intent rather than the resolver's
  internal phases (find-in-memory → find-in-storage → atomic
  `createOrLoadCurrentSessionOwner(...)`). Both refer to the same
  `harness.session(...)` algorithm.

Pairs that look similar but are **not** interchangeable:

- **Lease ≠ lock-mode.** A *lease* is the storage-owned write-ownership
  primitive with TTL and renewal (§5.2f); a *lock-mode* (`'wait'` /
  `'steal'` / `'fail'` per §5.8) is the conflict-resolution policy a caller
  selects when contending for that lease. The lease is the resource;
  lock-mode is how a caller asks for it.
- **Admission ≠ accepted.** *Admission* is the session-boundary validation
  phase that runs before a signal/queue/skill operation is recorded (§3,
  §4.2f). *Accepted* is the terminal classification an admitted operation
  receives once admission succeeds (`AgentSignalAccepted`, `status: 'accepted'`
  in §5.1d). Every accepted operation went through admission, but admission
  may also reject — and a rejected admission is not "an accepted operation
  that failed later".
- **`HarnessTask` ≠ `HarnessPlanTask`.** These are two unrelated constructs
  that both end in `Task`. **`HarnessTask`** (defined in the runtime contracts,
  not in this spec) is the internal *work-unit* primitive: a single scheduled or
  in-flight piece of runtime execution. It is unchanged by `HarnessPlanTask` and
  carries no `parentTaskId`, sibling order, or model-authored plan semantics.
  **`HarnessPlanTask`** (§4.8, §5.1k) is the durable, arbitrary-depth,
  *model-authored* agent task/todo **tree** — the persisted plan the agent
  builds and revises as it decomposes a goal. A `HarnessPlanTask` is a
  session-owned plan node (adjacency-list `parentTaskId`, sibling `order`,
  lifecycle `status`); it is never a work-unit and is never scheduled directly.
  When a section says "plan task" it always means `HarnessPlanTask`.
