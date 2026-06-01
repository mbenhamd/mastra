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
shares the parent's lease for *write ownership* — plan-task mutators are fenced on
**the owning session's** lease/version, and because a child shares the parent's
`ownerId`, the same live owner serializes both trees' writes. The reserved
`delegatedSubagentSessionId` field (TM-6) will later link a parent plan node to a
delegated child session, but in TM-2 it is never written and plan-task isolation
stays strictly per-session.
