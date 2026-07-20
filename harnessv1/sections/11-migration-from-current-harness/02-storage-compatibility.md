### 11.2 Storage import boundaries

This section owns old-data import boundaries. It is not the canonical owner for
persisted record shapes, thread metadata write semantics, storage adapter
methods, route payloads, or channel ledgers: §5.1 owns persisted records and
`ThreadMetadata`, §5.2 owns the storage adapter surface, §13 owns server and wire
projections, and §14 owns channel binding, inbox, action, and outbox rows.

Harness v1 does not silently bootstrap runtime state from old Harness metadata
or old process-local state. Existing thread/message rows may be imported only
when an explicit import path proves `harnessName`, `resourceId`, `threadId`,
message ordering, attachment references, and tenant safety. Everything else is
unsupported input until its owning section defines a current v1 mapping.

Import rules:

1. The v1 Harness registers a Harness-scoped storage view (§5.2). If the
   configured store cannot provide the required namespace, resource checks,
   shared message-log view, active/reopenable session uniqueness, and stable
   cursor ordering, registration or import fails.
2. Import creates or reuses a `HarnessThread` plus a current `SessionRecord`
   through the same storage-linearized resolver used by normal session creation.
   It does not write thread metadata first and then infer runtime state later.
3. Mode/model, permissions, channel bindings, subagent parentage, token usage,
   workspace state, queue, pending items, wakeups, outbox, and delivery receipts
   are imported only through explicit v1 fields owned by their canonical
   sections. Effective OM configuration moves to Agent Memory; import must not
   create new `SessionRecord.observationalMemory` overrides. Old top-level
   metadata is never a second authority.
4. Attachments must be normalized into `PersistedAttachment` refs before any new
   durable queue, signal, message-history, current-run, channel-inbox, wakeup, or
   outbox-projection row references them. Raw URLs, data URIs, base64 blobs,
   process-local paths, temporary provider URLs, and live handles reject unless
   an explicit ingestion step stores a Harness-owned ref first.
5. Old channel subscription fields do not create, modify, claim, or invalidate a
   v1 `ChannelBinding`. Provider-authenticated ingress creates or resolves
   bindings through §14.1, and outbound delivery is always outbox-backed.
6. Old subagent/fork metadata does not synthesize `parentSessionId`, depth, child
   sessions, or historical parent-stream events. A product/operator import must
   resolve and write child-session ownership explicitly.
7. Old state objects, live pending resolvers, current run IDs, thread locks,
   heartbeat registrations, and process-local queues are not durable data. They
   are not imported.
8. MastraCode goal judge history that was stored in synthetic threads such as
   `${threadId}-${goalId}` with `goalJudge` / `forkedSubagent` metadata is legacy
   implementation input only. Importers must either write an explicit
   `GoalState.judgeMemory` reference owned by the target session/goal or ignore
   those rows as ordinary history according to a named import policy. `clone`
   never copies `GoalState`, judge-memory references, or old synthetic judge
   threads.

Public thread metadata writes remain narrow: `session.setThreadSetting(...)`
writes only `thread.metadata.app[key]`, never raw top-level metadata. Reserved
top-level metadata stays framework-owned and is not consulted for session
hydration, mode/model selection, permission policy, Agent Memory OM configuration, token
accounting, channel routing, subagent ownership, or thread titles.
