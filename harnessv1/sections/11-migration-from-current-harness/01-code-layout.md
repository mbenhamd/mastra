### 11.1 Code layout

Harness v1 replaces the current Harness implementation at the normal package
entry point. The source layout should make that replacement explicit instead of
shipping a parallel public implementation.

**Cutover order** (dependency sequence; full prose in [§0 Mental model](../../00-mental-model.md)):

1. Storage — narrow Harness `SessionRecord`/queue/tombstone/lease rows plus composed wakeup/channel domain extensions.
2. Session — room runtime, resolver, lifecycle, multi-client `Session.signal()`.
3. Thread + Memory — bind session to thread; messages/OM via `MemoryStorage`.
4. Harness — session resolver at the front desk; remove thread-first product lifecycle.
5. Workers — claim/renew logbook rows; reopen sessions via Harness.
6. Live layer — pubsub, display, streams after Storage evidence exists.
7. §13 — routes and SDK last; project §4/§5/§10 without parallel semantics.

```
packages/core/src/harness/
├── index.ts                 # subpath: '@mastra/core/harness'
│                            # exports the v1 Harness contract
├── harness.ts               # `Harness` class: infrastructure/front desk
├── session.ts               # `Session` class: session-first runtime
├── request-context.ts       # `HarnessRequestContext`
├── storage/                 # Harness storage-domain contracts/adapters
├── channels/                # channel bridge, inbox/outbox, dispatch
└── ...
```

The package export must emit the normal ESM, CJS, and TypeScript declaration
targets for `@mastra/core/harness`. There is no `@mastra/core/harness`
compatibility subpath, no legacy production export, no shared base class, and no
runtime shim that preserves removed methods.

Stable names may keep their identifiers only when their v1 semantics match the
owning sections. Changed names are changed in place at the breaking boundary.
Removed names are removed. If an implementation needs code from the old Harness
while building the new one, that code must be moved behind the v1 owner that
enforces the v1 contract; it must not remain reachable as an alternate public
path.

Required replacement boundaries:

- Request context is rebuilt per tool execution as the §6.1
  `HarnessRequestContext` on a detached context/overlay. Caller-supplied
  top-level `harness` values reject.
- Tool execution does not expose generic `context.mastra` authority. The only
  Harness authority available to tools is the narrowed request-context surface.
- Events enter the public surface only through the closed §10 union with stamped
  session/harness identity. Stream chunks, pubsub topics, old display events, or
  route offsets are not v1 event IDs or replay cursors.
- Suspension, approval, question, and plan flows register durable pending state
  through the owning `SessionRecord` before execution is interrupted.
- Thread selection is replaced by session resolution. `currentThreadId`,
  process-local follow-up queues, thread locks, and auto-thread switching are not
  v1 lifecycle primitives.
- Workspace lifetime is resolved through the §2.7 ownership model and
  persisted/recovered through `SessionRecord.workspace` when needed.
- Scheduled/proactive work that must survive restart writes
  `HarnessWakeupItem` or another owning source row before execution.

MastraCode is part of the required migration surface, not an external example.
Its current entry points instantiate the old thread-first Harness directly and
therefore need a product adapter at the same cutover:

- `createMastraCode(...)` creates or opens a v1 `Session` before user input is
  admitted, exposes session identity to hooks/analytics/observability, and keeps
  `threadId` only as the durable history identifier.
- Headless and TUI product command names such as `--thread`, `--clone-thread`,
  `/thread`, `/threads`, `/new`, `/clone`, `/resource`, and `/name` may remain
  only as MastraCode-owned UX. Their implementation routes through session
  resolution, session clone, session rename, and session list/read models;
  they are not Harness API aliases.
- MastraCode live coordination (`SignalsPubSub`, Unix sockets, local pending
  signal state, filesystem thread locks) remains an optimization or legacy
  bootstrap input only. It must not be the durability, lease, queue, or result
  boundary for accepted v1 work.

Any old shape that cannot be routed through one of these replacement boundaries
is unsupported in v1 and must fail clearly where it is encountered.
