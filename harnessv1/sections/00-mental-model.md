## 0. Mental model

This page is the canonical vocabulary for Harness v1. When any later section
appears to conflict with it, this page wins unless an owning section explicitly
defines a narrower rule for its scope. §1 orients the architecture; §2–§15
apply this model in detail.

### Entities

| Entity         | Role                                                                                                                                                                                                                                                                                                                        | Product callers                                                                                                               | Must not own                                                                                                                                                               |
| -------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Harness**    | Building / front desk for sessions: mode policy, runtime references, storage views, workers, and session routing over Mastra-registered agents, tools, memory, workspaces, channels, pubsub, and background-task infrastructure. Coordinates conversations; **not** one conversation and not a second Mastra root registry. | `HarnessConfig`, `init`/`shutdown`, concrete `harness.session(...)`, catalogs, cross-session subscribe                        | Per-conversation lifecycle as the default path (`switchThread`, `currentThreadId`, thread-first singleton state); provider/channel/task registries already owned by Mastra |
| **Session**    | One active **room**: live or reopenable runtime — run, queue, pending decisions, channel binding, memory/context attachment, settings, execution ownership.                                                                                                                                                                 | `create/open`, `close`, `delete`, `rename`, `clone`, `session.signal()`, `session.queue()`, inbox responses, state/mode/model | Durable transcript as the only identity; blanket exclusive locks on read/subscribe/admitted signals                                                                        |
| **Thread**     | Durable **conversation record** behind the room: transcript/history, audit, replay substrate.                                                                                                                                                                                                                               | Indirect via session; low-level history/import only when explicit                                                             | Normal app lifecycle through thread-first Harness methods                                                                                                                  |
| **Memory**     | Room **notebook**: recall, grounding, working facts, summaries, observational learning.                                                                                                                                                                                                                                     | OM/processor config, message history via shared `MemoryStorage` view                                                          | What work exists, what must recover, or what was delivered (**Storage** owns that)                                                                                         |
| **Storage**    | Building **logbook**: session rows, queue/receipts/tombstones, leases/claims, accepted-signal evidence, attachment metadata, and source-specific durable work rows such as wakeups or channel inbox/action/outbox. Write **before** restart-sensitive execution or provider-visible delivery.                               | A narrow Harness storage domain plus source-owned domain extensions; not app lifecycle                                        | Live UX projection as source of truth; duplicating MemoryStorage, ChannelsStorage, BackgroundTasksStorage, scheduler, PubSub, or BlobStore authorities                     |
| **Workers**    | Recovery **loops**: claim logbook rows, renew ownership, reopen rooms via Harness front desk, rebuild notebook/runtime; **stop** if stale or unsafe to rebuild.                                                                                                                                                             | Registered in `HarnessConfig`; not a second public conversation API                                                           | Guessing across stale ownership; blocking multi-client read/signal                                                                                                         |
| **Live layer** | Lights / intercoms / whiteboards: streams, callbacks, heartbeat timers, sockets, pubsub, legacy `AgentChannels` live path.                                                                                                                                                                                                  | Optimization while room is open                                                                                               | Durability, admission evidence, recovery keys                                                                                                                              |

### Architecture rules

- **Session-first lifecycle** for create/open, close, delete, rename, settings,
  and clone. Partial-history fork is deferred from v1 until §5 and §13 define one
  canonical copy/storage/route contract.
- **`close`** means the conversation is no longer live but remains reopenable.
  **`delete`** removes the session and the durable conversation/artifacts it owns,
  subject to ownership and safety guards. A preserve option may exist only when
  explicit and well named.
- **`clone`** creates a new usable session/conversation; copies committed message
  history plus explicitly allowed app-owned metadata named by §5; **never** copies
  live process handles, active streams, stale leases, or in-flight tool execution
  ownership.
- **Multi-client:** multiple clients may attach to the same thread/session,
  observe events, and submit authorized input. User text, steering, reminders,
  approvals, and other runtime inputs enter the v1 runtime as admitted
  `Session.signal()` calls; "message" is a product/UI label for a
  `type: 'user-message'` signal, not a separate lifecycle primitive or generic
  thread write. Signals are runtime dispatches with stable identity,
  routing/result semantics, and active-vs-idle behavior. If a run is active, an
  authorized signal may interleave into that run; if the session is idle, it may
  wake a new run. Leases and claims must not block read, subscribe, append of
  authorized transcript messages, or admitted signals.
- **Harness** stays the orchestration boundary. **There is no
  `harness.threads.*` lifecycle surface**; thread helpers, when needed, live
  behind explicit operator/import history boundaries (§4.1).
- **Mastra remains the root infrastructure registry.** `HarnessConfig` is the
  session front-desk configuration: modes, session policy, storage-domain views,
  goals, subagents, channel bindings, worker knobs, and runtime references. It
  composes with Mastra-registered agents, tools, memory, workspaces, channels,
  pubsub/cache, workflows, and background-task infrastructure instead of owning
  parallel provider registries.
- **Product vs adapter:** product code uses Session concepts; storage adapters and
  server runtimes use lower-level contracts for registration, storage, leases,
  claims, cleanup, recovery, and channel coordination.

### Section map (entity ownership)

| Section                | Primary entities                                                       |
| ---------------------- | ---------------------------------------------------------------------- |
| §1 What the Harness is | Harness, Session, Storage, Workers (orientation)                       |
| §2 Core concepts       | Harness, Session, Thread, Memory, Resource, Workspace                  |
| §3 Concurrency model   | Session, Storage (admission/lease)                                     |
| §4 Public API          | Harness, Session; Thread helpers at Harness only for non-product paths |
| §5 Session persistence | Storage, Session, Thread, Memory                                       |
| §6 Tool authoring      | Session (via `HarnessRequestContext`)                                  |
| §7 Sandbox commands    | Harness config policy only                                             |
| §8 Subagent guarantees | Session, Harness                                                       |
| §9 Configuration       | Harness, Storage, Memory, Workers, Live layer (heartbeat hooks)        |
| §10 Events             | Session, Harness; Live layer as non-durable input                      |
| §11 Migration          | All entities; cutover order in §11.1                                   |
| §12 Usage examples     | Illustrative only — defer to §0 and owning sections                    |
| §13 Server integration | Harness front desk projecting Session/Storage/Events                   |
| §14 Channels           | Harness control plane, Storage logbook, Session admission              |
| §15 Verification       | Cross-cutting claim checks on Storage, Session, Workers                |

### Implementation cutover order

Build in dependency order. Do not treat §13 routes as the first milestone while
the in-process front desk is still thread-first.

1. **Storage** — narrow Harness session records plus composed storage-domain
   extensions for queue/tombstones, wakeup/channel rows, leases, and existing
   MemoryStorage/ChannelsStorage/BackgroundTasksStorage/PubSub/blob authorities
   (§5).
2. **Session** — room runtime, resolver, close/delete/clone, multi-client
   `Session.signal()` (§4.2, §5.3).
3. **Thread + Memory** — bind session to thread; OM and messages via
   `MemoryStorage`; no second work ledger.
4. **Harness** — session resolver at the front desk; remove thread-first product
   lifecycle from the singleton and `memory` accessor.
5. **Workers** — claim/renew logbook rows; reopen via Harness (§5.2, §14).
6. **Live layer** — pubsub, display, streams as optimizations after Storage
   evidence exists.
7. **§13** — routes and SDK last; project §4/§5/§10 without parallel semantics.

Duplicate-cutover traps are indexed in §11.6e.

**Admission identity (no duplicate definitions):** hash inputs and option rules
→ [§4.4b](04-public-api/04-operation-option-types/02-queue-and-skill-options.md);
durable receipts and tombstones →
[§5.1d](05-session-persistence/01-what-gets-persisted/10-queue-admission-and-tombstones.md).
