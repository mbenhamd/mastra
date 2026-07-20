## 1. What the Harness is

**Entity ownership:** Harness (front desk), Session (room), Storage (logbook),
Workers (recovery) — orientation only; see [§0 Mental model](../00-mental-model.md).

The Harness is an orchestration layer that sits between an application and the
Mastra agent runtime. It coordinates session resolution, mode policy, tool/skill
views, and the bridge between user-facing UIs and agent execution. It composes
with Mastra-owned infrastructure; it is not itself one conversation and not a
second Mastra root registry.

This section owns the high-level architecture orientation and the canonical
Harness/Session responsibility split. Detailed mechanics stay with their
owning sections: concurrency/admission in §3, public API shape in §4, durable
storage and recovery in §5, server and wire behavior in §13, channels in §14,
and verification in §15. When this overview names those areas, treat it as a
map to the later owner rather than a second source of truth.

Channel transports (Slack, Discord, Teams, SMS, email, etc.) are one kind of
user-facing UI. Provider adapters can still own platform credentials,
verification, formatting, and send/edit helpers, but Harness defines the bridge
that makes channel ingress and outbound delivery flow through durable `Session`
and storage rows instead of bypassing them.

Two roles, cleanly split:

- **`Harness`** — restartable orchestration infrastructure. Holds a
  session-facing view over Mastra-registered agents, modes, model resolution,
  tools, skills, memory, workspace factories, channel providers, storage domains,
  workers, and runtime policy. Created once per process. It may keep live
  caches, workers, intervals, listeners, and route lifecycle state, but it does
  not own the root provider/tool/channel/task registries and does not own durable
  per-conversation state; storage does.
- **`Session`** — active or reopenable per-conversation runtime. Owns the
  mutable state callers should reason about: current run state, queue, pending
  decisions, channel binding, memory/context, runtime settings, and any
  execution ownership needed to continue work.
- **`Thread`** — durable conversation record behind the room. It stores
  transcript/history and remains important for persistence, audit, replay, and
  memory/history substrate. Normal lifecycle APIs are session-first; app code
  should not be forced to drive lifecycle through thread-first Harness helpers.

A useful mental model:

> Think of the Harness as the building for the agent architecture. The
> front desk knows which agents, modes, models, tools, memory providers,
> workspaces, channels, storage backends, workers, and runtime policies are
> available. The building coordinates conversations; it is not itself one
> conversation.
>
> A Session is one active room in that building. It represents a live or
> reopenable conversation runtime: current run state, queue, pending decisions,
> channel binding, memory/context, runtime settings, and any execution ownership
> needed to continue work.
>
> A Thread is the durable conversation record behind the room. It stores the
> transcript/history and backs persistence, audit, replay, and memory/history
> substrates. Normal app lifecycle stays session-first.
>
> Memory is the room notebook. It gives the agent recall, grounding, working
> facts, summaries, and observational learning. It helps the room think, but it
> is not the source of truth for what work exists, what must recover, or what has
> already been delivered.
>
> Storage is the building logbook. Queue items, wakeups, channel inbox/actions,
> outbox items, session state, attachments/artifacts, leases/claims, and delivery
> evidence are written there before restart-sensitive execution or
> provider-visible delivery.
>
> Workers are Harness/server recovery loops in the building. They read the
> logbook, claim specific recoverable work rows when needed, renew ownership while
> doing that work, re-enter through the Harness front desk to reopen rooms, rebuild
> the notebook and runtime services, and continue after a crash. If ownership goes
> stale or the same room/runtime surface cannot be rebuilt safely, they stop
> instead of guessing.
>
> Live streams, callbacks, heartbeat timers, sockets, pubsub, and live channel
> adapter paths are the lights, intercoms, and whiteboards in the room. They make
> the room responsive while it is open and can fan out updates across processes,
> but they are not the durable architecture.

**Memory is advisory context, not a source of durability truth.** Agent Memory
processors rebuild working and observational memory from the persisted message
history and stored observations at runtime; Harness does not configure a second
per-session OM engine. Memory rows are eventually
consistent, guarded by process-local locking only, and are **not** subject to
the session lease or version CAS (§5.8). They must not be the proof boundary for
queue, channel, wakeup, approval, or goal decisions — those boundaries are owned
by the durable Harness storage records. If a backend cannot provide even
advisory memory consistency, it should fail at init rather than silently degrade
context.

At overview level, that simple model means:

- **Harness = building/front desk.** It owns session policy and wiring over
  Mastra-owned registries: agents, modes, model/provider resolution, tools,
  memory providers, workspace factories, channel adapters, storage domains,
  workers, readiness, shutdown, and runtime policy. It is created once per
  process and does not become one conversation, a second Mastra root, or the
  durable owner of conversation state.
- **Session = room.** The active or reopenable session owns one durable
  conversation for one `resourceId` / `threadId`: current mode/model, display
  state, custom state, pending approvals/questions/plans, current run, queue,
  channel binding, workspace state, memory context, and execution ownership.
  Multiple clients attach to that room rather than creating parallel active rooms
  for the same conversation.
- **Thread = transcript.** The thread is the durable message/history substrate
  behind a session. It is useful for reads, audit, replay, and memory grounding,
  but it is not the normal lifecycle object for app code.
- **Inputs = requests at the room door.** App calls, channel events, user
  actions, and scheduled work all enter through the Harness. The exact
  admission and durable-row rules live in §3, §5, §13, and §14.
- **Runtime = services brought into the room.** The Session assembles the
  request context, memory, workspace state, model, tools, MCP bindings, and
  agent/workflow runtime. Those dependencies are used by the Session, but they
  do not own Harness durability.
- **Outputs = notes back in the logbook.** Session state, run status, pending
  responses, and provider-visible output are recorded or projected through the
  owning storage/channel rules in §5 and §14.
- **Workers = recovery loops.** Workers are Harness/server execution machinery.
  They re-enter Harness for session admission or mutation instead of becoming a
  second path around Harness; cross-source recovery lives in §5.7, readiness
  and server lifecycle live in §13.6, channel-specific claim/dispatch mechanics
  live in §14, and §15 verifies those promises.
- **Recovery = read the logbook, reopen the room.** After a crash, Harness
  rebuilds its registry, Sessions hydrate from storage, and worker-owned work
  resumes only through the later recovery contracts.
- **Lease and claim scopes are narrow.** Lease/claim concepts are for exclusive
  recovery-sensitive work, worker ownership, provider-visible delivery, or
  side-effect execution. They are not a blanket lock on reading a thread,
  subscribing to updates, admitting authorized `signal` / `queue` / inbox
  operations, or dispatching admitted signals.
- **Live-only things are helpers, not promises.** SSE buffers, stream callbacks,
  in-memory pending resolvers, sockets, pubsub, process-local intervals, and live
  channel-adapter paths can observe or speed up work. They are not the recovery
  boundary.

Conceptual architecture flow, with Harness at the center:

Diagram blocks:

- **Ingress sources** — app calls, SDK/HTTP, channel callbacks, and scheduled or
  proactive work entering Harness.
- **Harness front desk** — the central orchestration point that resolves
  Sessions, records durable work, and routes recovery.
- **Harness storage logbook** — durable rows that survive restart: inbox/actions,
  wakeups, current run or queue items, session state, attachments/artifacts,
  leases/claims, delivery evidence, and outbox rows.
- **Recovery workers** — worker loops that claim durable rows and re-enter
  Harness instead of bypassing it.
- **Session room** — the per-conversation runtime that assembles memory, request
  context, workspace, model/tools, and the Mastra runtime.
- **Channel delivery** — outbox dispatch through the Harness/channel registry to
  the provider API.

The architecture below appears as five focused sub-diagrams. Each isolates one
phase so labels and arrows do not overlap; together they cover the full
lifecycle.
