### 10.5 Buffering and replay

Each session keeps a ring buffer of recent events (`sessions.eventBufferSize`,
default 1000; see §9). The buffer feeds two consumers:

- **`session.subscribe(...)` after the fact.** If the session is currently in a
  turn when a new subscriber attaches, the subscriber sees future events only — no
  automatic backfill. Callers that need to recover from a missed window should use
  `session.listMessages(...)` for content and the SSE replay path for live event
  continuation.
- **SSE replay over the wire.** The Mastra Server adapter (§13) honours
  `Last-Event-ID` on the SSE endpoint. The server replays buffer entries newer
  than `Last-Event-ID`, then live-tails. See the replay rules below.

The local `harness.subscribe(...)` control-plane stream is not backed by a
merged replay buffer. A late harness subscriber sees future harness-scoped
events
and future fan-out copies from live sessions only; it does not backfill previous
events from session buffers and cannot reconnect with `Last-Event-ID`.

**Epoch and event IDs.** Each in-memory Session instance has an `epoch` token,
generated fresh whenever the instance is constructed — first hydration,
rehydration after eviction, or hydration after a process restart. Event `id` is
`harness-v1:<epoch>:<seq>`, where `seq` is monotonic within the epoch and resets
when the epoch changes. Two events from different epochs are never comparable as
a sequence, even if they share the same `seq`. Harness-scoped events use the
same `harness-v1:<epoch>:<seq>` shape against the harness's own epoch+sequence.

**Replay ID grammar.** `Last-Event-ID` is accepted only as
`harness-v1:<epoch>:<seq>`. `<epoch>` is a non-empty base64url token generated
by the Session instance and never contains `:`. `<seq>` is a base-10 unsigned
integer with no sign, decimal point, exponent, or surrounding whitespace. The
server must parse the whole header value; any prefix, suffix, alternate
separator, empty field, non-decimal sequence, negative value, or value outside
JavaScript's safe integer range is malformed.

**Replay rules.** On reconnect with
`Last-Event-ID: harness-v1:<epoch>:<seq>`:

- If the epoch matches the current Session instance and `seq` is within the
  buffer, the server replays entries newer than the supplied ID and live-tails.
- If the epoch matches but `seq` is older than the buffer's oldest entry, the
  buffer has overflowed; the server returns `412 Precondition Failed`.
- If the epoch does not match the current Session instance, the prior epoch's
  buffer is gone (eviction or process restart). The server returns
  `412 Precondition Failed`.
- If `Last-Event-ID` is malformed or absent, the server starts the SSE stream
  from the live tail with no replay.

In every `412` case the client is expected to refetch the session snapshot via
`GET /sessions/:sessionId` and resubscribe. That route returns the
`SessionSnapshot` read model (§5.1): identity, lifecycle, current run
projection, queue item identifiers, session-owned pending inbox items, display
snapshot, goal state, channel binding summary, token usage, the bounded
durable-work summary, and a bounded message window or cursor for the persisted
thread message log. It does not synthesize missed `text_delta`, tool, or channel
events from storage. The display snapshot may include
`assistantDrafts`: bounded, coalesced assistant text/reasoning accumulated by
the session while streaming. Clients use those drafts as render recovery for
in-progress assistant output after a gap; they do not treat them as replayed
events or transcript messages.
Multi-session controllers apply this rule per affected session and rebuild their
view through the §13.4 controller recovery recipe rather than through
cross-session event replay.

**Scope.** The Harness session SSE replay contract is a live-session projection,
not a second durable event store. Implementations should compose existing Mastra
pubsub/cache replay primitives when they can enforce this section's session
scope, epoch, `Last-Event-ID`, overflow, and stale-cursor rules. If the selected
pubsub/cache implementation cannot enforce those rules, the Harness adapter uses
a per-session in-memory ring buffer and drops it on session eviction or harness
shutdown. **Durable Harness SSE replay across restarts is not a goal of v1**;
the durable recovery path is snapshot/message/result lookup, not synthetic event
history.

Current cache-backed, topic-indexed, or run-scoped replay paths may feed the §10
event adapter internally, but they are not independently authoritative for the
Harness SSE cursor contract. The epoch contract makes the "stale ID after
restart or eviction" path deterministic: any `Last-Event-ID` from a previous
epoch is detected at the server and yields `412`, even if a new event happens to
share the same `seq`. Synthesizing replay from message storage or any other
persisted state is explicitly out of scope. Session-scoped lifecycle
notifications such as `session_evicted`, when observed before the buffer
disappears, are still observer events only: they do not imply `closedAt`,
durable replay availability, or operation settlement. Harness-scoped
notifications such as `harness_shutdown` are delivered only to harness
subscribers and are not part of a per-session SSE replay buffer. Clients that
need durable history beyond a single epoch should use the snapshot's message
cursor and `GET /sessions/:sessionId/messages` (§13.2) for the persisted message
log through the verified session boundary, and treat the SSE stream as
live-only.
