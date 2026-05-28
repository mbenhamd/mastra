### 2.2 Thread vs Session

| | Thread | Session |
|---|---|---|
| What | Durable transcript/history behind the room | Active or reopenable per-conversation runtime + persisted runtime state |
| Storage | Shared `MemoryStorage` history rows, scoped by the Harness storage view | Narrow Harness session records plus composed source-specific domain rows |
| Lifetime | Until explicitly deleted with the owning conversation | Until explicitly deleted; `close` only makes it non-live/reopenable |
| Cardinality | One per conversation inside a Harness namespace | At most one current owner per `(harnessName, resourceId, threadId)` across Active, Closing, and Closed reopenable records |
| In memory? | Loaded on demand | Hydrated on demand; auto-evicted when idle |

A thread is the message history. A session is the live conversation that
operates on it. Closing a session does not delete the thread or create a new
conversation; it makes the room no longer live while keeping it reopenable.
Deleting the session is the destructive path that removes the session and the
durable conversation/artifacts it owns, subject to the §5.5 safety checks.

Harness v1 has exactly one current owner for a conversation inside one
registered Harness: the `SessionRecord` for
`(harnessName, resourceId, threadId)`. While Active, that record is the live
runtime owner; while Closing or Closed, it still reserves the reopen/delete owner
key. Multiple clients can attach to that same active session, but they do not
create independent active session records for the same thread in the same
Harness namespace. This keeps the mutable runtime —
queue, pending items, `currentRun`, display state, permissions, model/mode, and
workspace handle — behind one write lease. The lease does not prevent authorized
clients from reading, subscribing, or admitting signals through the owning
session.

Common cases all route to that same active session:

- The same human on a laptop and a phone, both attached to the conversation.
Each device gets a client connection or `RemoteSession` view over the same
active `sessionId`; both read and write through the same queue and run admission
boundary, with immediate input admitted through `Session.signal(...)` and
sequential work admitted through `queue(...)`.
- A long-running conversation rehydrated by a different server process on each
request. Ownership may move after lease release/expiry, but only one process
owns the active session at a time.
- Operator tooling resuming a thread programmatically alongside the original
user's live session. The operator either attaches to the active session under
the normal lease policy or uses explicit operator/admin tooling; it does not
create a second active owner for the same thread.

Multiple session records for one thread are not the normal lifecycle model.
Clone creates a new usable session/conversation with the copied durable state
named by §5; close/reopen keeps the same session identity. Partial-history fork
is deferred from v1 instead of being treated as another thread clone spelling.
Historical or deleted records may exist for audit, retention, or operator
repair, but normal app code should not rely on a fresh active record being
created simply because a session was closed.

Threads are **not** shared across resources in v1. A thread is permanently bound
to the `resourceId` it was created under, and it is only addressable by that
resource. Cross-tenant shared / collaborative threads are intentionally out of
scope (see §11.5 on what's not in v1).
