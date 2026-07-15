### 5.3 Resolution semantics

The `harness.session(...)` resolver runs find-in-memory → find-in-storage →
atomic create-or-load, while preserving the v1 ownership invariant: at most one
current `SessionRecord` owner may exist for a
`(harnessName, resourceId, threadId)` pair. Active, Closing, and Closed
reopenable records all occupy that current-owner key until explicit delete or
hidden cleanup. The resolver never performs a cold read miss followed by a blind
session insert. Any
path that may create or mutate an active session goes through
`createOrLoadCurrentSessionOwner(...)`; hydration for live execution happens only
after the caller proves or acquires write ownership. Read-only attach,
subscription, result lookup, and source-specific durable admission may resolve
the same identity without treating the lease as a blanket lock.
`loadSessionByThread(...)` is a non-atomic fast path for existing records only.
It must never be used as a creation pre-check followed by a separate insert;
only `createOrLoadCurrentSessionOwner(...)` is atomic for session admission. The
atomic boundary is `createOrLoadCurrentSessionOwner(...)`.

Orientation diagram (resolver phases only; lookup table below remains
authoritative):

<figure>
  <svg role="img" aria-labelledby="hx-resolution-title hx-resolution-desc" viewBox="0 0 1040 390" width="100%" style="max-width: 1100px; height: auto; display: block; margin: 1.5rem auto; background: #ffffff; border: 1px solid #e2e8f0; border-radius: 18px; padding: 16px; box-sizing: border-box;">
    <title id="hx-resolution-title">Session resolver phases</title>
    <desc id="hx-resolution-desc">Session resolution checks memory, storage, the atomic session-owner create-or-load boundary, ownership requirements, and hydration while preserving the owner-key invariant.</desc>
    <defs>
      <marker id="ah-resolution" markerWidth="10" markerHeight="10" refX="8" refY="5" orient="auto">
        <path d="M0,0 L10,5 L0,10 Z" fill="#334155" />
      </marker>
    </defs>

    <rect style="fill: #eef2ff; stroke: #6366f1; stroke-width: 2.5; rx: 16;" x="40" y="155" width="155" height="68" />
    <text style="font: 600 17px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #0f172a;" x="118" y="183" text-anchor="middle">session(...)</text>
    <text style="font: 500 13.5px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="118" y="205" text-anchor="middle">lookup shape</text>

    <rect style="fill: #f8fafc; stroke: #94a3b8; stroke-width: 2; rx: 14;" x="245" y="155" width="155" height="68" />
    <text style="font: 600 17px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #0f172a;" x="323" y="183" text-anchor="middle">Memory</text>
    <text style="font: 500 13.5px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="323" y="205" text-anchor="middle">live instance?</text>

    <rect style="fill: #f8fafc; stroke: #94a3b8; stroke-width: 2; rx: 14;" x="450" y="155" width="155" height="68" />
    <text style="font: 600 17px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #0f172a;" x="528" y="183" text-anchor="middle">Storage</text>
    <text style="font: 500 13.5px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="528" y="205" text-anchor="middle">existing record?</text>

    <rect style="fill: #ecfdf5; stroke: #22c55e; stroke-width: 2; rx: 14;" x="655" y="135" width="180" height="88" />
    <text style="font: 600 17px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #0f172a;" x="745" y="168" text-anchor="middle">Atomic boundary</text>
    <text style="font: 500 13.5px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="745" y="191" text-anchor="middle">createOrLoadActive</text>
    <text style="font: 500 13.5px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="745" y="210" text-anchor="middle">one active owner</text>

    <rect style="fill: #eef2ff; stroke: #6366f1; stroke-width: 2.5; rx: 16;" x="885" y="155" width="130" height="68" />
    <text style="font: 600 17px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #0f172a;" x="950" y="183" text-anchor="middle">Hydrate</text>
    <text style="font: 500 13.5px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="950" y="205" text-anchor="middle">read or write</text>

    <rect style="fill: #fff7ed; stroke: #f97316; stroke-width: 2; rx: 14;" x="450" y="285" width="385" height="58" />
    <text style="font: 600 17px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #0f172a;" x="643" y="310" text-anchor="middle">Closing / closed / conflict rows fail before new work</text>
    <text style="font: 500 13.5px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="643" y="331" text-anchor="middle">tenant-safe not-found, closed, closing, conflict, or corrupt errors</text>

    <path style="stroke: #334155; stroke-width: 2.2; fill: none; marker-end: url(#ah-resolution);" d="M195 189 L244 189" />
    <path style="stroke: #334155; stroke-width: 2.2; fill: none; marker-end: url(#ah-resolution);" d="M400 189 L449 189" />
    <path style="stroke: #334155; stroke-width: 2.2; fill: none; marker-end: url(#ah-resolution);" d="M605 189 L654 189" />
    <path style="stroke: #334155; stroke-width: 2.2; fill: none; marker-end: url(#ah-resolution);" d="M835 189 L884 189" />
    <path style="stroke: #334155; stroke-width: 2.2; fill: none; marker-end: url(#ah-resolution);" d="M528 223 L595 284" />
    <path style="stroke: #334155; stroke-width: 2.2; fill: none; marker-end: url(#ah-resolution);" d="M745 223 L690 284" />

  </svg>
<figcaption>Creation is linearized only at `createOrLoadCurrentSessionOwner(...)`; read paths prove identity, while live execution/write paths must also prove ownership.</figcaption>
</figure>

The `{ sessionId }` rows below are local/single-tenant or already
resource-scoped operator semantics. In multi-tenant server routes and external
SDK clients, the server must derive or verify `resourceId` from auth before
direct-ID lookup and then apply the same tenant-safe mismatch behavior as the
`{ sessionId, resourceId }` rows. A public route must not treat a bare
`sessionId` as globally sufficient authority.

| Input                                       | Live in memory?                                                                      | Record in storage?                                                                                           | Result                                                                                   |
| ------------------------------------------- | ------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------- |
| `{ sessionId }`                             | yes                                                                                  | n/a                                                                                                          | return live instance                                                                     |
| `{ sessionId }`                             | no                                                                                   | yes (active)                                                                                                 | hydrate for read, or acquire lease before live execution/write                           |
| `{ sessionId }`                             | no                                                                                   | yes (closed)                                                                                                 | reopen under the normal resource/lease checks, then hydrate                              |
| `{ sessionId }`                             | no                                                                                   | no                                                                                                           | throw `HarnessSessionNotFoundError`                                                      |
| `{ sessionId, resourceId }`                 | yes, `resourceId` matches                                                            | n/a                                                                                                          | return live instance                                                                     |
| `{ sessionId, resourceId }`                 | yes, `resourceId` mismatches                                                         | n/a                                                                                                          | throw `HarnessSessionNotFoundError`                                                      |
| `{ sessionId, resourceId }`                 | no                                                                                   | yes, `resourceId` matches, active                                                                            | hydrate for read, or acquire lease before live execution/write                           |
| `{ sessionId, resourceId }`                 | no                                                                                   | yes, `resourceId` matches, closed                                                                            | reopen under the normal resource/lease checks, then hydrate                              |
| `{ sessionId, resourceId }`                 | no                                                                                   | yes, `resourceId` mismatches                                                                                 | throw `HarnessSessionNotFoundError` (do not leak existence)                              |
| `{ sessionId, resourceId }`                 | no                                                                                   | no                                                                                                           | throw `HarnessSessionNotFoundError`                                                      |
| `{ sessionId, threadId, resourceId }`       | yes, all identities match                                                            | n/a                                                                                                          | return live instance                                                                     |
| `{ sessionId, threadId, resourceId }`       | yes, `sessionId` differs from active owner for `(harnessName, resourceId, threadId)` | n/a                                                                                                          | throw `HarnessSessionConflictError`                                                      |
| `{ sessionId, threadId, resourceId }`       | no                                                                                   | active record exists for `(harnessName, resourceId, threadId)`, `sessionId` matches                          | hydrate for read, or acquire lease before live execution/write                           |
| `{ sessionId, threadId, resourceId }`       | no                                                                                   | active record exists for `(harnessName, resourceId, threadId)`, `sessionId` differs                          | throw `HarnessSessionConflictError` before lease wait/steal                              |
| `{ sessionId, threadId, resourceId }`       | no                                                                                   | no active record, closed current owner exists for `(harnessName, resourceId, threadId)`, `sessionId` matches | reopen that session if the stored thread/resource match; otherwise conflict/not-found    |
| `{ sessionId, threadId, resourceId }`       | no                                                                                   | no active record, closed current owner exists for `(harnessName, resourceId, threadId)`, `sessionId` differs | throw `HarnessSessionConflictError`; do not create a replacement behind the closed owner |
| `{ sessionId, threadId, resourceId }`       | no                                                                                   | no current owner for `(harnessName, resourceId, threadId)` and no record at `sessionId`                      | create thread if needed, atomically create/load the active session with that ID          |
| `{ threadId, resourceId }`                  | yes                                                                                  | n/a                                                                                                          | return live instance                                                                     |
| `{ threadId, resourceId }`                  | no                                                                                   | active record exists, thread `resourceId` matches                                                            | hydrate for read, or acquire lease before live execution/write                           |
| `{ threadId, resourceId }`                  | no                                                                                   | closed owning record exists for matching resource                                                            | reopen the closed owning session; do not create a fresh active record                    |
| `{ threadId, resourceId }`                  | no                                                                                   | thread exists but belongs to a **different** resource                                                        | tenant-safe not-found; do not create using the colliding explicit `threadId`             |
| `{ threadId, resourceId }`                  | no                                                                                   | no                                                                                                           | create thread if needed, atomically create/load the active session, return               |
| `{ threadId: { fresh: true }, resourceId }` | n/a                                                                                  | n/a                                                                                                          | always create a fresh thread + fresh active session                                      |

Rows with `closingAt` present and `closedAt` absent are current owners but
not hydratable for new work. Any lookup shape above that would otherwise return
or hydrate that row throws `HarnessSessionClosingError` after the normal
`harnessName` / `resourceId` checks, and thread/resource lookup does not create
a
replacement session until the closing row reaches `closedAt`.
`closeSession(...)`
and `deleteSession(...)` are the exceptions: they may load a closing row for
idempotent close continuation or destructive delete repair (§5.5). If a resolver
observes a closing row whose `closeDeadlineAt` has passed, it may trigger or
hand off the close-finalization path, but it must not admit new work while doing
so.

Thread/resource lookup is the canonical attach path for multi-device and
reconnect flows. If a current record already owns the pair, every caller gets
that record regardless of whether it is Active or Closed and regardless of which
process hydrates it. A caller that supplies both `sessionId` and `threadId` is
asking for a specific current owner; if the pair already has a different current
`sessionId`, resolution fails with
`HarnessSessionConflictError` before applying `lockMode` wait/steal behavior.
Conflict beats lease acquisition because the caller requested the wrong owner,
not merely a busy owner.

Every "acquire lease" step in the table follows the §5.8 lock policy only when
the caller needs live execution/write ownership. Read-only attachment,
subscription, message-log, snapshot, retained-result, and source-specific
admission paths may resolve the same session identity without treating the lease
as a blanket lock. Ownership acquisition may throw `HarnessSessionLockedError`,
wait, or steal according to configuration.
If the target record is closed between lookup/create-or-load and lease
acquisition, the resolver treats that as a reopen candidate rather than
hydrating stale state or creating a parallel active record. Direct `sessionId`
lookup reopens that record after resource checks; thread/resource lookup resolves
the closed owning record and reopens it unless the caller explicitly requested a
fresh conversation.

The thread-and-resource lookup deliberately preserves closed records as
reopenable sessions. A common flow — finish a session, close it, then return to
the same conversation — reopens the same session instead of creating a new
active record on the same thread. Storage adapters enforce this by keeping a
single current owning `SessionRecord` for the tuple and by rejecting accidental
parallel active rows. Closed records remain reachable through
`loadSession({ sessionId })` and `listSessions({ includeClosed: true })` for
history, audit, and reopen flows.

Cold creation is linearized by storage, not by the caller. The harness first
resolves or creates the tenant-scoped thread, builds a candidate
`SessionRecord`, and calls `createOrLoadCurrentSessionOwner(...)` with an initial
lease for the current `ownerId`. If the caller supplied an explicit `threadId`
that already exists under a different `resourceId`, the resolver stops at the
tenant-safe not-found result and does not try to create another physical thread
with the same ID; callers that need a fresh conversation use
`{ threadId: { fresh: true }, resourceId }` or a different globally unique
`threadId`. If the candidate wins, the returned record is already leased by that
owner. If another caller won first and the original caller did not require a
specific `sessionId`, the harness discards the candidate, uses the returned
current owner, and applies the lease/reopen policy from §5.8 before hydrating. If the
caller did require a specific `sessionId` and the winning record has a different
ID, the resolver throws `HarnessSessionConflictError` and never waits on,
steals, or hydrates the wrong owner. The losing caller never saves its candidate
over the winner, and no recovery path is expected to tolerate two current owners
for the same `(harnessName, resourceId, threadId)`.

Harness v1 has no resource-only resolver. A product that wants "continue
latest" behavior first reads `listSessions({ resourceId, includeClosed })` or an
equivalent product-owned index, chooses a concrete `sessionId` or
`(threadId, resourceId)` target, and then calls `harness.session(...)`. A product
that wants a fresh conversation calls
`harness.session({ threadId: { fresh: true }, resourceId })`. This keeps
selection policy outside the core lifecycle API and prevents old
`selectOrCreateThread()` semantics from becoming a hidden fallback.

`{ sessionId, threadId, resourceId }` (all three) is the multi-tenant-server
pattern when the caller wants deterministic session IDs. The deterministic ID
must name the active owner for `(harnessName, resourceId, threadId)`, not a
per-device runtime. Resolution returns the live instance, hydrates the matching
active record, reopens a closed matching record, or creates a fresh record with
that ID bound to the thread when no owner exists. Deterministic IDs and closure
are compatible because close is reopenable. If the requested `sessionId` already
belongs to another
`(harnessName, resourceId, threadId)`, resolution throws
`HarnessSessionConflictError` for same-resource same-harness collisions and
`HarnessSessionNotFoundError` for cross-resource or cross-harness mismatches
that must not leak existence. A different active record for the same
`(harnessName, resourceId, threadId)` also throws `HarnessSessionConflictError`.

Direct `sessionId` hydration is still checked against the owner-key invariant.
If `loadSession({ sessionId })` returns an active record but
`loadSessionByThread({ threadId: record.threadId, resourceId: record.resourceId })`
returns a different current owner, the store is corrupt or operator-mutated;
the resolver throws `HarnessSessionCorruptError` before acquiring or stealing
the direct-ID record's lease.
