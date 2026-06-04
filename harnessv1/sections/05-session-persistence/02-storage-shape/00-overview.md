### 5.2 Storage shape

§5.1 is the canonical owner for persisted record field shapes, durable-state
inventory, and serialization constraints. This section owns the
`HarnessStorageDomain` adapter surface for session-owned rows plus the
composition requirements for existing storage domains: namespace binding,
atomicity, uniqueness, CAS/lease, claim/retry, indexing, and deletion
requirements. Source-specific rows that already have a Mastra domain owner
extend or compose with that owner; they are not folded into a monolithic Harness
table merely because Harness coordinates their recovery.

Harness v1 adds a narrow `harness` domain to the composite storage domain
pattern for session records, session leases, admission/result evidence, and
Harness-owned attachment metadata defined in §5.1. The §4.8
`HarnessStorage` type is the namespace-bound view of this
`HarnessStorageDomain` contract; it is not a claim that current deprecated
`MastraStorage` already exposes a direct `harness` property. Current core
storage exposes domains through `StorageDomains` / `getStore(...)`, and the
v1 implementation must wire the Harness domain through that composition model
or an equivalent explicit storage-domain adapter. Channel bridge ledgers,
wakeups, and reconstructable background-task claims may be surfaced through the
same bound facade for ergonomics, but their canonical ownership remains
source-specific as described below.

This domain is additive and source-specific, not a replacement for every
storage surface that Harness touches. Each row family below has exactly one
durable owner:

- `memory` remains the canonical thread/message row owner; Harness exposes a
  scoped view rather than writing a second conversation log.
- `channels` remains the canonical provider installation/config owner:
  credentials, OAuth/provisioning state, platform app config, and current
  `ChannelInstallation` rows stay there.
- `schedules` remains the canonical workflow schedule definition and tick owner
  until an explicit Harness schedule target exists; `HarnessWakeupItem` rows are
  the restart-safe handoff into session admission, not duplicate cron rows.
- `backgroundTasks` remains generic execution machinery and diagnostics. If a
  task row is itself restart-reconstructable, v1 extends that domain with stable
  executor/completion refs and claim fields; otherwise Harness channel/wakeup
  rows remain the recovery boundary and the task row is only diagnostic.
- `blobs` remains the canonical content-addressed byte store when the configured
  storage exposes it. Harness attachments own session-scoped metadata,
  reference checks, and guarded deletion, not a second byte-storage subsystem.
- `harness` owns only the session-first durability layer: `SessionRecord`,
  admission receipts/tombstones, session leases, accepted-signal result
  evidence, and Harness-owned attachment metadata.
- Channel bridge rows (`ChannelBinding`, provider callback bindings, inbox,
  action, and outbox rows) are Harness-scoped channel-domain extensions. They
  may be exposed through the bound Harness facade, but they do not replace
  `ChannelsStorage` provider installation/config ownership.
- Wakeup rows and source-specific work rows are claimable recovery handoffs.
  They should share one claim/renew/terminal-update pattern with existing
  background-task and scheduler machinery rather than inventing independent
  worker frameworks per source.

Implementations may expose these methods through one `HarnessStorage` facade for
ergonomics, but the facade must compose with the canonical owner above. It must
not dual-write a second thread log, shadow channel installation storage,
duplicate scheduler definitions, create a parallel background-task store, fork
PubSub replay/history, or store attachment bytes outside the configured
byte-storage owner.

Harness v1 does **not** create an independent second conversation log.
Thread/message reads and writes compose through the configured `MemoryStorage`
thread/message owner and its physical rows. The persisted conversation log for a
Harness thread is the row set read by `MessageHistory`, `SemanticRecall`,
thread-scoped `WorkingMemory`, observational-memory message scans,
`Session.listMessages(...)`, display reconstruction, and outbox projection.
Harness may use a namespace-bound memory history view for lifecycle and
operator-history work, but that view is not a second `HarnessStorageDomain`
store. It must read and write the same durable rows transactionally and keep the
same append, list, ordering, and `resourceId` behavior. A Harness-only message
mirror or best-effort dual-write to unrelated rows is not a valid v1 recovery
boundary.

Every `HarnessStorage` view (the §4.8 alias for the namespace-bound
`HarnessStorageDomain` shape) is bound to one immutable registered
`harnessName` (`default` in the single-harness examples). The method signatures below
elide that parameter where the owning Harness instance is the only caller, but
the adapter must still include the bound namespace in every durable Harness-domain
create/load/list/claim/delete key. Independently loadable Harness-domain records
persist `harnessName`, and every ordinary Harness instance scopes to its own
name or cross-checks the stored value before returning, mutating, claiming,
deleting, projecting, or terminalizing work. Cross-harness mismatch is
tenant-safe not-found/unavailable behavior, never retargeting.

A memory storage adapter shared by multiple registered harnesses must provide a
harness-scoped thread/message view over the shared `MemoryStorage` rows. It may
persist a `harnessName` column, use a physical schema/table/key prefix, or map
caller-visible `threadId` to a non-colliding physical memory-thread key, but
`Session.listMessages(...)`, memory processors, display reconstruction, and
outbox projection must all read the same scoped rows for that Harness. An
adapter that can only expose one unscoped global `threadId` keyspace is valid for
one registered Harness per physical namespace; Mastra Server boot fails if that
adapter is shared by more than one Harness. This is a namespace requirement, not
permission to create a second Harness-only message log.

Observational-memory records stay in the configured memory domain. Harness reads
them only through a session/resource-verified projection (`ObservationalMemorySnapshot`
in §4.8) and never treats the raw OM row as a SessionRecord, operation result,
receipt, or recovery boundary.

Raw OM rows used by Harness v1 must be scoped and addressable by a
Harness-bound namespace plus `(resourceId, threadId)`, where `threadId: string`
means thread-scoped OM and `threadId: null` means resource-scoped OM. This is an
adapter namespace requirement, not a requirement to add `harnessName` to the
canonical Memory/OM row model. A storage adapter may use a physical schema,
table, key prefix, bound memory view, or explicit column if Memory itself adopts
that ownership field; the important invariant is that reads, history reads, and
cleanup isolate rows to the bound Harness namespace. The adapter supplies the
namespace from the bound Harness storage view; it is not a caller-provided
per-operation parameter.

If the adapter exposes thread delete through the Harness view, it must also
remove or tombstone all OM rows and history scoped exactly to the deleted
`(harnessName, resourceId, threadId)`. `clearObservationalMemory(threadId,
resourceId)` or equivalent Harness cleanup must not delete rows from another
Harness, another resource, or resource-scoped OM (`threadId: null`). A cleanup
whose `threadId` is `null` targets only resource-scoped OM for that exact
`(harnessName, resourceId)` and must not delete thread-scoped rows sharing the
same resource. Resource-scoped OM is not removed by deleting one thread; any
resource-level OM retention or deletion policy is product/operator-owned
outside the v1 thread-delete lifecycle.

An adapter that cannot provide a Harness-scoped OM namespace may keep legacy OM
outside Harness v1, but Harness must not expose those rows as
`ObservationalMemorySnapshot` and must not claim thread-delete OM cleanup. If
observational memory is enabled on more than one registered Harness sharing one
physical namespace, an adapter that cannot isolate OM by `harnessName` fails
Mastra Server boot for that configuration, mirroring the shared
thread/message-log rule above.

Orientation diagram (storage surfaces only; method signatures below remain
authoritative):

<figure>
  <svg role="img" aria-labelledby="hx-storage-shape-title hx-storage-shape-desc" viewBox="0 0 1040 560" width="100%" style="max-width: 1100px; height: auto; display: block; margin: 1.5rem auto; background: #ffffff; border: 1px solid #e2e8f0; border-radius: 18px; padding: 16px; box-sizing: border-box;">
    <title id="hx-storage-shape-title">Harness storage surface map</title>
    <desc id="hx-storage-shape-desc">The HarnessStorage view owns session and result rows while composing channel work, wakeups, attachment metadata, thread/message rows, blob bytes, and replay with their canonical storage domains.</desc>
    <defs>
      <marker id="ah-storage-shape" markerWidth="10" markerHeight="10" refX="8" refY="5" orient="auto">
        <path d="M0,0 L10,5 L0,10 Z" fill="#334155" />
      </marker>
    </defs>

    <rect style="fill: #eef2ff; stroke: #6366f1; stroke-width: 2.5; rx: 16;" x="370" y="25" width="300" height="76" />
    <text style="font: 600 18px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #0f172a;" x="520" y="57" text-anchor="middle">HarnessStorage view</text>
    <text style="font: 500 14px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="520" y="80" text-anchor="middle">namespace-bound storage view</text>

    <rect style="fill: #f8fafc; stroke: #94a3b8; stroke-width: 2; rx: 14;" x="55" y="160" width="250" height="76" />
    <text style="font: 600 18px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #0f172a;" x="180" y="191" text-anchor="middle">Memory view</text>
    <text style="font: 500 14px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="180" y="214" text-anchor="middle">message log reuse</text>

    <rect style="fill: #f8fafc; stroke: #94a3b8; stroke-width: 2; rx: 14;" x="395" y="160" width="250" height="76" />
    <text style="font: 600 18px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #0f172a;" x="520" y="191" text-anchor="middle">Session ops</text>
    <text style="font: 500 14px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="520" y="214" text-anchor="middle">load / save / lease / CAS</text>

    <rect style="fill: #f8fafc; stroke: #94a3b8; stroke-width: 2; rx: 14;" x="735" y="160" width="250" height="76" />
    <text style="font: 600 18px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #0f172a;" x="860" y="191" text-anchor="middle">Result ops</text>
    <text style="font: 500 14px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="860" y="214" text-anchor="middle">receipts / tombstones</text>

    <rect style="fill: #f8fafc; stroke: #94a3b8; stroke-width: 2; rx: 14;" x="55" y="325" width="250" height="76" />
    <text style="font: 600 18px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #0f172a;" x="180" y="356" text-anchor="middle">Channel bridge view</text>
    <text style="font: 500 14px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="180" y="379" text-anchor="middle">extends channel owner</text>

    <rect style="fill: #f8fafc; stroke: #94a3b8; stroke-width: 2; rx: 14;" x="395" y="325" width="250" height="76" />
    <text style="font: 600 18px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #0f172a;" x="520" y="356" text-anchor="middle">Claimable work</text>
    <text style="font: 500 14px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="520" y="379" text-anchor="middle">shared claim pattern</text>

    <rect style="fill: #f8fafc; stroke: #94a3b8; stroke-width: 2; rx: 14;" x="735" y="325" width="250" height="76" />
    <text style="font: 600 18px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #0f172a;" x="860" y="356" text-anchor="middle">Attachment ops</text>
    <text style="font: 500 14px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="860" y="379" text-anchor="middle">metadata / refs / deletion</text>

    <rect style="fill: #ecfdf5; stroke: #22c55e; stroke-width: 2; rx: 14;" x="45" y="465" width="270" height="62" />
    <text style="font: 600 18px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #0f172a;" x="180" y="491" text-anchor="middle">MemoryStorage rows</text>
    <text style="font: 500 14px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="180" y="512" text-anchor="middle">thread/message log</text>

    <rect style="fill: #fff7ed; stroke: #f97316; stroke-width: 2; rx: 14;" x="365" y="455" width="310" height="76" />
    <text style="font: 600 18px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #0f172a;" x="520" y="486" text-anchor="middle">Worker claim renewal</text>
    <text style="font: 500 14px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="520" y="509" text-anchor="middle">fenced updates for claimed work</text>

    <path style="stroke: #334155; stroke-width: 2.2; fill: none; marker-end: url(#ah-storage-shape);" d="M415 101 C335 125 240 130 190 159" />
    <path style="stroke: #334155; stroke-width: 2.2; fill: none; marker-end: url(#ah-storage-shape);" d="M520 101 L520 159" />
    <path style="stroke: #334155; stroke-width: 2.2; fill: none; marker-end: url(#ah-storage-shape);" d="M625 101 C705 125 805 130 850 159" />
    <path style="stroke: #334155; stroke-width: 2.2; fill: none; marker-end: url(#ah-storage-shape);" d="M425 101 C290 170 205 240 184 324" />
    <path style="stroke: #334155; stroke-width: 2.2; fill: none; marker-end: url(#ah-storage-shape);" d="M520 101 L520 324" />
    <path style="stroke: #334155; stroke-width: 2.2; fill: none; marker-end: url(#ah-storage-shape);" d="M615 101 C750 170 840 240 858 324" />
    <path style="stroke: #64748b; stroke-width: 2; fill: none; stroke-dasharray: 7 7; marker-end: url(#ah-storage-shape);" d="M180 236 L180 464" />
    <path style="stroke: #334155; stroke-width: 2.2; fill: none; marker-end: url(#ah-storage-shape);" d="M305 363 L364 477" />
    <path style="stroke: #334155; stroke-width: 2.2; fill: none; marker-end: url(#ah-storage-shape);" d="M520 401 L520 454" />
  </svg>
  <figcaption>The harness storage view owns durable session rows and composes source-specific work, memory rows, replay, and blob bytes through their canonical owners.</figcaption>
</figure>
