### 5.2f Session Lease and Storage Requirements

Session lease time requirements: initial lease installation,
`acquireSessionLease`, `renewSessionLease`, `renewSessionLeaseSubtree`,
`saveSession`'s current-owner expiry check, `lockMode: 'steal'` expiry checks,
`lockMode: 'wait'` retry decisions, parent/descendant remaining-lease
calculations, and `closeDeadlineAt` calculation/comparison during bounded close
MUST use storage-authoritative time for lease and close-deadline comparisons. If
an adapter cannot use
storage-authoritative time, it must declare bounded `sessions.maxClockSkewMs`
(§9), and the harness must reject configurations whose renewal margin cannot
survive that skew. The `storageNow` returned by `createOrLoadCurrentSessionOwner`,
`acquireSessionLease`, `renewSessionLease`, and `renewSessionLeaseSubtree` is the
same time source used to compute or compare `expiresAt` and `closeDeadlineAt`;
adapters must not compute `expiresAt` / `closeDeadlineAt` from one clock and
report a different clock as `storageNow`.

Session storage requirements: current-owner `SessionRecord` rows are unique by
`(harnessName, resourceId, threadId)` across Active, Closing, and Closed
reopenable records until the session is deleted/hidden or an explicit
fresh/clone path creates a different thread/session identity. Closed
records remain addressable by both `(harnessName, sessionId)` and their
`(harnessName, resourceId, threadId)` owner key so normal open/reconnect can
reopen the same room instead of rotating identity. This matches the §5.2a
`createOrLoadCurrentSessionOwner(...)` and `loadSessionByThread(...)` rules. Adapters
should implement this with a unique current-owner index or sentinel where
available; stores without one must use an equivalent transaction, upsert,
compare-and-set, or backend-native single-writer primitive so
`createOrLoadCurrentSessionOwner(...)` is linearizable. Two concurrent calls for the
same owner key must return the same winning record, including Closed or Closing
records when one exists, and the losing caller must not create or overwrite a
second row for that thread. The same owner-key rule applies to child
`SessionRecord`s: a subagent session must use a `threadId` that does not collide
with an active ancestor/root record inside the same Harness namespace. Subagent
rows are admitted via `createOrLoadCurrentSessionOwner(...)` with the parent's
`harnessName`, `ownerId`, and a TTL ≤ the parent's remaining lease (§5.6, §5.8);
the resulting child lease entry is parent-bound and is not independently
`acquireSessionLease(...)`-ed or independently renewed. Parent/root renewal uses
`renewSessionLeaseSubtree(...)`, which renews the parent/root row and mirrors the
new expiry onto every active descendant row reachable by `parentSessionId` inside
the bound Harness namespace. Active descendants are rows with `closedAt` absent,
including Closing rows. The call uses one storage-authoritative time source and
returns success only when the caller still owns the unexpired parent/root lease
and every required descendant mirror for that renewal cycle has committed. The
returned `version` is the parent/root row's post-renewal version, and
`renewedDescendantCount` excludes the parent/root row. Adapters may implement the
subtree update with a transaction, backend-native conditional bulk update, or
internal helper, but Harness-visible success is never parent-only success. If the
adapter cannot prove every active descendant remains under the current lease, the
caller treats the result as lease renewal failure per §5.8 and fences the local
owner before further mutations, queue drain, pending-item resume, outbox
projection, or provider-visible work. This is a storage-visible subtree renewal
primitive, not a Harness-layer loop over independently renewable child leases:
partial helper progress, crash, or adapter uncertainty is reported as renewal
failure unless every active descendant mirror committed under the same
storage-linearized renewal cycle. Descendants created during the same-owner
renewal cycle either serialize after the successful renewal and inherit the new
parent/root expiry, or are included in the storage-linearized subtree renewal;
they must not be left with an older expiry under a successful call. Cascade close
(§5.5) first installs `closingAt` / `closeDeadlineAt` top-down, then terminates
every active descendant — live or persisted-only — bottom-up under the parent's
lease, asserting `harnessName` and `resourceId` match per descendant;
force-delete cleanup uses the same descendant walk plus delete fence defined in
§5.5. If an adapter cannot express the owner-key invariant with cross-process
linearizability, it is limited to local testing semantics and cannot claim
distributed Harness durability.

Current core `HarnessConfig.threadLock` and MastraCode filesystem `threadLock`
behavior are not implementations of this section. They coordinate current
thread ownership and may be useful as legacy bootstrap guards during migration,
but they do not provide
storage-authoritative time, linearizable owner-key uniqueness, subtree
renewal, CAS-scoped `saveSession(...)`, or durable claim evidence. V1 adapters
must not treat that lock as a blanket session lock and must not use it to block
ordinary read/list/subscribe, authorized message append, or admitted
`Session.signal(...)` paths. Only recovery-sensitive mutation, queue drain,
pending-item resume, outbox projection, provider-visible delivery, workspace
materialisation, and side-effect execution use the lease/claim scopes defined
here and in §5.8.
