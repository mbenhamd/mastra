---
'@mastra/core': patch
'@mastra/pg': patch
---

Added `requireOwnerLeaseLapsed` to `revokeTerminalGrant`. A recovery worker that decided from a plain read that a session owner's lease lapsed can pass the owner. The store then re-reads the lease under the session row lock, so it sees any renewal that held that lock before it. A live lease answers the new `owner_live` status and no grant tombstone is written. Only a revocation that may pass the option has `owner_live` in its receipt type.

Session lease renewals in `@mastra/pg` now read the clock after locking the session row (`FOR NO KEY UPDATE`, so message-result writes holding `FOR KEY SHARE` do not block them). A renewal that waited across expiry fails with a lease conflict instead of renewing a lease that a revocation already saw lapse.

A subtree renewal still re-adopts a lapsed same-owner descendant, except one with a grant revocation written at or after its lease expiry. That descendant is left out of the pass: it is not renewed, and it does not fail the renewal of the root and the other descendants. The new optional `fencedDescendantIds` field on the subtree renewal result lists it, and the Harness fences such a live descendant and its live subtree (`lease_lost`) instead of reporting its lease renewed, finishing that eviction outside the renewal. Persisted revocations are honoured whatever the renewing instance's terminal-handoff setting. A descendant that changed owner or closed during the renewal still fails the whole pass. [PF-6886]
