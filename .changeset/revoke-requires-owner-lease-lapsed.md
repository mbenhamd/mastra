---
'@mastra/core': patch
'@mastra/pg': patch
---

Added `requireOwnerLeaseLapsed` to `revokeTerminalGrant`. A recovery worker that decided from a plain read that a session owner's lease lapsed can pass the owner. The store then re-reads the lease under the session row lock, so it sees any renewal that held that lock before it. A live lease answers the new `owner_live` status and no grant tombstone is written. Only a revocation that may pass the option has `owner_live` in its receipt type.

Session lease renewals in `@mastra/pg` now read the clock after locking the session row, so a renewal that waited across expiry fails with a lease conflict instead of renewing a lease that a revocation already saw lapse. [PF-6886]
