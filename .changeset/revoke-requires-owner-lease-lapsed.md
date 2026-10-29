---
'@mastra/core': patch
'@mastra/pg': patch
---

Added `requireOwnerLeaseLapsed` to `revokeTerminalGrant`. A recovery worker that decided from a plain read that a session owner's lease lapsed can pass the owner. The store then re-reads the lease under the session row lock, after any in-flight renewal commits. A live lease answers the new `owner_live` status and no grant tombstone is written. [PF-6886]
