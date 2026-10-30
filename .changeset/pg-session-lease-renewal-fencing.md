---
'@mastra/pg': patch
---

Fixed a race between session lease renewal and grant revocation. A renewal that waited past its lease expiry now fails with a lease conflict. It no longer revives a lease that recovery already treated as lapsed.

Fixed lease renewals being blocked by concurrent writes to the same session's message results.

Subtree lease renewal now skips a child session whose grant was revoked after its lease lapsed, and every session below it. The skipped sessions are listed in `fencedDescendantIds`. The root and the other children still renew.
