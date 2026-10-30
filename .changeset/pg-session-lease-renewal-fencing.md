---
'@mastra/pg': patch
---

Fixed session lease renewals racing terminal grant revocations. A renewal that was delayed until after its lease expired now fails with a lease conflict instead of reviving a lease a recovery worker already treated as lapsed. Renewals are no longer held up by concurrent message-result writes on the same session.

Subtree lease renewals now skip a child session whose grant was revoked after its lease lapsed, together with every session below it, and report them in `fencedDescendantIds`. The root and its other children still renew. This holds even when the renewing store has terminal handoff disabled, and when the tables are found through the connection's `search_path`. A child that changed owner or closed during the renewal still fails the whole renewal. [PF-6886]
