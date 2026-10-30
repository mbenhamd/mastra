---
'@mastra/core': patch
---

Added the `requireOwnerLeaseLapsed` option to `revokeTerminalGrant`. A recovery worker passes the owner whose session lease it believes has lapsed. If that owner still holds a live lease when the revocation runs, for example because it renewed in the meantime, the revocation answers the new `owner_live` status and does not fence the grant. Calls without the option keep their previous receipt type, so `owner_live` only appears in the type when the option may be set.

Subtree lease renewals now report `fencedDescendantIds`: the child sessions left out because a revocation fenced their grant after their lease lapsed, plus every open session below each one. The Harness evicts exactly those sessions with the `lease_lost` reason instead of treating them as still leased, so lease extensions on them fail. The root and its other children keep renewing normally. [PF-6886]
