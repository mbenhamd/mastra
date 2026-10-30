---
'@mastra/core': patch
---

Added the `requireOwnerLeaseLapsed` option to `revokeTerminalGrant`. Pass the owner whose session lease you believe has lapsed. If that owner still holds a live lease when the revocation runs, the grant is left alone and the call returns the new `owner_live` status.

```ts
const receipt = await storage.revokeTerminalGrant({
  harnessName,
  sessionId,
  admissionId,
  executionGrant,
  reason: { code: 'recovery.owner_lapsed', message: 'owner lease lapsed' },
  requireOwnerLeaseLapsed: { ownerId: lastKnownOwnerId },
});

if (receipt.status === 'owner_live') {
  // The owner is still working: skip recovery for this turn.
  return;
}
```

Calls without the option keep their previous receipt type; `owner_live` only appears in the type when the option may be set. A call passing a variable typed as `HarnessTerminalGrantRevocationInput` (rather than an object literal) gets the wider receipt, so an exhaustive `switch` on its status must handle `owner_live`. With the option set and the owner still live, `owner_live` is returned even for a grant that is already admitted or revoked.

Subtree lease renewals now report `fencedDescendantIds`: child sessions that were not renewed because their grant was revoked after their lease lapsed, plus every open session below them. The Harness evicts those sessions, and any session created below them in the meantime, with the `lease_lost` reason, so lease extensions on them fail. The root and its other children keep renewing.
