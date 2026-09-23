---
'@mastra/core': patch
---

Hardened Harness v1 terminal handoff admission, settlement, and recovery.

- Terminal admission probes are scoped by session incarnation so a recreated session cannot resolve a previous incarnation's grant, and session incarnations are minted whenever terminal handoff is enabled.
- A grant generation now binds to a single admission across the harness; cancellation resolves the grant-wide winner before tombstoning, and delivery intents are namespaced by the durable admission identity.
- Duplicate and re-admission envelopes that carry a cancelled or fenced stored row surface the durable outcome instead of dispatching the provider; a duplicate waiting on durable evidence surfaces a cancellation or fencing tombstone promptly.
- Live duplicates wait behind the durable commit barrier, concurrent terminal committers share a single settlement attempt, and a settlement retry converging with the winner no longer emits a duplicate `agent_end`.
- A committed admission replays its durable receipt to duplicate stream retries, and a resumed run whose terminal settlement failed settles from the completed run output without invoking the provider again.
- A durable dispatch marker prevents provider re-execution when a dispatch outcome is ambiguous.
- A resume whose deferred admission was cancelled or fenced surfaces the terminal outcome without invoking the provider, and a retried resume after restart recovers the terminal output from the committed admission's durable evidence.
- Aborting, expiring, or failing to re-suspend a suspended turn cancels its deferred terminal admission, so a discarded segment cannot strand a pending grant with nothing able to settle it.
- Retained terminal observers are notified on abort, expiry, pre-commit failure, already-terminal admission, and session close instead of waiting forever.
- Stale settlement retries no longer discard newer suspended interactions, overwrite newer `switchMode` results, resurrect cleared pending interactions, or re-park a cached suspension whose admission already settled.
- Suspended message runs defer terminal settlement until the approval-gated resume commits the real outcome; error finishes commit `failed` terminal results with a projected public error.
- Terminal intent comparisons ignore the committer wall-clock `completedAt`.
- Cancelling a response that another writer already sealed reports the sealed result to waiting callers instead of a cancellation that never happened.
- A storage failure while recovering a suspended run is reported to terminal callbacks instead of leaving them silent, and a reservation stranded before terminal admission is re-driven on retry.
- Reopening a session no longer counts token usage that a suspended run already recorded, and retrying a message after a crash no longer counts token usage twice.
- Approving a plan applies the target mode even when the approval call crashed part way through, and goal judging runs once per settled run.
- Adopted duplicate streams settle through the canonical run-completion path and drain retained terminal observers with the indeterminate outcome when the barrier rejects.
- Terminal finalizer registration and admitted identity are validated before a settlement may commit; a missing or changed finalizer fails closed instead of committing under the wrong identity.
- Terminal JSON validation enforces a bounded nesting depth and terminal error messages are bounded, so oversized or deeply nested inputs produce typed validation errors instead of stranding admissions.
- The in-memory adapter honors `terminalHandoff.enabled` on `supportsTerminalHandoff` and rejects public terminal operations when disabled, reclaims intents whose claim lease expired, clears stale retry metadata on reclaim, claims intents in creation order, validates claim clocks and identities on renew/acknowledge/fail, and keeps an admission pending when the commit payload cannot be cloned.
