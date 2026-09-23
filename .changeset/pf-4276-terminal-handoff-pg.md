---
'@mastra/pg': patch
---

Fixed and improved the Postgres Harness terminal handoff adapter.

- `PostgresStore` now forwards the `terminalHandoff` option to the Harness domain, so `maxSeedBytes`, `maxPayloadBytes`, `maxAttempts`, `maxPendingIntents`, `maxPendingBytes`, and `claimLeaseMs` bounds configured at the store level reach the adapter.
- `supportsTerminalHandoff` now honors `terminalHandoff.enabled`, and public terminal operations reject calls when the capability is disabled; cleanup and fence paths remain callable for recovery regardless.
- Fenced admissions are now rejected on commit; cancelling an already-fenced admission reports `fenced` instead of claiming a `cancelled` transition storage never made, and a late commit on a fenced admission with a cancellation tombstone surfaces the fence instead of a stale `cancelled` receipt.
- Grant-scoped admit and cancel resolve the single bound admission across sessions, and `loadTerminalAdmissionByRun` probes the admission bound to a run across statuses for retried resume settlement.
- Terminal admission lookups by run and pending probes are scoped by session incarnation, matching the in-memory adapter, and `loadTerminalAdmission` verifies the caller's admission identity.
- Terminal intent lock ordering is consistent across acknowledgement and failure paths so concurrent transactions cannot deadlock, and the claim scan runs in creation order so older backlog rows are not starved by newer sessions' low revisions.
- Terminal commit replay comparisons evaluate result payloads in their persisted JSON form, so an identical retried commit carrying `Date` values converges instead of falsely conflicting; a committed admission also replays after its message evidence was compacted or deleted.
- Session-record projection outbox writes now require the projection feature itself, so terminal-only stores no longer emit undrained fence, intent, and capacity rows.
- Session incarnations are minted and preserved whenever terminal handoff is enabled, even without the session-record projection.
