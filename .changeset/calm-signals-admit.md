---
'@mastra/core': minor
'@mastra/libsql': patch
'@mastra/pg': patch
---

Add durable `admissionId` idempotency to Harness v1 `Session.signal()` for retry-safe active steering and idle wakes.

Admitted signals now use a storage-backed reservation, compare-and-swap dispatch claim with heartbeat renewal, and a separate durable native-acceptance fence. Competing or restarted dispatchers reuse the same signal and run identity. A native acknowledgement that remains ambiguous for 30 seconds, or is interrupted by caller abort, stops renewing its claim and can be retried after expiry without reusing the old pending acknowledgement. Expired idle dispatches retain the deterministic idle run identity, while a vanished accepted active run fails explicitly instead of being rerouted.

This is an at-least-once execution identity, not an exactly-once side-effect guarantee: provider or tool work may repeat after a crash or lost acknowledgement under the same stable run ID.

Harness storage adapters gain `compareAndSwapSignalDispatch()` and `compareAndSwapSignalTerminal()`. The built-in InMemory, LibSQL, and PostgreSQL adapters implement atomic claim, acceptance, and attempt-fenced terminal transitions. Admitted evidence now persists an explicit `message` or `signal` operation discriminator, so ordinary `Session.message()` completion and failure writes remain legal even when their pending row already has a run ID. Pre-discriminator signal rows migrate only when they carry dispatch state or the stable Harness channel-signal ID prefix; a run ID alone never classifies a row as a signal. Generic evidence writes cannot roll back or terminalize a dispatch-fenced admission, and a losing terminal writer returns the durable winner that every signal handle adopts. Custom adapters must implement both compare-and-swap methods for admitted signals; the base implementations fail closed. `WriteMessageResultEvidenceResult.applied` is required so an omitted outcome can never be interpreted as a successful durable write.
