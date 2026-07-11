# Durable workflow terminalization storage contract

## Objective

The workflow terminalization protocol lets a runtime recover one accepted terminal outcome and its framework-owned delivery intents after a worker crash, broker replay, workflow snapshot replacement, or normal run deletion.

It separates three facts that are not equivalent:

1. A terminal broker event was accepted by one fenced owner.
2. The terminal workflow snapshot and a producer intent were persisted.
3. A destination durably applied that intent.

The version 1 journal and fenced terminal-snapshot transition are the prerequisite storage contract. Producer outbox version 1 extends that foundation with retained terminal state and immutable delivery intents. Destination receipts and recovery orchestration are separate capability versions.

## Why the snapshot is insufficient

Workflow snapshots are mutable execution state. Existing APIs may replace or delete them, and concurrent terminal workers can observe the same broker event. Storing claim or outbox state only inside the snapshot would allow stale workers to overwrite a newer owner or lose the evidence needed for replay.

The journal, retained terminal state, effect records, and destination receipts therefore use storage-owned tables or maps with separate lifecycle rules. On stores advertising producer outbox version 1, the existing `persistWorkflowTerminalState()` journal operation also writes an isolated retained terminal artifact from the same adapter-materialized snapshot in the same atomic section. The canonical workflow row remains replaceable; the retained artifact is immutable protocol evidence. Deleting a workflow run removes only the normal row, so the artifact remains available to the fenced dispatcher. Explicit completed-record retention cleanup deletes the journal, terminal artifact, associated producer intents, and destination receipts together.

## Capability negotiation

Call `getWorkflowTerminalizationCapabilities()` and require the exact version needed by the runtime:

```ts
const capabilities = workflowsStorage.getWorkflowTerminalizationCapabilities();

if (
  capabilities.journalVersion !== 1 ||
  capabilities.producerOutboxVersion !== 1 ||
  capabilities.destinationReceiptVersion !== 1
) {
  throw new Error('The workflow store does not support terminal receipt reservation');
}
```

`supportsWorkflowTerminalizationJournal()` only answers whether the version 1 journal exists. It does not imply producer outbox, destination receipt, or recovery support. Every capability is negotiated independently, and base adapters return `unsupported` for protocol operations they do not implement.

## Producer flow

The live claim owner performs these storage operations:

1. `claimWorkflowTerminalization()` accepts the first `(eventKey, terminalStatus)` for a run and returns an owner ID, opaque claim token, generation, and bounded lease.
2. `persistWorkflowTerminalState()` atomically replaces the normal terminal snapshot, retains an immutable terminal artifact, and advances `terminalization_pending` to `run_state_persisted`.
3. `prepareWorkflowTerminalEffect()` atomically inserts one immutable intent and advances to `parent_outbox_pending` or `finish_outbox_pending`.
4. `getWorkflowTerminalEffectForDispatch()` returns the full intent, retained terminal snapshot, and retained resource identity only to the current live fenced owner.

The generic phase compare-and-set cannot create `run_state_persisted`, either `*_outbox_pending` phase, or either `*_effect_recorded` phase. Those transitions belong to specialized atomic methods. A later domain-application capability is responsible for proving application and is the only contract allowed to enter an `*_effect_recorded` phase.

## Destination receipt reservation

An adapter advertising `destinationReceiptVersion: 1` provides two fenced methods:

1. `reserveWorkflowTerminalDestinationReceipt()` creates one immutable receipt slot for `(effectKey, consumerId)` or returns the exact existing slot.
2. `getWorkflowTerminalDestinationReceipt()` returns that slot only to the current live claim owner.

PF-1770 creates only `applicationState: 'reserved'` with `dispatchState: 'none'`. It deliberately does not persist a continuation plan, mark a parent mutation applied, enqueue a destination dispatch, acknowledge a broker event, or advance the journal. Specialized atomic application APIs own those later facts.

Receipt identity includes the immutable producer effect key and a bounded, well-formed framework consumer ID. The deterministic destination hash describes the effect destination without exposing its structural fields in observations. Two consumers of one effect receive different receipt keys; a retry by the same consumer receives the exact stored receipt. Framework-owned deterministic logical consumers may use this protocol. Arbitrary external callbacks still own their idempotency contract.

Application and dispatch are orthogonal. Persisted rows accept only these combinations:

- `reserved / none`;
- `applied / none`;
- `applied / pending`;
- `applied / destination_applied`;
- `quarantined / none`.

The receipt identity fields never change. Later APIs may move through the legal state combinations monotonically, but there is no generic receipt mutation method. Contradictory persisted identity, hashes, states, or timestamps fail closed.

## Intent identity and payload boundary

Each intent contains only:

- protocol version;
- producer workflow name, run ID, source event key, and terminal status;
- effect kind;
- parent workflow name, parent run ID, parent step ID, and exact bounded structural execution path for a nested-run effect;
- deterministic effect key, deterministic payload hash, and creation time.

The key and payload hash use SHA-256 over length-prefixed UTF-8 fields with separate domain strings. Length framing prevents delimiter ambiguity. The payload hash binds the structural intent fields; it deliberately does not hash or authenticate the separately retained snapshot payload. Snapshot integrity is instead enforced by strict decoding plus its workflow/run/status/creation-time journal link. PostgreSQL recomputes both effect hashes while decoding persisted rows and fails closed if the structural fields and hashes disagree. It also verifies that the intent's source event, terminal status, run identity, and creation time agree with the owning journal.

Intents must never persist raw request context, processor arguments, complete workflow results, errors, tools, executable functions, or live object graphs. A finish intent references the separately retained terminal artifact by workflow name and run ID instead of copying the result. Parent execution paths contain 1-256 non-negative safe integers and are included in both deterministic hashes. Structural text identity fields are bounded, must contain well-formed Unicode, and cannot contain the null character rejected by PostgreSQL text storage.

Conflict and observation results omit structural destination fields and all live claim credentials. Full intents are returned only after the owner ID, claim token, generation, and lease are validated.

## Guarantees and limits

The producer protocol guarantees:

- first-terminal-wins journal identity per workflow run;
- monotonic claim generations and permanent fencing of stale workers;
- the journal capability's atomic canonical terminal snapshot plus phase persistence;
- one adapter-materialized terminal snapshot value driving both canonical and retained writes;
- retention of the exact terminal artifact independently from the normal run row;
- retention of the effective resource identity needed to reconstruct finish delivery after normal run deletion;
- one immutable producer intent per run and effect kind;
- stable keys across retries and broker event re-publication;
- retention of incomplete journal, terminal artifact, and intent evidence after run deletion.

Version 1 destination receipts additionally guarantee one immutable reserved slot per effect and logical consumer, stable receipt identity across retries, independent consumer slots, fenced disclosure of valid receipt data, and retention with the terminal evidence until explicit completed-journal cleanup. Present corrupt effect or receipt evidence fails closed before a stale-fence result; missing evidence does not mask a stale fence. Retained terminal state is inspected only after the fence and receipt operation succeed.

It does not claim that broker publication or receipt reservation equals destination application. It also cannot make arbitrary external callbacks exactly once. Destination-owned idempotency and a specialized atomic receipt-application protocol are required before claiming one framework-owned applied mutation per effect key.

## Adapter verification

An adapter advertising `producerOutboxVersion: 1` must test:

- concurrent claims and concurrent intent preparation from independent adapter instances;
- stale-owner, wrong-token, wrong-generation, expired-lease, and terminal-conflict paths;
- transaction rollback when the canonical snapshot, retained snapshot, journal phase, or completed cleanup write fails;
- deterministic keys for ambiguous strings and reordered object properties;
- bounded, dense, data-only parent execution paths and path-sensitive hashes;
- strict persisted-row validation, including forged but correctly shaped hashes;
- redaction of credentials and destination structure from conflict results;
- run deletion retention and atomic journal/artifact/intent cleanup;
- schema export and custom-schema qualification for every managed table.

The in-memory adapter is the behavioral reference for state-machine outcomes. Durable adapters must additionally use their database clock and a transaction or equivalent atomic primitive for every state-changing protocol operation.

An adapter advertising `destinationReceiptVersion: 1` must additionally test concurrent same-consumer reservation, independent consumers, exact retry identity, malformed consumer IDs, all legal and illegal state combinations, corruption-before-stale precedence for present effect/receipt evidence, stale-fence precedence over missing evidence, retained-state authorization, persisted-row corruption, run-deletion retention, transactional cleanup, schema export, and custom-schema qualification.
