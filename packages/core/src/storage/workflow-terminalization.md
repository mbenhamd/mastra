# Durable workflow terminalization storage contract

## Objective

The workflow terminalization protocol lets a runtime recover one accepted terminal outcome and its framework-owned delivery intents after a worker crash, broker replay, workflow snapshot replacement, or normal run deletion.

It separates three facts that are not equivalent:

1. A terminal broker event was accepted by one fenced owner.
2. The terminal workflow snapshot and a producer intent were persisted.
3. A destination durably applied that intent.

The version 1 journal and fenced terminal-snapshot transition are the prerequisite storage contract. Producer outbox version 1 extends that foundation with authenticated recovery evidence and immutable delivery intents. Destination receipts, recovery evidence, and recovery orchestration remain independently negotiated capabilities.

## Why the snapshot is insufficient

Workflow snapshots are mutable execution state. Existing APIs may replace or delete them, and concurrent terminal workers can observe the same broker event. Storing claim or outbox state only inside the snapshot would allow stale workers to overwrite a newer owner or lose the evidence needed for replay.

The journal, pre-terminal ancestry, authenticated terminal envelope, effect records, and destination receipts therefore use storage-owned tables or maps with separate lifecycle rules. A durable nested child captures one immutable data-only ancestry chain before execution; a brand-new child excluded by `shouldPersistSnapshot` does not acquire ownership or recovery evidence, while an already-retained child still replays admission on resume. `persistWorkflowTerminalState()` later canonicalizes the supplied recovery envelope once in Core, verifies that ancestry, hashes the canonical envelope, and atomically writes the same exact terminal result/error/final state/request-context patch into the replaceable workflow row and the retained artifact. Non-failed outcomes remove any stale replaceable-row error. Adapters do not interpret rich JavaScript values independently. Deleting a normal workflow row does not delete protocol evidence. Completed-record retention is dependency-aware and deletes leaf evidence before the journal only after descendant handoffs and dispatches are settled.

## Capability negotiation

Call `getWorkflowTerminalizationCapabilities()` and require the exact version needed by the runtime:

```ts
const capabilities = workflowsStorage.getWorkflowTerminalizationCapabilities();

if (
  capabilities.journalVersion !== 1 ||
  capabilities.producerOutboxVersion !== 1 ||
  capabilities.destinationReceiptVersion !== 1 ||
  capabilities.parentApplicationVersion !== 1 ||
  capabilities.recoveryVersion !== 1
) {
  throw new Error('The workflow store does not support atomic parent terminal application');
}
```

`supportsWorkflowTerminalizationJournal()` only answers whether the version 1 journal exists. It does not imply producer outbox, destination receipt, or recovery support. Every capability is negotiated independently, and base adapters return `unsupported` for protocol operations they do not implement.

## Producer flow

The live claim owner performs these storage operations:

1. `claimWorkflowTerminalization()` accepts the first `(eventKey, terminalStatus)` for a run and returns an owner ID, opaque claim token, generation, and bounded lease.
2. For nested runs, `admitWorkflowNestedRun()` atomically records the scalar child ID or one foreach `iterationRunIds[index]` together with the bounded child-to-root recovery ancestry before child execution. It validates the immediate source and graph under the parent lock and requires the ancestry tail to equal the parent's retained chain. A conflict writes neither half; a terminal parent returns `parent_terminal` without changing ownership, ancestry, or revision; and an exact replay returns stored state without reapplying the caller's running-result/request-context projection or changing the parent revision. The lower-level ownership and ancestry methods remain adapter contract probes and must not be composed as runtime admission.
3. `persistWorkflowTerminalState()` atomically replaces both canonical final-state views, retains an authenticated canonical recovery envelope, and advances `terminalization_pending` to `run_state_persisted`.
4. `prepareWorkflowTerminalEffect()` verifies that a parent destination exactly matches the immediate ancestry frame, binds the envelope hash into one immutable intent, and advances to `parent_outbox_pending` or `finish_outbox_pending`.
5. `getWorkflowTerminalEffectForDispatch()` returns the full intent and retained recovery evidence, including the retained resource identity, only to the current live fenced owner.

The generic phase compare-and-set cannot create `run_state_persisted`, either `*_outbox_pending` phase, or either `*_effect_recorded` phase. Those transitions belong to specialized atomic methods. A later domain-application capability is responsible for proving application and is the only contract allowed to enter an `*_effect_recorded` phase.

Ancestry admission and completed-evidence cleanup share the same identity lock boundary. PostgreSQL locks the child and every parent identity in deterministic order before inserting the normalized immediate-parent edge; cleanup locks the recovery root before traversing those edges. If cleanup wins, later admission against the now-terminal parent fails closed instead of creating an orphan dependency. If ancestry wins, cleanup observes the pending descendant and retains the parent evidence.

## Destination receipt reservation

An adapter advertising `destinationReceiptVersion: 1` provides two fenced methods:

1. `reserveWorkflowTerminalDestinationReceipt()` creates one immutable receipt slot for `(effectKey, consumerId)` or returns the exact existing slot.
2. `getWorkflowTerminalDestinationReceipt()` returns that slot only to the current live claim owner.

PF-1770 creates only `applicationState: 'reserved'` with `dispatchState: 'none'`. It deliberately does not persist a continuation plan, mark a parent mutation applied, enqueue a destination dispatch, acknowledge a broker event, or advance the journal. Specialized atomic application APIs own those later facts.

Receipt identity includes the immutable producer effect key and a bounded, well-formed framework consumer ID. The deterministic destination hash describes the effect destination without exposing its structural fields in observations. Two consumers of one effect receive different receipt keys; a retry by the same consumer receives the exact stored receipt. Version 1 atomically limits each effect to eight distinct consumer receipts. A ninth distinct consumer returns the categorical `consumer_limit_reached` result, while retries for any of the eight existing consumers continue to return their exact stored receipt. This bounded result makes unstable or per-attempt consumer IDs observable without permitting durable write amplification. Framework-owned deterministic logical consumers may use this protocol. Arbitrary external callbacks still own their idempotency contract.

Application and dispatch are orthogonal. Persisted rows accept only these combinations:

- `reserved / none`;
- `applied / none`;
- `applied / pending`;
- `applied / destination_applied`;
- `quarantined / none`.

The receipt identity fields never change. Later APIs may move through the legal state combinations monotonically, but there is no generic receipt mutation method. Contradictory persisted identity, hashes, states, or timestamps fail closed.

## Atomic parent application

An adapter advertising `parentApplicationVersion: 1` provides three methods:

1. `getWorkflowTerminalParentContext()` validates the live child fence, loads the retained parent effect and child terminal artifact, derives the parent identity from that effect, and returns one isolated parent snapshot with its opaque revision. Callers cannot read an arbitrary parent through this protocol.
2. `applyWorkflowTerminalParentEffect()` accepts only the exact `WorkflowTerminalParentContinuationContract` created by the graph-bound continuation layer. Under the parent lock it validates effect key and payload hash, child status, execution mode, expected revision, observed parent status, graph fingerprint, source ownership, action, and declarative patch. It materializes the next parent exclusively through `applyWorkflowTerminalParentContinuationPatch()` and commits the parent row, canonical receipt, immutable contract record, and journal evidence in one atomic section.
3. `getWorkflowTerminalContinuationPlan()` lets a restarted fenced owner recover the exact committed contract and receipt state without reconstructing a decision from the already-mutated parent.

The canonical consumer is fixed to `mastra.parent-application.v1`; callers cannot supply another consumer ID, duplicate parent identity, raw PubSub target, result mode, iteration index, or executable callback. Exact replay requires the same canonical contract hash and returns the original record without applying the parent patch twice, including after normal parent or child run deletion. A different valid contract returns a redacted `contract_conflict`. A forged, malformed, unbound, per-step, or unknown-version contract returns `invalid_contract` without reserving evidence. Durable retained-child corruption returns `corrupt_child_terminal_state`, while malformed or non-monotonic stored parent evidence returns `corrupt_parent_state`; neither categorical result exposes stored payloads or exception text.

The parent revision is an opaque compare-and-set token maintained by storage on every snapshot mutation. In-memory revisions retain tombstones across delete/recreate. PostgreSQL uses a managed monotonic revision/tombstone table instead of `xmin`, seeds generation 1 idempotently for snapshots that predate that table, and uses its database clock for revision audit timestamps. A mismatch returns `parent_conflict` with no parent, receipt, continuation, or journal mutation.

The committed action determines receipt and journal state:

- `run-entry`, `complete-entry`, `fail-parent`, `finish-parent`, `cancel-parent`, and `suspend-parent` write the PF-1781 parent patch, store `applied / pending`, derive a stable `frameworkActionKey`, and leave the journal at `parent_outbox_pending` until a later destination-application operation records the framework action.
- `wait` writes the parent patch, stores `applied / none`, and advances the journal to `parent_effect_recorded` immediately.
- `noop` stores `applied / none` without rewriting the parent and advances the journal to `parent_effect_recorded`.
- `quarantine` stores `quarantined / none`, does not rewrite the parent, and leaves the journal at `parent_outbox_pending` for explicit recovery or review.

Keeping a pending framework action in `parent_outbox_pending` prevents finish preparation or completed-record cleanup from deleting an undispatched continuation. PF-1772 owns the later destination-applied transition and safe acknowledgement ordering; storage does not pretend that recording an action means it executed.

The committed record stores the canonical contract, contract hash, effect and receipt binding, pre-apply parent revision, optional framework action key, and creation time. It does not store raw broker events, event payloads, complete workflow results, workflow definitions, request context payloads, tools, callbacks, or credentials. The retained child artifact and parent snapshot remain authoritative inputs to the pure patch during the first application only.

Atomicity requires the child journal, retained child state, parent run, revision evidence, receipt, and continuation record to share one physical transaction domain: the same `InMemoryDB`, or the same PostgreSQL database and schema. The capability must not be advertised by an adapter that routes those rows across stores, schemas, databases, or shards without an equivalent atomic primitive.

PF-1782 supplies durable recursive recovery envelopes, exact final-state production, foreach `iterationRunIds`, and strict retained-payload serialization/integrity. PF-1780 must still capability-gate runtime action dispatch instead of treating authenticated storage evidence as cross-process execution authorization. This storage layer fails closed when PF-1781 cannot validate retained state; it does not backfill the rejected pre-contract plan format or infer missing final state.

## Intent identity and payload boundary

Each intent contains only:

- protocol version;
- producer workflow name, run ID, source event key, and terminal status;
- effect kind;
- parent workflow name, parent run ID, parent step ID, and exact bounded structural execution path for a nested-run effect;
- deterministic effect key, deterministic payload hash, and creation time.

The key and payload hash use SHA-256 over length-prefixed UTF-8 fields with separate domain strings. Length framing prevents delimiter ambiguity. The effect identity remains structural, while its payload hash also binds the canonical recovery-envelope hash. The retained envelope hash covers the normalized terminal result/error, exact final state, selected request-context patch, child graph fingerprint, and recursive ancestry. Every read recomputes envelope and effect integrity before returning evidence. PostgreSQL and InMemory therefore authenticate the same canonical bytes instead of relying on adapter-specific snapshot cloning or JSON serialization.

Intents must never persist raw request context, processor arguments, complete workflow results, errors, tools, executable functions, or live object graphs. A finish intent references the separately retained terminal artifact by workflow name and run ID instead of copying the result. Parent execution paths contain 1-256 non-negative safe integers and are included in both deterministic hashes. Structural text identity fields are bounded, must contain well-formed Unicode, and cannot contain the null character rejected by PostgreSQL text storage.

Conflict and observation results omit structural destination fields and all live claim credentials. Full intents are returned only after the owner ID, claim token, generation, and lease are validated.

## Guarantees and limits

The producer protocol guarantees:

- first-terminal-wins journal identity per workflow run;
- monotonic claim generations and permanent fencing of stale workers;
- the journal capability's atomic canonical terminal snapshot plus phase persistence;
- one Core-canonicalized recovery envelope driving both canonical and retained writes;
- exact event-local final state replacing both `context.__state` and `value`;
- authenticated recursive ancestry and per-iteration nested-run ownership;
- retention of the exact terminal artifact independently from the normal run row;
- retention of the effective resource identity needed to reconstruct finish delivery after normal run deletion;
- one immutable producer intent per run and effect kind;
- stable keys across retries and broker event re-publication;
- retention of incomplete journal, terminal artifact, and intent evidence after run deletion.

Version 1 destination receipts additionally guarantee one immutable reserved slot per effect and logical consumer, at most eight distinct consumer slots per effect, stable receipt identity across retries, independent consumer slots, fenced disclosure of valid receipt data, and retention with the terminal evidence until explicit completed-journal cleanup. Present corrupt effect or receipt evidence fails closed before a stale-fence result; missing evidence does not mask a stale fence. Retained terminal state is inspected only after the fence and receipt operation succeed.

Version 1 parent application additionally guarantees one canonical PF-1781-bound parent mutation per child effect, parent-revision and delete/recreate conflict detection with no orphan evidence, immutable and recoverable contract identity, exact contract-only replay, and atomic parent/receipt/continuation/journal evidence. Normal workflow-run deletion does not remove terminal evidence. Explicit completed-terminalization cleanup removes the continuation, canonical receipt, effect, retained snapshot, and journal in one adapter-atomic operation, and a pending framework action cannot reach that completed cleanup state.

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

An adapter advertising `destinationReceiptVersion: 1` must additionally test concurrent same-consumer reservation, independent consumers, the eight-consumer boundary, concurrent distinct reservations at that boundary, exact retry identity at the limit, malformed consumer IDs, all legal and illegal state combinations, corruption-before-stale precedence for present effect/receipt evidence, stale-fence precedence over missing evidence, retained-state authorization, persisted-row corruption, run-deletion retention, transactional cleanup, schema export, and custom-schema qualification.

The combined PF-1781 semantic suite and an adapter advertising `parentApplicationVersion: 1` must test every action/patch family; exact snapshot equivalence with the pure patch helper; scalar and foreach child-run ownership; graph, effect, revision, status, action, and patch conflicts; failure and cancellation without next-step continuation; exact replay and redacted contract conflict; parent mutation races; delete/recreate ABA; pending versus none versus quarantined receipt states; corruption-before-stale precedence for present continuation evidence; alias isolation; full transaction rollback; managed revision and continuation DDL; custom schemas; restart recovery; and cleanup ordering. Each adapter must exercise its own atomic/CAS boundary and state-family mappings; PostgreSQL tests must prove that the parent row, revision generation, receipt, continuation, and journal roll back together.
