# Terminal parent continuation contract

This directory is the internal semantic boundary between retained child-run
terminal evidence and the later durable application/recovery work. It exists so
storage adapters and runtime recovery do not independently invent how a nested
workflow result changes its parent.

PF-1781 deliberately added no public API, storage capability, runtime call site,
event publication, or compatibility path. It accepts continuous parent
execution only; PF-1800 owns durable per-step pause/resume semantics. PF-1771
consumes the structural contract inside atomic storage operations; PF-1779's
pure planner chooses one contract from locked parent state; PF-1780 will execute
only the committed action.

## Invariants

- A contract binds the exact terminal effect key and payload hash, execution
  mode, parent revision, parent status, serialized graph fingerprint, source
  coordinate, closed action, and declarative parent patch.
- The durable contract contains no child result, error, state, request context,
  callback, executable workflow object, tool, event payload, or provider data.
- Graph targets name real steps, sleep entries, or containers. Containers never
  receive invented step IDs. Sequential, branch, loop, and foreach actions are
  bound to their source topology and locked sibling/iteration state.
- Loop callbacks are evaluated outside this module. The contract stores only the
  evaluated decision and consecutive iteration counts.
- `finish-parent`, `cancel-parent`, and `suspend-parent` are framework actions.
  They are not aliases for publishing a raw `workflow.end` event.
- The pure patch reference canonicalizes touched values to JSON-observable data,
  uses retained terminal evidence, preserves the parent's step payload, overlays
  child metadata, fixes `nestedRunId`, replaces both persisted state views,
  canonicalizes the existing parent request context before overlaying the child
  context, and uses a monotonic storage-provided clock.
- Contract, effect, parent snapshot, and retained child evidence share one
  descriptor-safe input budget: depth 64, 4,096 visited nodes, 16,384
  collection/object entries, and 1 MiB of UTF-8 strings and keys. Cycles,
  shared-reference amplification, proxies, accessors, symbols, non-JSON `bigint` values,
  unsupported opaque structured-clone objects, and over-budget evidence fail
  with controlled contract errors before cloning or normalization.
- Foreach stores raw output and a framework-owned per-index terminal status
  sidecar together. The sidecar distinguishes pending `null`, successful
  `null`/`undefined`, failure, and cancellation after JSON persistence.
- Foreach source evidence must contain a dense started-output array bounded by
  the original input payload; phantom iterations never become suspension or
  resume-label evidence.
- No-op and quarantine contracts apply no parent patch.
- Scalar sources must still be the exact active `nestedRunId`; concurrent
  foreach sources use a separate `iterationRunIds[index]` ownership sidecar.
- All-accounted branch/foreach suspension is a committed `suspend-parent`
  action that derives suspended paths and resume labels. It is not conflated
  with waiting for an active sibling.
- Foreach aggregate suspension preserves JSON-observable, non-framework
  suspend payloads in a framework-owned per-index map and rebuilds path metadata
  plus validated resume labels. A single suspended iteration is also hoisted for
  current single-iteration consumers; multiple iterations deliberately do not
  expose one arbitrary payload as if it applied to every label.
- Child cancellation is an explicit `cancel-parent` action that aborts remaining
  parent work. This intentionally corrects the current evented path that can
  finish a parent as successful while carrying a canceled final result.

## Responsibilities

| Layer            | Owns                                                                             | Must not own                                              |
| ---------------- | -------------------------------------------------------------------------------- | --------------------------------------------------------- |
| PF-1781 contract | canonicalization, hashing, graph binding, action/patch matrix, pure parent patch | planning, storage CAS, callback execution, event dispatch |
| PF-1771 storage  | atomic parent patch plus committed plan/receipt evidence                         | choosing a new action during replay                       |
| PF-1779 planner  | selecting the one immediate action from locked state and pre-evaluated decisions | persistence or publication                                |
| PF-1783 decision | restartable callback frame, one-attempt evaluator, re-evaluation policy          | storage CAS, committed-plan lookup, or event dispatch     |
| PF-1780 runtime  | bounded replan, committed-action execution, recovery and acknowledgement         | adapter-specific semantic rewrites                        |

## Pure planner

`planWorkflowTerminalParentContinuation()` accepts only a versioned parent
effect, opaque parent revision, payload-free structural planning view of the
locked parent snapshot, and an optional bound loop decision. It derives the
parent/source identity, graph fingerprint, action, and patch; creates the
canonical contract; and validates the final binding before returning.

It does not accept retained child output, state, request context, workflow
definitions, callbacks, tools, storage, PubSub, clocks, randomness,
caller-selected source coordinates, or caller-selected actions/patches. The planning
view preserves only topology and control facts required by PF-1781: parent
status, graph, source ownership, active paths, sibling states, loop iteration
count, and foreach ownership/terminal sidecars.

Structural readers reject proxies before invoking reflection, never coerce
unvalidated values, and cap planner-only collection/map materialization. Fields
outside the selected control projection are ignored without reading their
descriptors, so user payloads and unrelated accessors cannot affect planning.

Loop callbacks remain outside the planner. The runtime first derives a
`WorkflowTerminalLoopDecisionRequestV1` from the locked context, evaluates the
callback, and returns only a boolean attached to that request's deterministic
decision key. The key binds the effect key/payload hash, parent revision/status,
graph fingerprint, exact source, loop type, and previous iteration count. A CAS
retry must discard the old decision and derive a new request; exact committed
replay never evaluates the callback again.

## Durable loop decision evaluation

PF-1783 defines the corrected restartable callback subset without switching the
live processor. `materializeWorkflowTerminalLoopConditionFrame()` validates the
locked graph, effect, ancestry, retained recovery hash, and parent revision. It
reuses the pure child-merge semantics to expose isolated canonical `inputData`,
final `state`, merged request context, projected step results, a persisted source
`resumePayload` when present, and the next iteration count. Raw process-local
`ParentWorkflow.resumeData` is not recovery evidence and is never inferred.
The projected source result includes the just-completed child output before the
condition runs, matching the default engine's merge-before-condition ordering.
This intentionally corrects the current evented helper's pre-merge
`stepResults`; PF-1780 activation must preserve that cutover explicitly.
The callback still receives own `inputData` and `resumeData` properties with an
`undefined` value when that canonical evidence is absent, matching the live
evented callback shape.
Parent CAS replans are not workflow-step retries, so the callback-visible
`retryCount` remains zero while the decision key changes with the parent revision.

The frame also binds the serialized condition ID and a hash of its well-formed
source. The evented evaluator compares these with the registered callback before
invoking it and rejects proxied, bound, native, malformed, or changed source.
Stored source text is evidence, not semantic function identity: closure captures
remain unsupported and must not influence a durable predicate. Actor-dependent
conditions remain unsupported until actor propagation is composed separately.

`evaluateEventedWorkflowTerminalLoopDecision()` runs exactly one callback
attempt. It composes caller cancellation with a bounded cooperative timeout,
requires a boolean, and reports throw, abort, timeout, registration mismatch,
same-key frame mismatch, invalid return, denied capability use, input mutation,
or capacity exhaustion as a typed non-decision. None of those outcomes can be
passed to the planner or storage as `false`. Native abort intrinsics prevent
hostile own `AbortSignal` properties from executing. A decision-key-local
evaluator validates callback registration and canonical frame identity before
every cache lookup, coalesces concurrent work in one process, and retains
successful decisions. The first admitted attempt owns the coalesced cancellation
signal and deadline; followers observe that attempt's result. A same-key frame
mismatch leaves the existing valid entry untouched. When abort or timeout wins
after callback invocation, the key remains occupied until the underlying
callback actually settles, so redelivery cannot overlap a non-cooperative
attempt. Late values and errors are discarded; settlement releases the key and
capacity. Permanently pending callbacks remain retained without a TTL. The cache
is bounded and exposes retained-in-flight and capacity-exhaustion counters;
callback and frame mismatches are never admitted. Pre-invocation aborts,
failures, invalid results, and unexpected evaluator rejections are evicted
rather than poisoning a later same-key attempt.

Durable loop conditions must be pure predicates. Changes to isolated canonical
data values, keys, or reference topology in state, request context, or step
results are rejected and never leak into retained evidence. Descriptor-only
reflection and deliberate private-field or global monkey-patching are outside
this arbitrary-JavaScript contract. The evaluator supplies denied Mastra,
writer, PubSub, engine, bail, and observability capabilities so those framework
side effects cannot run through the callback context. Native observability keys
remain present and `tracingContext` aliases `tracing`, preserving presence and
identity checks without exposing a shared mutable no-op object. Network,
database, global, and closure effects still cannot be detected or rolled back
for arbitrary JavaScript and are unsupported. A separate durable decision
primitive is required for them. The canonical request-context
classifier withholds framework namespaces and known bare infrastructure slots
during trusted frame materialization while preserving application-owned keys;
the exported frame validator rejects forged infrastructure entries. The complete
frame shares one bounded canonical byte budget.

A fresh parent revision produces a fresh decision key and permits at most three
live attempts (`MAX_WORKFLOW_TERMINAL_LOOP_DECISION_ATTEMPTS`: one initial plus
two replans). A crash before atomic apply
loses the process-local cache and may execute the callback again. PF-1780 must
look up committed plan evidence first; a crash after commit therefore replays the
stored decision without invoking this evaluator. The bound is not a durable or
global callback execution count.

The planner is synchronous and side-effect free. Invalid or unfingerprintable
graphs throw. Durable graph validation follows Mastra's real serialized callback
identity convention (`<step-id>-condition`) and treats optional serialized
fields whose values are `undefined` as absent. A fingerprintable effect/graph coordinate mismatch produces a
deterministic `graph-conflict` quarantine. Contradictory ownership or sibling
state produces a structural-only `plan-conflict` quarantine whose digest never
contains output, errors, request context, or complete snapshots.

PF-1779 does not change the live evented processor. PF-1780 owns characterization
parity, bounded CAS/replan, capability gating, and cutover.

## Known source gaps exposed by the contract

The current evented processor is the behavioral reference, not an authority for
known unsafe recovery behavior. PF-1781 intentionally exposes these differences:

- PF-1782 recovery evidence carries the exact event-local final state and
  atomically replaces both persisted state views. The pure patch continues to
  fail closed when a caller supplies unauthenticated or incomplete evidence.
- Current nested completion can forward the child's `activeStepsPath` instead of
  the parent's map. Planner and runtime adoption must use the locked parent
  snapshot.
- Current nested cancellation can persist the child as canceled but continue or
  finish the parent inconsistently. The contract chooses explicit parent
  cancellation; runtime adoption must update the evented behavior and its tests.
- PF-1782 canonicalizes terminal result/error, final state, request-context
  patch, graph identity, and ancestry in Core before either InMemory or a JSON
  adapter sees them. Native Error diagnostics and rejection behavior are now
  adapter-independent.
- The foreach sidecar is not consumed by the current evented processor. PF-1779
  planning and PF-1780 dispatch must use the shared sidecar key instead of the
  current raw-`null` pending heuristic before runtime adoption.
- Evented nested starts populate `iterationRunIds` per foreach index through an
  adapter-atomic ownership write. The planner still validates the selected
  child against the locked parent snapshot before accepting recovery.
- Current label-directed foreach resume consumers read only the aggregate
  step-level suspend payload. PF-1780/PF-1769 must select the exact
  `iterationSuspendPayloads[foreachIndex]` entry before runtime adoption; until
  then multi-iteration recovery must fail closed rather than restore another
  iteration's agent or tool state.
- Per-step execution is excluded. PF-1800 must define pause generation,
  deferred resume plans, concurrent effects while paused, and callback-free
  replay before PF-1780 can advertise per-step support.

These gaps are not hidden behind PapersFlow-side wrappers or alternate runtimes.
Terminal-effect framed identity hashing and integrity validation live in this
workflow-layer module so storage and downstream planners share one validator
without a workflow-to-storage dependency. Binding still assumes callers invoke
that validator before constructing a contract; both supported storage paths do.
Terminal cleanup intentionally retains `tracingContext` for observability while
clearing resumable run-control fields.

## Verification

Focused contract verification:

```sh
pnpm --filter @mastra/core test -- --run src/workflows/terminal-continuation
pnpm --dir packages/core exec vitest run src/workflows/evented/workflow-event-processor/terminal-loop-decision-evaluator.test.ts --config vitest.config.ts
pnpm --filter @mastra/core check
```

The tests cover graph sensitivity and JSON stability, hostile structural input,
negative-zero normalization, hash integrity, effect/revision/status/coordinate
binding, every planner action/patch family, scalar and foreach ownership, bound
loop decisions, structural conflict digests, exact successor targeting, input
immutability, and missing-evidence failure. PF-1771 separately exercises the
InMemory and PostgreSQL atomic/CAS boundaries.
