# Terminal parent continuation contract

This directory is the internal semantic boundary between retained child-run
terminal evidence and the later durable application/recovery work. It exists so
storage adapters and runtime recovery do not independently invent how a nested
workflow result changes its parent.

PF-1781 deliberately adds no public API, storage capability, runtime call site,
event publication, or compatibility path. It accepts continuous parent
execution only; PF-1800 owns durable per-step pause/resume semantics. PF-1771 will consume the structural
contract inside atomic storage operations; PF-1779 will choose one contract from
locked parent state; PF-1780 will execute only the committed action.

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
  uses retained terminal evidence, preserves the
  parent's step payload, overlays child metadata, fixes `nestedRunId`, replaces
  both persisted state views, merges request context, and uses a monotonic
  storage-provided clock.
- Foreach stores raw output and a framework-owned per-index terminal status
  sidecar together. The sidecar distinguishes pending `null`, successful
  `null`/`undefined`, failure, and cancellation after JSON persistence.
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
| PF-1780 runtime  | bounded replan, committed-action execution, recovery and acknowledgement         | adapter-specific semantic rewrites                        |

## Known source gaps exposed by the contract

The current evented processor is the behavioral reference, not an authority for
known unsafe recovery behavior. PF-1781 intentionally exposes these differences:

- Retained PF-1770 terminal state does not yet guarantee the exact event-local
  final `context.__state`. The pure patch fails closed when it is absent. The
  producer integration must make this evidence durable before runtime adoption.
- Current nested completion can forward the child's `activeStepsPath` instead of
  the parent's map. Planner and runtime adoption must use the locked parent
  snapshot.
- Current nested cancellation can persist the child as canceled but continue or
  finish the parent inconsistently. The contract chooses explicit parent
  cancellation; runtime adoption must update the evented behavior and its tests.
- In-memory workflow snapshots can preserve richer JavaScript values than JSON
  adapters. The patch normalizes every touched payload/state/context value to a
  JSON-observable form, but the terminal producer must serialize native errors
  before a JSON adapter can discard their diagnostics.
- The foreach sidecar is not consumed by the current evented processor. PF-1779
  planning and PF-1780 dispatch must use the shared sidecar key instead of the
  current raw-`null` pending heuristic before runtime adoption.
- Current producers do not populate `iterationRunIds` per foreach index. PF-1782
  must make that ownership evidence durable before the contract can accept a
  recovered concurrent iteration.
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
pnpm --filter @mastra/core check
```

The tests cover graph sensitivity and JSON stability, hostile structural input,
hash integrity, effect/revision/status/coordinate binding, action/patch matrices,
scalar and foreach application, loop metadata, final state and request context,
input immutability, and missing-evidence failure. PF-1771 must add the same cases
against both InMemory and PostgreSQL atomic implementations before advertising a
storage capability.
