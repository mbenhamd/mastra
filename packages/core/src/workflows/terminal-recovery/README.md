# Terminal workflow recovery envelope

This module defines the internal, versioned data contract retained for durable
workflow terminal recovery. It projects evented runtime state into bounded,
canonical JSON before either InMemory or PostgreSQL crosses its storage
boundary. The envelope is protocol evidence; it is not an executable workflow,
an event payload, or cross-process execution authorization.

## Envelope authority

Version 1 retains:

- the child workflow/run identity, terminal status, and continuous execution
  mode;
- canonical terminal result, exact event-local final state, and an explicitly
  selected request-context patch;
- the serialized child-graph fingerprint; and
- an immediate-parent-first ancestry chain containing only parent identity,
  graph fingerprint, exact source coordinate, structural input/result pointers,
  and resume-control metadata.

It never retains executable step graphs, workflow instances, callbacks, tools,
provider clients, request objects, raw parent snapshots, complete captured
`stepResults`, or raw resume payloads. Framework-owned request-context names
(`mastra__*`, `mastra:*`, `__mastra*`, `__harness*`, and known bare
infrastructure slots) are rejected.
Producers must supply an allowlisted application projection and must not place
provider tokens, authorization headers, or other secrets in that patch.

Ancestry is captured before a durable nested child begins execution. A new
child for which `shouldPersistSnapshot` returns `false` remains outside this
protocol; a later transition for an already-retained child still replays its
admission so a resume cannot silently discard recovery evidence. Terminal
persistence later requires the exact same ancestry and hashes the complete
canonical envelope. A parent effect includes that envelope hash in its payload
integrity, so the continuation contract and destination receipt transitively
bind the exact retained result, error, final state, request-context patch,
graph identity, and ancestry.

Nested child identity is deterministic for one parent run and exact graph
coordinate, so broker redelivery before admission does not invent a second
child. Restart and time travel reuse retained ownership when it exists; a new
execution generation requires a new parent run identity. This is durable
identity, not exactly-once dispatch: a nonterminal admitted child may still be
redispatched until a dedicated durable child-start outbox owns publication.

## Canonical data rules

- Object keys are sorted; `-0` becomes `0`.
- Object-property `undefined` is omitted; array `undefined` becomes `null`.
- A missing or `undefined` request-context patch becomes `{}`; `null` is
  rejected.
- Valid `Date` values become ISO strings without invoking `toJSON`.
- Native `Error` values retain data-only `name`, `message`, optional `stack`,
  `cause`, and safe enumerable custom fields.
- Proxies, accessors, symbols, functions, BigInt, Map, Set, cycles, sparse
  arrays, non-finite numbers, custom prototypes, malformed Unicode, and null
  characters fail closed.
- Ancestry depth, value depth, collection entries, total nodes, error stack
  bytes, path length, and total UTF-8 bytes are bounded by exported constants.

The canonicalizer never calls getters, `toJSON`, constructors, custom
iterators, or provider code.

## Runtime and storage boundary

Supported storage adapters atomically retain pre-terminal ancestry together
with parent ownership admission and create the initial durable child snapshot
when it is absent. Exact replay repairs a missing initial row after an admission
crash but never replaces retained running or suspended progress. Later workflow
snapshot updates remain independently replaceable. Every admission carries the
expected child graph fingerprint even when no initializer is requested, so a
nonpersisting durable replay cannot dispatch retained state from a different
graph. A terminal child marker or snapshot returns `child_terminal` without
resurrecting a deleted canonical row. Malformed, wrong-run, unknown-status, or
graph-drifted retained state returns `child_snapshot_conflict`; these failure
paths write neither parent ownership nor recovery ancestry. Malformed or
incomplete locked parent state returns `parent_snapshot_conflict` before parent
ownership, recovery ancestry, or child initialization changes. Terminal
persistence rejects incomplete required run state as `invalid_snapshot` before
advancing the journal or retaining evidence, then produces the same envelope
and hash in every adapter and validates them on every read. Nested scalar ownership is stored as
`nestedRunId`; concurrent foreach ownership is stored as
`iterationRunIds[index]` under the established workflow metadata key. Matching
ownership-and-ancestry replay does not reapply the transient running result or
request-context projection; aside from create-if-absent child initialization it
is read-only. An ancestry, graph, or ownership mismatch writes neither half.

Recovery v1 accepts continuous execution only. PF-1780 owns planner invocation,
bounded CAS/replan, committed framework-action dispatch, and runtime cutover.
PF-1783 now derives loop callback input only from authenticated recovery evidence
and the locked parent snapshot. It uses retained final state, terminal output,
request-context patch, projected step results, and persisted source
`resumePayload`; process-local raw resume data is not reconstructed. A callback
may run again after a crash before atomic apply or a parent CAS conflict, and
side-effecting callbacks remain unsupported. Callback frames use the same
canonical request-context classifier as recovery envelopes, rejecting framework
namespaces and known bare infrastructure slots while preserving application
keys. PF-1800 owns durable per-step recovery. No callback exactly-once, broker
ACK, publication, merge, package release, or deployment behavior is introduced
here.

## Verification

```sh
pnpm --dir packages/core exec vitest run src/workflows/terminal-recovery --config vitest.config.ts
pnpm --dir packages/core exec vitest run src/workflows/evented/workflow-event-processor --config vitest.config.ts
pnpm --filter @mastra/core check
```

Adapter implementations must additionally prove byte-equivalent recovery
evidence, native Error parity, corruption rejection, concurrent ancestry and
foreach ownership, transaction rollback, run-row replacement isolation, and
leaf-first dependency-aware cleanup.
