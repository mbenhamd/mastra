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
`stepResults`, or raw resume payloads. The framework credential key
`mastra__authToken` is rejected. Application-defined request-context keys are
not classified heuristically; producers must supply an allowlisted projection
and must not place provider tokens, authorization headers, or other secrets in
that patch.

Ancestry is captured before a nested child begins execution. Terminal
persistence later requires the exact same ancestry and hashes the complete
canonical envelope. A parent effect includes that envelope hash in its payload
integrity, so the continuation contract and destination receipt transitively
bind the exact retained result, error, final state, request-context patch,
graph identity, and ancestry.

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
with parent ownership admission, separately from the replaceable child workflow
snapshot. Terminal persistence produces the same envelope and hash in every
adapter and validates them on every read. Nested scalar ownership is stored as
`nestedRunId`; concurrent foreach ownership is stored as
`iterationRunIds[index]` under the established workflow metadata key. Matching
ownership-and-ancestry replay is read-only and does not reapply the transient
running result or request-context projection; an ancestry, graph, or ownership
mismatch writes neither half.

Recovery v1 accepts continuous execution only. PF-1780 owns planner invocation,
bounded CAS/replan, committed framework-action dispatch, and runtime cutover.
PF-1783 owns loop callback retry semantics. PF-1800 owns durable per-step
recovery. No callback exactly-once, broker ACK, publication, merge, package
release, or deployment behavior is introduced here.

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
