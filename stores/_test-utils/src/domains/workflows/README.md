# Workflow terminal storage conformance

`expectWorkflowTerminalStorageContract` and `expectWorkflowTerminalParentStorageContract`
run the same public storage operations against independent handles sharing one backend.
The in-memory test uses one `InMemoryDB`; the PostgreSQL test uses separate connection
pools and a randomly named schema that it removes after the suite.

These helpers reuse the canonical contracts exported by `@mastra/core/storage` and
`@mastra/core/workflows`. They do not define another wire format or inspect adapter internals.

| Contract                              | Shared assertions                                                                                                                                                                |
| ------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `WorkflowTerminalizationCapabilities` | Both handles advertise all five version 1 capabilities. Missing capabilities fail this full conformance suite.                                                                   |
| Terminal journal and retained state   | One concurrent claimant wins; competing terminal outcomes remain distinct; observations omit live credentials; terminal state survives retries.                                  |
| Producer outbox                       | Concurrent preparation converges on the same effect identity and payload hash.                                                                                                   |
| Destination receipt                   | Concurrent reservations for one consumer converge on the same durable receipt.                                                                                                   |
| Lease fencing                         | Reacquisition increases the generation and changes the token. Old credentials cannot persist or access dispatch evidence. New credentials recover existing effects and receipts. |
| Parent application                    | A stale parent revision rejects without applying child output. Concurrent valid application changes the parent once and returns the same continuation plan on retry.             |
| Failure classification                | Missing runs, missing journals, terminal conflicts, stale fences, and invalid transitions remain distinct typed outcomes.                                                        |

Pass two distinct adapters as `primary` and `concurrent`, and a workflow namespace owned
by the test. The parent helper uses fixed parent/child run identifiers within that namespace.
The caller owns cleanup: normal workflow deletion deliberately retains incomplete terminal
recovery evidence, so it is not a substitute for disposing of the isolated test storage.

This is a shared baseline for the full version 1 capability tuple. It complements the
owning adapters’ broader corruption, migration, rollback, and retention suites. Adapters exposing individual capabilities can
still support those primitives; partial support does not pass this suite. Storage exceptions
must remain exceptions rather than becoming `unsupported` or a successful result.

The suite verifies storage boundaries, not a running recovery coordinator. In particular,
a reserved receipt is not proof that a destination applied an effect. Generic phase advancement
cannot turn a finish outbox reservation into completed delivery, and incomplete journal cleanup
must retain recovery evidence. Runtime delivery, acknowledgement, restart recovery, and the
atomic destination-application boundary are separate work; these tests do not certify them.

Run the suites from the workspace root:

```sh
pnpm --filter @internal/storage-test-utils exec vitest run src/domains/workflows/terminalization.test.ts
POSTGRES_DB=mastra pnpm --filter @mastra/pg exec vitest run src/storage/domains/workflows/terminal-contract.test.ts
```

The PostgreSQL command expects the local test database to be available. It follows the
adapter's `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, and
`POSTGRES_PASSWORD` environment configuration. No provider credentials are needed.
