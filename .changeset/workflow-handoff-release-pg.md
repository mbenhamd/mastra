---
'@mastra/pg': patch
---

Added `releaseWorkflowSnapshotHandoff` to the PostgreSQL workflow store, so a run whose snapshot handoff is done can be deleted or pruned again. It follows the same rules as the in-memory store (see `@mastra/core`).

```ts
await workflows.releaseWorkflowSnapshotHandoff({
  workflowName: 'research-flow',
  runId: 'run-123',
  expectedClaimGeneration: 1,
  deleteRun: true,
});
```

- **Concurrent calls.** Release, claim, transition and completion running at the same time on one run always give consistent results.
- **Retried calls.** A retried or late release never removes a newer claim of the run.
- **Execution closures.** A released handoff is neither exported nor restored, so an import never brings back pre-release state or recreates a run deleted with `deleteRun`.
- **Upgrading with `init()`.** Existing handoff tables gain two columns (`claim_generation` and `released_at`) on the next `init()`. Existing handoffs keep working unchanged.
- **Upgrading with init disabled.** If you manage the schema yourself (`disableInit` or `MASTRA_DISABLE_STORAGE_INIT=true`), run this idempotent statement before deploying, with your schema in place of `public`:

  ```sql
  SET lock_timeout = '5s';
  ALTER TABLE "public"."mastra_workflow_snapshot_handoffs"
    ADD COLUMN IF NOT EXISTS "claim_generation" BIGINT NOT NULL DEFAULT 1 CHECK ("claim_generation" >= 1),
    ADD COLUMN IF NOT EXISTS "released_at" BIGINT CHECK ("released_at" >= 0);
  ```

  - **How to run it.** Use `psql -X -v ON_ERROR_STOP=1 -f <file>`, as the table's owner or migration role, against the database's direct endpoint, not a transaction pooler.
  - **Lock timeout.** The `ALTER` briefly takes an exclusive lock on the table. `lock_timeout` makes it fail fast instead of queueing behind long transactions; if it times out, run it again.
  - **Order.** It is safe to apply before upgrading: the previous version's inserts don't name the new columns, so they take the defaults (generation 1, not released).

  `WorkflowsPG.getWorkflowSnapshotHandoffReleaseDDL(schemaName)` returns the same statement for a given schema.
  - Until you run it, every handoff operation, every native write or deletion that checks the handoff, and retention all fail with an error naming the missing columns. Nothing is written.
  - The store picks up the columns as soon as they exist, without a restart.
  - The same check also catches a table created without the columns by an older version's `init()` running at the same time.

- **Capability.** The store now reports `releaseVersion: 1`.
- **Released rows accumulate.** Released handoffs stay in the table until you remove them. To drop ones released before a cutoff (epoch milliseconds), with your schema in place of `public`:

  ```sql
  DELETE FROM "public"."mastra_workflow_snapshot_handoffs"
  WHERE "released_at" IS NOT NULL AND "released_at" < $1;
  ```

  Removing a released, completed handoff also drops its kept outcome.
