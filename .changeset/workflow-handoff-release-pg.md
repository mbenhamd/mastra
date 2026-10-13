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
  ALTER TABLE "public"."mastra_workflow_snapshot_handoffs"
    ADD COLUMN IF NOT EXISTS "claim_generation" BIGINT NOT NULL DEFAULT 1 CHECK ("claim_generation" >= 1),
    ADD COLUMN IF NOT EXISTS "released_at" BIGINT CHECK ("released_at" >= 0);
  ```

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
