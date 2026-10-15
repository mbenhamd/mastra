---
'@mastra/pg': minor
---

Added `deleteWorkflowRunClosure` and `listWorkflowRunClosuresByResource` to the PostgreSQL workflow store, with the same rules as the in-memory store (see `@mastra/core`). The store now reports `closureDeleteVersion: 1`.

```ts
const result = await workflows.deleteWorkflowRunClosure({
  workflowName: 'research-flow',
  runId: 'run-123',
  expectedResourceId: 'workspace-1:user-1',
});
```

- **Atomic.** Each erasure runs in one transaction and serializes with the run's other writers.
- **Faster resource lookups.** `init()` creates resource indexes on the snapshot, handoff and terminal snapshot tables.
- **Run-id reuse is safe.** If an erased run's id is used again, its next handoff claim starts above the erased claims, so a delayed `releaseWorkflowSnapshotHandoff` from the erased run answers `conflict` and changes nothing. This uses a new nullable `handoff_claim_generation_hwm` column on `mastra_workflow_parent_revisions`, which `init()` adds.
- **Upgrading with init disabled.** If you manage the schema yourself (`disableInit` or `MASTRA_DISABLE_STORAGE_INIT=true`), add the column and create the indexes before deploying. Until the column exists, handoff claims and closure erasure fail with an error naming it; until the indexes exist, `init()` fails validation. `exportSchemas(schemaName)` emits the same statements, and `WorkflowsPG.getWorkflowParentRevisionHandoffHwmDDL(schemaName)` returns the column statement. With your schema in place of `public`:

  ```sql
  SET lock_timeout = '5s';
  ALTER TABLE "public"."mastra_workflow_parent_revisions"
    ADD COLUMN IF NOT EXISTS "handoff_claim_generation_hwm" BIGINT;
  CREATE INDEX CONCURRENTLY IF NOT EXISTS "mastra_workflow_snapshot_resource_run_idx" ON "public"."mastra_workflow_snapshot" ("resourceId", "workflow_name" COLLATE "C" ASC, "run_id" COLLATE "C" ASC);
  CREATE INDEX CONCURRENTLY IF NOT EXISTS "mastra_workflow_snapshot_handoffs_resource_idx" ON "public"."mastra_workflow_snapshot_handoffs" ("resource_id", "workflow_name" COLLATE "C" ASC, "run_id" COLLATE "C" ASC);
  CREATE INDEX CONCURRENTLY IF NOT EXISTS "mastra_workflow_terminal_snapshots_v2_resource_idx" ON "public"."mastra_workflow_terminal_snapshots_v2" ("resource_id", "workflow_name" COLLATE "C" ASC, "run_id" COLLATE "C" ASC);
  ```

  Run each `CREATE INDEX CONCURRENTLY` on its own (not inside a transaction), against the database's direct endpoint. A schema outside `public` prefixes the snapshot index name; take the exact name from `exportSchemas(schemaName)`.

- **Order and rollback.** Earlier versions reject the new column: their `init()` (with init enabled) and their `exportSchemas` migration block fail once it exists. Add the column when you deploy this version, not before. To roll back to an earlier version, first drop the column (`ALTER TABLE ... DROP COLUMN "handoff_claim_generation_hwm"`), which also drops the run-id reuse fence.
- **Execution closures.** Export and import carry the column. An import whose value is not a positive integer, or a re-import over a destination row with a lower value, fails instead of weakening the fence.
