---
'@mastra/pg': patch
---

Added `deleteWorkflowRunClosure` and `listWorkflowRunClosuresByResource` to the PostgreSQL workflow store, with the same rules as the in-memory store (see `@mastra/core`). The store now reports `closureDeleteVersion: 1`.

```ts
const result = await workflows.deleteWorkflowRunClosure({
  workflowName: 'research-flow',
  runId: 'run-123',
  expectedResourceId: 'workspace-1:user-1',
});
```

- **One transaction.** The delete takes the run's terminal advisory lock, then the parent revision row, the handoff row and the snapshot row, in the order existing writers use. It deletes terminal leaves first and the journal last, then the handoff and the snapshot, and bumps the kept parent revision.
- **New indexes.** Resource lookups no longer scan whole tables. `init()` creates three indexes:
  - `mastra_workflow_snapshot_resource_run_idx` on `mastra_workflow_snapshot ("resourceId", workflow_name, run_id)` (schema-prefixed outside `public`, like the other snapshot indexes);
  - `mastra_workflow_snapshot_handoffs_resource_idx` on `mastra_workflow_snapshot_handoffs (resource_id, workflow_name, run_id)`;
  - `mastra_workflow_terminal_snapshots_v2_resource_idx` on `mastra_workflow_terminal_snapshots_v2 (resource_id, workflow_name, run_id)`.
- **Run-id reuse is fenced.** `mastra_workflow_parent_revisions` gains a nullable `handoff_claim_generation_hwm BIGINT` column. Erasure records the deleted handoff's claim generation there (the max of the stored and the deleted value) in the same transaction, under the parent revision lock. A later claim of the same run id starts at that value plus one, so a delayed `releaseWorkflowSnapshotHandoff` from the erased lifetime answers `conflict` and changes nothing. `init()` adds the column; handoff claims and erasure fail with an error naming the missing column until it exists.
- **Upgrading with init disabled.** If you manage the schema yourself (`disableInit` or `MASTRA_DISABLE_STORAGE_INIT=true`), `init()` validates that these indexes exist, as it does for the existing default indexes, and fails until they do. Add the column and create the indexes before deploying; `exportSchemas(schemaName)` emits the same statements, and `WorkflowsPG.getWorkflowParentRevisionHandoffHwmDDL(schemaName)` returns the column statement. With your schema in place of `public`:

  ```sql
  SET lock_timeout = '5s';
  ALTER TABLE "public"."mastra_workflow_parent_revisions"
    ADD COLUMN IF NOT EXISTS "handoff_claim_generation_hwm" BIGINT;
  CREATE INDEX CONCURRENTLY IF NOT EXISTS "mastra_workflow_snapshot_resource_run_idx" ON "public"."mastra_workflow_snapshot" ("resourceId", "workflow_name" COLLATE "C" ASC, "run_id" COLLATE "C" ASC);
  CREATE INDEX CONCURRENTLY IF NOT EXISTS "mastra_workflow_snapshot_handoffs_resource_idx" ON "public"."mastra_workflow_snapshot_handoffs" ("resource_id", "workflow_name" COLLATE "C" ASC, "run_id" COLLATE "C" ASC);
  CREATE INDEX CONCURRENTLY IF NOT EXISTS "mastra_workflow_terminal_snapshots_v2_resource_idx" ON "public"."mastra_workflow_terminal_snapshots_v2" ("resource_id", "workflow_name" COLLATE "C" ASC, "run_id" COLLATE "C" ASC);
  ```

  Run each `CREATE INDEX CONCURRENTLY` on its own (not inside a transaction), against the database's direct endpoint. A schema outside `public` prefixes the snapshot index name; take the exact name from `exportSchemas(schemaName)`.

  - **Order and rollback.** Earlier versions check the parent revision table's exact shape and treat the new column as incompatible: their `init()` (with init enabled) and their `exportSchemas` migration block fail once it exists. Add the column when you deploy this version, not before. To roll back to an earlier version, first drop the column (`ALTER TABLE ... DROP COLUMN "handoff_claim_generation_hwm"`), which also drops the run-id reuse fence.
