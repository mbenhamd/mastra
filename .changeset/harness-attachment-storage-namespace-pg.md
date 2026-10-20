---
'@mastra/pg': minor
---

The PostgreSQL Harness store binds the attachment byte owner's `storageNamespace` (see `@mastra/core`) to every upload in the same transaction that reserves it, before the owner writes any bytes. `cancel` receives that namespace and the recorded `blobRef`.

The sweep (`reconcileAttachmentOperations`) only selects uploads bound to the configured owner's namespace. Others stay pending, and a retry of one is refused, until an owner for the original namespace is configured again.

**Upgrading existing data**

`init()` adds one nullable column:

```sql
ALTER TABLE "<schema>"."mastra_harness_attachment_operations" ADD COLUMN IF NOT EXISTS "storage_namespace" TEXT
```

With an externally managed schema (`disableInit`), apply that statement before you start the new version, or `init()` reports `missing required columns: storage_namespace`.

Upload rows written before this version have no namespace and are never guessed or backfilled:

- An unsettled upload row stays pending and keeps its session from being deleted. A retry returns `HarnessStorageAttachmentPendingError`.
- A completed upload row rejects a re-save with `HarnessStorageAttachmentConflictError`, without writing bytes.

An operator who has verified where a pending row's bytes went can qualify that one row:

```sql
UPDATE "<schema>"."mastra_harness_attachment_operations"
SET storage_namespace = '<verified namespace>'
WHERE id = '<operation id>' AND kind = 'put' AND storage_namespace IS NULL
  AND status NOT IN ('completed', 'cleaned');
```

Only do this with evidence of the bucket the upload used, and use the exact `storageNamespace` string that owner declares. A wrong value lets the sweep cancel in the wrong namespace, which is the failure this change prevents.
