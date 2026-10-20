---
'@mastra/pg': minor
---

The PostgreSQL Harness store binds the attachment byte owner's `storageNamespace` (see `@mastra/core`) to every upload in the same transaction that reserves it, before the owner writes any bytes. `cancel` receives that namespace and the recorded `blobRef`.

**Before**

`reconcileAttachmentOperations` cancelled or verified every unsettled upload through whichever owner was configured. After a restart with another bucket, an upload whose bytes were in the old bucket was fenced in the new one and marked `cleaned`. A retry could also write and commit the attachment in the new bucket.

**After**

The sweep only selects uploads bound to the configured owner's namespace. Others stay pending, keep their session from being deleted, and refuse a retry with `HarnessStorageAttachmentPendingError`. That lasts until an owner for the original namespace is configured again. A retry of a completed upload through another namespace returns `HarnessStorageAttachmentConflictError` without writing bytes.

**Upgrading existing data**

`init()` adds one nullable column:

```sql
ALTER TABLE "<schema>"."mastra_harness_attachment_operations" ADD COLUMN IF NOT EXISTS "storage_namespace" TEXT
```

With an externally managed schema (`disableInit`), apply that statement before you start the new version, or `init()` reports `missing required columns: storage_namespace`.

Upload rows written before this version have no namespace and are never guessed or backfilled:

- Before, an unsettled row was reconciled through the configured owner. Now it stays pending and keeps its session from being deleted, and a retry returns `HarnessStorageAttachmentPendingError`.
- Before, a re-save of a completed row succeeded as `already_stored`. Now it returns `HarnessStorageAttachmentConflictError` without writing bytes.

An operator who has verified where a pending row's bytes went can qualify that one row. The next sweep through an owner for that namespace then settles it:

```sql
UPDATE "<schema>"."mastra_harness_attachment_operations"
SET storage_namespace = '<verified namespace>'
WHERE id = '<operation id>' AND kind = 'put' AND storage_namespace IS NULL
  AND status NOT IN ('completed', 'cleaned');
```

Only do this with evidence of the bucket the upload used, and use the exact `storageNamespace` string that owner declares. A wrong value lets the sweep cancel in the wrong namespace, which is the failure this change prevents.
