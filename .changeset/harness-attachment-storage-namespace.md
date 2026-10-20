---
'@mastra/core': minor
'@mastra/pg': minor
---

Attachment byte owners now declare a `storageNamespace`, such as an endpoint plus bucket identity. The PostgreSQL Harness store binds it to every attachment upload before any bytes are written, and passes it with the recorded `blobRef` to `cancel`.

```ts
class R2AttachmentOwner implements HarnessAttachmentByteOwner {
  readonly storageNamespace = `r2:${endpointHost}/${bucket}`;
  // save, load, delete, cancel
}
```

If the process restarts with a different namespace, the store does not cancel or settle uploads bound to the old one. They stay pending until an owner for the original namespace is configured again. A later retry of the same upload is refused in the same way, and an owner must refuse a `cancel` for a namespace or reference that isn't its own.

The schema change is one nullable `storage_namespace` column on `mastra_harness_attachment_operations`, added on `init()`. Rows written before this change have no namespace. They're never guessed or backfilled: an unsettled upload row without one stays pending until an operator records its namespace.
