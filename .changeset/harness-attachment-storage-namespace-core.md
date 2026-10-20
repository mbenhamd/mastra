---
'@mastra/core': minor
---

Attachment byte owners now declare a `storageNamespace`, such as an endpoint plus bucket identity. A native Harness store binds it to every attachment upload before any bytes are written, and only cancels or reconciles that upload through an owner for the same namespace.

**Before**

```ts
class R2AttachmentOwner implements HarnessAttachmentByteOwner {
  async cancel({ owner, operationId, expectedBytes, expectedSha256 }) {
    // No way to tell which bucket the upload went to.
  }
  // save, load, delete
}
```

A native store could only ask the owner configured now, so an uncertain upload could be fenced and reported clean in another bucket while its bytes stayed where they were written.

**After**

```ts
class R2AttachmentOwner implements HarnessAttachmentByteOwner {
  readonly storageNamespace = `r2:${endpointHost}/${bucket}`;

  async cancel({ owner, operationId, expectedBytes, expectedSha256, storageNamespace, blobRef }) {
    if (storageNamespace !== this.storageNamespace) throw new HarnessAttachmentByteOwnerConflictError();
    // blobRef is the recorded reference, when the upload returned one.
  }
  // save, load, delete
}
```

`storageNamespace` is a required member, and `save` and `cancel` receive the bound namespace. An owner must throw a conflict error for a namespace or reference that isn't its own. That applies to `load` and `delete` too: they must never read a foreign reference as missing or `already_absent`.

`InMemoryHarnessAttachmentByteOwner` declares `memory://<providerId>` by default, accepts a `storageNamespace` option, and derives every reference under it.
