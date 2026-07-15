### 4.4e Thread and File Options

`CreateThreadOptions` is not used by an in-process public thread-create method on
the v1 Harness class (§4.1). It exists for the optional operator/import
`POST /harness/:name/operator/threads` wire helper (§13.2) and for internal
session resolution when minting a fresh backing thread. Product lifecycle uses
`harness.session({ threadId: { fresh: true }, resourceId })` or
`POST /sessions`.

The thread option types in this section are lower-level history/import DTOs.
They must not be surfaced as ordinary app lifecycle helpers in `Session`,
`RemoteSafeSession`, or the default client SDK. Session lifecycle and naming use
the session methods in §4.2 and the `/sessions` routes in §13.2.

```ts
// Same-resource thread row creation (operator/import wire path and internal
// minting only). `resourceId` is supplied by route auth in §13.2 or by session
// resolution; callers cannot create a thread for another resource inside this
// object.
interface CreateThreadOptions {
  // Optional thread ID. Omit to let the harness mint one. A collision in the
  // same `(harnessName, resourceId)` scope rejects with `HarnessValidationError`
  // before any message or session state is created.
  threadId?: string;
  // Optional user-facing conversation label.
  title?: string;
  // Optional initial application metadata. It is stored as
  // `HarnessThread.metadata.app`; top-level Harness/Mastra/Memory/channel
  // metadata remains framework-owned and cannot be supplied here.
  metadata?: Record<string, JsonValue>;
}

// Same-resource thread copy. `sourceThreadId` must name a thread in the
// caller's `(harnessName, resourceId)` scope; v1 does not expose cross-resource
// or cross-harness clone through this API. The clone operation creates a new
// thread row plus a full committed-message snapshot of the source thread. It is
// not a session operation and does not copy runtime ownership, session
// records, queue or pending work, channel rows, wakeups, outbox/action/inbox
// rows, per-session workspace state, or memory/observational-memory rows.
// It also does not mutate an existing active session: clone never switches that
// session's thread, changes its locks, resets token accounting, or creates an
// active session for the clone.
//
// Clone is non-idempotent unless the caller supplies `newThreadId`; retrying an
// ambiguous call without `newThreadId` may create another clone. When
// `newThreadId` is supplied, an existing thread in the same resource is a
// validation conflict rather than an overwrite or idempotent success.
interface CloneThreadOptions {
  sourceThreadId: string;
  // Optional destination thread ID. Omit to let the harness mint one.
  newThreadId?: string;
  // Optional title override. Omit to copy the source thread title.
  title?: string;
  // Defaults to false. When true, copies only `HarnessThread.metadata.app`;
  // reserved top-level metadata stays Harness/Mastra/Memory/channel-owned.
  copyAppMetadata?: boolean;
}

// File attachments. Three caller input forms:
//   - Inline: bytes in-memory, which the harness flushes to the attachment
//     store before any durable admission that references the file.
//   - URL: already-hosted asset (S3, signed CDN URL, etc.). This is an input
//     convenience only; durable records never persist the raw URL. Durable
//     admissions fetch/copy it into Harness-owned attachment storage and then
//     persist a `ref`, or reject before the durable write.
//   - Ref: previously uploaded Harness-owned bytes.
//
// Pre-uploaded inline files reference a previously-stored attachment by ID.
// Bytes are stored through the configured blob owner; Harness storage persists
// attachment metadata and refs (see §5.2 and the wire protocol in §13).
type FileAttachment =
  | {
      kind: 'inline';
      name: string;
      mimeType: string;
      data: Uint8Array;
    }
  | {
      kind: 'url';
      name: string;
      mimeType: string;
      url: string;
    }
  | {
      kind: 'ref';
      name: string;
      mimeType: string;
      attachmentId: string; // reference to a previously-stored attachment
    };
```
