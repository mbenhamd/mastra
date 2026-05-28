### 13.3c Conditional Session-Version Mutations

**Conditional session-version mutations.** `GET /sessions/:sessionId` and
`GET /sessions/:sessionId/state` return an HTTP `ETag` header whose opaque value
represents the addressed `SessionRecord.version`. `PATCH /state`,
`PATCH /thread-settings/:key`, and `PATCH /title` require that value in
`If-Match`; the server rejects weak ETags, `*`, multiple validators, and
malformed validators with `harness.validation`. If the current stored version no
longer matches, the route returns `409 harness.state_conflict` before attempting
the state merge, thread app-metadata write, or session title rename. This is a
session-level validator, not field-level state or metadata CAS: any durable
session write can advance it. HTTP clients must treat the ETag as opaque even
when a deployment encodes the numeric version directly.

Thread setting payloads are intentionally narrower than raw thread metadata:

```ts
interface ThreadSettingRequest {
  value: JsonValue;
}

interface RenameSessionRequest {
  title: string;
}

interface CloneThreadRequest {
  newThreadId?: string;
  title?: string;
  copyAppMetadata?: boolean;
}
```

The route parameter `:key` names an app-owned key under
`HarnessThread.metadata.app`. Clients cannot send top-level metadata keys or
reserved Harness/Mastra/Memory/channel/legacy namespaces on the wire.
For `POST /operator/threads/:threadId/clone`, the route parameter names the
source thread. Operator/import callers do not send `sourceThreadId` or
`resourceId` in the body; the server derives both from the URL and authenticated
resource.
