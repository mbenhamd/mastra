### 4.4b Queue and Skill Options

```ts
// `QueueOptions` deliberately omits `addTools`; §4.3 owns the override rule
// and rationale.
interface QueueOptions extends Omit<HarnessOverrides, 'addTools'> {
  // Signal type to use when the queued item drains. Defaults to
  // `'user-message'`. Queue is still sequential work; this field exists so
  // channel/proactive/product controllers do not need a parallel queue DTO.
  type?: string;
  contents:
    | string
    | Array<{ type: 'text'; text: string } | { type: 'file'; attachmentId: string; mediaType: string; name?: string }>;
  files?: FileAttachment[];
  // JSON-safe operation annotations persisted with the queued item and copied
  // to the drained signal. These are caller/application metadata, not mutable
  // process handles or product-local queued actions.
  attributes?: Record<string, JsonValue>;
  metadata?: Record<string, JsonValue>;
  // Optional caller-supplied idempotency key. When present, `queue(...)`
  // enforces uniqueness under the active session lease and returns the existing
  // queued item/result metadata for exact duplicate admissions. The harness
  // still mints and persists an internal admissionId for every queued item when
  // callers omit this field, because crash recovery must retry the drain
  // admission with a stable key.
  admissionId?: string;
  requestContext?: RequestContextInput;
  tracingContext?: TracingContext;
  tracingOptions?: TracingOptions;
}

// `admissionId` duplicate detection uses a stable admission hash. For
// signal/queue inputs, the hash inputs are the normalized signal `type`,
// contents, persisted file references or content digests, JSON-safe attributes
// and metadata, serializable `requestContext`, and relevant serializable turn
// overrides (`model`, `mode`, `yolo`). For untyped
// skill invocation, the hash also includes the skill name, resolved skill source
// and content digest, validated args, and expanded prompt. Because the hash must
// be reproducible, `admissionId` cannot be combined with non-serializable
// overrides such as `addTools`; `queue(...)` omits `addTools`, and
// `signal(...)` / untyped `useSkill(...)` reject that combination
// with `HarnessValidationError`. `signal({ sync: true, output, admissionId })`
// and `useSkill({ output, admissionId })` are also rejected until a separate
// generate-admission receipt exists. When `admissionId` is present,
// `requestContext` and every hash input must validate against the Harness
// stable-hash canonicalization profile (§5.1); non-serializable values reject
// with `HarnessValidationError` before admission. Exact retries with the same
// `admissionId` and hash return the original metadata while the owning
// operation evidence is retained; a retry with the same `admissionId` and a
// different hash throws `HarnessAdmissionConflictError`. §5.1 and §5.7 own the
// concrete signal evidence, `QueueAdmissionReceipt`, `OperationAdmissionTombstone`,
// retention, post-compaction lookup, and recovery behavior; §15 owns the
// corresponding verification invariants.

interface UseSkillOptions<S extends PublicSchema | undefined = undefined> extends HarnessOverrides {
  args?: Record<string, unknown>; // injected into the skill prompt
  files?: FileAttachment[];
  output?: S; // typed result
  // Optional caller-supplied idempotency key for untyped skill invocations from
  // retrying transports. Valid only when `output` is absent; typed skill output
  // shares the sync-generate path and rejects `admissionId` in v1.
  admissionId?: string;
  requestContext?: RequestContextInput;
  tracingContext?: TracingContext;
  tracingOptions?: TracingOptions;
}
```

Core Harness v1 queue rows are signal-shaped work: serializable type, contents,
attachments, request context, attributes/metadata, and run-start overrides. The
HTTP queue body in §13.2 is the wire projection of this same DTO; it must not
accept fields that are absent from `QueueOptions` or from the persisted
`QueuedItem` row. Product controllers such as MastraCode may offer richer local
queued actions for slash commands, custom commands, or skill activations, but
those actions must be normalized before they enter the durable Harness boundary.
A durable product queued action stores only stable command identity, JSON-safe
args, persisted attachment refs, and an explicit target operation kind
(`signal`, `queue`, or `useSkill`); it must not store closures, component
handles, process-local callbacks, live prompt objects, or in-memory editor
state. At drain time the controller admits through `session.signal(...)`,
`session.queue(...)`, or `session.useSkill(...)` and records normal operation
evidence.
