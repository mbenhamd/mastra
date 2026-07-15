### 4.2f Required Agent Signal Boundary

`Session.signal(...)`, untyped `useSkill(...)`, and drained `queue(...)` rely
on an internal agent boundary. It is not exposed to remote clients, but an agent
implementation that cannot satisfy it cannot support Harness v1's independent
per-signal promises.

This boundary is an adapter contract over the agent's signal runtime, not a
second dispatcher. The v1 implementation must route active-run and idle-run
delivery through the same signal machinery that currently owns agent execution
(`AgentThreadStreamRuntime` in core), then add the missing durable admission and
result-correlation evidence around that path. Treat the TypeScript shape below
as an internal adapter requirement for the existing agent runtime; do not export
or implement it as a parallel Harness signal service. A separate Harness-only
signal queue, run loop, or terminal-result writer would be a duplicate runtime
and is not a valid implementation of this interface.

```ts
interface AgentSignalBoundary {
  sendSignal(input: AgentSignalBoundaryInput): Promise<AgentSignalAccepted>;
  getSignalResult(input: AgentSignalResultLookup): Promise<AgentSignalResultStatus>;
  subscribeSignalResults?(
    input: AgentSignalSubscription,
    listener: (event: AgentSignalTerminalEvent) => void,
  ): () => void;
}

interface AgentSignalBoundaryInput {
  harnessName: string;
  sessionId: string;
  resourceId: string;
  threadId: string;
  type: 'user-message' | 'system-reminder' | string;
  contents:
    | string
    | Array<{ type: 'text'; text: string } | { type: 'file'; attachmentId: string; mediaType: string; name?: string }>;
  attachments: PersistedAttachment[];
  attributes?: Record<string, JsonValue>;
  metadata?: Record<string, JsonValue>;
  providerOptions?: Record<string, JsonValue>;
  requestContext?: PersistedRequestContextInput;
  admissionId?: string;
  admissionHash?: string;
  source: { kind: 'signal' } | { kind: 'queue'; queuedItemId: string } | { kind: 'use-skill'; skillName: string };
  // Present only when the signal is intended to drain into an already-active
  // run. When absent, the agent starts a new run using the committed run
  // surface selected by the session owner: the effective mode resolves to a
  // stable `HarnessMode.agentId`, the effective opaque model ID resolves
  // through `HarnessConfig.resolveModel(...)`, and `currentRun.agentId` /
  // `modeId` / `modelId` are committed before the selected Agent is invoked
  // with the resolved model. The run-boundary overrides below (`model`,
  // `mode`, `yolo`) are populated only when `runId` is absent;
  // admission rejects `yolo: true` before a runId-bearing active signal reaches
  // this boundary.
  runId?: string;
  model?: string;
  mode?: string;
  yolo?: boolean;
  tracingContext?: TracingContext;
  tracingOptions?: TracingOptions;
}

interface AgentSignalAccepted {
  runId: string;
  signalId: string;
  duplicate: boolean;
}

interface AgentSignalResultLookup {
  harnessName: string;
  sessionId: string;
  resourceId: string;
  threadId: string;
  signalId: string;
}

interface AgentSignalSubscription {
  harnessName: string;
  sessionId: string;
  resourceId: string;
  threadId: string;
  signalIds?: string[];
}

type AgentSignalResultStatus =
  | { status: 'pending'; signalId: string; runId?: string }
  | { status: 'completed'; signalId: string; runId: string; result: AgentResult }
  | { status: 'failed'; signalId: string; runId?: string; error: { code: string; message: string } };

type AgentSignalTerminalEvent = Extract<AgentSignalResultStatus, { status: 'completed' | 'failed' }>;
```

Signal contents follow the same conceptual shape as current Mastra agent
signals: text parts and file/reference parts after attachment admission. The
public `SignalOptions` and `QueueOptions` use the same `contents` spelling; any
legacy `content` field is an import/controller compatibility concern that must
be normalized before this boundary. The boundary deliberately preserves `type`,
`attributes`, and `metadata` instead of
flattening all caller input into a string. MastraCode already relies on
`type: 'system-reminder'`, goal/judge metadata, and `attributes.delivery =
'while-active' | 'message'`; v1 keeps those semantics while moving admission to
the session-first `Session.signal(...)` contract.

`sendSignal(...)` is the post-acceptance durability boundary named in §5.7.
When `admissionId` is present, an exact duplicate with the same
`admissionHash` returns the original `{ runId, signalId, duplicate: true }`
without accepting a second signal. The same `admissionId` with a different hash
fails before new work is admitted and surfaces as `HarnessAdmissionConflictError`
at the session/API layer. `admissionHash` is computed by the session owner from the normalized operation
inputs defined in [§4.4b](../04-operation-option-types/02-queue-and-skill-options.md);
the agent boundary compares the hash, but does not invent a second hashing algorithm.
Durable tombstones and receipts: [§5.1d](../../05-session-persistence/01-what-gets-persisted/10-queue-admission-and-tombstones.md).

`getSignalResult(...)` and, when implemented, `subscribeSignalResults(...)`
report operation-scoped terminal status by `signalId`. A completed result is the
answer attributable to that accepted signal, not the whole run's aggregate
output; a failed result is terminal for that signal. Run-level `agent_end`,
run-level `error`, stream close, eviction, shutdown, and session lifecycle
events are inspection/display signals only and never settle a specific
`signalId`. If full result evidence has compacted while an
`OperationAdmissionTombstone` remains, Harness result routes return the `expired`
wire state from the tombstone (§13.3) rather than calling `sendSignal(...)`
again or treating the operation as pending.

The write side for terminal signal evidence is the retained result-correlation
record required by §5.1 and the recovery/lifecycle terminalization path in
§5.7. `AgentSignalBoundary` intentionally exposes admission, lookup, and
optional subscription; it does not define a separate public terminal-write
method, and forced failure writes must still update the same evidence projected
by `getSignalResult(...)`.

Current Mastra `AgentThreadStreamRuntime` signal support is the implementation
base for this boundary, but it is not sufficient by itself. Its active-run maps
and pending idle-signal queues are process-local, and its persisted signal
messages are history records, not operation-scoped terminal result evidence.
Current signal-message reconstruction helpers that decode old
`metadata.signal.contents` shapes are old-data import/read support inside the
current agent layer only. Harness v1 must not accept those old shapes at
the `AgentSignalBoundary`, silently coerce invalid signal contents, or use them
as a fallback for new v1 admission; v1 signal inputs are the normalized
`AgentSignalBoundaryInput` shape above after attachment admission.
The v1 rewrite must adapt that runtime with a durable result-correlation layer
that can answer by `(harnessName, sessionId, signalId)` after restart,
distinguish two same-text signals in the same run, return exact duplicate
admissions by `admissionId` / `admissionHash`, and terminalize accepted signals
even when live stream subscribers, PubSub messages, or the original process
disappear. The durable layer records evidence for the existing runtime; it does
not own an alternate path for active/idle signal delivery.
