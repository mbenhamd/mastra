### 10.3 Custom events

Tools call `requestContext.get('harness').emitCustomEvent(input)` to surface
tool-level signals (progress, partial results, telemetry). The harness validates
the input at call time and fills event and session identity fields before
dispatching. Rules:

- **Type must use a dotted prefix.** `myorg.tool.progress`, `acme.scan.matched`,
  etc. The leading segment should identify the publisher; the trailing segments
  are the publisher's choice.
- **Payload goes through `input.payload`** and must be JSON-serializable
  (`JsonValue` — see §6.1). The harness passes it to subscribers verbatim as the
  emitted event's `payload` field.
- **The harness fills in the event and session identity fields** (`id`,
  `sessionId`, `timestamp`, `resourceId`, `threadId`). Tools supply only `type`
  and optional `payload`. Parent-surfaced subagent copies also carry the
  attribution fields defined in §10.6.
- **Built-in types are rejected at call time.** The harness validates `type`
  against the built-in union (§10.2) — any exact match to a built-in event type or
  any type starting with a reserved internal-prefix family (`agent_`, `tool_`,
  `text_`, `message_`, `queue_`, `subagent_`, `state_`, `mode_`, `model_`,
  `session_`, `token_`, `channel_`, `goal_`, `attachment_`, `display_`,
  `storage_`, or the exact type `error`) throws `HarnessValidationError`. The
  `HarnessCustomEventInput` type (§6.1) enforces only the dotted shape
  structurally; reserved-name checks are runtime validation.

`emitCustomEvent(input)` is the only author-facing Harness v1 custom-event API.
The JavaScript input object may contain only `type` and optional `payload`;
extra top-level fields, including event identity or subagent attribution fields,
throw `HarnessValidationError`. Names such as `id`, `sessionId`, or `source`
inside `payload` remain nested JSON payload data and never override the
Harness-stamped event envelope. Raw built-in/internal event emitters remain
Harness-private and may be used only by Harness-owned tools or event adapters.
Current `writer.custom()` / `data-*` chunks are a separate agent/workflow stream
mechanism unless a v1 adapter explicitly projects them at the session event
boundary through this same validation and stamping path.

The emitted subscriber event has the `CustomEvent` shape from §10.2, intersected
with `HarnessEventBase`. Custom events go through the same ordering and replay
rules as built-in events. Subscribers should narrow by `type` and tolerate
unknown types (forward-compatibility).

**Plan-task event (TM-3 / TM-5).** Every MUTATING plan-task tool (§6.4) emits the
dotted custom event `papersflow.plan_task.updated` through this same
`emitCustomEvent` validation + stamping path when the live owner mutates its
plan tree (add / decompose / reparent / update / complete / status rollup —
§5.1k). It is a CUSTOM event — NOT a member of the closed built-in union in
§10.2 and NOT a reserved built-in-prefix family — so subscribers narrow by
`type` and tolerate it like any other custom event. The read-only
`plan_task_check` tool emits no event.

**Payload (TM-5).** The payload is:

```ts
{
  op: 'add' | 'decompose' | 'reparent' | 'update';
  affectedTaskIds: string[]; // op-meaningful order (e.g. [parent, ...children])
  deltas: Array<{
    taskId: string;
    parentTaskId?: string;            // present iff the node has a parent
    status: HarnessPlanTaskStatus;    // the COMMITTED post-status
    statusSource: 'explicit' | 'derived';
    order: number;
    content?: string;                 // present iff this op set content
  }>;
}
```

`deltas` carries one compact post-image entry for EVERY task the op CHANGED —
the directly-edited rows AND every ancestor whose DERIVED status flipped from the
rollup cascade (a `complete`/`update` that satisfies a parent's children emits a
delta for that parent with its rolled-up `status` + `statusSource: 'derived'`).
It is BOUNDED to the affected set (never the whole tree) and JSON-safe, so a UI
applies an incremental patch without re-reading the tree. `content` is omitted on
a pure-rollup delta to stay compact. The list/detail summary is on the
display-state snapshot (§4.2 / §5.1k); the bounded subtree read is
`plan_task_check`.
