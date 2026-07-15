### 4.1 Harness

Orientation diagram (surface families only; the TypeScript API below remains
authoritative):

<figure>
  <svg role="img" aria-labelledby="hx-harness-api-title hx-harness-api-desc" viewBox="0 0 1040 430" width="100%" style="max-width: 1100px; height: auto; display: block; margin: 1.5rem auto; background: #ffffff; border: 1px solid #e2e8f0; border-radius: 18px; padding: 16px; box-sizing: border-box;">
    <title id="hx-harness-api-title">Harness public API surface</title>
    <desc id="hx-harness-api-desc">The public Harness class exposes lifecycle, session resolution, catalogs, and local event subscriptions. Channel bridges, background diagnostics, workspace administration, and history helpers are internal or operator surfaces.</desc>
    <defs>
      <marker id="ah-harness-api" markerWidth="10" markerHeight="10" refX="8" refY="5" orient="auto">
        <path d="M0,0 L10,5 L0,10 Z" fill="#334155" />
      </marker>
    </defs>

    <rect style="fill: #eef2ff; stroke: #6366f1; stroke-width: 2.5; rx: 16;" x="405" y="25" width="230" height="72" />
    <text style="font: 600 18px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #0f172a;" x="520" y="55" text-anchor="middle">Harness</text>
    <text style="font: 500 14px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="520" y="78" text-anchor="middle">session front desk</text>

    <rect style="fill: #f8fafc; stroke: #94a3b8; stroke-width: 2; rx: 14;" x="55" y="150" width="200" height="68" />
    <text style="font: 600 18px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #0f172a;" x="155" y="178" text-anchor="middle">Lifecycle</text>
    <text style="font: 500 14px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="155" y="200" text-anchor="middle">init / shutdown</text>

    <rect style="fill: #f8fafc; stroke: #94a3b8; stroke-width: 2; rx: 14;" x="300" y="150" width="200" height="68" />
    <text style="font: 600 18px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #0f172a;" x="400" y="178" text-anchor="middle">Sessions</text>
    <text style="font: 500 14px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="400" y="200" text-anchor="middle">resolve / list</text>

    <rect style="fill: #fff7ed; stroke: #f97316; stroke-width: 2; rx: 14;" x="545" y="150" width="200" height="68" />
    <text style="font: 600 18px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #0f172a;" x="645" y="178" text-anchor="middle">Internal Bridges</text>
    <text style="font: 500 14px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="645" y="200" text-anchor="middle">not public API</text>

    <rect style="fill: #f8fafc; stroke: #94a3b8; stroke-width: 2; rx: 14;" x="790" y="150" width="200" height="68" />
    <text style="font: 600 18px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #0f172a;" x="890" y="178" text-anchor="middle">Operator History</text>
    <text style="font: 500 14px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="890" y="200" text-anchor="middle">not lifecycle</text>

    <rect style="fill: #f8fafc; stroke: #94a3b8; stroke-width: 2; rx: 14;" x="175" y="300" width="200" height="68" />
    <text style="font: 600 18px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #0f172a;" x="275" y="328" text-anchor="middle">Catalogs</text>
    <text style="font: 500 14px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="275" y="350" text-anchor="middle">modes / models / categories</text>

    <rect style="fill: #fff7ed; stroke: #f97316; stroke-width: 2; rx: 14;" x="420" y="300" width="200" height="68" />
    <text style="font: 600 18px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #0f172a;" x="520" y="328" text-anchor="middle">Events</text>
    <text style="font: 500 14px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="520" y="350" text-anchor="middle">harness subscriptions</text>

    <rect style="fill: #f8fafc; stroke: #94a3b8; stroke-width: 2; rx: 14;" x="665" y="300" width="200" height="68" />
    <text style="font: 600 18px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #0f172a;" x="765" y="328" text-anchor="middle">Diagnostics</text>
    <text style="font: 500 14px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="765" y="350" text-anchor="middle">operator-only</text>

    <path style="stroke: #334155; stroke-width: 2.2; fill: none; marker-end: url(#ah-harness-api);" d="M440 97 C340 120 210 125 165 149" />
    <path style="stroke: #334155; stroke-width: 2.2; fill: none; marker-end: url(#ah-harness-api);" d="M500 97 C455 120 420 130 405 149" />
    <path style="stroke: #334155; stroke-width: 2.2; fill: none; marker-end: url(#ah-harness-api);" d="M560 97 C605 120 635 130 640 149" />
    <path style="stroke: #334155; stroke-width: 2.2; fill: none; marker-end: url(#ah-harness-api);" d="M610 97 C720 120 850 125 885 149" />
    <path style="stroke: #334155; stroke-width: 2.2; fill: none; marker-end: url(#ah-harness-api);" d="M470 97 C350 170 285 235 275 299" />
    <path style="stroke: #334155; stroke-width: 2.2; fill: none; marker-end: url(#ah-harness-api);" d="M520 97 L520 299" />
    <path style="stroke: #334155; stroke-width: 2.2; fill: none; marker-end: url(#ah-harness-api);" d="M575 97 C690 170 755 235 765 299" />

  </svg>
  <figcaption>The public Harness class is a session-first control surface. Channel bridges, history helpers, background diagnostics, workspace administration, and force-delete operations live behind internal or operator boundaries rather than app-facing methods.</figcaption>
</figure>

```ts
class Harness<TState = Record<string, unknown>> {
  constructor(config: HarnessConfig<TState>);

  // Lifecycle
  init(): Promise<void>;
  shutdown(): Promise<void>;

  // Sessions — open a concrete session or bind one to a concrete backing thread.
  // See §5.
  //
  // `session` resolves a session from concrete identity, never from
  // "latest/current thread" selection:
  //   - by sessionId (must already exist in memory or storage)
  //   - by (threadId, resourceId)  — find/reopen the current owner, or create when none exists
  //   - by ({ fresh: true }, resourceId) — create a new backing thread and session
  //
  // The returned `Session` is always live in memory. Storage is consulted
  // transparently when the session isn't already hydrated. For a given
  // `(harnessName, resourceId, threadId)`, all callers attach to or reopen the
  // same current `SessionRecord`; v1 does not allow independent active session
  // records to share one thread. If the caller supplies `sessionId` for a
  // `(harnessName, resourceId, threadId)` pair already owned by a different
  // current session, resolution throws `HarnessSessionConflictError` instead of
  // creating a second record or silently switching IDs.
  //
  // All overloads enforce single-tenant scoping (see §2.3): if `resourceId` is
  // supplied, it is cross-checked against the stored record and a mismatch is
  // surfaced as `HarnessSessionNotFoundError` (sessions) or treated as
  // "doesn't exist" (threads). The ID-only overload is allowed for
  // single-tenant deployments only. Multi-tenant callers must pass `resourceId`;
  // product controllers that want "continue latest" semantics resolve the
  // concrete `sessionId` or `(threadId, resourceId)` before calling Harness.
  session(opts: { sessionId: string }): Promise<Session<TState>>;
  session(opts: { sessionId: string; resourceId: string }): Promise<Session<TState>>;
  session(opts: {
    threadId: string | { fresh: true };
    resourceId: string;
    sessionId?: string;
    parentSessionId?: string; // mark this session as a child of another
  }): Promise<Session<TState>>;

  listSessions(
    opts: {
      resourceId: string;
    } & ListSessionsOptions,
  ): Promise<ListPage<SessionListItem>>;

  // Catalogs
  listModes(): HarnessMode[];
  listAvailableModels(): Promise<AvailableModel[]>;
  getToolCategory(opts: { toolName: string }): ToolCategory | null;
  // Local/in-process convenience only. Returns the configured
  // `HarnessConfig.defaultResourceId`, or `undefined` when no default tenant is
  // configured. It does not infer from the registered `harnessName`, and it is
  // not a resource catalog or remote resource-authority surface (§13.2/§13.5).
  getDefaultResourceId(): string | undefined;
  // No mutable default-model setter: default model policy is immutable
  // HarnessConfig bootstrapping policy (§9). Runtime model changes are
  // session-owned.

  // Local runtime hooks. Process-local only; not a durable scheduler or
  // ordinary app-facing conversation API.
  registerHeartbeat(handler: HeartbeatHandler): () => Promise<void>; // returns async unsubscribe
  stopHeartbeats(): Promise<void>;

  // Local/in-process control-plane stream. Receives harness-scoped events plus
  // a live fan-out copy of every session-scoped event from every live Session
  // owned by this Harness instance, including child/subagent sessions. See §10
  // for delivery, ordering, and replay boundaries. Not exposed remotely (§13.5).
  subscribe(listener: HarnessListener): () => void;
}
```

Channel ingress/outbox helpers, force delete, thread import/history helpers,
workspace teardown, resource enumeration, ID-only destructive lifecycle helpers,
background-task diagnostics, and raw Mastra access are intentionally absent from
the public app-facing class above. Normal product lifecycle is
`Session.close()` / `Session.delete()`. ID-based close/delete helpers used by
server routes, single-tenant local runtimes, or operator/admin tools live behind
those explicit internal/operator boundaries and must always apply the §5.5
session-first semantics before mutating storage. Default SDKs and product app
code must not wrap them as ordinary Harness lifecycle methods.
