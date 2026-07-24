### 9.1 HarnessConfig

Orientation diagram (config bucket topology only; the TypeScript shape below
remains authoritative for required vs optional fields, defaults, and types):

<figure>
  <svg role="img" aria-labelledby="hx-harness-config-title hx-harness-config-desc" viewBox="0 0 1040 520" width="100%" style="max-width: 1100px; height: auto; display: block; margin: 1.5rem auto; background: #ffffff; border: 1px solid #e2e8f0; border-radius: 18px; padding: 16px; box-sizing: border-box;">
    <title id="hx-harness-config-title">HarnessConfig bucket topology</title>
    <desc id="hx-harness-config-desc">HarnessConfig groups required runtime wiring, runtime contract generations, per-session lifecycle policy, channel records, per-feature config, catalogs, durable workers, and pagination/state defaults.</desc>
    <defs>
      <marker id="ah-harness-config" markerWidth="10" markerHeight="10" refX="8" refY="5" orient="auto">
        <path d="M0,0 L10,5 L0,10 Z" fill="#334155" />
      </marker>
    </defs>

    <rect style="fill: #eef2ff; stroke: #6366f1; stroke-width: 2.5; rx: 16;" x="280" y="28" width="480" height="80" />
    <text style="font: 600 19px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #0f172a;" x="520" y="60" text-anchor="middle">HarnessConfig&lt;TState&gt;</text>
    <text style="font: 500 13px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="520" y="84" text-anchor="middle">bound to one registered harnessName by Mastra Server</text>

    <rect style="fill: #fff7ed; stroke: #f97316; stroke-width: 2; rx: 14;" x="780" y="28" width="240" height="80" />
    <text style="font: 600 15px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #0f172a;" x="900" y="56" text-anchor="middle">runtimeCompatibilityGeneration</text>
    <text style="font: 500 12px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="900" y="78" text-anchor="middle">opaque operator token;</text>
    <text style="font: 500 12px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="900" y="94" text-anchor="middle">guards run hydration on change</text>

    <rect style="fill: #ecfdf5; stroke: #22c55e; stroke-width: 2.2; rx: 14;" x="40" y="138" width="960" height="68" />
    <text style="font: 600 16px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #0f172a;" x="60" y="164">Required wiring</text>
    <text style="font: 500 13px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="60" y="186">agents · modes · resolveModel · storage (bound HarnessStorage view) · defaults for resourceId and modelId</text>

    <rect style="fill: #f8fafc; stroke: #94a3b8; stroke-width: 2; rx: 14;" x="40" y="234" width="230" height="100" />
    <text style="font: 600 15px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #0f172a;" x="155" y="262" text-anchor="middle">sessions</text>
    <text style="font: 500 12px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="155" y="284" text-anchor="middle">lease + lock mode</text>
    <text style="font: 500 12px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="155" y="302" text-anchor="middle">live cap, idle timeout, flush</text>
    <text style="font: 500 12px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="155" y="320" text-anchor="middle">queue + subagent depth · retention</text>

    <rect style="fill: #ecfeff; stroke: #06b6d4; stroke-width: 2; rx: 14;" x="280" y="234" width="230" height="100" />
    <text style="font: 600 15px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #0f172a;" x="395" y="262" text-anchor="middle">channels</text>
    <text style="font: 500 12px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="395" y="284" text-anchor="middle">Record&lt;channelId,</text>
    <text style="font: 500 12px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="395" y="302" text-anchor="middle">HarnessChannelConfig&gt;</text>
    <text style="font: 500 12px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="395" y="320" text-anchor="middle">→ §9.3</text>

    <rect style="fill: #f8fafc; stroke: #94a3b8; stroke-width: 2; rx: 14;" x="520" y="234" width="230" height="100" />
    <text style="font: 600 15px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #0f172a;" x="635" y="262" text-anchor="middle">per-feature config</text>
    <text style="font: 500 12px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="635" y="284" text-anchor="middle">workspace · goals</text>
    <text style="font: 500 12px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="635" y="302" text-anchor="middle">OM recovery (disabled)</text>
    <text style="font: 500 12px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="635" y="320" text-anchor="middle">files (upload caps)</text>

    <rect style="fill: #f8fafc; stroke: #94a3b8; stroke-width: 2; rx: 14;" x="760" y="234" width="240" height="100" />
    <text style="font: 600 15px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #0f172a;" x="880" y="262" text-anchor="middle">catalogs &amp; policy</text>
    <text style="font: 500 12px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="880" y="284" text-anchor="middle">skills · subagents</text>
    <text style="font: 500 12px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="880" y="302" text-anchor="middle">tools · toolCategoryResolver</text>
    <text style="font: 500 11px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="880" y="320" text-anchor="middle">initialPermissionRules + defaultPermissionPolicy</text>

    <rect style="fill: #fff7ed; stroke: #f97316; stroke-width: 2; rx: 14;" x="40" y="358" width="300" height="100" />
    <text style="font: 600 15px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #0f172a;" x="190" y="386" text-anchor="middle">durable workers</text>
    <text style="font: 500 12px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="190" y="408" text-anchor="middle">backgroundTasks (executors,</text>
    <text style="font: 500 12px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="190" y="426" text-anchor="middle">completionPolicies, claim TTL)</text>
    <text style="font: 500 12px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="190" y="444" text-anchor="middle">wakeups (poll, retry, missed-fire)</text>

    <rect style="fill: #f8fafc; stroke: #94a3b8; stroke-width: 2; rx: 14;" x="360" y="358" width="300" height="100" />
    <text style="font: 600 15px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #0f172a;" x="510" y="386" text-anchor="middle">lifecycle hooks</text>
    <text style="font: 500 12px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="510" y="408" text-anchor="middle">heartbeatHandlers registered via</text>
    <text style="font: 500 12px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="510" y="426" text-anchor="middle">registerHeartbeat (not durable)</text>

    <rect style="fill: #f8fafc; stroke: #94a3b8; stroke-width: 2; rx: 14;" x="680" y="358" width="320" height="100" />
    <text style="font: 600 15px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #0f172a;" x="840" y="386" text-anchor="middle">defaults &amp; state</text>
    <text style="font: 500 12px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="840" y="408" text-anchor="middle">lists (pagination defaults +</text>
    <text style="font: 500 12px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="840" y="426" text-anchor="middle">per-route caps)</text>
    <text style="font: 500 12px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="840" y="444" text-anchor="middle">initialState&lt;TState&gt;</text>

    <path style="stroke: #334155; stroke-width: 2; fill: none; marker-end: url(#ah-harness-config);" d="M520 108 L520 137" />
    <path style="stroke: #334155; stroke-width: 2; fill: none; marker-end: url(#ah-harness-config);" d="M380 206 L155 233" />
    <path style="stroke: #334155; stroke-width: 2; fill: none; marker-end: url(#ah-harness-config);" d="M480 206 L395 233" />
    <path style="stroke: #334155; stroke-width: 2; fill: none; marker-end: url(#ah-harness-config);" d="M580 206 L635 233" />
    <path style="stroke: #334155; stroke-width: 2; fill: none; marker-end: url(#ah-harness-config);" d="M680 206 L880 233" />
    <path style="stroke: #334155; stroke-width: 2; fill: none; marker-end: url(#ah-harness-config);" d="M380 206 L190 357" />
    <path style="stroke: #334155; stroke-width: 2; fill: none; marker-end: url(#ah-harness-config);" d="M500 206 L510 357" />
    <path style="stroke: #334155; stroke-width: 2; fill: none; marker-end: url(#ah-harness-config);" d="M620 206 L840 357" />

    <path style="stroke: #94a3b8; stroke-width: 1.8; fill: none; stroke-dasharray: 6 6; marker-end: url(#ah-harness-config);" d="M780 68 L760 68" />

    <rect style="fill: #f1f5f9; stroke: #cbd5e1; stroke-width: 1.5; stroke-dasharray: 5 5; rx: 12;" x="40" y="478" width="960" height="34" />
    <text style="font: 500 13px system-ui, -apple-system, BlinkMacSystemFont, Segoe UI, sans-serif; fill: #475569;" x="60" y="500">Validation, missing required fields, and configuration drift surface through §9.5 validation; per-feature owners (§4.7 goals, §2.7 workspace, §14 channels) govern semantics.</text>

  </svg>
  <figcaption>HarnessConfig groups required wiring, an opaque runtime contract generation, per-session lifecycle policy, channels, per-feature config, durable workers, hooks, and pagination/state defaults under one registered harness name.</figcaption>
</figure>

```ts
// Per-route pagination policy used by the `lists` config block below.
// `defaultLimit` is applied when callers omit `limit`; `maxLimit` is a hard
// validation cap, not a silent clamp.
interface ListLimitConfig {
  defaultLimit?: number;
  maxLimit?: number;
}

interface HarnessConfig<TState = Record<string, unknown>> {
  // Required
  agents: Record<string, Agent>; // Session-facing view of Mastra-registered agents keyed by ID.
  //   Harness does not become a second root registry:
  //   these entries must be the same Agent instances or
  //   stable references registered through Mastra. Harness
  //   may choose which registered agent a session/mode uses,
  //   but provider/tool/workflow/channel ownership stays
  //   with Mastra's root registries.
  modes: HarnessMode[]; // Available modes
  resolveModel: (modelId: string) => LanguageModel; // Model resolver
  storage: HarnessStorage; // Bound HarnessStorage view (§4.8/§5.2): session durability and a scoped composition facade over canonical MemoryStorage, ChannelsStorage, BackgroundTasksStorage, scheduler/wakeup, pubsub/replay, and blob owners.
  //   Ordinary Mastra registration resolves this from the configured
  //   storage adapter/domain composition (for example `getStore('harness')`
  //   plus channel/background/blob domain extensions) and binds it to the
  //   registered `harnessName`; callers must not construct a parallel
  //   standalone storage facade that bypasses Mastra's storage-domain owner.
  //   Sharing one physical adapter across multiple harnesses is valid only
  //   when the adapter enforces that namespace for session/thread/message
  //   rows, tombstones, attachments, channel rows, and wakeups. It composes
  //   with ChannelsStorage for provider installation/config and channel
  //   bridge ledgers, BackgroundTasksStorage for reconstructable task
  //   execution, WorkflowScheduler for schedule definitions, PubSub/cache
  //   for replay where possible, and BlobStore for bytes; it must not mirror
  //   those domains into second canonical stores.

  // Opaque operator-managed contract generation for the configured runtime
  // surface: agents and their prompts/tools, mode-to-agent bindings, model
  // named model targets, tool schemas, MCP bindings, workspace provider wiring, and wrappers
  // that affect run semantics. Harness does not derive or validate its contents.
  // Operators bump it when a change makes non-terminal persisted runs unsafe to
  // resume under the new runtime. All workers in one runtime domain must use the
  // same value. When set, run start snapshots it onto
  // `currentRun.runtimeCompatibilityGeneration`.
  // Hydrating an active run with a mismatched generation fails closed with
  // row `error.code = 'runtime_dependency_drifted'` (bare HarnessRowErrorCode
  // per §4.5d; wire projection per §13.3f.1 is `harness.runtime_drift`).
  runtimeCompatibilityGeneration?: string;

  // Sessions
  defaultResourceId?: string; // Optional local/in-process default tenant.
  //   When omitted, `getDefaultResourceId()` returns
  //   `undefined`; server routes never use this value
  //   to identify clients, because they derive
  //   `resourceId` from auth (§13.2).
  defaultModelId?: string; // Immutable default used only to bootstrap
  //   a SessionRecord with no selected model.
  //   Existing SessionRecord.modelId wins
  //   until session.switchModel(...) commits.
  //   Harness v1 exposes no mutable default
  //   model API or distributed config
  //   propagation surface.
  sessions?: {
    maxLive?: number; // Cap on hydrated sessions. Default: Infinity (no cap).
    idleTimeoutMs?: number; // Auto-evict after this idle period. Default: 2 * 60 * 60 * 1000 (2 hours).
    //   Sessions with a pending approval/suspension/question/plan
    //   are exempt from this check — see §5.4.
    flushDebounceMs?: number; // Debounce window for writing dirty state. Default: 500
    maxFlushFailures?: number; // Consecutive debounced-flush failures tolerated
    //   before the session goes into storage-error mode.
    //   Default: 5. See §5.7.
    closeTimeoutMs?: number; // Maximum time `closeSession` waits for live work
    //   across the whole session/subagent subtree after
    //   committing `closingAt`. Default: 30_000.
    eventBufferSize?: number; // Per-session ring buffer size for event replay
    //   on SSE reconnect (`Last-Event-ID`).
    //   Default: 1000. See §13.3.
    admissionReceiptRetentionMs?: number; // How long full terminal admission/result evidence remains
    //   available before it may compact to an
    //   `OperationAdmissionTombstone`. Default: 24 hours.
    admissionTombstoneRetentionMs?: number; // How long compact signal/queue admission tombstones remain
    //   available for duplicate admission conflict checks and
    //   `expired` result lookup. Must be greater than or equal
    //   to `admissionReceiptRetentionMs`. Default: 7 days.
    inboxResponseReceiptRetentionMs?: number; // How long inbox response receipts remain available
    //   for exact retry de-dupe after the pending item is
    //   consumed. Default: 24 hours.

    // Terminal source-specific rows such as channel inbox/action/outbox rows,
    // action tokens, wakeup items, and reconstructable background-task rows
    // have no Harness-managed TTL or sweeper in v1. They remain governed by
    // delete cleanup (§5.5) and deployment/source-specific retention policy
    // (§15.3); while retained, terminal rows stay excluded from worker claim
    // scans and point duplicate reads return their stored terminal state.

    maxSubagentDepth?: number; // Finite non-negative integer cap on
    //   descendant depth across the
    //   persisted `parentSessionId` chain.
    //   Default: 1 (direct children allowed).
    //   0 disables new child session creation
    //   through the built-in `subagent` tool,
    //   direct local resolution, and wire
    //   routes before mutation. Existing
    //   valid descendants stay addressable
    //   after a later cap decrease but cannot
    //   spawn further descendants beyond the
    //   current cap. See §8 and §13.2.

    maxQueueDepth?: number; // Cap on the active session/thread
    //   `SessionRecord.pendingQueue` length.
    //   When at the cap, `session.queue(...)` rejects
    //   with `HarnessQueueFullError` *before* mutating
    //   storage. The capacity check + durable append
    //   are atomic under the active session's write lease
    //   (§5.8). Default: Infinity (unbounded).
    //   Cap deliberately does *not* trigger
    //   `HarnessBusyError` — busy state is what queue
    //   exists for. See §3 and §5.7.

    // Write-concurrency — see §5.8.
    lockMode?: 'fail' | 'wait' | 'steal'; // Behavior when another instance owns the lease.
    //   Default: 'fail'. 'wait' is recommended for
    //   browser/SSE deployments. 'steal' is reserved
    //   for operator tools and tests; it is
    //   operator-only and must write a storage-owned
    //   operator audit record before the version bump
    //   commits. Not selectable by
    //   `RemoteSession`, channel ingress, recovery
    //   workers, or background tasks (§5.8).
    lockTtlMs?: number; // Lease TTL. The owner renews on every flush
    //   and on a `lockRenewMs` interval. After TTL
    //   without renewal the lease is reclaimable.
    //   Default: 30_000.
    lockRenewMs?: number; // Keep-alive interval for lease renewal even
    //   when no flush has happened. Default: 10_000.
    lockWaitMs?: number; // Maximum time `harness.session(...)` blocks
    //   when `lockMode = 'wait'` before throwing
    //   `HarnessSessionLockedError`. Default: 5_000.
    maxClockSkewMs?: number; // Required when adapter time is not
    //   storage-authoritative for session lease
    //   expiry comparisons. See §5.2 and §5.8.
  };

  // Read/list pagination. Route-specific values override the top-level
  // defaults. The top-level `ListLimitConfig` shape (`defaultLimit`,
  // `maxLimit`) applies when no per-route override is set; `defaultLimit`
  // defaults to 50 and `maxLimit` defaults to 200. `defaultLimit` is
  // applied when callers omit `limit`; `maxLimit` is a hard validation
  // cap, not a silent clamp. Storage adapters may enforce a lower backend
  // safety cap, but public route validation must fail before a scan when
  // the requested limit exceeds the configured maximum.
  lists?: ListLimitConfig & {
    sessions?: ListLimitConfig;
    threads?: ListLimitConfig;
    messages?: ListLimitConfig;
    activity?: ListLimitConfig;
    subagentInbox?: ListLimitConfig;
    channelDiagnostics?: ListLimitConfig;
    backgroundTasks?: ListLimitConfig;
  };

  // Skills
  skills?: HarnessSkill[]; // Code-registered skills (precedence over workspace-resolved skills)

  // Subagents — code-registered spawnable definitions for the built-in
  // `subagent` tool. The array supplies the `agentType` catalog and
  // per-definition instructions/tool/model defaults; it is not the depth
  // policy owner. Omit or leave empty to omit the built-in `subagent` tool.
  // Depth is governed independently by `sessions.maxSubagentDepth`.
  subagents?: HarnessSubagent[];

  // File attachments
  files?: {
    maxInlineBytes?: number; // Inline attachments larger than this are rejected.
    //   Default: 10 * 1024 * 1024 (10 MiB).
    maxUrlBytes?: number; // URL ingestion aborts once streamed stored bytes exceed this.
    //   Default: 50 * 1024 * 1024 (50 MiB).
    urlFetchTimeoutMs?: number; // End-to-end URL ingestion timeout. Default: 30_000.
    maxUrlRedirects?: number; // Redirect hop cap for URL ingestion. Default: 5.
    stagedAttachmentRetentionMs?: number; // Minimum time an unreferenced staged attachment
    //   (pre-uploaded or copied during failed URL
    //   ingestion) remains eligible for client retry before
    //   adapter/deployment garbage collection. Default: 24 hours.
    //   Attachments with any durable reference returned by
    //   §5.2 listAttachmentReferences(...) are not removed by
    //   this timer.
    allowPrivateNetworkUrls?: boolean; // Default false. When false, URL ingestion rejects
    //   loopback, link-local, private, multicast, reserved,
    //   and cloud metadata-service targets at every DNS
    //   resolution and redirect hop.
    allowedUrlMimeTypes?: string[]; // Optional allow-list of MIME types or type/* patterns
    //   for URL-ingested attachments. Even when omitted,
    //   declared, response, and sniffed MIME evidence must
    //   remain compatible before admission.
    //   Larger files must be pre-uploaded via the wire
    //   protocol's file route or supplied as `kind: 'url'`
    //   that the server can ingest into managed attachment
    //   storage before durable admission (see §13).
  };

  // Channels — per-harness bridges over Mastra-level channel providers. The
  // record key is the Harness `channelId`; by default it also names the
  // Mastra `channels[channelId]` provider used for bot identity, credentials,
  // provider routes, and platform delivery. See §14.
  channels?: Record<string, HarnessChannelConfig>;

  // Goals — see §4.7
  goals?: {
    defaultJudgeModel?: string; // Used when `setGoal({ judgeModel })` omits the field.
    //   No default — `setGoal` throws if the goal has no
    //   judge model and no default is configured.
    defaultMaxTurns?: number; // Default: 50
  };

  // Workspace — see §2.7 for ownership models and the provider contract.
  // Sugar: passing a `Workspace` is equivalent to `{ kind: 'shared', instance }`.
  // Sugar: passing a function is equivalent to `{ kind: 'per-session', provider:
  //   nonDurableProvider(fn) }` (resumable: false; an existing active session
  //   whose workspace was materialised fails closed after restart/eviction
  //   rather than receiving a replacement workspace).
  workspace?: HarnessWorkspaceConfig | Workspace | WorkspaceFactoryFn;

  // Observational Memory is not currently enabled through HarnessConfig.
  // `true` or any enabled object validates and then rejects; `false`, omission,
  // or `{ enabled: false }` keeps this surface disabled. Configure OM directly
  // on each Agent Memory instance instead. `session.om.clearOverride()` exists
  // only to recover old SessionRecords carrying unsupported persisted intent.
  observationalMemory?: ObservationalMemoryConfig;

  // Tooling
  tools?: ToolsetInput; // Available tools
  // Authoritative tool -> category mapping. The function form is primary:
  // categorization is a function of tool identity, and adapters may need to
  // inspect tool metadata, configured groups, or naming conventions rather
  // than enumerate every tool. Returning `null` means "no configured
  // category" and follows the §4.2e permission rules after create-time
  // `initialPermissionRules` seeding (per-tool rule -> category rule ->
  // `defaultPermissionPolicy` -> `ask`).
  toolCategoryResolver?: (toolName: string) => ToolCategory | null;
  // Optional rule seed copied into newly created SessionRecord.permissionRules.
  // Use this for product defaults that are not a single scalar policy, such as
  // MastraCode's read=allow and edit/execute/mcp=ask category defaults. This is
  // create-time bootstrap only; after the SessionRecord exists, the session's
  // own permissionRules/sessionGrants are canonical.
  initialPermissionRules?: PermissionRules;
  defaultPermissionPolicy?: PermissionPolicy; // Default approval behavior

  // Lifecycle hooks
  heartbeatHandlers?: HeartbeatHandler[]; // Registered at init via `registerHeartbeat`

  // Reconstructable background task workers — see §5.1, §5.2, §5.7, §15.
  // This registry is required only for rows whose durability is
  // `reconstructable`. Closure-backed `TaskContext` work that cannot be rebuilt
  // from these stable ids remains a diagnostic task behind an owning Harness
  // row. The configured ids and generations are process-local executable
  // registrations bridged from Mastra/Core task or tool registration surfaces;
  // persisted rows store only the stable refs. This is not a generic
  // Harness-owned task registry.
  backgroundTasks?: {
    executors?: Record<string, BackgroundTaskExecutorRegistration>;
    completionPolicies?: Record<string, BackgroundTaskCompletionPolicyRegistration>;
    maxAttempts?: number; // Default: 10
    claimTtlMs?: number; // Default: 30_000
    claimRenewMs?: number; // Default: claimTtlMs / 3
    maxClockSkewMs?: number; // Required when adapter time is not storage-authoritative
    batchSize?: number; // Default: 50
    pollIntervalMs?: number; // Default: 1_000 for built-in workers
    retryBackoffMs?: (attempt: number) => number; // Default: exponential with jitter
  };

  // Durable scheduled/proactive work — see §5.1, §5.2, §14.6, §15.
  // This config controls workers that claim `HarnessWakeupItem` rows. It does
  // not make `heartbeatHandlers` durable.
  wakeups?: {
    maxAttempts?: number; // Default: 10
    claimTtlMs?: number; // Default: 30_000
    claimRenewMs?: number; // Default: claimTtlMs / 3
    maxClockSkewMs?: number; // Required when adapter time is not storage-authoritative
    batchSize?: number; // Default: 50
    pollIntervalMs?: number; // Default: 1_000 for built-in workers
    retryBackoffMs?: (attempt: number) => number; // Default: exponential with jitter
    missedFirePolicy?: 'coalesce' | 'backfill' | 'skip'; // Default: 'coalesce'
  };

  // State
  initialState?: TState;
}
```

`runtimeCompatibilityGeneration` is Harness-owned. Mastra core registries carry
stable registry/config IDs (`packages/core/src/mastra/index.ts` stores agents,
tools, and workflows under a registration key, defaulting to the primitive id
when no custom key is supplied) and the scheduling domain carries optional
per-row ownership fields (`packages/core/src/storage/domains/schedules/base.ts`
records `ownerType`/`ownerId` for ownership/filter scoping). Harness needs one
operator-managed fence because it rehydrates active-run state (`currentRun`,
workflow snapshots, pending items, workspace references, and reconstructable
background-task references in §5.1b.2) under a stricter contract than core's
stateless per-call dispatch. When the config surface that produced the run state
changes in an incompatible way, the fence prevents silent resumption with
drifted semantics. When the token is set in config but absent on a persisted run
snapshot, the fail-closed check falls back to ID-only validation per §5.7c:
persisted run state references stable registry IDs, with
`nonRehydratableToolSurface` as the safety valve for ephemeral surfaces. V1 does
not introduce a second generation-token layer; any later core-owned fence would
replace or explicitly redefine this single Harness check instead of layering an
adapter around it.
