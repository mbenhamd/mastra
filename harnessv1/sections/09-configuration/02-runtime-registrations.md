### 9.2 Runtime Registrations

```ts
interface HarnessMode {
  id: string;                         // Unique mode ID, e.g. 'build', 'plan'.
  name?: string;
  default?: boolean;                  // Optional bootstrap default. If none is set, modes[0] is the default.
  agentId: string;                    // References `HarnessConfig.agents`.
  defaultModelId?: string;            // Mode-level model bootstrap default before a session model is selected.
}

interface HarnessSubagent {
  id: string;                         // Stable `agentType` ID exposed by the built-in `subagent` tool.
  name: string;                       // Human-readable display name.
  description: string;                // Tool catalog description shown to the parent agent.
  instructions: AgentInstructions;    // Default instructions for this spawnable subagent type.
  tools?: ToolsetInput;               // Direct tools available to this subagent type.
  allowedHarnessTools?: string[];     // Subset of `HarnessConfig.tools` delegated to this subagent type.
  allowedWorkspaceTools?: string[];   // Optional workspace-tool allow-list after workspace tool wrapping.
  defaultModelId?: string;            // Subagent model bootstrap default before session override.
  maxSteps?: number;                  // Optional execution-loop step cap for this subagent type.
  workspace?: 'inherit' | 'fresh';    // Default: 'inherit'. Fresh requires a per-session workspace provider (§8).
}

interface BackgroundTaskExecutorRegistration {
  // v1 reconstructable background tasks execute only stable tool-kind
  // executors. `toolName` must resolve through the Mastra/Core tool registration
  // surface exposed to this Harness, not through a second Harness-owned task
  // framework. Every worker in the same runtime domain must resolve the same
  // stable id/generation before claiming a reconstructable row.
  kind: 'tool';
  toolName: string;
  generation?: string;                // Optional executor/schema compatibility token.
}

interface BackgroundTaskCompletionPolicyRegistration {
  // The registered policy implementation owns how a task result or failure is
  // projected into durable Harness/session state. It must commit that durable
  // state or enqueue outbox-producing work before task success is reported.
  generation?: string;                // Optional policy/metadata compatibility token.
  validateMetadata?: (metadata: JsonValue) => boolean; // Optional row-level metadata validator.
}

type ObservationalMemoryConfig =
  | boolean
  | {
      enabled?: boolean;              // `false` disables OM; omitted means enabled when object form is present.
      scope?: 'thread' | 'resource';  // Defaults to 'thread'. Creation-time lookup scope for OM records.
                                      // `resource` is an explicit privacy/authorization choice:
                                      // OM snapshots may summarize other threads for the same
                                      // authenticated resource. Existing sessions do not change
                                      // scope implicitly.
      model?: string;                 // Default model ID for both observer and reflector.
      observation?: {
        model?: string;               // Observer model ID.
        messageTokens?: number;       // Observation trigger threshold.
      };
      reflection?: {
        model?: string;               // Reflector model ID.
        observationTokens?: number;   // Reflection trigger threshold.
      };
      // Opaque adapter-owned OM processor options. Implementations may reject
      // unsupported keys before init/session persistence. These values never
      // define Harness v1 API, storage, route, event, display, or recovery
      // semantics, and non-JSON values are rejected.
      processorOptions?: Record<string, JsonValue>;
    };

interface HeartbeatHandler {
  id: string;
  intervalMs: number;                                 // Tick interval
  handler: () => void | Promise<void>;
  immediate?: boolean;                                // Fire once on registration. Default: false
  shutdown?: () => void | Promise<void>;              // Called when the heartbeat is removed
}

// Heartbeat handlers are process-local hooks. They are useful for local
// maintenance, telemetry, and demos, but they are not durable scheduler or
// channel-autonomy primitives. Restart-safe scheduled/proactive work uses
// `HarnessWakeupItem` rows (§5.1e, §14.6, §15), not heartbeat handlers.
// `registerHeartbeat(...)` validates non-empty `id` uniqueness, positive
// `intervalMs`, and a function-valued handler before registration; duplicate
// IDs are rejected instead of replacing a live handler. A given heartbeat ID
// never overlaps itself: if a tick is still running when the next tick would
// fire, the later tick is skipped. The unsubscribe function,
// `stopHeartbeats()`, and `harness.shutdown()` stop future ticks, await the
// in-flight handler for each affected heartbeat, and then await the optional
// `shutdown` hook. Missed or skipped heartbeat ticks are not persisted,
// retried, or replayed.

```
