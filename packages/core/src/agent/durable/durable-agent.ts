import type { MastraServerCache } from '../../cache/base';
import { InMemoryServerCache } from '../../cache/inmemory';
import { MastraError, ErrorDomain, ErrorCategory } from '../../error';
import { CachingPubSub } from '../../events/caching-pubsub';
import { EventEmitterPubSub } from '../../events/event-emitter';
import { isLeaseProvider, NoopLeaseProvider } from '../../events/pubsub';
import type { LeaseProvider, PubSub } from '../../events/pubsub';
import { isRunLocalTopic } from '../../events/topics';
import { validateMaxSteps } from '../../llm/model/max-steps';
import type { MastraLanguageModel } from '../../llm/model/shared.types';
import { createTimeoutAbortSignal } from '../../loop/timeout';
import type { Mastra } from '../../mastra';
import { createObservabilityContext, getOrCreateSpan, SpanType, EntityType } from '../../observability';
import {
  MASTRA_AUTH_TOKEN_KEY,
  MASTRA_RESOURCE_ID_KEY,
  MASTRA_THREAD_ID_KEY,
  MASTRA_VERSIONS_KEY,
  RequestContext,
  isInfrastructureRequestContextKey,
  mergeVersionOverrides,
} from '../../request-context';
import type { VersionOverrides } from '../../request-context';
import type { DeclaredAgentSchedule } from '../../schedules/define';
import { toStandardSchema } from '../../schema';
import type { WorkflowsStorage } from '../../storage';
import type { FullOutput, MastraModelOutput } from '../../stream/base/output';
import type { ChunkType, MastraOnFinishCallback, MastraStreamTransformOptions } from '../../stream/types';
import { ChunkFrom } from '../../stream/types';
import { deepMerge } from '../../utils';
import type {
  ShouldPersistSnapshotFn,
  WorkflowFinishCallbackResult,
  WorkflowRunState,
  WorkflowRunStatus,
} from '../../workflows/types';
import { Agent } from '../agent';
import type { AgentListSuspendedRunsOptions, AgentListSuspendedRunsResult, AgentRunToolCall } from '../agent';
import type { AgentExecutionOptions, AgentExecutionOptionsBase } from '../agent.types';
import type { ResolvedAgentMemory } from '../execution-memory';
import { snapshotAgentExecutionOptions, snapshotAgentExecutionValue } from '../execution-snapshot';
import { beginGoalActivity, stopGoalActivity } from '../goal';
import { mergeAgentExecutionOptions } from '../merge-execution-options';
import { MessageList } from '../message-list';
import type { MessageListInput } from '../message-list';
import { stableStringify } from '../message-list/cache/stable-stringify';
import { SaveQueueManager } from '../save-queue';
import { AgentThreadLeaseConflictError, agentThreadStreamRuntime } from '../thread-stream-runtime';
import type { AgentThreadRunRegistration } from '../thread-stream-runtime';
import type {
  AgentAbortThreadOptions,
  AgentMemoryOption,
  AgentModelManagerConfig,
  StructuredOutputOptions,
  ToolsInput,
} from '../types';
import { isSupportedLanguageModel } from '../utils';

import { publishAbortRequest } from './abort-transport';
import { AGENT_CONTROL_TOPIC, AGENT_STREAM_TOPIC, DurableStepIds } from './constants';
import {
  DURABLE_STREAM_UNTIL_IDLE_HANDOFF,
  runDurableStreamUntilIdle,
  runResumeDurableStreamUntilIdle,
} from './durable-stream-until-idle';
import type { DurableStreamUntilIdleHandoff } from './durable-stream-until-idle';
import { prepareForDurableExecution } from './preparation';
import type { PreparationResult } from './preparation';
import {
  clearPinnedRunRegistryEntry,
  deleteBoundRunRegistryEntry,
  endRunSpansWithError,
  ExtendedRunRegistry,
  getBoundRunRegistryEntry,
  getGlobalRunRegistryEntry,
  globalRunRegistry,
  pinGlobalRunRegistryEntry,
  registerGlobalRunRegistryEntry,
  unpinGlobalRunRegistryEntry,
} from './run-registry';
import { createDurableAgentStream, emitChunkEvent, emitErrorEvent, emitFinishEvent } from './stream-adapter';
import type { DurableAgentStreamResult as DurableStreamAdapterResult } from './stream-adapter';
import type {
  AgentStepFinishEventData,
  AgentSuspendedEventData,
  DurableAgenticWorkflowInput,
  RunRegistryEntry,
  SerializableModelListEntry,
} from './types';
import {
  createRuntimeDependencyFingerprint,
  serializeModelConfig,
  serializeModelList,
  serializeToolsMetadata,
} from './utils/serialize-state';
import { assertDurableToolHookPolicyAvailable } from './utils/tool-hook-policy';
import { createDurableAgenticWorkflow } from './workflows';
import { MAP_FINAL_OUTPUT_STEP_ID } from './workflows/durable-loop-builder';

/** Exact recovery execution owned by a returned handle; never resolved again through a reusable run ID. */
const RECOVERY_WORKFLOW_EXECUTION = Symbol('mastra.durable.recoveryWorkflowExecution');
const pendingDurableRunIds = new Set<string>();
const RESOLVED_EXECUTION_OPTIONS = Symbol('mastra.durable.resolvedExecutionOptions');
/**
 * Marker stamped by `resume({ untilIdle })` onto the resolved-defaults object
 * it hands the idle wrapper (the `_resolvedDefaultOptions` lane) after it has
 * fully FGA-authorized the public call. The wrapper's inner `resume()` for the
 * SAME runId consumes it to skip only the duplicate provider authorization —
 * every other fail-closed gate (principal check, request-context validation,
 * owner verification) still runs. The key is a module-local Symbol, so a
 * public caller who forges a plain `_resolvedDefaultOptions` object can never
 * attach it, and a marker minted for one run never authorizes another.
 */
const RESUME_PREFLIGHT_AUTHORIZED = Symbol('mastra.durable.resumePreflightAuthorized');
const RECOVERY_LEASE_TTL_MS = 30_000;
const RECOVERY_LEASE_RENEW_INTERVAL_MS = 10_000;
const localRecoveryClaims = new Map<string, string>();

interface RecoveryLease {
  assertOwned(): void;
  getLossError(): MastraError | undefined;
  waitForLoss(): Promise<MastraError>;
  release(): Promise<void>;
}

interface RehydratedRecoveryState {
  requestContext: RequestContext;
  threadId?: string;
  resourceId?: string;
  messageList: MessageList;
  recoverAgentSpan: any;
  registryEntry: RunRegistryEntry;
}

type RecoveryRaceResult<T> = { kind: 'result'; value: T } | { kind: 'lease-lost'; error: MastraError };

/** Bind freshly resolved fallback models to the ids persisted for the run. */
function rebindRecoveredModelList(
  persisted: SerializableModelListEntry[],
  enabledLive: AgentModelManagerConfig[],
): AgentModelManagerConfig[] | undefined {
  const pool = [...enabledLive];
  const bound = persisted.map(entry => {
    const index = pool.findIndex(live => live.id === entry.id);
    return index === -1 ? undefined : pool.splice(index, 1)[0];
  });
  const identityMatches = bound.filter(Boolean).length;

  // Generated ids cannot match across resolutions. Positional binding is safe
  // only when every unmatched entry on both sides is explicitly known to have
  // a generated id. Equal cardinality alone must never turn an explicit-id
  // rename into a valid recovery binding.
  const unmatchedPersisted = persisted.filter((_entry, index) => !bound[index]);
  if (
    unmatchedPersisted.length === pool.length &&
    unmatchedPersisted.every(entry => entry.generatedId === true) &&
    pool.every(entry => entry.generatedId === true)
  ) {
    for (let index = 0; index < bound.length; index++) {
      bound[index] ??= pool.shift();
    }
  }

  const modelList = persisted.flatMap((entry, index) => {
    const live = bound[index];
    return live ? [{ ...live, id: entry.id, enabled: true }] : [];
  });
  return modelList.length ? modelList : undefined;
}

/** Restore the persisted fallback order while retaining freshly resolved model instances. */
function rebindRecoveredExecutionModels(
  persistedModelList: SerializableModelListEntry[] | undefined,
  resolvedModels: Awaited<ReturnType<Agent['__resolveExecutionModels']>>,
): {
  model: Awaited<ReturnType<Agent['__resolveExecutionModels']>>['model'];
  modelList: AgentModelManagerConfig[] | undefined;
} {
  const enabledResolvedModels = (resolvedModels.modelList ?? []).filter(entry => entry.enabled !== false);
  const modelList = persistedModelList?.length
    ? rebindRecoveredModelList(persistedModelList, enabledResolvedModels)
    : enabledResolvedModels.length
      ? enabledResolvedModels
      : undefined;

  // A persisted fallback list owns execution order. Select its rebound first
  // model only when every persisted entry was recovered; the exact list fence
  // below will reject partial or drifted recovery before execution continues.
  const model =
    persistedModelList?.length && modelList?.length === persistedModelList.length
      ? modelList[0]!.model
      : resolvedModels.model;
  return { model, modelList };
}

/** Merge caller context with registered run state while preserving run-owned memory coordinates. */
function mergeRegisteredRunRequestContext(
  caller: RequestContext | undefined,
  registered: RequestContext | undefined,
  durableApplicationKeys: readonly string[],
): RequestContext | undefined {
  const merged = caller ?? registered;
  if (!merged) return undefined;

  if (caller && registered && caller !== registered) {
    const allowedApplicationKeys = new Set(durableApplicationKeys);
    for (const [key, value] of registered.entries()) {
      if (
        key !== 'MastraMemory' &&
        allowedApplicationKeys.has(key) &&
        !isInfrastructureRequestContextKey(key) &&
        !caller.has(key)
      ) {
        caller.set(key, value);
      }
    }
  }
  if (registered?.has('MastraMemory')) {
    merged.set('MastraMemory', registered.get('MastraMemory'));
  } else {
    merged.delete('MastraMemory');
  }
  return merged;
}

/**
 * How many candidate `running` rows `listActiveRuns()` fetches from storage
 * per batch. Bounds peak memory to one batch of hydrated snapshots instead of
 * every matching row's snapshot at once (#21501).
 */
const LIST_ACTIVE_RUNS_STORAGE_BATCH_SIZE = 100;

/**
 * Options for DurableAgent.stream()
 */
export interface DurableAgentStreamOptions<OUTPUT = undefined> {
  /** Signal chunks to hide from this caller's stream. Does not affect generated results. */
  hideSignals?: AgentExecutionOptions<OUTPUT>['hideSignals'];
  /** Custom instructions that override the agent's default instructions for this execution */
  instructions?: AgentExecutionOptions<OUTPUT>['instructions'];
  /** Additional context messages to provide to the agent */
  context?: AgentExecutionOptions<OUTPUT>['context'];
  /** Native identity for the admitted input and the response segments it owns. */
  logicalMessageIdentity?: AgentExecutionOptions<OUTPUT>['logicalMessageIdentity'];
  /** Memory configuration for conversation persistence and retrieval */
  memory?: AgentExecutionOptions<OUTPUT>['memory'];
  /** Unique identifier for this execution run */
  runId?: string;
  /** Request Context containing dynamic configuration and state */
  requestContext?: AgentExecutionOptions<OUTPUT>['requestContext'];
  /** Maximum number of steps to run */
  maxSteps?: number;
  /**
   * Conditions for stopping execution (e.g., step count, token limit).
   *
   * The predicate is non-serializable, so it's parked on the in-process run
   * registry and evaluated by the durable loop on every iteration. Cross-process
   * durable engines (e.g. Inngest after a worker restart) cannot recover the
   * closure and degrade to `maxSteps` only.
   */
  stopWhen?: AgentExecutionOptions<OUTPUT>['stopWhen'];
  /** Additional tool sets that can be used for this execution */
  toolsets?: AgentExecutionOptions<OUTPUT>['toolsets'];
  /** Whether toolsets augment (default) or replace every other agent tool source */
  toolsetsMode?: AgentExecutionOptions<OUTPUT>['toolsetsMode'];
  /** Client-side tools available during execution */
  clientTools?: AgentExecutionOptions<OUTPUT>['clientTools'];
  /**
   * Per-execution tool hooks. These closures work while the original durable
   * run registry is available. Cold/cross-process execution fails closed;
   * configure security-enforcing hooks on the wrapped Agent when they must be
   * reconstructible on another worker.
   */
  hooks?: AgentExecutionOptions<OUTPUT>['hooks'];
  /** Tool selection strategy */
  toolChoice?: AgentExecutionOptions<OUTPUT>['toolChoice'];
  /** Tool names enabled for this execution */
  activeTools?: AgentExecutionOptions<OUTPUT>['activeTools'];
  /** Model-specific settings like temperature */
  modelSettings?: AgentExecutionOptions<OUTPUT>['modelSettings'];
  /** Provider-specific options forwarded to the model (serialized into the durable workflow input) */
  providerOptions?: AgentExecutionOptions<OUTPUT>['providerOptions'];
  /** Require approval for tool calls. Boolean (gate all / none) or a per-call function policy. */
  requireToolApproval?: AgentExecutionOptions<OUTPUT>['requireToolApproval'];
  /** Automatically resume suspended tools */
  autoResumeSuspendedTools?: boolean;
  /** Maximum number of tool calls to execute concurrently, or an object with `limit`/`strategy` */
  toolCallConcurrency?: AgentExecutionOptions<OUTPUT>['toolCallConcurrency'];
  /** Whether to include raw chunks in the stream output */
  includeRawChunks?: boolean;
  /** Experimental transforms applied whenever `fullStream` is consumed. */
  experimentalTransform?: MastraStreamTransformOptions<OUTPUT>;
  /** Maximum processor retries */
  maxProcessorRetries?: number;
  /** Structured output configuration */
  structuredOutput?: AgentExecutionOptions<OUTPUT>['structuredOutput'];
  /** Whether to return detailed scoring data in the response */
  returnScorerData?: boolean;
  /** Version overrides for sub-agent delegation */
  versions?: AgentExecutionOptions<OUTPUT>['versions'];
  /** Callback when chunk is received */
  onChunk?: (chunk: ChunkType<OUTPUT>) => void | Promise<void>;
  /** Callback when step finishes */
  onStepFinish?: (result: AgentStepFinishEventData) => void | Promise<void>;
  /** Callback when execution finishes — receives rich step data (text, steps, toolResults) */
  onFinish?: MastraOnFinishCallback<OUTPUT>;
  /** Callback on error */
  onError?: ({ error }: { error: Error | string }) => void | Promise<void>;
  /** Callback when workflow suspends (e.g., for tool approval) */
  onSuspended?: (data: AgentSuspendedEventData) => void | Promise<void>;
  /** Callback when execution is aborted via abortSignal */
  onAbort?: AgentExecutionOptions<OUTPUT>['onAbort'];
  /** Callback fired after each agentic-loop iteration */
  onIterationComplete?: AgentExecutionOptions<OUTPUT>['onIterationComplete'];
  /** Additional system message appended after context but before user messages. */
  system?: AgentExecutionOptions<OUTPUT>['system'];
  /** When true, background tasks are disabled for this run. */
  disableBackgroundTasks?: AgentExecutionOptions<OUTPUT>['disableBackgroundTasks'];
  /** Execution-scoped background dispatch policy for delegated agents. */
  backgroundTaskPolicy?: AgentExecutionOptions<OUTPUT>['backgroundTaskPolicy'];
  /** Tracing options forwarded to the agent/model spans. */
  tracingOptions?: AgentExecutionOptions<OUTPUT>['tracingOptions'];
  /** Per-call actor signal forwarded to FGA checks and tool execution. */
  actor?: AgentExecutionOptions<OUTPUT>['actor'];
  /** MCP protocol context forwarded to tools for in-process durable runs. */
  mcp?: AgentExecutionOptions<OUTPUT>['mcp'];
  /**
   * Per-invocation tool payload transform policy. The closure rides on the
   * in-process run registry; only the JSON-safe `targets` shadow is serialized
   * for cross-process engines.
   */
  transform?: AgentExecutionOptions<OUTPUT>['transform'];
  /**
   * Per-step preparation hook. Closure-only: stored on the in-process run
   * registry and invoked as a `PrepareStepProcessor` at the start of every
   * iteration. Cross-process resumes lose the hook.
   */
  prepareStep?: AgentExecutionOptions<OUTPUT>['prepareStep'];
  /**
   * Per-call `isTaskComplete` policy. Scorer instances and `onComplete` are
   * closure-only and live on the in-process run registry; the JSON-safe
   * primitives (`strategy`, `timeout`, `parallel`, `suppressFeedback`,
   * `scorerNames`) are serialized for cross-process observability.
   */
  isTaskComplete?: AgentExecutionOptions<OUTPUT>['isTaskComplete'];
  /**
   * Sub-agent delegation hooks (`onDelegationStart`, `onDelegationComplete`,
   * `messageFilter`, etc.). The callbacks are forwarded into `convertTools`
   * at prepare time and burned into the sub-agent `CoreTool` wrappers on the
   * in-process run registry. Cross-process resumes lose the callbacks (only
   * `includeSubAgentToolResultsInModelContext` would be JSON-safe), so a
   * fresh worker degrades to default delegation behaviour.
   */
  delegation?: AgentExecutionOptions<OUTPUT>['delegation'];
  /**
   * When set, `stream()` delegates to the idle-loop wrapper that keeps the
   * outer stream open across background-task continuations — the same
   * behaviour as the now-deprecated `streamUntilIdle()`.
   *
   * Pass `true` for default idle timeout (5 min), or `{ maxIdleMs }` to
   * customise.
   *
   * @example
   * ```typescript
   * const { output, cleanup } = await durableAgent.stream('Research topic', {
   *   untilIdle: true,
   *   memory: { thread: 't1', resource: 'u1' },
   * });
   * ```
   */
  untilIdle?: boolean | { maxIdleMs?: number };
  /** When true, the in-loop background task check step skips waiting (streamUntilIdle sets this) */
  _skipBgTaskWait?: boolean;
  /**
   * @internal Pre-resolved dynamic defaults carried by the until-idle wrapper
   * into inner resume segments so `getDefaultOptions` resolves exactly once
   * per public call.
   */
  _resolvedDefaultOptions?: AgentExecutionOptions<OUTPUT>;
  /**
   * External abort signal. The durable agent always installs its own internal
   * `AbortController` for the run; when this signal is provided, its `abort`
   * event is forwarded to the internal controller so either source can cancel
   * the run.
   *
   * Cross-process resumes (e.g. Inngest after a worker restart) cannot
   * recover the signal — call `resume(runId, ..., { abortSignal })` with a
   * fresh signal on each segment if you need abortability post-resume.
   */
  abortSignal?: AbortSignal;
  /**
   * Whether this caller's stream closes when the run suspends (e.g. for tool
   * approval). Defaults to `false`: the stream stays open across suspension so
   * a later resume can continue streaming on this same reader.
   *
   * Set to `true` so `fullStream`, `text`, and `getFullOutput()` resolve at the
   * suspension boundary (matching non-durable `Agent.stream()`), letting callers
   * such as AG-UI or A2A react instead of hanging. Resume the run with
   * `resumeStream()`/`resume()` — those always return a fresh stream.
   */
  closeOnSuspend?: boolean;
}

/**
 * Runtime callbacks plus caller identity used to resume a durable segment.
 * Tool hooks are fixed when the run starts and cannot be added or replaced on
 * resume.
 */
export interface DurableAgentResumeOptions<OUTPUT = undefined> extends Omit<
  DurableAgentStreamOptions<OUTPUT>,
  'hooks'
> {
  /** Exact workflow resume label for the suspended tool call. */
  toolCallId?: string;
}

/**
 * Result from DurableAgent.stream()
 */
export interface DurableAgentStreamResult<OUTPUT = undefined> {
  /** The streaming output */
  output: MastraModelOutput<OUTPUT>;
  /** The full stream - delegates to output.fullStream for server compatibility */
  readonly fullStream: ReadableStream<any>;
  /** The unique run ID for this execution */
  runId: string;
  /** Thread ID if using memory */
  threadId?: string;
  /** Resource ID if using memory */
  resourceId?: string;
  /** Cleanup function to call when done (unsubscribes from pubsub) */
  cleanup: () => void;
  /**
   * Abort the run. Flips the internal `AbortController` for this run, which
   * surfaces as an `AbortError` inside the durable LLM-execution step and
   * is bridged to the user's `onAbort` callback via the run's pubsub topic.
   *
   * Safe to call after the run has already finished — it's a no-op in that
   * case.
   *
   * Also publishes an abort request over pubsub so the abort reaches the
   * process executing the run, which in a load-balanced deployment is usually
   * not this one. That process flips its own controller and unwinds normally;
   * the workflow run is never hard-cancelled, so the terminal `finish` event
   * still reaches stream consumers. Await the returned promise to confirm the
   * request was dispatched; it rejects when binding or transport state cannot
   * provide that guarantee.
   */
  abort: (reason?: unknown) => Promise<void>;
}

/** Public, capability-safe result from DurableAgent.prepare(). */
export interface DurableAgentPreparationResult {
  runId: string;
  messageId: string;
  workflowInput: DurableAgenticWorkflowInput;
  threadId?: string;
  resourceId?: string;
  /** Releases an abandoned one-time handoff without exposing its runtime bindings. */
  cleanup: () => void;
}

/**
 * Configuration for DurableAgent - wraps an existing Agent with durable execution
 */
export interface DurableAgentConfig<
  TAgentId extends string = string,
  TTools extends ToolsInput = ToolsInput,
  TOutput = undefined,
> {
  /**
   * The Agent to wrap with durable execution capabilities.
   * All agent methods (getModel, listTools, etc.) delegate to this agent.
   */
  agent: Agent<TAgentId, TTools, TOutput>;

  /**
   * Optional ID override. Defaults to agent.id.
   */
  id?: TAgentId;

  /**
   * Optional name override. Defaults to agent.name.
   */
  name?: string;

  /**
   * PubSub instance for streaming events.
   * Optional - if not provided, defaults to EventEmitterPubSub.
   */
  pubsub?: PubSub;

  /**
   * Cache instance for storing stream events.
   * Enables resumable streams - clients can disconnect and reconnect
   * without missing events.
   *
   * - If not provided: Inherits from Mastra instance, or uses InMemoryServerCache
   * - If provided: Uses the provided cache backend (e.g., Redis)
   * - If set to `false`: Disables caching (streams are not resumable)
   */
  cache?: MastraServerCache | false;

  /**
   * Maximum steps for the agentic loop.
   * Defaults to the workflow default if not specified.
   */
  maxSteps?: number;

  /**
   * Timeout in milliseconds before automatic cleanup of registry entries
   * after a stream finishes or errors. This provides a grace period for
   * late observers to access the stream.
   *
   * Defaults to 30000 (30 seconds).
   * Set to 0 to disable auto-cleanup (manual cleanup() required).
   */
  cleanupTimeoutMs?: number;

  /**
   * Explicit non-secret application RequestContext keys that may be persisted
   * in durable workflow input. Infrastructure keys and raw credentials are
   * always forbidden. Persist stable IDs/references and re-resolve credentials
   * from a trusted provider after restart.
   */
  durableRequestContextKeys?: readonly string[];

  /**
   * Overrides the snapshot-persistence policy for this agent's durable
   * workflows. Returning false for a status skips that snapshot write.
   *
   * The default policy always persists `pending | paused | suspended`
   * (required for human-in-the-loop `resume()`), and persists `running`
   * only when the Mastra instance is configured with
   * `recovery: { durableAgents: 'auto' }` — `running` checkpoints exist
   * solely so `listActiveRuns()` / `recover()` / `recoverActiveRuns()` can
   * see in-flight runs after a crash.
   *
   * Footguns when providing a custom predicate:
   * - Excluding `paused` / `suspended` breaks human-in-the-loop resume.
   * - Excluding `running` makes the agent invisible to crash recovery.
   * A guardrail warning is logged for both cases (detected by probing the
   * predicate with an empty `stepResults`, so predicates that read
   * `stepResults` may probe inaccurately — the warning is best-effort).
   *
   * EventedAgent and InngestAgent own their persistence policy and ignore
   * this option with a warning.
   */
  shouldPersistSnapshot?: ShouldPersistSnapshotFn;

  /**
   * Per-topic opt-out of the replay cache.
   *
   * Return `false` to publish a topic straight through to the underlying
   * PubSub without recording it in the cache. Subscribers of that topic then
   * receive live events only and cannot resume from an offset. Use this to
   * trade replay for minimum publish latency on hot topics when the cache is
   * remote (e.g. cross-region Redis). Run-local topics are always excluded,
   * regardless of this option.
   */
  shouldCache?: (topic: string) => boolean;
}

/**
 * DurableAgent wraps an existing Agent with durable execution capabilities.
 *
 * Key features:
 * 1. Resumable streams - clients can disconnect and reconnect without missing events
 * 2. Serializable workflow inputs - works with durable execution engines
 * 3. PubSub-based streaming - events flow through pubsub for distribution
 *
 * DurableAgent extends Agent, delegating most methods to the wrapped agent.
 * It overrides stream() to use durable execution with the agentic workflow.
 *
 * Subclasses (EventedAgent, InngestAgent) override executeWorkflow() to
 * customize how the workflow is executed.
 *
 * @example
 * ```typescript
 * import { Agent } from '@mastra/core/agent';
 * import { DurableAgent } from '@mastra/core/agent/durable';
 *
 * const agent = new Agent({
 *   id: 'my-agent',
 *   instructions: 'You are a helpful assistant',
 *   model: openai('gpt-4'),
 * });
 *
 * const durableAgent = new DurableAgent({ agent });
 *
 * const { output, runId, cleanup } = await durableAgent.stream('Hello!');
 * const text = await output.text;
 * cleanup();
 * ```
 */

/**
 * Statuses of durable agent runs discoverable via {@link DurableAgent.listActiveRuns}.
 *
 * `running` is the status reported by the workflow engine while the durable
 * agent's agentic loop is actively executing (i.e. between suspend
 * boundaries). Persisted `running` snapshots are the recovery source for runs
 * orphaned by a process restart.
 */
export type DurableAgentActiveRunStatus = Extract<WorkflowRunStatus, 'running'>;

/**
 * Filters for {@link DurableAgent.listActiveRuns}. Mirrors the
 * `listWorkflowRuns` filter contract, plus the agent-level `threadId` /
 * `resourceId` filters used by the base {@link Agent.listSuspendedRuns}.
 */
export interface DurableAgentListActiveRunsOptions {
  /** Only return runs that belong to this memory thread. */
  threadId?: string;
  /** Only return runs that belong to this memory resource. */
  resourceId?: string;
  /** Only return runs created at or after this date. */
  fromDate?: Date;
  /** Only return runs created at or before this date. */
  toDate?: Date;
  /**
   * Number of items per page. Pagination is applied when both `perPage` and
   * `page` are provided; otherwise all matching runs are returned.
   */
  perPage?: number;
  /** Zero-indexed page number. */
  page?: number;
}

/**
 * A durable agent run currently reported as `running` in workflow snapshot
 * storage. These are the runs that a boot-time or operator-initiated
 * recovery would re-drive after a process restart.
 */
export interface DurableAgentActiveRun {
  /** Run ID accepted by {@link DurableAgent.recoverActiveRuns} and workflow `restart`. */
  runId: string;
  status: DurableAgentActiveRunStatus;
  threadId?: string;
  resourceId?: string;
  /** When the run's snapshot was last persisted while running. */
  updatedAt: Date;
}

export interface DurableAgentListActiveRunsResult {
  runs: DurableAgentActiveRun[];
  /** Total number of matching runs, before pagination. */
  total: number;
}

/**
 * Outcome of a single run restart attempted by
 * {@link DurableAgent.recoverActiveRuns}. `success` means `run.restart()`
 * returned; `failed` means it threw and the error was captured so recovery
 * of remaining runs could proceed.
 */
export interface DurableAgentRecoveredRun {
  runId: string;
  status: 'success' | 'failed';
  /** Populated only when `status === 'failed'`. */
  error?: Error;
}

/**
 * Filters for {@link DurableAgent.recoverActiveRuns}. Reuses the
 * {@link DurableAgentListActiveRunsOptions} discovery filters and adds an
 * escape hatch for targeting a specific run ID.
 */
export interface DurableAgentRecoverActiveRunsOptions extends DurableAgentListActiveRunsOptions {
  /**
   * Recover a specific run by ID. When set, the discovery filters and
   * pagination fields are ignored. Useful when the caller already knows the
   * run ID from another source (e.g. their own bookkeeping).
   */
  runId?: string;
}

export interface DurableAgentRecoverActiveRunsResult {
  recovered: DurableAgentRecoveredRun[];
  /** Number of runs that restarted successfully. */
  succeeded: number;
  /** Number of runs whose restart threw. */
  failed: number;
}

/**
 * Options for {@link DurableAgent.recover}, a single-run streamable recovery
 * counterpart to {@link DurableAgent.resume}.
 *
 * `recover()` rebuilds the run's non-serializable state from the persisted
 * workflow snapshot (message list, model, tools, memory, saveQueueManager,
 * request context, agent span) and returns a fresh {@link DurableAgentStreamResult}
 * whose `fullStream` observes the recovered run through pubsub. Callbacks
 * mirror `stream()` / `resume()`.
 */
export interface DurableAgentRecoverOptions<OUTPUT = undefined> {
  /** Callback when chunk is received */
  onChunk?: (chunk: ChunkType<OUTPUT>) => void | Promise<void>;
  /** Experimental transforms applied whenever `fullStream` is consumed. */
  experimentalTransform?: MastraStreamTransformOptions<OUTPUT>;
  /** Callback when a step finishes */
  onStepFinish?: (result: AgentStepFinishEventData) => void | Promise<void>;
  /** Callback when the recovered run finishes */
  onFinish?: MastraOnFinishCallback<OUTPUT>;
  /** Callback when the recovered run errors */
  onError?: ({ error }: { error: Error | string }) => void | Promise<void>;
  /** Callback when the recovered run suspends again */
  onSuspended?: (data: AgentSuspendedEventData) => void | Promise<void>;
  /**
   * Optional abort signal for the recovered segment. Forwarded onto a fresh
   * internal `AbortController` installed on the run's registry entry, so
   * `result.abort()` and the external signal can both cancel the recovered run.
   */
  abortSignal?: AbortSignal;
}

export class DurableAgent<
  TAgentId extends string = string,
  TTools extends ToolsInput = ToolsInput,
  TOutput = undefined,
> extends Agent<TAgentId, TTools, TOutput> {
  /** Explicitly identifies the built-in recovery lifecycle to structural consumers. */
  readonly supportsRunRecovery = true as const;

  /** The wrapped agent */
  readonly #wrappedAgent: Agent<TAgentId, TTools, TOutput>;

  /** Registry for per-run non-serializable state */
  readonly #runRegistry: ExtendedRunRegistry;

  /** The durable workflow for agent execution */
  #workflow: ReturnType<typeof createDurableAgenticWorkflow> | null = null;

  /**
   * The engine the workflow instance actually runs on, resolved on first use
   * (see {@link DurableAgent.resolveWorkflowEngine}). `null` until resolved.
   */
  #resolvedWorkflowEngine: 'default' | 'evented' | null = null;

  /** Maximum steps for the agentic loop */
  readonly #maxSteps?: number;

  /** Inner pubsub (before CachingPubSub wrapper) */
  #innerPubsub: PubSub;

  /** Whether the user explicitly provided a pubsub (don't override with mastra.pubsub) */
  readonly #hasCustomPubsub: boolean;

  /** User-provided cache (undefined = inherit from mastra, false = disabled) */
  #cacheConfig: MastraServerCache | false | undefined;

  /** Resolved cache instance (lazily initialized) */
  #resolvedCache: MastraServerCache | null = null;

  /** CachingPubSub instance (lazily initialized) */
  #cachingPubsub: PubSub | null = null;

  /** Mastra instance (set via __setMastra when registered) */
  #mastra: Mastra | undefined;

  /** Active streamUntilIdle wrappers keyed by scope (threadId|resourceId) */
  #activeStreamUntilIdle = new Map<string, () => void>();

  /** Stable identities for non-plain request values used in prepared handoffs. */
  #preparedRequestObjectIds = new WeakMap<object, number>();

  #nextPreparedRequestObjectId = 1;

  #preparedRequestSymbolIds = new Map<symbol, number>();

  #nextPreparedRequestSymbolId = 1;

  /** One-time prepare() results, kept private so callers cannot rewrite the handoff. */
  #preparedExecutions = new Map<
    string,
    { requestFingerprint: string; integrityFingerprint: string; preparation: PreparationResult<TOutput> }
  >();

  /** Timeout for auto-cleanup after stream finishes (0 = disabled) */
  readonly #cleanupTimeoutMs: number;

  /** Explicit application context allowlist for durable serialization. */
  readonly #durableRequestContextKeys: readonly string[];

  /** User-supplied per-topic cache policy (see DurableAgentConfig.shouldCache) */
  readonly #shouldCache: ((topic: string) => boolean) | undefined;

  /**
   * User-supplied snapshot-persistence policy
   * (see DurableAgentConfig.shouldPersistSnapshot). Protected so subclasses
   * that pin their own policy (EventedAgent) can detect and warn when set.
   */
  protected readonly userShouldPersistSnapshot: ShouldPersistSnapshotFn | undefined;

  /** Whether the one-time persistence-policy guardrail warnings have run */
  #warnedPersistencePolicy = false;

  /**
   * Create a new DurableAgent that wraps an existing Agent
   */
  constructor(config: DurableAgentConfig<TAgentId, TTools, TOutput>) {
    const {
      agent,
      id: idOverride,
      name: nameOverride,
      pubsub,
      cache,
      maxSteps,
      cleanupTimeoutMs,
      durableRequestContextKeys,
      shouldCache,
      shouldPersistSnapshot,
    } = config;

    // Use provided id/name or fall back to agent.id/agent.name
    const agentId = idOverride ?? agent.id;
    const agentName = nameOverride ?? agent.name ?? agent.id;
    validateMaxSteps(maxSteps);

    // Call Agent constructor with minimal config - we delegate to the wrapped agent
    super({
      id: agentId as TAgentId,
      name: agentName,
      // Delegate to wrapped agent's instructions
      instructions: ({ requestContext }) => agent.getInstructions({ requestContext }),
      // Keep the base Agent model lazy. Eagerly calling getModel() here resolves
      // tenant-scoped model factories with an empty RequestContext before any
      // execution/admission boundary and can also create an unhandled rejected
      // promise during construction.
      model: ({ requestContext }) => agent.getModel({ requestContext }),
    });

    this.#wrappedAgent = agent;
    this.#runRegistry = new ExtendedRunRegistry();
    this.#maxSteps = maxSteps;
    this.#hasCustomPubsub = !!pubsub;
    this.#innerPubsub = pubsub ?? new EventEmitterPubSub();
    this.#cacheConfig = cache;
    this.#cleanupTimeoutMs = cleanupTimeoutMs ?? 30_000;
    this.#durableRequestContextKeys = Object.freeze([...(durableRequestContextKeys ?? [])]);
    this.#shouldCache = shouldCache;
    this.userShouldPersistSnapshot = shouldPersistSnapshot;
  }

  // ===========================================================================
  // Lazy PubSub/Cache initialization (allows inheriting cache from Mastra)
  // ===========================================================================

  /**
   * Get the resolved cache instance.
   * Lazily initialized to allow inheriting from Mastra.
   */
  get cache(): MastraServerCache | null {
    this.#ensurePubsubInitialized();
    return this.#resolvedCache;
  }

  /**
   * Get the PubSub instance.
   * Returns CachingPubSub if caching is enabled, otherwise the inner pubsub.
   */
  get pubsub(): PubSub {
    this.#ensurePubsubInitialized();
    return this.#cachingPubsub!;
  }

  /**
   * Claim exclusive ownership of a recovery attempt before exposing the run
   * through the thread stream. The thread lease cannot provide this guarantee:
   * its owner is the logical runId, so two processes recovering the same run
   * are indistinguishable to an idempotent lease backend.
   */
  async #acquireRecoveryLease(runId: string, abortController: AbortController): Promise<RecoveryLease> {
    const pubsub = this.pubsub;
    const unwrap = (pubsub as { getLeaseProvider?: () => LeaseProvider | undefined }).getLeaseProvider;
    const provider =
      typeof unwrap === 'function'
        ? (unwrap.call(pubsub) ?? NoopLeaseProvider)
        : isLeaseProvider(pubsub)
          ? pubsub
          : NoopLeaseProvider;
    const key = `mastra:durable-agent-recovery:v1:${JSON.stringify([this.id, runId])}`;
    const owner = crypto.randomUUID();

    const localOwner = localRecoveryClaims.get(key);
    if (localOwner) {
      throw new MastraError({
        id: 'DURABLE_AGENT_RECOVER_ALREADY_IN_PROGRESS',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" recover(${runId}): another process is already recovering this run.`,
        details: { agentName: this.name, runId },
      });
    }
    localRecoveryClaims.set(key, owner);

    const leaseAcquireStartedAt = Date.now();
    let acquired: Awaited<ReturnType<LeaseProvider['acquireLease']>>;
    try {
      acquired = await provider.acquireLease(key, owner, RECOVERY_LEASE_TTL_MS);
    } catch (cause) {
      if (localRecoveryClaims.get(key) === owner) localRecoveryClaims.delete(key);
      throw new MastraError(
        {
          id: 'DURABLE_AGENT_RECOVER_LEASE_ACQUIRE_FAILED',
          domain: ErrorDomain.AGENT,
          category: ErrorCategory.SYSTEM,
          text: `DurableAgent "${this.name}" recover(${runId}): failed to acquire the recovery lease.`,
          details: { agentName: this.name, runId },
        },
        cause,
      );
    }

    if (!acquired.acquired) {
      if (localRecoveryClaims.get(key) === owner) localRecoveryClaims.delete(key);
      throw new MastraError({
        id: 'DURABLE_AGENT_RECOVER_ALREADY_IN_PROGRESS',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" recover(${runId}): another process is already recovering this run.`,
        details: { agentName: this.name, runId },
      });
    }

    let released = false;
    let releasePromise: Promise<void> | undefined;
    let renewalInFlight: Promise<void> | undefined;
    let leaseExpiresAt = leaseAcquireStartedAt + RECOVERY_LEASE_TTL_MS;
    let lossError: MastraError | undefined;
    let resolveLoss!: (error: MastraError) => void;
    const loss = new Promise<MastraError>(resolve => {
      resolveLoss = resolve;
    });
    const stopOnLeaseLoss = (cause?: unknown) => {
      if (released || lossError) return;
      lossError = new MastraError(
        {
          id: 'DURABLE_AGENT_RECOVER_LEASE_LOST',
          domain: ErrorDomain.AGENT,
          category: ErrorCategory.SYSTEM,
          text: `DurableAgent "${this.name}" recover(${runId}): recovery lease was lost while the run was active.`,
          details: { agentName: this.name, runId },
        },
        cause,
      );
      if (!abortController.signal.aborted) {
        abortController.abort(lossError);
      }
      resolveLoss(lossError);
      this.#mastra?.getLogger?.()?.error?.(lossError.message);
    };
    const renewalTimer = setInterval(() => {
      if (released) return;
      if (Date.now() >= leaseExpiresAt) {
        stopOnLeaseLoss();
        return;
      }
      if (renewalInFlight) return;
      const renewalStartedAt = Date.now();
      const renewal: Promise<void> = provider
        .renewLease(key, owner, RECOVERY_LEASE_TTL_MS)
        .then(renewed => {
          if (renewed) {
            leaseExpiresAt = renewalStartedAt + RECOVERY_LEASE_TTL_MS;
            return;
          }
          stopOnLeaseLoss();
        })
        .catch(cause => {
          if (Date.now() >= leaseExpiresAt) {
            stopOnLeaseLoss(cause);
            return;
          }
          this.#mastra
            ?.getLogger?.()
            ?.warn?.(`[DurableAgent] recover(${runId}) lease renewal failed, retrying: ${cause}`);
        })
        .finally(() => {
          if (renewalInFlight === renewal) renewalInFlight = undefined;
        });
      renewalInFlight = renewal;
    }, RECOVERY_LEASE_RENEW_INTERVAL_MS);
    renewalTimer.unref?.();

    return {
      assertOwned: () => {
        if (!lossError && Date.now() >= leaseExpiresAt) {
          stopOnLeaseLoss();
        }
        if (lossError) throw lossError;
      },
      getLossError: () => lossError,
      waitForLoss: () => loss,
      release: () => {
        if (releasePromise) return releasePromise;
        clearInterval(renewalTimer);
        releasePromise = (async () => {
          try {
            // Never release underneath a still-running renewal. Some providers
            // implement these as separate RPCs; ordering them prevents a late
            // renewal from extending ownership after release has returned. Keep
            // loss detection active until that renewal has been classified.
            await renewalInFlight;
            await provider.releaseLease(key, owner);
          } catch (error) {
            this.#mastra
              ?.getLogger?.()
              ?.warn?.(`[DurableAgent] recover(${runId}) failed to release recovery lease: ${error}`);
          } finally {
            released = true;
            if (localRecoveryClaims.get(key) === owner) localRecoveryClaims.delete(key);
          }
        })();
        return releasePromise;
      },
    };
  }

  async #loadRecoverableSnapshot(
    workflowsStore: WorkflowsStorage,
    runId: string,
  ): Promise<{ snapshot: WorkflowRunState; workflowInput: DurableAgenticWorkflowInput }> {
    const persisted = await workflowsStore.getWorkflowRunById({
      runId,
      workflowName: DurableStepIds.AGENTIC_LOOP,
    });
    if (!persisted) {
      throw new MastraError({
        id: 'DURABLE_AGENT_RECOVER_SNAPSHOT_NOT_FOUND',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text:
          `DurableAgent "${this.name}" recover(${runId}): no persisted workflow snapshot found. ` +
          `The run may have already completed or been cleaned up.`,
        details: { agentName: this.name, runId },
      });
    }

    const snapshot =
      typeof persisted.snapshot === 'string'
        ? (JSON.parse(persisted.snapshot) as WorkflowRunState)
        : persisted.snapshot;
    const workflowInput = snapshot?.context?.input as DurableAgenticWorkflowInput | undefined;
    if (!workflowInput || workflowInput.__workflowKind !== 'durable-agent') {
      throw new MastraError({
        id: 'DURABLE_AGENT_RECOVER_INVALID_SNAPSHOT',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.SYSTEM,
        text: `DurableAgent "${this.name}" recover(${runId}): persisted snapshot does not contain a durable-agent workflow input.`,
        details: { agentName: this.name, runId },
      });
    }

    if (workflowInput.agentId !== this.id) {
      throw new MastraError({
        id: 'DURABLE_AGENT_RECOVER_AGENT_MISMATCH',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" recover(${runId}): persisted run belongs to agent "${workflowInput.agentId}", not "${this.id}".`,
        details: { agentName: this.name, runId, ownerAgentId: workflowInput.agentId },
      });
    }
    if (workflowInput.requiredRequestContextCapabilities?.authToken === true) {
      throw new MastraError({
        id: 'DURABLE_AGENT_RECOVER_AUTH_TOKEN_REQUIRED',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" recover(${runId}) requires trusted auth-token reinjection and cannot reuse persisted credentials.`,
        details: { agentName: this.name, runId },
      });
    }
    assertDurableToolHookPolicyAvailable({ serialized: workflowInput.options.toolHookPolicy });

    return { snapshot, workflowInput };
  }

  /**
   * Rebuild and register the stream for a claimed recovery attempt. Rolls back
   * every partial registration and releases the claim if setup fails.
   */
  async #setupRecoveredStream({
    runId,
    workflowInput,
    requestContext,
    threadId,
    resourceId,
    messageList,
    registryEntry,
    options,
    scheduleAutoCleanup,
    recoveryLease,
  }: {
    runId: string;
    workflowInput: DurableAgenticWorkflowInput;
    requestContext: RequestContext;
    threadId?: string;
    resourceId?: string;
    messageList: MessageList;
    registryEntry: RunRegistryEntry;
    options?: DurableAgentRecoverOptions<TOutput>;
    scheduleAutoCleanup: () => void;
    recoveryLease: RecoveryLease;
  }): Promise<{
    stream: DurableStreamAdapterResult<TOutput>;
    threadRegistration?: AgentThreadRunRegistration;
  }> {
    let streamCleanup: (() => void) | undefined;
    let streamOutput: MastraModelOutput<TOutput> | undefined;
    let threadRegistration: AgentThreadRunRegistration | undefined;
    try {
      recoveryLease.assertOwned();
      registryEntry.messageList = messageList;
      // Registration stays run-identity scoped: the entry carries the immutable
      // runtime binding this recovery resolved, and a colliding registration for
      // the same runId is rejected instead of silently rebinding it. The
      // collision fence itself runs in `recover()` before this method is
      // entered — see the comment there — so a refused duplicate never reaches
      // this try's catch. `registerGlobalRunRegistryEntry` still throws on a
      // collision as a last-resort backstop; it cannot fire here because the
      // fence and this registration run in the same synchronous turn.
      registerGlobalRunRegistryEntry(runId, registryEntry);
      this.#runRegistry.registerWithMessageList(runId, registryEntry, messageList, { threadId, resourceId });

      // Persistent backends may retain chunks from the pre-crash segment.
      const recoverOffset = await this.#getPubsubOffset(runId);
      recoveryLease.assertOwned();
      const stream = createDurableAgentStream<TOutput>({
        pubsub: this.pubsub,
        runId,
        messageId: workflowInput.messageId ?? crypto.randomUUID(),
        model: {
          modelId: workflowInput.modelConfig?.modelId,
          provider: workflowInput.modelConfig?.provider,
          version: 'v3',
        },
        threadId,
        resourceId,
        offset: recoverOffset,
        onChunk: options?.onChunk,
        experimentalTransform: options?.experimentalTransform,
        onStepFinish: options?.onStepFinish,
        onFinish: options?.onFinish,
        onStreamFinished: scheduleAutoCleanup,
        onError: async error => {
          await options?.onError?.(error);
          scheduleAutoCleanup();
        },
        onSuspended: options?.onSuspended,
        // Keep recovered runs observable if they suspend again so a later
        // resume or recovery can pick them up.
        messageList,
        // SAFETY: registry entries store the schema without the stream's OUTPUT type
        // parameter; the stream adapter re-casts it at its own consumption boundary.
        structuredOutput: registryEntry.structuredOutput as any,
        requestContext: registryEntry.requestContext,
        outputProcessors: registryEntry.outputProcessors,
        returnScorerData: workflowInput.options?.returnScorerData,
        tracingContext: registryEntry.agentSpan ? { currentSpan: registryEntry.agentSpan } : undefined,
      });
      streamCleanup = stream.cleanup;
      streamOutput = stream.output;
      await this.#raceRecoveryLease(stream.ready, recoveryLease);
      recoveryLease.assertOwned();

      const recoverStreamOptions: AgentExecutionOptions<TOutput> = {
        runId,
        requestContext,
        ...(threadId
          ? {
              memory: {
                thread: threadId,
                ...(resourceId ? { resource: resourceId } : {}),
              },
            }
          : {}),
      } as AgentExecutionOptions<TOutput>;
      recoveryLease.assertOwned();
      threadRegistration = await agentThreadStreamRuntime.registerRun(
        this as unknown as Agent<any, any, any, any>,
        stream.output,
        recoverStreamOptions,
        this.getPubSub(),
        {
          strict: true,
          continuation: 'across-suspension',
          validate: () => recoveryLease.assertOwned(),
        },
      );
      recoveryLease.assertOwned();
      return { stream, threadRegistration };
    } catch (error) {
      try {
        await threadRegistration?.rollback({ releaseLease: !recoveryLease.getLossError() });
      } catch (rollbackError) {
        this.#mastra
          ?.getLogger?.()
          ?.warn?.(`[DurableAgent] recover(${runId}) failed to roll back thread registration: ${rollbackError}`);
      }
      if (streamOutput) {
        agentThreadStreamRuntime.closeRunContinuation(streamOutput, this.getPubSub());
      }
      streamCleanup?.();
      if (this.#runRegistry.get(runId) === registryEntry) {
        this.#runRegistry.cleanupBound(runId, registryEntry.runtimeBindingId);
      }
      if (getGlobalRunRegistryEntry(runId) === registryEntry) {
        clearPinnedRunRegistryEntry(runId);
        deleteBoundRunRegistryEntry(runId, registryEntry.runtimeBindingId);
      }
      await this.#reportRecoveryFailure(runId, recoveryLease.getLossError() ?? error);
      await recoveryLease.release();
      throw error;
    }
  }

  async #reportRecoveryFailure(runId: string, error: unknown): Promise<boolean> {
    const normalizedError = error instanceof Error ? error : new Error(String(error));
    if (
      (normalizedError instanceof MastraError && normalizedError.id === 'DURABLE_AGENT_RECOVER_LEASE_LOST') ||
      normalizedError instanceof AgentThreadLeaseConflictError
    ) {
      return false;
    }

    try {
      await this.emitError(runId, normalizedError);
      return true;
    } catch (reportingError) {
      this.#mastra
        ?.getLogger?.()
        ?.warn?.(`[DurableAgent] recover(${runId}) failed to publish terminal error: ${reportingError}`);
      return false;
    }
  }

  async #raceRecoveryLease<T>(operation: Promise<T>, recoveryLease: RecoveryLease): Promise<T> {
    const outcome = await Promise.race<RecoveryRaceResult<T>>([
      operation.then(value => ({ kind: 'result', value })),
      recoveryLease.waitForLoss().then(error => ({ kind: 'lease-lost', error })),
    ]);
    if (outcome.kind === 'lease-lost') throw outcome.error;
    return outcome.value;
  }

  #createRecoveryFencedPubSub(recoveryLease: RecoveryLease): PubSub {
    const pubsub = this.pubsub;
    const publish: PubSub['publish'] = async (topic, event, options) => {
      recoveryLease.assertOwned();
      await pubsub.publish(topic, event, options);
      recoveryLease.assertOwned();
    };

    return new Proxy(pubsub, {
      get(target, property) {
        if (property === 'publish') return publish;
        const value = Reflect.get(target, property, target);
        return typeof value === 'function' ? value.bind(target) : value;
      },
    });
  }

  async #rehydrateRecoveryState({
    runId,
    workflowInput,
    abortController,
    recoveryLease,
  }: {
    runId: string;
    workflowInput: DurableAgenticWorkflowInput;
    abortController: AbortController;
    recoveryLease: RecoveryLease;
  }): Promise<RehydratedRecoveryState> {
    // 1. Rebuild the RequestContext from the persisted JSON-safe snapshot.
    const requestContext: RequestContext = workflowInput.requestContextEntries
      ? new RequestContext(Object.entries(workflowInput.requestContextEntries) as Iterable<readonly [string, unknown]>)
      : new RequestContext();
    if (workflowInput.versions) {
      requestContext.set(MASTRA_VERSIONS_KEY, structuredClone(workflowInput.versions));
    }

    // 2. Rebuild MessageList from the persisted state. threadId/resourceId
    //    come from the workflow input's `state` block when present; older
    //    snapshots may only have them under `messageListState.memoryInfo`.
    const messageListMemoryInfo = (
      workflowInput.messageListState as { memoryInfo?: { threadId?: string; resourceId?: string } } | undefined
    )?.memoryInfo;
    const threadId = workflowInput.state?.threadId ?? messageListMemoryInfo?.threadId;
    const resourceId = workflowInput.state?.resourceId ?? messageListMemoryInfo?.resourceId;
    const observationalMemorySourceWriteGuard =
      workflowInput.options?.observationalMemorySourceWriteGuard ??
      workflowInput.messageListState?.memoryInfo?.observationalMemorySourceWriteGuard;

    // `requestContextEntries` captures caller state before preparation adds
    // the run-owned memory context. Rebuild it from the persisted run so a
    // recovered execution cannot inherit a caller or parent thread.
    if (threadId && resourceId) {
      requestContext.set('MastraMemory', {
        thread: { id: threadId },
        resourceId,
        memoryConfig: workflowInput.state?.memoryConfig,
        ...(observationalMemorySourceWriteGuard ? { observationalMemorySourceWriteGuard } : {}),
      });
    } else {
      requestContext.delete('MastraMemory');
    }
    const messageList = new MessageList({ threadId, resourceId });
    try {
      messageList.deserialize(workflowInput.messageListState);
    } catch (err) {
      // Fresh (never-executed) snapshots may have a minimal `messageListState`;
      // fall back to an empty MessageList so recovery still proceeds. The
      // workflow steps rebuild the real MessageList from serialized input.
      this.#mastra?.getLogger?.()?.warn?.(`[DurableAgent] recover(${runId}) messageList deserialize skipped: ${err}`);
    }

    // 3. Rebuild the exact native agent runtime. Registry-required runs cannot
    // fall back to name-based substitution inside a workflow step.
    const wrapped = this.#wrappedAgent as Agent<string, any, TOutput>;
    if (workflowInput.options?.toolSurfaceFence !== undefined) {
      throw new MastraError({
        id: 'DURABLE_AGENT_RECOVER_REPLACEMENT_TOOLS_UNRECOVERABLE',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" recover(${runId}) cannot reconstruct call-time replacement tool implementations.`,
        details: { agentName: this.name, runId },
      });
    }
    const defaultOptionsPromise = wrapped.getDefaultOptions({ requestContext });
    const memoryPromise = wrapped.getMemory({ requestContext });
    const resolvedMemoryPromise = memoryPromise.then(value => ({ value }));
    const resolvedModelsPromise = wrapped.__resolveExecutionModels({ requestContext });
    const recoveredModelsPromise = resolvedModelsPromise.then(resolvedModels =>
      rebindRecoveredExecutionModels(workflowInput.modelList, resolvedModels),
    );
    const processorsPromise = resolvedMemoryPromise
      .then(resolvedMemory => wrapped.__resolveExecutionProcessors(requestContext, resolvedMemory))
      .catch(cause => {
        throw new MastraError(
          {
            id: 'DURABLE_AGENT_RECOVER_PROCESSOR_RESOLUTION_FAILED',
            domain: ErrorDomain.AGENT,
            category: ErrorCategory.USER,
            text: `DurableAgent "${this.name}" recover(${runId}) could not reconstruct its processor surface.`,
            details: { agentName: this.name, runId },
          },
          cause,
        );
      });
    const recoverableProcessorsPromise = processorsPromise.then(processors => {
      if (
        workflowInput.hasProcessors ||
        processors.inputProcessors.length > 0 ||
        processors.outputProcessors.length > 0 ||
        processors.errorProcessors.length > 0
      ) {
        throw new MastraError({
          id: 'DURABLE_AGENT_RESUME_PROCESSOR_STATE_UNRECOVERABLE',
          domain: ErrorDomain.AGENT,
          category: ErrorCategory.USER,
          text: `DurableAgent "${this.name}" cannot cold-recover run "${runId}" because request-local processor state was not persisted.`,
          details: { agentName: this.name, runId },
        });
      }
      return processors;
    });
    const toolsPromise = Promise.all([
      defaultOptionsPromise,
      resolvedMemoryPromise,
      recoveredModelsPromise,
      recoverableProcessorsPromise,
    ]).then(([resolvedDefaultOptions, resolvedMemory, recoveredModels, processors]) =>
      wrapped.getToolsForExecution({
        runId,
        threadId,
        resourceId,
        requestContext,
        memoryConfig: workflowInput.state?.memoryConfig,
        autoResumeSuspendedTools: workflowInput.options?.autoResumeSuspendedTools,
        agentId: this.id,
        agentName: this.name,
        resolvedDefaultOptions: resolvedDefaultOptions as AgentExecutionOptions<any>,
        resolvedMemory,
        resolvedModel: recoveredModels.model,
        resolvedInputProcessors: processors.configuredInputProcessors,
        processorMessages: messageList.get.all.db(),
      }),
    );
    const [tools, recoveredModels, memory, workspace, processors] = await Promise.all([
      toolsPromise,
      recoveredModelsPromise,
      memoryPromise,
      wrapped.getWorkspace({ requestContext }),
      recoverableProcessorsPromise,
    ]);
    recoveryLease.assertOwned();
    const { model, modelList } = recoveredModels;
    if (!isSupportedLanguageModel(model)) {
      throw new MastraError({
        id: 'DURABLE_AGENT_RECOVER_UNSUPPORTED_MODEL',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" recover(${runId}) requires an AI SDK v5+ model.`,
        details: { agentName: this.name, runId },
      });
    }
    if (stableStringify(serializeToolsMetadata(tools)) !== stableStringify(workflowInput.toolsMetadata)) {
      throw new MastraError({
        id: 'DURABLE_AGENT_RECOVER_TOOL_BINDING_MISMATCH',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" recover(${runId}) resolved a different tool surface.`,
        details: { agentName: this.name, runId },
      });
    }
    if (stableStringify(serializeModelConfig(model)) !== stableStringify(workflowInput.modelConfig)) {
      throw new MastraError({
        id: 'DURABLE_AGENT_RECOVER_MODEL_BINDING_MISMATCH',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" recover(${runId}) resolved a different model.`,
        details: { agentName: this.name, runId },
      });
    }
    if (
      stableStringify(modelList ? serializeModelList(modelList) : undefined) !==
      stableStringify(workflowInput.modelList)
    ) {
      throw new MastraError({
        id: 'DURABLE_AGENT_RECOVER_MODEL_BINDING_MISMATCH',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" recover(${runId}) resolved a different model list.`,
        details: { agentName: this.name, runId },
      });
    }
    if (
      stableStringify({
        memory: createRuntimeDependencyFingerprint(memory),
        workspace: createRuntimeDependencyFingerprint(workspace),
      }) !== stableStringify(workflowInput.runtimeBindings ?? {})
    ) {
      throw new MastraError({
        id: 'DURABLE_AGENT_RECOVER_RUNTIME_BINDING_MISMATCH',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" recover(${runId}) resolved different runtime services.`,
        details: { agentName: this.name, runId },
      });
    }
    const saveQueueManager = memory
      ? new SaveQueueManager({ logger: this.#mastra?.getLogger?.() as any, memory })
      : undefined;

    // Re-wire background-task state so the recovered segment can wait for
    // pre-crash tasks (via `bg-task-check`), dispatch new background tool
    // calls (via `tool-call`), and inject the background-task system prompt
    // (via `llm-execution`). The manager is storage-backed, so in-flight
    // tasks spawned before the crash are still discoverable via
    // `bgManager.listTasks(...)`.
    const backgroundTasksConfig = this.getBackgroundTasksConfig?.();
    const backgroundTaskManager = this.#mastra?.backgroundTaskManager;

    // Resolve the live surface to detect processor additions or drift. Active
    // cold recovery fails closed above because the pre-crash request-local
    // processor state, buffered chunks, and per-call overrides were not persisted.
    const { inputProcessors, llmRequestInputProcessors, outputProcessors, errorProcessors } = processors;
    const processorStates = new Map<string, any>();

    // 4. Re-open an AGENT_RUN span for the recovered segment. Follow the same
    //    pattern as resume(): reuse the original traceId when possible so the
    //    recovered run stays linked to the original agent trace.
    const origAgentSpanData = workflowInput.agentSpanData as { traceId?: string; id?: string } | undefined;
    let recoverAgentSpan: any;
    if (this.#mastra?.observability) {
      try {
        const rawConfig =
          typeof (wrapped as any).toRawConfig === 'function' ? (wrapped as any).toRawConfig() : undefined;
        const resolvedVersionId = rawConfig?.resolvedVersionId as string | undefined;
        const agentTracingPolicy =
          typeof wrapped.getTracingPolicy === 'function' ? wrapped.getTracingPolicy() : undefined;
        recoverAgentSpan = getOrCreateSpan({
          type: SpanType.AGENT_RUN,
          name: `agent run: '${wrapped.id}' (recovered)`,
          entityType: EntityType.AGENT,
          entityId: wrapped.id,
          entityName: wrapped.name,
          metadata: {
            runId,
            recovered: true,
            ...(origAgentSpanData?.id ? { recoveredFromSpanId: origAgentSpanData.id } : {}),
            ...(resolvedVersionId ? { entityVersionId: resolvedVersionId } : {}),
          },
          tracingPolicy: agentTracingPolicy,
          tracingOptions: origAgentSpanData?.traceId ? { traceId: origAgentSpanData.traceId } : undefined,
          requestContext,
          mastra: this.#mastra,
        });
      } catch (error) {
        // Span bookkeeping must never block recovery.
        this.#mastra?.getLogger?.()?.warn?.(`[DurableAgent] Failed to open recover span: ${error}`);
      }
    }
    recoveryLease.assertOwned();

    // 5. Assemble the RunRegistryEntry for the recovered segment. Every runtime
    //    dependency above was verified against the persisted fingerprints, so
    //    the recovered run is bound to the exact tools/model/services the
    //    original run used.
    const registryEntry = {
      // Restore the original run's flag from the persisted snapshot so a
      // warm resume after recovery keeps returning scoringData without the
      // caller re-passing the option.
      returnScorerData: workflowInput.options?.returnScorerData,
      mastra: this.#mastra,
      runtimeBindingId: workflowInput.runtimeBindingId ?? crypto.randomUUID(),
      agentId: this.id,
      threadId,
      resourceId,
      tools,
      model,
      modelList,
      memory,
      workspace,
      saveQueueManager,
      requestContext,
      versions: workflowInput.versions ? structuredClone(workflowInput.versions) : undefined,
      messageList,
      agentSpan: recoverAgentSpan,
      // abortController/abortSignal are installed by
      // #installAbortWithTotalTimeout below, with the run-level budget
      // composed in.
      abortController,
      abortSignal: abortController.signal,
      // Restore the run-level execution budget from the persisted snapshot so
      // a recovered session is bounded like the original one (#21724).
      timeoutTotalMs: workflowInput.options?.modelSettings?.timeout?.totalMs,
      // Rebuild the live structured output config from the persisted JSON Schema
      // so the recovered stream still emits `object-result` chunks.
      // SAFETY: the schema was serialized from a typed StandardSchemaWithJSON — the
      // original OUTPUT type parameter is not recoverable at this boundary, and the
      // stream adapter re-narrows it at its own consumption boundary.
      structuredOutput: workflowInput.options?.structuredOutput?.schema
        ? ({
            ...workflowInput.options.structuredOutput,
            schema: toStandardSchema(workflowInput.options.structuredOutput.schema),
          } as StructuredOutputOptions)
        : undefined,
      backgroundTaskManager,
      backgroundTasksConfig,
      inputProcessors,
      llmRequestInputProcessors,
      outputProcessors,
      errorProcessors,
      processorStates,
      drainPendingSignals: (scope?: 'pending' | 'pre-run') => wrapped.__getDrainPendingSignals()(runId, scope),
      cleanup: () => {},
    };
    this.#installAbortWithTotalTimeout(registryEntry as unknown as RunRegistryEntry, abortController);

    return {
      requestContext,
      threadId,
      resourceId,
      messageList,
      recoverAgentSpan,
      registryEntry,
    };
  }

  /**
   * Ensure pubsub and cache are initialized.
   * Called lazily on first access to allow inheriting cache from Mastra.
   */
  #ensurePubsubInitialized(): void {
    if (this.#cachingPubsub) return;

    if (this.#cacheConfig === false) {
      // Caching explicitly disabled
      this.#cachingPubsub = this.#innerPubsub;
      this.#resolvedCache = null;
    } else if (this.#innerPubsub instanceof CachingPubSub) {
      // The inner pubsub already provides caching/replay. This happens when the
      // user passes a CachingPubSub to `new Mastra({ pubsub })`: on registration
      // the agent adopts mastra.pubsub as its inner transport. Wrapping it again
      // in a second CachingPubSub that shares the same cache would store every
      // event twice (once per layer, with consecutive indices), so observe()/
      // replay would deliver the buffered prefix doubled (issue #18148). Reuse
      // the existing instance instead of double-wrapping.
      this.#cachingPubsub = this.#innerPubsub;
      this.#resolvedCache = this.#cacheConfig ?? this.#mastra?.serverCache ?? null;
      if (this.#mastra) this.#innerPubsub.__setSource(this.#mastra.pubsub);
      if (this.#shouldCache) {
        // The existing wrapper owns the caching policy; a per-agent filter
        // cannot be applied without double-wrapping, so it is ignored.
        this.logger.warn(
          `[DurableAgent:${this.id}] 'shouldCache' is ignored because the configured pubsub is already a CachingPubSub. Pass 'shouldCache' to that CachingPubSub instead.`,
        );
      }
    } else {
      // Resolve cache: user-provided > mastra's cache > default InMemoryServerCache
      const resolvedCache = this.#cacheConfig ?? this.#mastra?.serverCache ?? new InMemoryServerCache();
      this.#resolvedCache = resolvedCache;
      // Run-local topics must never reach the cache. This wrapper sits *above*
      // the `mastra.pubsub` proxy that tags publishes `localOnly`, so that flag
      // is set too late for `CachingPubSub` to observe it — the policy has to be
      // declared here instead. Without it, per-run `workflow.events.v2.*` watch
      // events (cumulative step results, often megabytes) are RPUSHed into a
      // shared store that no other instance can ever read from (issue #20646).
      //
      // `source: mastra.pubsub` makes the cache follow the Mastra-level bus.
      // The evented engine publishes agent-stream events there (from whichever
      // worker executes a step) instead of through this agent's pubsub, so
      // without the source wiring those events are never cached and — when the
      // agent has a custom pubsub on a different transport — never reach the
      // stream's subscribers at all: streams would resolve with null
      // finish/suspend data (Phase 2 Item 5). When agent and Mastra share the
      // underlying transport, the follower only caches (fixing replay) and
      // never double-delivers.
      const userShouldCache = this.#shouldCache;
      this.#cachingPubsub = new CachingPubSub(this.#innerPubsub, resolvedCache, {
        shouldCache: topic => !isRunLocalTopic(topic) && (userShouldCache?.(topic) ?? true),
        source: this.#mastra?.pubsub,
      });
    }
  }

  // ===========================================================================
  // Delegate to wrapped agent
  // ===========================================================================

  /**
   * Get the wrapped agent instance.
   */
  get agent(): Agent<TAgentId, TTools, TOutput> {
    return this.#wrappedAgent;
  }

  /**
   * File-based schedules live on the wrapped agent: `assembleAgentFromFsEntry`
   * attaches them to the inner `Agent` before it is wrapped for durable
   * execution, and `#declaredSchedules` is private to each instance. Without
   * this delegate the wrapper would report none of its own and Mastra would
   * never sync a durable agent's `schedules/` directory.
   */
  public override getDeclaredSchedules(): DeclaredAgentSchedule[] {
    return this.#wrappedAgent.getDeclaredSchedules();
  }

  /**
   * Mirrors {@link getDeclaredSchedules} so attaching schedules to an
   * already-wrapped agent lands on the instance the getter reads from.
   */
  public override __setDeclaredSchedules(schedules: DeclaredAgentSchedule[]): void {
    this.#wrappedAgent.__setDeclaredSchedules(schedules);
  }

  /**
   * Get the run registry (for testing and advanced usage)
   */
  get runRegistry(): ExtendedRunRegistry {
    return this.#runRegistry;
  }

  /**
   * Get the max steps configured for this agent
   */
  get maxSteps(): number | undefined {
    return this.#maxSteps;
  }

  /**
   * Get the cleanup timeout in milliseconds.
   * Returns 0 if auto-cleanup is disabled.
   */
  get cleanupTimeoutMs(): number {
    return this.#cleanupTimeoutMs;
  }

  // ===========================================================================
  // Delegate Agent methods to wrapped agent
  //
  // DurableAgent's super() only passes id, name, instructions, and model.
  // All other private fields (#tools, #memory, #workspace, #processors, etc.)
  // are empty on the DurableAgent instance. Every public/protected method that
  // reads those fields must be overridden to delegate to the wrapped agent.
  // ===========================================================================

  // --- Model & LLM ---
  override getModel(options?: any) {
    return this.#wrappedAgent.getModel(options);
  }

  override getLLM(options?: any) {
    return this.#wrappedAgent.getLLM(options);
  }

  override async getModelList(requestContext?: any) {
    return this.#wrappedAgent.getModelList(requestContext);
  }

  // --- Instructions, description, metadata ---
  override getInstructions(options?: any) {
    return this.#wrappedAgent.getInstructions(options);
  }

  override getDescription() {
    return this.#wrappedAgent.getDescription();
  }

  override getMetadata(options?: any) {
    return this.#wrappedAgent.getMetadata(options);
  }

  override getTracingPolicy() {
    return this.#wrappedAgent.getTracingPolicy();
  }

  // --- Tools ---
  override listTools(options?: any) {
    return this.#wrappedAgent.listTools(options);
  }

  override getConfiguredToolHooks() {
    return this.#wrappedAgent.getConfiguredToolHooks();
  }

  // --- Default options ---
  override getDefaultOptions(options?: any) {
    return this.#wrappedAgent.getDefaultOptions(options);
  }

  async #resolveExecutionOptions(
    options?: DurableAgentStreamOptions<TOutput>,
  ): Promise<DurableAgentStreamOptions<TOutput>> {
    if ((options as any)?.[RESOLVED_EXECUTION_OPTIONS]) {
      return options!;
    }

    // The until-idle wrapper resolves dynamic defaults once per public call
    // and carries them into inner segments; never re-resolve behind it.
    const carriedDefaultOptions = options?._resolvedDefaultOptions;
    const defaultOptions =
      carriedDefaultOptions ?? (await this.getDefaultOptions({ requestContext: options?.requestContext }));
    if (options?._resolvedDefaultOptions !== undefined) {
      const { _resolvedDefaultOptions: _carried, ...publicOptions } = options;
      options = publicOptions as DurableAgentStreamOptions<TOutput>;
    }
    const resolvedOptions = deepMerge(
      (defaultOptions ?? {}) as Record<string, unknown>,
      (options ?? {}) as Record<string, unknown>,
    ) as DurableAgentStreamOptions<TOutput>;
    // Actor is a per-call trust signal, so an explicit value replaces the
    // default actor as a whole rather than inheriting any of its fields.
    if (options?.actor !== undefined) {
      resolvedOptions.actor = options.actor;
    }
    // Preserve the marker when the until-idle wrapper spreads these options.
    Object.defineProperty(resolvedOptions, RESOLVED_EXECUTION_OPTIONS, { value: true, enumerable: true });
    return resolvedOptions;
  }

  override getDefaultGenerateOptionsLegacy(options?: any) {
    return this.#wrappedAgent.getDefaultGenerateOptionsLegacy(options);
  }

  override getDefaultStreamOptionsLegacy(options?: any) {
    return this.#wrappedAgent.getDefaultStreamOptionsLegacy(options);
  }

  override getDefaultNetworkOptions(options?: any) {
    return this.#wrappedAgent.getDefaultNetworkOptions(options);
  }

  // --- Memory ---
  override getMemory(options?: any) {
    return this.#wrappedAgent.getMemory(options);
  }

  override hasOwnMemory(): boolean {
    return this.#wrappedAgent.hasOwnMemory();
  }

  // --- Workspace ---
  override getWorkspace(options?: any) {
    return this.#wrappedAgent.getWorkspace(options);
  }

  override hasOwnWorkspace(): boolean {
    return this.#wrappedAgent.hasOwnWorkspace?.() ?? false;
  }

  // --- Voice ---
  override getVoice(options?: any) {
    return this.#wrappedAgent.getVoice(options);
  }

  override get voice() {
    return this.#wrappedAgent.voice;
  }

  // --- Request context ---
  override get requestContextSchema() {
    return this.#wrappedAgent.requestContextSchema;
  }

  // --- Processors ---
  override async getConfiguredProcessorWorkflows() {
    return this.#wrappedAgent.getConfiguredProcessorWorkflows();
  }

  override async listInputProcessors(requestContext?: any) {
    return this.#wrappedAgent.listInputProcessors(requestContext);
  }

  override async listOutputProcessors(requestContext?: any) {
    return this.#wrappedAgent.listOutputProcessors(requestContext);
  }

  override async listErrorProcessors(requestContext?: any) {
    return this.#wrappedAgent.listErrorProcessors(requestContext);
  }

  override async resolveProcessorById<TId extends string = string>(processorId: TId, requestContext?: any) {
    return this.#wrappedAgent.resolveProcessorById(processorId, requestContext);
  }

  override async listConfiguredInputProcessors(requestContext?: any) {
    return this.#wrappedAgent.listConfiguredInputProcessors(requestContext);
  }

  override async listConfiguredOutputProcessors(requestContext?: any) {
    return this.#wrappedAgent.listConfiguredOutputProcessors(requestContext);
  }

  override async getConfiguredProcessorIds(requestContext?: any) {
    return this.#wrappedAgent.getConfiguredProcessorIds(requestContext);
  }

  // --- Sub-agents ---
  override listAgents(options?: any) {
    return this.#wrappedAgent.listAgents(options);
  }

  override __getStaticAgents() {
    return this.#wrappedAgent.__getStaticAgents();
  }

  override __hasSubAgentsConfigured() {
    return this.#wrappedAgent.__hasSubAgentsConfigured();
  }

  // --- Workflows ---
  override async listWorkflows(options?: any) {
    return this.#wrappedAgent.listWorkflows(options);
  }

  // --- Skills ---
  override async getSkill(skillName: string, options?: any) {
    return this.#wrappedAgent.getSkill(skillName, options);
  }

  override async listSkills(options?: any) {
    return this.#wrappedAgent.listSkills(options);
  }

  // --- Scorers ---
  override async listScorers(options?: any) {
    return this.#wrappedAgent.listScorers(options);
  }

  // --- Background tasks ---
  override getBackgroundTasksConfig() {
    return this.#wrappedAgent.getBackgroundTasksConfig();
  }

  override disableBackgroundTasks() {
    this.#wrappedAgent.disableBackgroundTasks();
  }

  override enableBackgroundTasks() {
    this.#wrappedAgent.enableBackgroundTasks();
  }

  // --- Tool payload transform & goal ---
  override getToolPayloadTransform() {
    return this.#wrappedAgent.getToolPayloadTransform();
  }

  override __getGoalConfig() {
    return this.#wrappedAgent.__getGoalConfig();
  }

  // --- Browser ---
  override get browser() {
    return this.#wrappedAgent.browser;
  }

  override setBrowser(browser: any) {
    this.#wrappedAgent.setBrowser(browser);
  }

  override hasOwnBrowser() {
    return this.#wrappedAgent.hasOwnBrowser();
  }

  // --- Channels ---
  override getChannels() {
    return this.#wrappedAgent.getChannels();
  }

  override setChannels(agentChannels: any) {
    this.#wrappedAgent.setChannels(agentChannels);
  }

  // --- PubSub (base Agent fields — DurableAgent has its own pubsub) ---
  override getPubSub() {
    // AgentController registration supplies a shared inherited transport. Standalone
    // DurableAgents still need the thread runtime and inherited Agent APIs to use the
    // durable stream's actual transport instead of the process-global fallback.
    return super.getPubSub() ?? this.pubsub;
  }

  override hasOwnPubSub() {
    return this.#wrappedAgent.hasOwnPubSub();
  }

  // --- Setters called by AgentController — forward to BOTH wrapper and wrapped ---
  // We propagate to both so that:
  //  - The wrapped agent sees the value for its own internal use.
  //  - The DurableAgent's inherited getPubSub()/getMemory()/getWorkspace()
  //    also work (they read #inheritedPubSub / #memory / #workspace set by super).
  override __setMemory(memory: any) {
    super.__setMemory(memory);
    this.#wrappedAgent.__setMemory(memory);
  }

  override __setPubSub(pubsub: any) {
    super.__setPubSub(pubsub);
    this.#wrappedAgent.__setPubSub(pubsub);
  }

  override __setWorkspace(workspace: any) {
    super.__setWorkspace(workspace);
    this.#wrappedAgent.__setWorkspace(workspace);
  }

  // ===========================================================================
  // Editor / fork delegation
  //
  // The base Agent serves tools/instructions/model from its own private fields,
  // but a DurableAgent serves all of them from the wrapped agent (see the
  // delegating getters above). The editor applies stored overrides per request
  // by calling `__fork()` and then mutating the fork via `__updateInstructions`
  // / `__updateModel` / `__setTools`, and inspecting it via `__getEditorConfig`
  // / `__getOverridableFields`. If those operated on the DurableAgent's own
  // (unused) base fields the served agent would silently lose its tools and
  // ignore overrides, so forward them to the wrapped agent — it stays the single
  // source of truth.
  // ===========================================================================

  override __getEditorConfig() {
    return this.#wrappedAgent.__getEditorConfig();
  }

  override __getOverridableFields() {
    return this.#wrappedAgent.__getOverridableFields();
  }

  override __updateInstructions(instructions: Parameters<Agent<TAgentId, TTools, TOutput>['__updateInstructions']>[0]) {
    this.#wrappedAgent.__updateInstructions(instructions);
  }

  override __updateModel(config: Parameters<Agent<TAgentId, TTools, TOutput>['__updateModel']>[0]) {
    this.#wrappedAgent.__updateModel(config);
  }

  override __setTools(tools: Parameters<Agent<TAgentId, TTools, TOutput>['__setTools']>[0]) {
    this.#wrappedAgent.__setTools(tools);
  }

  /**
   * Raw stored config lives on the wrapped agent for the same reason as the
   * mutators above: durable preparation and tracing read
   * `resolvedVersionId` from the wrapped agent's rawConfig, and the editor
   * stamps it on the outer fork via `__setRawConfig` after
   * `applyStoredOverrides`. Without this delegation the stamp lands on the
   * (unread) DurableAgent-level field and version metadata silently
   * disappears from spans and suspend snapshots.
   */
  override toRawConfig(): Record<string, unknown> | undefined {
    return this.#wrappedAgent.toRawConfig();
  }

  override __setRawConfig(rawConfig: Record<string, unknown>): void {
    this.#wrappedAgent.__setRawConfig(rawConfig);
  }

  /**
   * Create a per-request clone for applying stored editor overrides.
   *
   * The base `Agent.__fork()` builds a bare `new Agent(...)`, which for a
   * DurableAgent would drop the wrapped agent and every delegating override
   * (tools, model, memory, voice, durable streaming) — the served fork ends up a
   * plain `Agent` with no tools. Instead, fork the wrapped agent (so overrides
   * applied to this fork don't mutate the singleton) and re-wrap it in the same
   * durable subclass, preserving pubsub/cache/run configuration.
   *
   * @internal
   */
  override __fork(): Agent<TAgentId, TTools, TOutput> {
    const innerFork = this.#wrappedAgent.__fork();

    const Ctor = this.constructor as new (
      config: DurableAgentConfig<TAgentId, TTools, TOutput>,
    ) => DurableAgent<TAgentId, TTools, TOutput>;

    const fork = new Ctor({
      agent: innerFork,
      id: this.id,
      name: this.name,
      pubsub: this.#hasCustomPubsub ? this.#innerPubsub : undefined,
      cache: this.#cacheConfig,
      maxSteps: this.#maxSteps,
      cleanupTimeoutMs: this.#cleanupTimeoutMs,
      durableRequestContextKeys: this.#durableRequestContextKeys,
      shouldCache: this.#shouldCache,
      shouldPersistSnapshot: this.userShouldPersistSnapshot,
    });

    // Preserve runtime state set after construction (mastra registration and the
    // wired inner pubsub, e.g. mastra.pubsub) without re-triggering registration
    // side effects — mirrors Agent.__fork().
    if (this.#mastra) {
      fork.#mastra = this.#mastra;
    }
    fork.#innerPubsub = this.#innerPubsub;
    fork.source = this.source;
    // `_agentNetworkAppend` is a private base-class flag; copy it via an indexed
    // cast (the same idiom the base uses in `toRawConfig()`) so the fork mirrors
    // `Agent.__fork()` without widening the field's visibility.
    (fork as unknown as { _agentNetworkAppend: unknown })._agentNetworkAppend = (
      this as unknown as { _agentNetworkAppend: unknown }
    )._agentNetworkAppend;

    // DurableAgent intentionally diverges from Agent's `stream` signature, so the
    // re-wrapped fork is bridged to the base `Agent` return type here. The editor's
    // fork-then-mutate contract only relies on the base Agent surface.
    return fork as unknown as Agent<TAgentId, TTools, TOutput>;
  }

  // ===========================================================================
  // Protected methods for subclass overrides
  // ===========================================================================

  /**
   * Get the PubSub instance for use by subclasses.
   * @internal
   */
  protected get pubsubInternal(): PubSub {
    return this.pubsub;
  }

  /**
   * Get the run registry for use by subclasses.
   * @internal
   */
  protected get runRegistryInternal(): ExtendedRunRegistry {
    return this.#runRegistry;
  }

  /**
   * Which workflow execution engine the durable agentic loop runs on.
   * Subclasses override this (EventedAgent → 'evented').
   * @internal
   */
  protected get workflowEngine(): 'default' | 'evented' {
    return 'default';
  }

  /**
   * Resolve the engine this agent's workflow instance actually runs on.
   *
   * An evented agent without a Mastra host cannot execute evented runs — the
   * evented engine's `createRun` requires the host's registries, storage, and
   * event workers. Before the evented engine was re-enabled, a hostless
   * EventedAgent silently streamed on the default in-process engine; that is
   * released behavior, so we preserve it here as a fallback (with a warning)
   * instead of throwing.
   *
   * The same applies to a host whose storage cannot apply concurrent workflow
   * updates atomically. The evented engine advances a run from concurrent
   * workers, so `createRun()` refuses to start on such a store. Letting that
   * throw would turn a working durable agent into a hard failure on upgrade
   * for stores like Redis, so durable agents degrade to the in-process engine
   * the same way a hostless one does. A workflow that opts into the evented
   * engine directly (by declaring a `schedule`) still gets the error, because
   * there is no other engine it could have meant.
   *
   * The result is memoized: `getWorkflow()` caches the created workflow, so
   * an agent that first streamed hostless keeps its default-engine workflow
   * even if it is registered on a Mastra instance afterwards — identical to
   * the previously shipped behavior. The cache is deliberately not
   * invalidated.
   *
   * @internal
   */
  protected resolveWorkflowEngine(): 'default' | 'evented' {
    if (this.#resolvedWorkflowEngine) return this.#resolvedWorkflowEngine;
    let engine = this.workflowEngine;
    if (engine === 'evented' && !this.#mastra) {
      engine = 'default';
      this.logger.warn(
        `EventedAgent '${this.id}' has no Mastra host; running on the default in-process engine. ` +
          `Register the agent on a Mastra instance (with storage) to get evented execution.`,
      );
    } else if (engine === 'evented') {
      // Read `stores` directly rather than `await getStore('workflows')`: engine
      // resolution is synchronous (`getWorkflow()` is), and `getStore()` is only
      // async by signature — it returns `this.stores?.[name]` without awaiting
      // init, so this sees exactly what the engine's own gate sees.
      const storage = this.#mastra?.getStorage();
      const workflowsStore = storage?.stores?.workflows;
      if (workflowsStore && !(workflowsStore.supportsConcurrentUpdates?.() ?? false)) {
        engine = 'default';
        this.logger.warn(
          `EventedAgent '${this.id}' is registered with ${storage?.name ?? 'a'} storage, which does not apply concurrent ` +
            `workflow updates atomically (\`supportsConcurrentUpdates()\`); running the durable loop on the default ` +
            `in-process engine instead. In-flight runs will not resume on their own after a process restart. Use a ` +
            `storage adapter that supports atomic concurrent updates (for example @mastra/libsql, @mastra/pg or ` +
            `@mastra/mysql) to get evented execution.`,
        );
      }
    }
    this.#resolvedWorkflowEngine = engine;
    return engine;
  }

  /**
   * Evented-engine runs execute via pubsub events consumed by in-process
   * workers — without them `run.start()`/`resume()`/`restart()` never
   * resolve (they wait on the `workflows-finish` topic). No-op on the
   * default engine; idempotent on Mastra's side.
   * @internal
   */
  protected async ensureEngineWorkersStarted(): Promise<void> {
    if (this.resolveWorkflowEngine() === 'evented') {
      await this.#mastra?.__ensureExecutionWorkersStarted();
    }
  }

  /**
   * Execute the durable workflow.
   *
   * Subclasses override this method to customize how the workflow is executed:
   * - DurableAgent (this): Runs the workflow directly via createRun + start
   * - EventedAgent: Awaits the native run.start() persisted-finish receipt
   * - InngestAgent: Uses inngest.send() to trigger Inngest function
   *
   * @param runId - The unique run ID
   * @param workflowInput - The serialized workflow input
   * @internal
   */
  protected async executeWorkflow(runId: string, workflowInput: DurableAgenticWorkflowInput): Promise<void> {
    const workflow = this.getWorkflow();
    const entry = getBoundRunRegistryEntry(runId, workflowInput.runtimeBindingId);
    const pinnedEntry = pinGlobalRunRegistryEntry(runId);
    if (pinnedEntry !== entry) this.#throwRunIdConflict(runId);
    const requestContext = entry?.requestContext;
    const messageListMemoryInfo = (
      workflowInput.messageListState as { memoryInfo?: { resourceId?: string } } | undefined
    )?.memoryInfo;
    const resourceId = workflowInput.state?.resourceId ?? messageListMemoryInfo?.resourceId;

    try {
      const run = await workflow.createRun({
        runId,
        resourceId,
        pubsub: this.pubsub,
      });
      // Parent the workflow run under the AGENT_RUN span so the trace exports under it.
      await run.start({
        inputData: workflowInput,
        requestContext,
        actor: workflowInput.options?.actor,
        ...createObservabilityContext({ currentSpan: entry?.agentSpan }),
      });
    } finally {
      unpinGlobalRunRegistryEntry(runId, workflowInput.runtimeBindingId);
    }
  }

  /** Handle terminal lifecycle for synchronous and evented workflow engines. */
  protected async onDurableWorkflowFinish(result: WorkflowFinishCallbackResult): Promise<void> {
    if (['running', 'suspended', 'waiting', 'pending', 'paused'].includes(result.status)) return;
    try {
      await this.deleteTerminalRunSnapshots(result.runId);
    } catch (error) {
      // Terminal cleanup is deliberately best-effort. Storage implementations
      // normally fail closed inside deleteTerminalRunSnapshots(), but an
      // override or adapter must not prevent failure delivery either.
      this.#mastra?.getLogger?.()?.warn?.('[DurableAgent] Failed to delete terminal workflow snapshots', {
        runId: result.runId,
        error,
      });
    }
    if (result.status === 'failed' || result.status === 'tripwire') {
      await this.emitError(result.runId, new Error(result.error?.message ?? 'Workflow execution failed'));
    }
  }

  /**
   * Create the durable workflow for this agent.
   *
   * Subclasses can override this method to use a different workflow implementation:
   * - DurableAgent (this): Uses createDurableAgenticWorkflow()
   * - InngestAgent: Uses createInngestDurableAgenticWorkflow()
   *
   * @internal
   */
  protected createWorkflow(): ReturnType<typeof createDurableAgenticWorkflow> {
    return createDurableAgenticWorkflow(
      {
        maxSteps: this.#maxSteps,
        onFinish: result => this.onDurableWorkflowFinish(result),
        // Resolved, not raw: a hostless evented agent falls back to the default
        // in-process engine (see resolveWorkflowEngine).
        engine: this.resolveWorkflowEngine(),
        shouldPersistSnapshot: this.resolveShouldPersistSnapshot(),
      },
      this.logger,
    );
  }

  /**
   * Resolve the effective snapshot-persistence policy for this agent's
   * workflows: the user-supplied predicate when set, otherwise the
   * recovery-aware default. Subclasses that own their persistence policy
   * (EventedAgent) override this to pin their required policy.
   *
   * @internal
   */
  protected resolveShouldPersistSnapshot(): ShouldPersistSnapshotFn {
    return this.userShouldPersistSnapshot ?? this.#recoveryAwarePersistencePolicy;
  }

  /**
   * Default snapshot-persistence policy for plain durable agents.
   *
   * Always persists `pending | paused | suspended` — those records are the
   * resume artifacts human-in-the-loop flows depend on. Persists `running`
   * only when crash recovery is enabled (`recovery.durableAgents: 'auto'`):
   * `running` checkpoints exist solely to feed `listActiveRuns()` /
   * `recover()` / `recoverActiveRuns()`, and are pure write amplification
   * when nothing consumes them (issue #23915).
   *
   * Reads the recovery config lazily (per persist call, via the Mastra
   * reference) so the policy stays correct regardless of whether the
   * workflow was built before or after Mastra registration.
   */
  readonly #recoveryAwarePersistencePolicy: ShouldPersistSnapshotFn = ({ workflowStatus }) => {
    return (
      workflowStatus === 'pending' ||
      workflowStatus === 'paused' ||
      workflowStatus === 'suspended' ||
      (workflowStatus === 'running' && this.#mastra?.recoveryConfig?.durableAgents === 'auto')
    );
  };

  /**
   * Emit an error event to pubsub.
   *
   * @param runId - The run ID
   * @param error - The error to emit
   * @internal
   */
  protected async emitError(runId: string, error: Error): Promise<void> {
    // End the root spans on error so the trace exports (mirrors the non-durable map-results-step).
    endRunSpansWithError(runId, error);
    await emitErrorEvent(this.pubsub, runId, error);
  }

  /**
   * `emitError` for fire-and-forget call sites. A pubsub that is already
   * closing (for example during shutdown) must not turn a run's own failure
   * into an unhandledRejection.
   */
  protected emitErrorInBackground(runId: string, error: Error): void {
    this.emitError(runId, error).catch(publishError => {
      this.logger.warn(`Failed to publish error event for run ${runId}`, { runId, error: publishError });
    });
  }

  /**
   * Abort the thread's active run.
   *
   * The base implementation flips the run's prepared `AbortController`, which a
   * durable run never has: its controller lives on this agent's run registry,
   * and the steps reading it may execute in another process. Without the abort
   * request below, aborting a thread whose active run is durable records an
   * intent nothing reads and lets the run stream on.
   */
  abortThreadStream(options: AgentAbortThreadOptions): boolean {
    // Resolve the run before the base call: aborting releases the thread lease,
    // after which the thread no longer has an active run to look up.
    const runId = agentThreadStreamRuntime.getActiveThreadRunId(options, this.getPubSub());
    // Upstream expectedRunId: a stale caller must not abort whichever run now
    // owns the thread. Let the base record its (no-op) decision and stop.
    if (options.expectedRunId !== undefined && options.expectedRunId !== runId) {
      return super.abortThreadStream(options);
    }
    // Upstream localOnly: never reach a durable execution this instance does
    // not own (no remote abort request for another process's run).
    if (options.localOnly && runId && !this.#runRegistry.get(runId)) {
      return super.abortThreadStream(options);
    }
    const registryEntry = runId ? (this.#runRegistry.get(runId) ?? getGlobalRunRegistryEntry(runId)) : undefined;
    if (registryEntry && registryEntry.agentId !== this.id) return false;
    const controller = registryEntry?.abortController;
    const runtimeBindingId = registryEntry?.runtimeBindingId;
    const wasExecuting = runId ? this.#isRunExecuting(runId) : false;
    const aborted = super.abortThreadStream(options);
    if (runId) this.#abortDurableRun(runId, controller, runtimeBindingId);
    return aborted || wasExecuting;
  }

  /** Abort a durable execution by run id without weakening its binding fence. */
  abortRunStream(runId: string): boolean {
    const registryEntry = this.#runRegistry.get(runId) ?? getGlobalRunRegistryEntry(runId);
    if (registryEntry && registryEntry.agentId !== this.id) return false;
    const controller = registryEntry?.abortController;
    const runtimeBindingId = registryEntry?.runtimeBindingId;
    const wasExecuting = this.#isRunExecuting(runId);
    // Unknown durable IDs must not create a runId-only tombstone: a future
    // execution may legitimately reuse the caller-supplied ID under a new
    // runtime binding. The same identity rule forbids an asynchronous storage
    // lookup from adopting a binding that appears after this call returns.
    const aborted = agentThreadStreamRuntime.abortRun(runId, this.getPubSub());
    this.#abortDurableRun(runId, controller, runtimeBindingId);
    return aborted || wasExecuting;
  }

  #isRunExecuting(runId: string): boolean {
    return (
      this.#runRegistry.get(runId) !== undefined ||
      getGlobalRunRegistryEntry(runId) !== undefined ||
      agentThreadStreamRuntime.hasThreadRun(runId, this.getPubSub())
    );
  }

  /** Apply only the controller and binding captured before abort cleanup can mutate registry state. */
  #abortDurableRun(runId: string, controller: AbortController | undefined, runtimeBindingId: string | undefined): void {
    if (controller && !controller.signal.aborted) {
      controller.abort(new Error('Aborted'));
    }
    if (runtimeBindingId) {
      void this.requestRemoteAbort(runId, runtimeBindingId).catch(() => {
        // requestRemoteAbort records the transport failure; local abort remains effective.
      });
    }
  }

  /** Abort the exact durable execution captured by a result handle. */
  protected async requestRemoteAbort(runId: string, runtimeBindingId: string | undefined): Promise<void> {
    if (!runtimeBindingId) {
      const error = new Error(`Cannot publish durable agent abort for ${runId} without a runtime binding`);
      this.#mastra?.getLogger?.()?.warn?.(error.message, {
        agentId: this.id,
        runId,
      });
      throw error;
    }
    try {
      await publishAbortRequest(this.pubsub, runId, runtimeBindingId);
    } catch (error) {
      this.#mastra?.getLogger?.()?.warn?.('Failed to publish durable agent abort request', {
        agentId: this.id,
        runId,
        error: error instanceof Error ? error.message : String(error),
      });
      throw error;
    }
  }

  /** Best-effort deletion of the outer and nested snapshots for a terminal run. */
  protected async deleteTerminalRunSnapshots(runId: string): Promise<void> {
    const workflowsStore = await this.#mastra?.getStorage()?.getStore('workflows');
    const deleteWithRetry = async (deletion: () => Promise<void>) => {
      try {
        await deletion();
        return;
      } catch {
        // Retry once for transient driver failures. If both attempts fail,
        // preserve the original terminal row and its status for later cleanup.
        try {
          await deletion();
          return;
        } catch (cause) {
          throw cause;
        }
      }
    };
    const deletions = await Promise.allSettled([
      deleteWithRetry(() => this.getWorkflow().deleteWorkflowRunById(runId)),
      deleteWithRetry(async () => {
        await workflowsStore?.deleteWorkflowRunById({
          runId,
          workflowName: DurableStepIds.AGENTIC_EXECUTION,
        });
      }),
    ]);
    const errors = deletions
      .filter((result): result is PromiseRejectedResult => result.status === 'rejected')
      .map(result => result.reason);
    if (errors.length > 0) {
      this.#mastra?.getLogger()?.warn?.('[DurableAgent] Failed to delete terminal workflow snapshots', {
        runId,
        errors,
      });
    }
  }

  /**
   * Install `abortController` on a registry entry with the run-level execution
   * budget (`modelSettings.timeout.totalMs`, #21724 parity port) composed in.
   *
   * Every durable step reads `abortSignal` off the registry, so composing here
   * bounds the whole session — every loop iteration, tool call and retry.
   * Pass-through when no budget is configured. The budget is armed per
   * execution session (stream/generate/resume), matching the main loop where
   * each session gets a fresh timer. The timer is torn down via the entry's
   * `cleanup` slot, which every settle path invokes through the run registry.
   */
  #installAbortWithTotalTimeout(entry: RunRegistryEntry, abortController: AbortController): void {
    entry.abortController = abortController;
    const totalTimeout = createTimeoutAbortSignal({
      parentSignal: abortController.signal,
      timeoutMs: entry.timeoutTotalMs,
      timeoutType: 'total',
    });
    entry.abortSignal = totalTimeout.signal;
    const previousCleanup = entry.cleanup;
    entry.cleanup = () => {
      totalTimeout.cleanup();
      previousCleanup?.();
    };
  }

  /**
   * Delete the persisted workflow snapshot rows for a completed durable run.
   *
   * A durable agent write two rows per run: one for the outer `AGENTIC_LOOP`
   * workflow and one for the nested `AGENTIC_EXECUTION` workflow (persisted
   * under the same `runId`). Once the run reaches a non-suspended terminal
   * state neither row is needed again — leaving them behind fills snapshot
   * storage with stale `pending`/`running` rows for every completed run and
   * pollutes `listActiveRuns` / `recoverActiveRuns` on the next boot.
   *
   * Best-effort: a cleanup failure must never turn a finished run into an
   * error — a stale row is preferable to a broken exit path.
   *
   * @internal
   */
  protected async deleteRunSnapshots(runId: string): Promise<void> {
    try {
      const workflow = this.getWorkflow();
      await workflow.deleteWorkflowRunById(runId);
      const workflowsStore = await this.#mastra?.getStorage()?.getStore('workflows');
      await workflowsStore?.deleteWorkflowRunById({
        runId,
        workflowName: DurableStepIds.AGENTIC_EXECUTION,
      });
    } catch (error) {
      this.#mastra
        ?.getLogger?.()
        ?.warn?.(`[DurableAgent] Failed to delete workflow snapshot rows after terminal state`, { runId, error });
    }
  }

  #throwRunIdConflict(runId: string): never {
    throw new MastraError({
      id: 'DURABLE_AGENT_RUN_ID_CONFLICT',
      domain: ErrorDomain.AGENT,
      category: ErrorCategory.USER,
      text: `DurableAgent "${this.name}" cannot claim runId "${runId}" because it is already in use.`,
      details: {
        agentName: this.name,
        runId,
      },
    });
  }

  #getPreparedRequestObjectId(value: object): number {
    const existing = this.#preparedRequestObjectIds.get(value);
    if (existing !== undefined) return existing;
    const id = this.#nextPreparedRequestObjectId++;
    this.#preparedRequestObjectIds.set(value, id);
    return id;
  }

  #normalizePreparedPropertyKey(key: PropertyKey): unknown {
    if (typeof key !== 'symbol') return { $stringKey: String(key) };
    const globalKey = Symbol.keyFor(key);
    if (globalKey !== undefined) return { $globalSymbolKey: globalKey };
    let identity = this.#preparedRequestSymbolIds.get(key);
    if (identity === undefined) {
      identity = this.#nextPreparedRequestSymbolId++;
      this.#preparedRequestSymbolIds.set(key, identity);
    }
    return { $localSymbolKey: identity, $description: key.description ?? '' };
  }

  #normalizePreparedObjectProperties(
    value: object,
    seen: WeakMap<object, number>,
    nextReference: { value: number },
  ): Array<[unknown, unknown]> {
    const normalizedKeys = new Map<PropertyKey, unknown>();
    const keys = Reflect.ownKeys(value).sort((left, right) => {
      const normalizedLeft = this.#normalizePreparedPropertyKey(left);
      const normalizedRight = this.#normalizePreparedPropertyKey(right);
      normalizedKeys.set(left, normalizedLeft);
      normalizedKeys.set(right, normalizedRight);
      return stableStringify(normalizedLeft).localeCompare(stableStringify(normalizedRight));
    });
    return keys.map(key => {
      const normalizedKey = normalizedKeys.get(key) ?? this.#normalizePreparedPropertyKey(key);
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (!descriptor) {
        return [normalizedKey, { $missingDescriptor: true }] as [unknown, unknown];
      }
      const normalizedDescriptor =
        'value' in descriptor
          ? {
              configurable: descriptor.configurable,
              enumerable: descriptor.enumerable,
              writable: descriptor.writable,
              value: this.#normalizePreparedRequestValue(descriptor.value, seen, nextReference),
            }
          : {
              configurable: descriptor.configurable,
              enumerable: descriptor.enumerable,
              get: this.#normalizePreparedRequestValue(descriptor.get, seen, nextReference),
              set: this.#normalizePreparedRequestValue(descriptor.set, seen, nextReference),
            };
      return [normalizedKey, normalizedDescriptor] as [unknown, unknown];
    });
  }

  #normalizePreparedRequestValue(
    value: unknown,
    seen = new WeakMap<object, number>(),
    nextReference = { value: 1 },
  ): unknown {
    if (value === null || typeof value === 'string' || typeof value === 'boolean') {
      return value;
    }
    if (value === undefined) return { $undefined: true };
    if (typeof value === 'number') {
      if (Number.isNaN(value)) return { $number: 'NaN' };
      if (value === Infinity) return { $number: 'Infinity' };
      if (value === -Infinity) return { $number: '-Infinity' };
      if (Object.is(value, -0)) return { $number: '-0' };
      return value;
    }
    if (typeof value === 'bigint') return { $bigint: String(value) };
    if (typeof value === 'symbol') return this.#normalizePreparedPropertyKey(value);
    const existingReference = seen.get(value);
    if (existingReference !== undefined) return { $ref: existingReference };
    const reference = nextReference.value++;
    seen.set(value, reference);
    const properties = () => this.#normalizePreparedObjectProperties(value, seen, nextReference);

    if (typeof value === 'function') {
      return {
        $id: reference,
        $identity: this.#getPreparedRequestObjectId(value),
        $type: 'function',
        $properties: properties(),
      };
    }

    if (value instanceof RequestContext) {
      return {
        $id: reference,
        $requestContext: this.#normalizePreparedRequestValue(Array.from(value.entries()), seen, nextReference),
        $properties: properties(),
      };
    }
    if (value instanceof Date) {
      return {
        $id: reference,
        $date: Number.isNaN(value.getTime()) ? 'Invalid Date' : value.toISOString(),
        $properties: properties(),
      };
    }
    if (value instanceof RegExp && Object.getPrototypeOf(value) === RegExp.prototype) {
      return {
        $id: reference,
        $regexp: value.source,
        $flags: value.flags,
        $properties: properties(),
      };
    }
    if (value instanceof Map) {
      return {
        $id: reference,
        $map: Array.from(value.entries(), ([key, entryValue]) => [
          this.#normalizePreparedRequestValue(key, seen, nextReference),
          this.#normalizePreparedRequestValue(entryValue, seen, nextReference),
        ]),
        $properties: properties(),
      };
    }
    if (value instanceof Set) {
      return {
        $id: reference,
        $set: Array.from(value, entry => this.#normalizePreparedRequestValue(entry, seen, nextReference)),
        $properties: properties(),
      };
    }
    if (Array.isArray(value)) {
      return {
        $id: reference,
        $array: value.map(item => this.#normalizePreparedRequestValue(item, seen, nextReference)),
        $properties: properties(),
      };
    }

    const prototype = Object.getPrototypeOf(value);
    if (prototype !== Object.prototype && prototype !== null) {
      return {
        $id: reference,
        $identity: this.#getPreparedRequestObjectId(value),
        $type: value.constructor?.name ?? 'object',
        $properties: properties(),
      };
    }

    return {
      $id: reference,
      $prototype: Object.getPrototypeOf(value) === null ? 'null' : 'object',
      $object: properties(),
    };
  }

  #preparedRequestFingerprint(
    messages: MessageListInput,
    options?: AgentExecutionOptions<TOutput> | DurableAgentStreamOptions<TOutput>,
  ): string {
    const {
      runId: _runId,
      onChunk: _onChunk,
      onStepFinish: _onStepFinish,
      onFinish: _onFinish,
      onError: _onError,
      onAbort: _onAbort,
      onSuspended: _onSuspended,
      ...preparationOptions
    } = (options ?? {}) as AgentExecutionOptions<TOutput> & DurableAgentStreamOptions<TOutput>;
    return stableStringify(this.#normalizePreparedRequestValue({ messages, options: preparationOptions }));
  }

  #preparedExecutionIntegrityFingerprint(preparation: PreparationResult<TOutput>): string {
    return stableStringify(
      this.#normalizePreparedRequestValue({
        runId: preparation.runId,
        messageId: preparation.messageId,
        workflowInput: preparation.workflowInput,
        runtimeBindings: this.#preparedRegistryIntegrityFingerprint(preparation.registryEntry),
        threadId: preparation.threadId,
        resourceId: preparation.resourceId,
      }),
    );
  }

  #preparedRegistryIntegrityFingerprint(entry: RunRegistryEntry): string {
    const {
      cleanup: _cleanup,
      workflowExecution: _workflowExecution,
      messageList: _messageList,
      ...runtimeBindings
    } = entry;
    return stableStringify(this.#normalizePreparedRequestValue(runtimeBindings));
  }

  #createPrivatePreparation(preparation: PreparationResult<TOutput>): PreparationResult<TOutput> {
    const registryEntry: RunRegistryEntry = {
      ...preparation.registryEntry,
      tools: Object.fromEntries(
        Object.entries(preparation.registryEntry.tools).map(([toolId, tool]) => [toolId, { ...tool }]),
      ),
      modelList: preparation.registryEntry.modelList?.map(entry => ({ ...entry })),
      versions: preparation.registryEntry.versions ? structuredClone(preparation.registryEntry.versions) : undefined,
      // Keep the private context object captured by converted tool closures.
      // prepare() snapshots the caller's options before preparation, so cloning
      // again here would only sever runtime signals such as delegation bail.
      requestContext: preparation.registryEntry.requestContext,
      inputProcessors: preparation.registryEntry.inputProcessors?.slice(),
      outputProcessors: preparation.registryEntry.outputProcessors?.slice(),
      errorProcessors: preparation.registryEntry.errorProcessors?.slice(),
      processorStates: preparation.registryEntry.processorStates
        ? new Map(preparation.registryEntry.processorStates)
        : undefined,
    };
    return {
      ...preparation,
      workflowInput: snapshotAgentExecutionValue(preparation.workflowInput),
      registryEntry,
    };
  }

  #getPreparedExecution(runId: string, requestFingerprint: string) {
    const prepared = this.#preparedExecutions.get(runId);
    const localEntry = this.#runRegistry.get(runId);
    const globalEntry = getGlobalRunRegistryEntry(runId);
    const messageList = this.#runRegistry.getMessageList(runId);
    if (!prepared) {
      if (localEntry || globalEntry || messageList) this.#throwRunIdConflict(runId);
      return undefined;
    }
    if (
      !localEntry ||
      !globalEntry ||
      !messageList ||
      globalEntry !== localEntry ||
      prepared.requestFingerprint !== requestFingerprint ||
      prepared.integrityFingerprint !== this.#preparedExecutionIntegrityFingerprint(prepared.preparation) ||
      this.#preparedRegistryIntegrityFingerprint(globalEntry) !==
        this.#preparedRegistryIntegrityFingerprint(prepared.preparation.registryEntry) ||
      globalEntry.messageList !== messageList ||
      prepared.preparation.registryEntry !== localEntry ||
      prepared.preparation.messageList !== messageList
    ) {
      this.#throwRunIdConflict(runId);
    }
    return prepared.preparation;
  }

  #consumePreparedExecution(runId: string): void {
    this.#preparedExecutions.delete(runId);
  }

  #coordinateRegistryCleanup(runId: string, entry: RunRegistryEntry): void {
    const existingCleanup = entry.cleanup;
    let cleaning = false;
    entry.cleanup = () => {
      if (cleaning) return;
      cleaning = true;
      try {
        this.#preparedExecutions.delete(runId);
        if (this.#runRegistry.get(runId) === entry) {
          this.#runRegistry.cleanupBound(runId, entry.runtimeBindingId);
        }
        if (getGlobalRunRegistryEntry(runId) === entry) {
          clearPinnedRunRegistryEntry(runId);
          deleteBoundRunRegistryEntry(runId, entry.runtimeBindingId);
        }
        existingCleanup?.();
      } finally {
        cleaning = false;
      }
    };
  }

  #activeRegistryPairIsValid(runId: string, expectedEntry: RunRegistryEntry): boolean {
    const localEntry = this.#runRegistry.get(runId);
    const globalEntry = getGlobalRunRegistryEntry(runId);
    const messageList = this.#runRegistry.getMessageList(runId);
    if (!localEntry && !globalEntry && !messageList) return false;
    if (
      localEntry !== expectedEntry ||
      globalEntry !== expectedEntry ||
      !globalEntry ||
      !messageList ||
      globalEntry.messageList !== messageList ||
      globalEntry.runtimeBindingId !== expectedEntry.runtimeBindingId ||
      globalEntry.agentId !== expectedEntry.agentId ||
      globalEntry.threadId !== expectedEntry.threadId ||
      globalEntry.resourceId !== expectedEntry.resourceId ||
      globalEntry.tools !== expectedEntry.tools ||
      globalEntry.model !== expectedEntry.model ||
      globalEntry.modelList !== expectedEntry.modelList ||
      globalEntry.memory !== expectedEntry.memory ||
      globalEntry.saveQueueManager !== expectedEntry.saveQueueManager ||
      globalEntry.workspace !== expectedEntry.workspace ||
      globalEntry.requestContext !== expectedEntry.requestContext ||
      globalEntry.versions !== expectedEntry.versions ||
      globalEntry.inputProcessors !== expectedEntry.inputProcessors ||
      globalEntry.llmRequestInputProcessors !== expectedEntry.llmRequestInputProcessors ||
      globalEntry.outputProcessors !== expectedEntry.outputProcessors ||
      globalEntry.errorProcessors !== expectedEntry.errorProcessors ||
      globalEntry.processorStates !== expectedEntry.processorStates ||
      globalEntry.backgroundTaskManager !== expectedEntry.backgroundTaskManager ||
      globalEntry.backgroundTasksConfig !== expectedEntry.backgroundTasksConfig ||
      globalEntry.cleanup !== expectedEntry.cleanup
    ) {
      this.#throwRunIdConflict(runId);
    }
    return true;
  }

  #isReusablePreparedEntry(
    runId: string,
    expectedOwner?: Pick<RunRegistryEntry, 'agentId' | 'threadId' | 'resourceId'>,
  ): boolean {
    if (!expectedOwner) return false;
    if (!this.#preparedExecutions.has(runId)) return false;
    const entries = [this.#runRegistry.get(runId), getGlobalRunRegistryEntry(runId)];
    return entries.every(
      entry =>
        entry !== undefined &&
        entry.workflowExecution === undefined &&
        entry.agentId === this.id &&
        entry.agentId === expectedOwner.agentId &&
        entry.threadId === expectedOwner.threadId &&
        entry.resourceId === expectedOwner.resourceId,
    );
  }

  #assertNoRegistryCollision(
    runId: string,
    ignorePending = false,
    reusablePreparedOwner?: Pick<RunRegistryEntry, 'agentId' | 'threadId' | 'resourceId'>,
  ): void {
    const canReusePreparedEntry = this.#isReusablePreparedEntry(runId, reusablePreparedOwner);
    if (
      (!ignorePending && pendingDurableRunIds.has(runId)) ||
      ((this.#runRegistry.has(runId) || getGlobalRunRegistryEntry(runId)) && !canReusePreparedEntry)
    ) {
      this.#throwRunIdConflict(runId);
    }
  }

  async #assertRunIdAvailable(
    runId: string,
    ignorePending = false,
    reusablePreparedOwner?: Pick<RunRegistryEntry, 'agentId' | 'threadId' | 'resourceId'>,
  ): Promise<void> {
    this.#assertNoRegistryCollision(runId, ignorePending, reusablePreparedOwner);
    const workflowsStore = await this.#mastra?.getStorage()?.getStore('workflows');
    if (!workflowsStore) return;
    let persisted: unknown[];
    try {
      persisted = await Promise.all([
        workflowsStore.getWorkflowRunById({ workflowName: DurableStepIds.AGENTIC_LOOP, runId }),
        workflowsStore.getWorkflowRunById({ workflowName: DurableStepIds.AGENTIC_EXECUTION, runId }),
      ]);
    } catch {
      // An unreadable row still occupies the identifier and must not be overwritten.
      this.#throwRunIdConflict(runId);
    }
    if (persisted.some(Boolean)) this.#throwRunIdConflict(runId);
  }

  async #reserveRunId(
    runId: string,
    reusablePreparedOwner?: Pick<RunRegistryEntry, 'agentId' | 'threadId' | 'resourceId'>,
  ): Promise<() => void> {
    if (pendingDurableRunIds.has(runId)) this.#throwRunIdConflict(runId);
    pendingDurableRunIds.add(runId);
    try {
      await this.#assertRunIdAvailable(runId, true, reusablePreparedOwner);
    } catch (error) {
      pendingDurableRunIds.delete(runId);
      throw error;
    }
    return () => pendingDurableRunIds.delete(runId);
  }

  #getDurableWorkflowInput(snapshot: any): DurableAgenticWorkflowInput | undefined {
    const input = snapshot?.context?.input as DurableAgenticWorkflowInput | undefined;
    return input?.__workflowKind === 'durable-agent' ? input : undefined;
  }

  async #assertPublicAgentResumePreflight(options: {
    requestContext?: RequestContext;
    memory?: AgentMemoryOption;
    runId?: string;
    snapshotMemoryInfo?: { threadId?: string; resourceId?: string };
    actor?: AgentExecutionOptions<any>['actor'];
    authorize?: boolean;
  }): Promise<void> {
    await this.#wrappedAgent.__assertAgentResumePreflight({
      ...options,
      agentId: this.id,
      agentName: this.name,
    });
  }

  /**
   * Ownership/routing agreement for the shared durable pair check.
   *
   * The native engine dispatches nested resumes from the suspension-routing
   * identity (`context[AGENTIC_EXECUTION].suspendPayload.__workflow_meta.runId`),
   * while resume/rehydration/listing fetch the row named by the owned identity
   * (`context[AGENTIC_EXECUTION].metadata.nestedRunId`). When owned metadata is
   * present and names a different run, both identities must name the same run:
   * a contradictory or invalid routing identity rejects the pair before engine
   * dispatch — it is never rewritten and never falls back to another row. The
   * legacy/default case without owned metadata (or a same-ID owner) keeps the
   * existing lookup with no routing contract.
   */
  #durableNestedRoutingIsConsistent(runId: string, outerSnapshot: any): boolean {
    const step = outerSnapshot?.context?.[DurableStepIds.AGENTIC_EXECUTION];
    const metadata = step && typeof step === 'object' ? (step as { metadata?: unknown }).metadata : undefined;
    if (!metadata || typeof metadata !== 'object' || !('nestedRunId' in metadata)) return true;
    const owned = (metadata as { nestedRunId?: unknown }).nestedRunId;
    if (typeof owned !== 'string' || owned.length === 0) return false;
    if (owned === runId) return true;
    const routing = (step as { suspendPayload?: unknown }).suspendPayload as { __workflow_meta?: unknown } | undefined;
    const routingRunId =
      routing && typeof routing === 'object'
        ? (routing.__workflow_meta as { runId?: unknown } | undefined)?.runId
        : undefined;
    if (typeof routingRunId !== 'string' || routingRunId.length === 0) return false;
    return routingRunId === owned;
  }

  #durableSnapshotPairIsConsistent(
    runId: string,
    outerRun: any,
    outerSnapshot: any,
    nestedRun: any,
    nestedSnapshot: any,
  ): boolean {
    // Ownership/routing agreement first: when the outer snapshot names an
    // owned nested run, the suspension-routing identity must name the same
    // run. A contradictory or invalid routing identity rejects the pair
    // before engine dispatch — never rewritten, never another row.
    if (!this.#durableNestedRoutingIsConsistent(runId, outerSnapshot)) return false;
    if (!nestedRun || nestedSnapshot?.status !== 'suspended') return false;
    const outerInput = this.#getDurableWorkflowInput(outerSnapshot);
    const nestedInput = this.#getDurableWorkflowInput(nestedSnapshot);
    if (!outerInput || !nestedInput) return false;
    if (
      outerInput.runId !== runId ||
      nestedInput.runId !== runId ||
      outerInput.agentId !== nestedInput.agentId ||
      (outerRun.resourceId && nestedRun.resourceId && outerRun.resourceId !== nestedRun.resourceId)
    ) {
      return false;
    }
    const outerCalls = this.#getDurableSuspendedToolCalls(outerSnapshot);
    const nestedCalls = this.#getDurableSuspendedToolCalls(nestedSnapshot);
    if (outerCalls.hasLabelConflict || nestedCalls.hasLabelConflict) return false;
    if (stableStringify(outerCalls.toolCalls) !== stableStringify(nestedCalls.toolCalls)) return false;

    const ownerValues = (input: DurableAgenticWorkflowInput, row: any) => ({
      threadIds: [input.state?.threadId, input.messageListState?.memoryInfo?.threadId].filter(
        (value): value is string => typeof value === 'string' && value.length > 0,
      ),
      resourceIds: [row?.resourceId, input.state?.resourceId, input.messageListState?.memoryInfo?.resourceId].filter(
        (value): value is string => typeof value === 'string' && value.length > 0,
      ),
    });
    const outerOwner = ownerValues(outerInput, outerRun);
    const nestedOwner = ownerValues(nestedInput, nestedRun);
    if (new Set([...outerOwner.threadIds, ...nestedOwner.threadIds]).size > 1) return false;
    if (new Set([...outerOwner.resourceIds, ...nestedOwner.resourceIds]).size > 1) return false;

    const recoveryContract = (input: DurableAgenticWorkflowInput) => ({
      versions: input.versions,
      hasProcessors: input.hasProcessors,
      runtimeBindings: input.runtimeBindings,
      runtimeResolution: input.runtimeResolution,
      toolsMetadata: input.toolsMetadata,
      modelConfig: input.modelConfig,
      modelList: input.modelList,
      options: input.options,
      requestContextEntries: input.requestContextEntries,
      requiredRequestContextCapabilities: input.requiredRequestContextCapabilities,
    });
    return stableStringify(recoveryContract(outerInput)) === stableStringify(recoveryContract(nestedInput));
  }

  #findDurableResumeLabel(snapshot: any, stepId: string): string | undefined {
    const matchingLabels = Object.entries(snapshot?.resumeLabels ?? {}).filter(
      ([, target]) => (target as { stepId?: string } | undefined)?.stepId === stepId,
    );
    return matchingLabels.length === 1 ? matchingLabels[0]![0] : undefined;
  }

  #getDurableSuspendedToolCalls(snapshot: any): { toolCalls: AgentRunToolCall[]; hasLabelConflict: boolean } {
    const toolCalls: AgentRunToolCall[] = [];
    let hasLabelConflict = false;
    for (const stepId in snapshot?.context) {
      const step = snapshot.context[stepId];
      if (step?.status !== 'suspended') continue;
      const payload = step.suspendPayload;
      if (!payload || typeof payload !== 'object') continue;

      const approval = payload.requireToolApproval ?? (payload.type === 'approval' ? payload : undefined);
      if (approval) {
        const resumeLabel = this.#findDurableResumeLabel(snapshot, stepId);
        if (!resumeLabel || (approval.toolCallId && approval.toolCallId !== resumeLabel)) {
          hasLabelConflict = true;
          continue;
        }
        toolCalls.push({
          toolCallId: resumeLabel,
          toolName: approval.toolName,
          args: approval.args,
          requiresApproval: true,
        });
      } else if (payload.toolCallSuspended || payload.type === 'suspension') {
        const resumeLabel = this.#findDurableResumeLabel(snapshot, stepId);
        if (!resumeLabel || (payload.toolCallId && payload.toolCallId !== resumeLabel)) {
          hasLabelConflict = true;
          continue;
        }
        toolCalls.push({
          toolCallId: resumeLabel,
          toolName: payload.toolName,
          requiresApproval: false,
          suspendPayload: payload.toolCallSuspended,
        });
      }
    }
    return { toolCalls, hasLabelConflict };
  }

  /**
   * Resolve the owned nested iteration-workflow run id for an outer loop snapshot.
   *
   * Default-engine nested runs share the parent runId, so the same-runId lookup
   * hits. The evented engine assigns derived nested run ids (`wfn:v1:…`, stable
   * across broker redelivery) and records the owned id on the parent step's
   * metadata (`nestedRunId`) — the engine's own native contract for locating
   * owned nested runs. Returns the metadata-named id only when it names a
   * different run. When present, this owned id is authoritative: callers fetch
   * that row exclusively and fail closed when it is missing or malformed, even
   * if a stale same-parent-ID row exists. The same-parent-ID lookup applies
   * only to the legacy/default case without owned metadata. Callers still
   * enforce the full suspended-status / ownership / tool-call pair check, so
   * the fence is unchanged — this only looks in the right place.
   */
  #resolveNestedExecutionRunId(runId: string, snapshot: any): string | undefined {
    const nestedRunId = snapshot?.context?.[DurableStepIds.AGENTIC_EXECUTION]?.metadata?.nestedRunId;
    return typeof nestedRunId === 'string' && nestedRunId.length > 0 && nestedRunId !== runId ? nestedRunId : undefined;
  }

  /**
   * Fetch the authoritative nested iteration-workflow run row.
   *
   * When the outer snapshot names an owned nested run id, that id selects the
   * row exclusively — the same-parent-ID row is never consulted, so a stale
   * row under the parent id can neither satisfy nor reject the resume. A
   * missing or malformed owned row surfaces exactly like a missing or
   * malformed same-ID row in the legacy path (absent row vs invalid-snapshot
   * throw), and storage errors propagate to the caller unchanged.
   */
  async #fetchOwnedNestedExecutionRun(
    workflowsStore: WorkflowsStorage,
    runId: string,
    nestedRunId: string | undefined,
  ): Promise<{ nestedRun: any; nestedSnapshot: any }> {
    const targetRunId = nestedRunId ?? runId;
    let nestedRun: any;
    try {
      nestedRun = await workflowsStore.getWorkflowRunById({
        workflowName: DurableStepIds.AGENTIC_EXECUTION,
        runId: targetRunId,
      });
    } catch (cause) {
      if (cause instanceof SyntaxError) {
        throw new MastraError(
          {
            id: 'DURABLE_AGENT_RESUME_INVALID_SNAPSHOT',
            domain: ErrorDomain.AGENT,
            category: ErrorCategory.SYSTEM,
            text: `DurableAgent "${this.name}" found an invalid nested snapshot for run "${runId}".`,
            details: { agentName: this.name, runId },
          },
          cause,
        );
      }
      throw cause;
    }
    if (!nestedRun) return { nestedRun: undefined, nestedSnapshot: undefined };
    let nestedSnapshot: any = nestedRun.snapshot;
    if (typeof nestedSnapshot === 'string') {
      try {
        nestedSnapshot = JSON.parse(nestedSnapshot);
      } catch (cause) {
        throw new MastraError(
          {
            id: 'DURABLE_AGENT_RESUME_INVALID_SNAPSHOT',
            domain: ErrorDomain.AGENT,
            category: ErrorCategory.SYSTEM,
            text: `DurableAgent "${this.name}" found an invalid nested snapshot for run "${runId}".`,
            details: { agentName: this.name, runId },
          },
          cause,
        );
      }
    }
    return { nestedRun, nestedSnapshot };
  }

  async #resolveDurableResumeLabel(
    runId: string,
    requestedToolCallId?: string,
  ): Promise<{ label?: string; persisted: boolean }> {
    const workflowsStore = await this.#mastra?.getStorage()?.getStore('workflows');
    if (!workflowsStore) return { label: requestedToolCallId, persisted: false };
    let persisted;
    try {
      persisted = await workflowsStore.getWorkflowRunById({
        workflowName: DurableStepIds.AGENTIC_LOOP,
        runId,
      });
    } catch (cause) {
      if (!(cause instanceof SyntaxError)) throw cause;
      throw new MastraError(
        {
          id: 'DURABLE_AGENT_RESUME_INVALID_SNAPSHOT',
          domain: ErrorDomain.AGENT,
          category: ErrorCategory.SYSTEM,
          text: `DurableAgent "${this.name}" found an invalid persisted snapshot for run "${runId}".`,
          details: { agentName: this.name, runId },
        },
        cause,
      );
    }
    if (!persisted) return { label: requestedToolCallId, persisted: false };

    let snapshot: any = persisted.snapshot;
    if (typeof snapshot === 'string') {
      try {
        snapshot = JSON.parse(snapshot);
      } catch {
        throw new MastraError({
          id: 'DURABLE_AGENT_RESUME_INVALID_SNAPSHOT',
          domain: ErrorDomain.AGENT,
          category: ErrorCategory.SYSTEM,
          text: `DurableAgent "${this.name}" found an invalid persisted snapshot for run "${runId}".`,
          details: { agentName: this.name, runId },
        });
      }
    }
    // The owned nested id recorded on the outer snapshot is authoritative:
    // resolve it before fetching the nested row so the same-parent-ID row is
    // consulted only for the legacy/default case without owned metadata.
    const ownedNestedRunId = this.#resolveNestedExecutionRunId(runId, snapshot);
    const { nestedRun: nestedPersisted, nestedSnapshot } = await this.#fetchOwnedNestedExecutionRun(
      workflowsStore,
      runId,
      ownedNestedRunId,
    );
    if (snapshot?.status !== 'suspended') {
      throw new MastraError({
        id: 'DURABLE_AGENT_RESUME_RUN_NOT_SUSPENDED',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" cannot resume run "${runId}" because it is not suspended.`,
        details: { agentName: this.name, runId },
      });
    }
    const input = this.#getDurableWorkflowInput(snapshot);
    if (!input || input.agentId !== this.id) {
      throw new MastraError({
        id: 'DURABLE_AGENT_RESUME_AGENT_MISMATCH',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" cannot resume run "${runId}" because it is not owned by this agent.`,
        details: { agentName: this.name, runId },
      });
    }
    if (!this.#durableSnapshotPairIsConsistent(runId, persisted, snapshot, nestedPersisted, nestedSnapshot)) {
      throw new MastraError({
        id: 'DURABLE_AGENT_RESUME_SNAPSHOT_PAIR_CONFLICT',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.SYSTEM,
        text: `DurableAgent "${this.name}" found inconsistent outer and nested snapshots for run "${runId}".`,
        details: { agentName: this.name, runId },
      });
    }

    const { toolCalls, hasLabelConflict } = this.#getDurableSuspendedToolCalls(snapshot);
    if (hasLabelConflict) {
      throw new MastraError({
        id: 'DURABLE_AGENT_RESUME_LABEL_CONFLICT',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.SYSTEM,
        text: `DurableAgent "${this.name}" found conflicting persisted resume labels for run "${runId}".`,
        details: { agentName: this.name, runId },
      });
    }
    if (toolCalls.length === 0) return { label: requestedToolCallId, persisted: true };
    if (requestedToolCallId) {
      const matchingCalls = toolCalls.filter(toolCall => toolCall.toolCallId === requestedToolCallId);
      if (matchingCalls.length === 1) return { label: requestedToolCallId, persisted: true };
      throw new MastraError({
        id: 'DURABLE_AGENT_RESUME_TOOL_CALL_MISMATCH',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" cannot find suspended tool call "${requestedToolCallId}" in run "${runId}".`,
        details: { agentName: this.name, runId, toolCallId: requestedToolCallId },
      });
    }

    const [onlyCall] = toolCalls;
    if (toolCalls.length === 1 && onlyCall?.toolCallId) return { label: onlyCall.toolCallId, persisted: true };
    throw new MastraError({
      id: 'DURABLE_AGENT_RESUME_AMBIGUOUS_TOOL_CALL',
      domain: ErrorDomain.AGENT,
      category: ErrorCategory.USER,
      text: `DurableAgent "${this.name}" requires an exact toolCallId to resume run "${runId}".`,
      details: { agentName: this.name, runId },
    });
  }

  #assertWarmResumeVersionSelectors(
    runId: string,
    entry: RunRegistryEntry,
    options: Pick<DurableAgentResumeOptions<TOutput>, 'requestContext' | 'versions'>,
  ): void {
    const originalVersions = entry.versions;
    const resumeRequestContext = (options.requestContext ?? entry.requestContext) as RequestContext | undefined;
    const requestedVersions = resumeRequestContext?.get(MASTRA_VERSIONS_KEY) as VersionOverrides | undefined;
    let effectiveVersions = mergeVersionOverrides(this.#mastra?.getVersionOverrides(), originalVersions);
    effectiveVersions = mergeVersionOverrides(effectiveVersions, requestedVersions);
    if (options.versions) effectiveVersions = mergeVersionOverrides(effectiveVersions, options.versions);

    if (stableStringify(effectiveVersions ?? {}) !== stableStringify(originalVersions ?? {})) {
      throw new MastraError({
        id: 'DURABLE_AGENT_RESUME_VERSION_MISMATCH',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" cannot resume run "${runId}" with different version selectors.`,
        details: { agentName: this.name, runId },
      });
    }
    if (effectiveVersions && resumeRequestContext) {
      resumeRequestContext.set(MASTRA_VERSIONS_KEY, structuredClone(effectiveVersions));
    }
  }

  async #rehydrateSuspendedRunRegistry(
    runId: string,
    options: Pick<DurableAgentResumeOptions<TOutput>, 'requestContext' | 'memory' | 'versions' | 'actor'> = {},
  ): Promise<{
    requestContext: RequestContext;
    defaultOptions: AgentExecutionOptions<TOutput>;
    resolvedMemory: ResolvedAgentMemory;
  }> {
    const workflowsStore = await this.#mastra?.getStorage()?.getStore('workflows');
    if (!workflowsStore) {
      throw new MastraError({
        id: 'DURABLE_AGENT_RESUME_NO_STORAGE',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" cannot recover suspended run "${runId}" without workflow storage.`,
        details: { agentName: this.name, runId },
      });
    }

    let persisted;
    try {
      persisted = await workflowsStore.getWorkflowRunById({
        workflowName: DurableStepIds.AGENTIC_LOOP,
        runId,
      });
    } catch (cause) {
      if (!(cause instanceof SyntaxError)) throw cause;
      throw new MastraError(
        {
          id: 'DURABLE_AGENT_RESUME_INVALID_SNAPSHOT',
          domain: ErrorDomain.AGENT,
          category: ErrorCategory.SYSTEM,
          text: `DurableAgent "${this.name}" found an invalid persisted snapshot for run "${runId}".`,
          details: { agentName: this.name, runId },
        },
        cause,
      );
    }
    if (!persisted) {
      throw new MastraError({
        id: 'DURABLE_AGENT_RESUME_SNAPSHOT_NOT_FOUND',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" could not find suspended run "${runId}".`,
        details: { agentName: this.name, runId },
      });
    }

    let snapshot: any = persisted.snapshot;
    if (typeof snapshot === 'string') {
      try {
        snapshot = JSON.parse(snapshot);
      } catch (cause) {
        throw new MastraError(
          {
            id: 'DURABLE_AGENT_RESUME_INVALID_SNAPSHOT',
            domain: ErrorDomain.AGENT,
            category: ErrorCategory.SYSTEM,
            text: `DurableAgent "${this.name}" found an invalid persisted snapshot for run "${runId}".`,
            details: { agentName: this.name, runId },
          },
          cause,
        );
      }
    }
    // The owned nested id recorded on the outer snapshot is authoritative:
    // resolve it before fetching the nested row so the same-parent-ID row is
    // consulted only for the legacy/default case without owned metadata.
    // A missing or malformed owned row fails closed even when a stale
    // same-parent-ID row exists — the pair check below then decides on the
    // actual owned row.
    const ownedNestedRunId = this.#resolveNestedExecutionRunId(runId, snapshot);
    const { nestedRun: nestedPersisted, nestedSnapshot } = await this.#fetchOwnedNestedExecutionRun(
      workflowsStore,
      runId,
      ownedNestedRunId,
    );

    const workflowInput = this.#getDurableWorkflowInput(snapshot);
    if (!workflowInput) {
      throw new MastraError({
        id: 'DURABLE_AGENT_RESUME_INVALID_SNAPSHOT',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.SYSTEM,
        text: `DurableAgent "${this.name}" found no durable-agent input in run "${runId}".`,
        details: { agentName: this.name, runId },
      });
    }

    const persistedMemoryInfo = workflowInput.messageListState?.memoryInfo ?? undefined;
    const threadIds = [workflowInput.state?.threadId, persistedMemoryInfo?.threadId].filter(
      (value): value is string => typeof value === 'string' && value.length > 0,
    );
    const resourceIds = [persisted.resourceId, workflowInput.state?.resourceId, persistedMemoryInfo?.resourceId].filter(
      (value): value is string => typeof value === 'string' && value.length > 0,
    );
    if (new Set(threadIds).size > 1 || new Set(resourceIds).size > 1) {
      throw new MastraError({
        id: 'DURABLE_AGENT_RESUME_SNAPSHOT_OWNER_CONFLICT',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.SYSTEM,
        text: `DurableAgent "${this.name}" found conflicting persisted ownership for run "${runId}".`,
        details: { agentName: this.name, runId },
      });
    }
    const threadId = threadIds[0];
    const resourceId = resourceIds[0];
    const observationalMemorySourceWriteGuard =
      workflowInput.options?.observationalMemorySourceWriteGuard ??
      workflowInput.messageListState?.memoryInfo?.observationalMemorySourceWriteGuard;
    const callerRequestContext = options.requestContext ?? new RequestContext();
    const requestContext: RequestContext = workflowInput.requestContextEntries
      ? new RequestContext(Object.entries(workflowInput.requestContextEntries) as Iterable<readonly [string, unknown]>)
      : new RequestContext();
    for (const [key, value] of callerRequestContext.entries()) {
      requestContext.set(key, value);
    }
    // Caller context is fresh authority, but `MastraMemory` is framework-owned
    // run state. Never let a parent/caller thread replace the persisted owner.
    if (threadId && resourceId) {
      requestContext.set('MastraMemory', {
        thread: { id: threadId },
        resourceId,
        memoryConfig: workflowInput.state?.memoryConfig,
        ...(observationalMemorySourceWriteGuard ? { observationalMemorySourceWriteGuard } : {}),
      });
    } else {
      requestContext.delete('MastraMemory');
    }
    const requestVersions = callerRequestContext.get(MASTRA_VERSIONS_KEY) as VersionOverrides | undefined;
    let mergedVersions = mergeVersionOverrides(this.#mastra?.getVersionOverrides(), workflowInput.versions);
    mergedVersions = mergeVersionOverrides(mergedVersions, requestVersions);
    if (options.versions) {
      mergedVersions = mergeVersionOverrides(mergedVersions, options.versions);
    }
    if (mergedVersions) {
      requestContext.set(MASTRA_VERSIONS_KEY, mergedVersions);
    }
    if (
      workflowInput.requiredRequestContextCapabilities?.authToken === true &&
      !callerRequestContext.has(MASTRA_AUTH_TOKEN_KEY)
    ) {
      throw new MastraError({
        id: 'DURABLE_AGENT_RESUME_AUTH_TOKEN_REQUIRED',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" requires a fresh trusted auth token to recover run "${runId}".`,
        details: { agentName: this.name, runId },
      });
    }
    // Persisted context is runtime input, never proof of the current caller.
    // Ownership must come from the live request context (or explicit memory
    // coordinates when FGA is not configured).
    const resourceIdFromContext = callerRequestContext.get(MASTRA_RESOURCE_ID_KEY);
    const hasFga = Boolean(this.#mastra?.getServer()?.fga);
    const callerResourceId = resourceIdFromContext ?? (hasFga ? undefined : options.memory?.resource);
    const requestedThread = options.memory?.thread;
    const memoryThreadId = typeof requestedThread === 'string' ? requestedThread : requestedThread?.id;
    const threadIdFromContext = callerRequestContext.get(MASTRA_THREAD_ID_KEY);
    const callerThreadId = threadIdFromContext ?? (hasFga ? undefined : memoryThreadId);

    // Cold recovery has no trusted in-process reservation. Fail closed unless
    // the caller proves the exact persisted resource and, when supplied, thread.
    if (!resourceId || !threadId || callerResourceId !== resourceId || callerThreadId !== threadId) {
      throw new MastraError({
        id: 'DURABLE_AGENT_RESUME_OWNER_MISMATCH',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" cannot recover run "${runId}" from a different thread or resource.`,
        details: { agentName: this.name, runId },
      });
    }

    if (snapshot?.status !== 'suspended') {
      throw new MastraError({
        id: 'DURABLE_AGENT_RESUME_RUN_NOT_SUSPENDED',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" cannot resume run "${runId}" because it is not suspended.`,
        details: { agentName: this.name, runId },
      });
    }
    if (workflowInput.agentId !== this.id) {
      throw new MastraError({
        id: 'DURABLE_AGENT_RESUME_AGENT_MISMATCH',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" cannot resume run "${runId}" because it is not owned by this agent.`,
        details: { agentName: this.name, runId },
      });
    }
    assertDurableToolHookPolicyAvailable({ serialized: workflowInput.options.toolHookPolicy });
    if (!this.#durableSnapshotPairIsConsistent(runId, persisted, snapshot, nestedPersisted, nestedSnapshot)) {
      throw new MastraError({
        id: 'DURABLE_AGENT_RESUME_SNAPSHOT_PAIR_CONFLICT',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.SYSTEM,
        text: `DurableAgent "${this.name}" found inconsistent outer and nested snapshots for run "${runId}".`,
        details: { agentName: this.name, runId },
      });
    }

    await this.#assertPublicAgentResumePreflight({
      requestContext,
      memory: options.memory,
      runId,
      snapshotMemoryInfo: { threadId, resourceId },
      actor: options.actor,
    });

    if (stableStringify(mergedVersions ?? {}) !== stableStringify(workflowInput.versions ?? {})) {
      throw new MastraError({
        id: 'DURABLE_AGENT_RESUME_VERSION_MISMATCH',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" cannot recover run "${runId}" with different version selectors.`,
        details: { agentName: this.name, runId },
      });
    }
    const messageList = new MessageList({ threadId, resourceId });
    try {
      messageList.deserialize(workflowInput.messageListState);
    } catch (cause) {
      throw new MastraError(
        {
          id: 'DURABLE_AGENT_RESUME_INVALID_MESSAGE_STATE',
          domain: ErrorDomain.AGENT,
          category: ErrorCategory.SYSTEM,
          text: `DurableAgent "${this.name}" could not restore messages for run "${runId}".`,
          details: { agentName: this.name, runId },
        },
        cause,
      );
    }

    const memoryConfig = workflowInput.state?.memoryConfig;
    const defaultOptionsPromise = this.#wrappedAgent.getDefaultOptions({ requestContext });
    const memoryPromise = this.#wrappedAgent.getMemory({ requestContext });
    const resolvedMemoryPromise = memoryPromise.then(value => ({ value }));
    const resolvedModelsPromise = this.#wrappedAgent.__resolveExecutionModels({ requestContext });
    const recoveredModelsPromise = resolvedModelsPromise.then(resolvedModels =>
      rebindRecoveredExecutionModels(workflowInput.modelList, resolvedModels),
    );
    const [
      resolvedDefaultOptions,
      tools,
      recoveredModels,
      memory,
      workspace,
      inputProcessors,
      outputProcessors,
      errorProcessors,
    ] = await Promise.all([
      defaultOptionsPromise,
      Promise.all([defaultOptionsPromise, resolvedMemoryPromise, recoveredModelsPromise]).then(
        ([resolvedDefaultOptions, resolvedMemory, recoveredModels]) =>
          this.#wrappedAgent.getToolsForExecution({
            threadId,
            resourceId,
            runId,
            requestContext,
            memoryConfig,
            autoResumeSuspendedTools: workflowInput.options?.autoResumeSuspendedTools,
            agentId: this.id,
            agentName: this.name,
            resolvedDefaultOptions: resolvedDefaultOptions as AgentExecutionOptions<any>,
            resolvedMemory,
            resolvedModel: recoveredModels.model,
            processorMessages: messageList.get.all.db(),
          }),
      ),
      recoveredModelsPromise,
      memoryPromise,
      this.#wrappedAgent.getWorkspace({ requestContext }),
      resolvedMemoryPromise.then(resolvedMemory =>
        this.#wrappedAgent.listInputProcessors(requestContext, resolvedMemory),
      ),
      resolvedMemoryPromise.then(resolvedMemory =>
        this.#wrappedAgent.listOutputProcessors(requestContext, resolvedMemory),
      ),
      this.#wrappedAgent.listErrorProcessors(requestContext),
    ]);
    const { model, modelList } = recoveredModels;
    if (
      workflowInput.hasProcessors ||
      inputProcessors.length > 0 ||
      outputProcessors.length > 0 ||
      errorProcessors.length > 0
    ) {
      throw new MastraError({
        id: 'DURABLE_AGENT_RESUME_PROCESSOR_STATE_UNRECOVERABLE',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" cannot cold-recover run "${runId}" because request-local processor state was not persisted.`,
        details: { agentName: this.name, runId },
      });
    }
    const suspendedToolCalls = this.#getDurableSuspendedToolCalls(snapshot);
    if (suspendedToolCalls.hasLabelConflict) {
      throw new MastraError({
        id: 'DURABLE_AGENT_RESUME_LABEL_CONFLICT',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.SYSTEM,
        text: `DurableAgent "${this.name}" found conflicting persisted resume labels for run "${runId}".`,
        details: { agentName: this.name, runId },
      });
    }
    const missingToolNames = suspendedToolCalls.toolCalls
      .map(toolCall => toolCall.toolName)
      .filter((toolName): toolName is string => Boolean(toolName && !tools[toolName]));
    if (missingToolNames.length > 0) {
      throw new MastraError({
        id: 'DURABLE_AGENT_RESUME_TOOL_NOT_FOUND',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" cannot restore suspended run "${runId}" because its tool is no longer available.`,
        details: {
          agentName: this.name,
          runId,
          toolNames: [...new Set(missingToolNames)].join(', '),
        },
      });
    }
    if (
      stableStringify({
        memory: createRuntimeDependencyFingerprint(memory),
        workspace: createRuntimeDependencyFingerprint(workspace),
      }) !== stableStringify(workflowInput.runtimeBindings ?? {})
    ) {
      throw new MastraError({
        id: 'DURABLE_AGENT_RESUME_RUNTIME_BINDING_MISMATCH',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" cannot recover run "${runId}" because its runtime services changed.`,
        details: { agentName: this.name, runId },
      });
    }
    if (stableStringify(serializeToolsMetadata(tools)) !== stableStringify(workflowInput.toolsMetadata)) {
      throw new MastraError({
        id: 'DURABLE_AGENT_RESUME_TOOL_BINDING_MISMATCH',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" cannot recover run "${runId}" because its resolved tools changed.`,
        details: { agentName: this.name, runId },
      });
    }
    if (
      stableStringify(serializeModelConfig(model as MastraLanguageModel)) !== stableStringify(workflowInput.modelConfig)
    ) {
      throw new MastraError({
        id: 'DURABLE_AGENT_RESUME_MODEL_BINDING_MISMATCH',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" cannot recover run "${runId}" because its resolved model changed.`,
        details: { agentName: this.name, runId },
      });
    }
    if (
      stableStringify(modelList ? serializeModelList(modelList) : undefined) !==
      stableStringify(workflowInput.modelList)
    ) {
      throw new MastraError({
        id: 'DURABLE_AGENT_RESUME_MODEL_BINDING_MISMATCH',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" cannot recover run "${runId}" because its resolved model list changed.`,
        details: { agentName: this.name, runId },
      });
    }
    const saveQueueManager = memory ? new SaveQueueManager({ logger: this.#mastra?.getLogger(), memory }) : undefined;
    const registryEntry: RunRegistryEntry = {
      returnScorerData: workflowInput.options?.returnScorerData,
      runtimeBindingId: workflowInput.runtimeBindingId ?? crypto.randomUUID(),
      agentId: this.id,
      threadId,
      resourceId,
      tools,
      model: model as MastraLanguageModel,
      modelList,
      memory,
      saveQueueManager,
      workspace,
      requestContext: snapshotAgentExecutionValue(requestContext),
      versions: workflowInput.versions ? structuredClone(workflowInput.versions) : undefined,
      inputProcessors,
      llmRequestInputProcessors: [],
      outputProcessors,
      errorProcessors,
      processorStates: new Map(),
      drainPendingSignals: (scope?: 'pending' | 'pre-run') =>
        this.#wrappedAgent.__getDrainPendingSignals()(runId, scope),
      backgroundTaskManager: this.#mastra?.backgroundTaskManager,
      backgroundTasksConfig: this.#wrappedAgent.getBackgroundTasksConfig(),
      messageList,
      cleanup: () => {},
    };
    this.#coordinateRegistryCleanup(runId, registryEntry);
    this.#assertNoRegistryCollision(runId);
    this.#runRegistry.registerWithMessageList(runId, registryEntry, messageList, { threadId, resourceId });
    registerGlobalRunRegistryEntry(runId, registryEntry);
    return {
      requestContext,
      defaultOptions: resolvedDefaultOptions as unknown as AgentExecutionOptions<TOutput>,
      resolvedMemory: { value: memory },
    };
  }

  // ===========================================================================
  // Public API
  // ===========================================================================

  /** List this durable agent's suspended runs from durable workflow storage. */
  override async listSuspendedRuns(options: AgentListSuspendedRunsOptions = {}): Promise<AgentListSuspendedRunsResult> {
    options = snapshotAgentExecutionOptions(options, [
      'threadId',
      'resourceId',
      'requestContext',
      'fromDate',
      'toDate',
      'perPage',
      'page',
    ]);
    const { threadId, resourceId, requestContext, fromDate, toDate, perPage, page } = options;
    if (perPage !== undefined && (!Number.isInteger(perPage) || perPage <= 0)) {
      throw new MastraError({
        id: 'DURABLE_AGENT_LIST_SUSPENDED_RUNS_INVALID_PER_PAGE',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" listSuspendedRuns() requires perPage to be a positive integer.`,
        details: { agentName: this.name, perPage },
      });
    }
    if (page !== undefined && (!Number.isInteger(page) || page < 0)) {
      throw new MastraError({
        id: 'DURABLE_AGENT_LIST_SUSPENDED_RUNS_INVALID_PAGE',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" listSuspendedRuns() requires page to be a non-negative integer.`,
        details: { agentName: this.name, page },
      });
    }

    const hasFga = Boolean(this.#mastra?.getServer()?.fga);
    const contextResourceId = requestContext?.get(MASTRA_RESOURCE_ID_KEY);
    const contextThreadId = requestContext?.get(MASTRA_THREAD_ID_KEY);
    if (hasFga && (typeof contextResourceId !== 'string' || contextResourceId.trim().length === 0)) {
      throw new MastraError({
        id: 'AGENT_LIST_SUSPENDED_RUNS_OWNER_UNVERIFIED',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" listSuspendedRuns() requires a verified resource id.`,
        details: { agentName: this.name },
      });
    }
    if (typeof contextResourceId === 'string' && resourceId && contextResourceId !== resourceId) {
      throw new MastraError({
        id: 'AGENT_LIST_SUSPENDED_RUNS_OWNER_MISMATCH',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" listSuspendedRuns() cannot list a different resource.`,
        details: { agentName: this.name },
      });
    }
    if (typeof contextThreadId === 'string' && threadId && contextThreadId !== threadId) {
      throw new MastraError({
        id: 'AGENT_LIST_SUSPENDED_RUNS_THREAD_MISMATCH',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" listSuspendedRuns() cannot list a different thread.`,
        details: { agentName: this.name },
      });
    }
    const scopedResourceId =
      typeof contextResourceId === 'string' && contextResourceId.trim().length > 0 ? contextResourceId : resourceId;
    const scopedThreadId =
      typeof contextThreadId === 'string' && contextThreadId.trim().length > 0 ? contextThreadId : threadId;
    await this.#assertPublicAgentResumePreflight({
      requestContext,
      memory: scopedResourceId && scopedThreadId ? { resource: scopedResourceId, thread: scopedThreadId } : undefined,
      runId: 'list-suspended-runs',
    });

    const workflowsStore = await this.#mastra?.getStorage()?.getStore('workflows');
    if (!workflowsStore) {
      throw new MastraError({
        id: 'AGENT_LIST_SUSPENDED_RUNS_NO_STORAGE',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" listSuspendedRuns() requires workflow storage.`,
        details: { agentName: this.name },
      });
    }

    const [{ runs }, { runs: nestedRuns }] = await Promise.all([
      workflowsStore.listWorkflowRuns({
        workflowName: 'durable-agentic-loop',
        status: 'suspended',
        resourceId: scopedResourceId,
        fromDate,
        toDate,
        perPage: false,
      }),
      workflowsStore.listWorkflowRuns({
        workflowName: DurableStepIds.AGENTIC_EXECUTION,
        status: 'suspended',
        resourceId: scopedResourceId,
        perPage: false,
      }),
    ]);
    const nestedRunsById = new Map(nestedRuns.map(run => [run.runId, run]));
    const matchedRuns: AgentListSuspendedRunsResult['runs'] = [];
    for (const run of runs) {
      let snapshot: any = run.snapshot;
      if (typeof snapshot === 'string') {
        try {
          snapshot = JSON.parse(snapshot);
        } catch {
          continue;
        }
      }
      if (snapshot?.status !== 'suspended') continue;
      const input = this.#getDurableWorkflowInput(snapshot);
      if (!input || input.agentId !== this.id) continue;

      const memoryInfo = input.messageListState?.memoryInfo ?? undefined;
      const threadIds = [input.state?.threadId, memoryInfo?.threadId].filter(
        (value): value is string => typeof value === 'string' && value.length > 0,
      );
      const resourceIds = [run.resourceId, input.state?.resourceId, memoryInfo?.resourceId].filter(
        (value): value is string => typeof value === 'string' && value.length > 0,
      );
      if (new Set(threadIds).size > 1 || new Set(resourceIds).size > 1) continue;
      const runThreadId = threadIds[0];
      const runResourceId = resourceIds[0];
      if (scopedThreadId && scopedThreadId !== runThreadId) continue;
      if (scopedResourceId && scopedResourceId !== runResourceId) continue;

      // The owned nested id recorded on the outer snapshot is authoritative:
      // when present, select that row exclusively so a stale same-parent-ID
      // row can neither satisfy nor reject the pair check below. The
      // same-parent-ID row applies only to the legacy/default case without
      // owned metadata. A missing or malformed owned row skips this run.
      const nestedRunId = this.#resolveNestedExecutionRunId(run.runId, snapshot);
      const nestedRun = nestedRunId ? nestedRunsById.get(nestedRunId) : nestedRunsById.get(run.runId);
      let nestedSnapshot: any = nestedRun?.snapshot;
      if (typeof nestedSnapshot === 'string') {
        try {
          nestedSnapshot = JSON.parse(nestedSnapshot);
        } catch {
          continue;
        }
      }
      if (!this.#durableSnapshotPairIsConsistent(run.runId, run, snapshot, nestedRun, nestedSnapshot)) continue;

      matchedRuns.push({
        runId: run.runId,
        status: 'suspended',
        workflowName: 'durable-agentic-loop',
        threadId: runThreadId,
        resourceId: runResourceId,
        suspendedAt: run.updatedAt,
        toolCalls: this.#getDurableSuspendedToolCalls(snapshot).toolCalls,
      });
    }

    const total = matchedRuns.length;
    const paginatedRuns =
      perPage !== undefined && page !== undefined
        ? matchedRuns.slice(page * perPage, (page + 1) * perPage)
        : matchedRuns;
    return { runs: paginatedRuns, total };
  }

  /** Preserve caller IDs exactly, but retry the vanishingly rare generated-ID collision. */
  #allocateRunId(requestedRunId?: string): string {
    if (requestedRunId !== undefined) {
      if (this.#runRegistry.has(requestedRunId) || globalRunRegistry.has(requestedRunId)) {
        throw new Error(
          `Durable run ${requestedRunId} is already active. Refusing to replace its runtime dependencies.`,
        );
      }
      return requestedRunId;
    }

    for (let attempt = 0; attempt < 32; attempt++) {
      const candidate = crypto.randomUUID();
      if (!this.#runRegistry.has(candidate) && !globalRunRegistry.has(candidate)) return candidate;
    }

    throw new Error('Unable to allocate a unique durable run identifier after 32 attempts.');
  }

  // ===========================================================================
  // Public API
  // ===========================================================================

  /**
   * Stream a response from the agent using durable execution.
   */
  // @ts-expect-error - Intentionally different signature for durable execution
  async stream(
    messages: MessageListInput,
    options?: DurableAgentStreamOptions<TOutput>,
  ): Promise<DurableAgentStreamResult<TOutput>> {
    messages = snapshotAgentExecutionValue(messages);
    options = options
      ? snapshotAgentExecutionOptions(options, ['runId', 'requestContext', 'memory', 'versions'])
      : undefined;
    const idleLoopHandoff = (
      options as
        | (DurableAgentStreamOptions<TOutput> & {
            [DURABLE_STREAM_UNTIL_IDLE_HANDOFF]?: DurableStreamUntilIdleHandoff<TOutput>;
          })
        | undefined
    )?.[DURABLE_STREAM_UNTIL_IDLE_HANDOFF];
    if (options && idleLoopHandoff) {
      const { [DURABLE_STREAM_UNTIL_IDLE_HANDOFF]: _idleLoopHandoff, ...publicOptions } =
        options as DurableAgentStreamOptions<TOutput> & {
          [DURABLE_STREAM_UNTIL_IDLE_HANDOFF]?: DurableStreamUntilIdleHandoff<TOutput>;
        };
      options = publicOptions;
    }

    // Delegate to the idle-loop wrapper when `untilIdle` is set.
    // Strip `untilIdle` before passing to the wrapper so its internal
    // agent.stream() call doesn't recurse.
    if (options?.untilIdle) {
      const { untilIdle, ...rest } = options;
      const maxIdleMs = typeof untilIdle === 'object' ? untilIdle.maxIdleMs : undefined;
      let preflightedDefaultOptions: AgentExecutionOptions<TOutput> | undefined;
      if (this.requestContextSchema || this.#mastra?.getServer()?.fga) {
        // Resolve dynamic defaults exactly once per public call and preflight
        // with the merged options, so the effective default actor (and any
        // defaults-provided context) is what gets authorized. The resolved
        // defaults are handed to the idle wrapper so they are never
        // re-resolved for the inner stream segments.
        preflightedDefaultOptions = (await this.getDefaultOptions({
          requestContext: rest.requestContext,
        })) as AgentExecutionOptions<TOutput>;
        const resolvedOptions = mergeAgentExecutionOptions(
          preflightedDefaultOptions as Record<string, any>,
          rest as Record<string, any>,
        ) as AgentExecutionOptions<TOutput>;
        // Actor is a per-call trust signal: an explicit value replaces the
        // default actor as a whole rather than field-merging with it.
        if (rest.actor !== undefined) resolvedOptions.actor = rest.actor;
        await this.#assertPublicAgentResumePreflight({
          requestContext: resolvedOptions.requestContext,
          memory: resolvedOptions.memory,
          runId: rest.runId,
          actor: resolvedOptions.actor,
        });
      }
      return runDurableStreamUntilIdle<TOutput>(
        this as unknown as DurableAgent<any, any, TOutput>,
        messages,
        { ...rest, maxIdleMs },
        {
          activeStreams: this.#activeStreamUntilIdle,
          bgManager: this.#mastra?.backgroundTaskManager,
          prepareContinuation:
            this.requestContextSchema || this.#mastra?.getServer()?.fga
              ? async resolvedOptions => {
                  await this.#assertPublicAgentResumePreflight({
                    requestContext: resolvedOptions.requestContext,
                    memory: resolvedOptions.memory,
                    actor: resolvedOptions.actor,
                  });
                  return resolvedOptions;
                }
              : undefined,
        },
        preflightedDefaultOptions,
      );
    }

    const allocatedRunId = options?.runId ?? this.#allocateRunId();
    let releaseRunIdReservation: (() => void) | undefined;
    let reusedPreparedExecution = false;
    let preparation: PreparationResult<TOutput> | undefined;
    try {
      const prepared = this.#preparedExecutions.get(allocatedRunId)?.preparation;
      if (prepared) {
        const snapshotMemoryInfo = { threadId: prepared.threadId, resourceId: prepared.resourceId };
        await this.#assertPublicAgentResumePreflight({
          requestContext: options?.requestContext,
          memory: options?.memory,
          runId: allocatedRunId,
          snapshotMemoryInfo,
          actor: options?.actor,
        });
        const requestFingerprint = this.#preparedRequestFingerprint(messages, options);
        const matchedPreparation = this.#getPreparedExecution(allocatedRunId, requestFingerprint);
        if (!matchedPreparation) this.#throwRunIdConflict(allocatedRunId);
        preparation = matchedPreparation;
        releaseRunIdReservation = await this.#reserveRunId(allocatedRunId, {
          agentId: preparation.registryEntry.agentId,
          threadId: preparation.threadId,
          resourceId: preparation.resourceId,
        });
        await this.#assertPublicAgentResumePreflight({
          requestContext: options?.requestContext,
          memory: options?.memory,
          runId: allocatedRunId,
          snapshotMemoryInfo,
          actor: options?.actor,
        });
        if (this.#getPreparedExecution(allocatedRunId, requestFingerprint) !== preparation) {
          this.#throwRunIdConflict(allocatedRunId);
        }
        reusedPreparedExecution = true;
      } else {
        if (options?.runId !== undefined) {
          await this.#assertPublicAgentResumePreflight({
            requestContext: options.requestContext,
            memory: options.memory,
            runId: allocatedRunId,
            actor: options.actor,
          });
          releaseRunIdReservation = await this.#reserveRunId(allocatedRunId);
        }
        preparation = await prepareForDurableExecution<TOutput>({
          agent: this.#wrappedAgent as Agent<string, any, TOutput>,
          messages,
          options: options as AgentExecutionOptions<TOutput>,
          resolvedDefaultOptions: idleLoopHandoff?.defaultOptions,
          resolvedMemory: idleLoopHandoff?.resolvedMemory,
          durableRequestContextKeys: this.#durableRequestContextKeys,
          runId: allocatedRunId,
          requestContext: options?.requestContext,
          logger: this.logger,
          mastra: this.#mastra,
          durableAgentId: this.id,
          durableAgentName: this.name,
        });
        preparation.workflowInput.runtimeResolution = 'registry-required';
      }
    } finally {
      releaseRunIdReservation?.();
    }

    if (!preparation) throw new Error('Durable execution preparation did not complete.');
    const { runId, messageId, workflowInput, registryEntry, messageList, threadId, resourceId } = preparation;

    // 1a. Install the abort controller for this run. The controller is owned
    // by this DurableAgent instance; the result's abort() method flips it,
    // and the durable LLM-execution step reads `abortSignal` off the registry
    // to thread it into the model call + abort short-circuits. If the caller
    // also supplied an external signal, forward its abort to the internal
    // controller so either source can cancel the run.
    const abortController = new AbortController();
    if (options?.abortSignal) {
      if (options.abortSignal.aborted) {
        abortController.abort((options.abortSignal as AbortSignal & { reason?: unknown }).reason);
      } else {
        options.abortSignal.addEventListener(
          'abort',
          () => abortController.abort((options.abortSignal as AbortSignal & { reason?: unknown }).reason),
          { once: true },
        );
      }
    }
    if (agentThreadStreamRuntime.isRunAborted(runId, this.getPubSub())) {
      abortController.abort();
    }
    this.#installAbortWithTotalTimeout(registryEntry, abortController);

    // 2. Register one exact non-serializable capability in both registries.
    if (reusedPreparedExecution) {
      this.#consumePreparedExecution(runId);
    } else {
      registryEntry.messageList = messageList;
      this.#coordinateRegistryCleanup(runId, registryEntry);
      registerGlobalRunRegistryEntry(runId, registryEntry);
      this.#runRegistry.registerWithMessageList(runId, registryEntry, messageList, { threadId, resourceId });
    }
    const runtimeBindingId = registryEntry.runtimeBindingId;

    // Track cleanup state to avoid double cleanup
    let cleanedUp = false;
    let autoCleanupTimer: ReturnType<typeof setTimeout> | null = null;
    // Assigned once the stream is created below. Declared here so the shared
    // performCleanup() closure can unsubscribe the pubsub reader (via
    // streamCleanup) from both the auto-cleanup timer and the explicit
    // cleanup() path — mirroring observe().
    let streamCleanup: (() => void) | undefined;

    // Single cleanup path for both the auto-cleanup timer and the explicit
    // cleanup(). Revokes continuation before unsubscribing the pubsub reader,
    // then tears down the registry entries and pubsub topic. Idempotent via
    // `cleanedUp`.
    const performCleanup = () => {
      if (autoCleanupTimer) {
        clearTimeout(autoCleanupTimer);
        autoCleanupTimer = null;
      }
      if (cleanedUp) return;

      agentThreadStreamRuntime.closeRunContinuation(output, this.getPubSub());
      streamCleanup?.();
      // Registry teardown stays keyed to the runtime binding observed when this
      // handle was created: a reused runId must never have a newer execution's
      // entry removed by an older observer.
      if (runtimeBindingId) this.#cleanupBoundRun(runId, runtimeBindingId);
      cleanedUp = true;
    };

    // Schedule automatic registry cleanup after stream ends
    const scheduleAutoCleanup = () => {
      if (autoCleanupTimer || cleanedUp || this.#cleanupTimeoutMs === 0) return;
      autoCleanupTimer = setTimeout(performCleanup, this.#cleanupTimeoutMs);
    };

    // Whether this caller's stream closes at the suspension boundary. Defaults
    // to false (stream stays open for a same-reader resume). The same value
    // gates the `across-suspension` continuation so the two cannot drift.
    const closeOnSuspend = options?.closeOnSuspend ?? false;

    // 3. Create the durable agent stream (subscribes to pubsub)
    const {
      output,
      cleanup: createdStreamCleanup,
      ready,
    } = createDurableAgentStream<TOutput>({
      pubsub: this.pubsub,
      runId,
      messageId,
      model: {
        modelId: workflowInput.modelConfig.modelId,
        provider: workflowInput.modelConfig.provider,
        version: 'v3',
      },
      threadId,
      resourceId,
      onChunk: options?.onChunk,
      experimentalTransform: options?.experimentalTransform,
      onStepFinish: options?.onStepFinish,
      onFinish: options?.onFinish,
      onStreamFinished: scheduleAutoCleanup,
      onError: async error => {
        await options?.onError?.(error);
        scheduleAutoCleanup();
      },
      onSuspended: options?.onSuspended,
      onAbort: async data => {
        try {
          await (options?.onAbort as ((event: any) => void | Promise<void>) | undefined)?.(data);
        } finally {
          scheduleAutoCleanup();
        }
      },
      // onIterationComplete is NOT forwarded here — the dowhile predicate
      // now calls it in-process from globalRunRegistry and honors its return
      // value ({ continue, feedback }). The pubsub ITERATION_COMPLETE event
      // still fires for external observability subscribers.
      closeOnSuspend,
      hideSignals: options?.hideSignals,
      structuredOutput: registryEntry.structuredOutput as any,
      outputProcessors: registryEntry.outputProcessors,
      requestContext: registryEntry.requestContext,
      returnScorerData: workflowInput.options.returnScorerData,
      tracingContext: registryEntry.agentSpan ? { currentSpan: registryEntry.agentSpan } : undefined,
      messageList,
    });
    streamCleanup = createdStreamCleanup;

    // 4. Claim the thread before starting the durable workflow, then register
    // the output handle. This gives durable streams the same one-run-per-thread
    // admission invariant as regular Agent.stream(), including the explicit
    // fresh-turn handoff from a retained suspended run.
    const runtimePubSub = this.getPubSub();
    const optionMemory = options?.memory;
    const threadStreamOptions = {
      ...(options ?? {}),
      runId,
      memory: threadId
        ? {
            ...(optionMemory && typeof optionMemory === 'object' ? optionMemory : {}),
            thread: threadId,
            ...(resourceId ? { resource: resourceId } : {}),
          }
        : optionMemory,
    } as AgentExecutionOptions<TOutput> & { _threadRunReservationOwner?: boolean };
    let releaseThreadReservation = agentThreadStreamRuntime.reserveRun(threadStreamOptions, runtimePubSub, this.id);
    try {
      await agentThreadStreamRuntime.waitForCrossAgentThreadRun(
        this as unknown as Agent<any, any, any, any>,
        threadStreamOptions,
        runtimePubSub,
        Boolean(releaseThreadReservation),
      );
      // Read-only runs are outside the reservation protocol (reserveRun and
      // waitForThreadRunReservation both return immediately), so retrying
      // them would spin forever — the same guard Agent.stream() applies.
      const threadMemory = threadStreamOptions.memory;
      const isReadOnlyThreadRun = Boolean(
        threadMemory && typeof threadMemory === 'object' && 'options' in threadMemory && threadMemory.options?.readOnly,
      );
      while (!releaseThreadReservation && threadId && !isReadOnlyThreadRun) {
        releaseThreadReservation = agentThreadStreamRuntime.reserveRun(threadStreamOptions, runtimePubSub, this.id);
        if (releaseThreadReservation) break;
        await agentThreadStreamRuntime.waitForThreadRunReservation(threadStreamOptions, runtimePubSub, this.id);
      }

      // registerRun installs the record synchronously; its return value tracks
      // terminal delivery. Do not await that promise here: a suspended durable
      // stream can intentionally remain open until resume, and stream() must
      // return the output handle before then.
      // Upstream #24265: a stream that stays open across the suspension keeps
      // its registration continuable so a same-process resume re-attaches via
      // continueRun instead of racing a second registration.
      void agentThreadStreamRuntime.registerRun(
        this as unknown as Agent<any, any, any, any>,
        output,
        threadStreamOptions,
        runtimePubSub,
        closeOnSuspend ? undefined : { continuation: 'across-suspension' },
      );
    } catch (error) {
      releaseThreadReservation?.();
      streamCleanup();
      this.#cleanupBoundRun(runId, runtimeBindingId);
      cleanedUp = true;
      throw error;
    }

    // 4b. Wait for the durable subscription to be ready, then execute the
    // workflow. Registration happens first so thread subscribers cannot miss
    // an immediately emitted start/tool event.
    const workflowExecution = ready
      .then(async () => {
        // Emit 'start' chunk before the workflow begins (matches regular agent's stream.ts).
        // Only the initial stream() path emits 'start'; resume() does not.
        await emitChunkEvent(this.pubsub, runId, {
          type: 'start',
          runId,
          from: ChunkFrom.AGENT,
          payload: { id: workflowInput.agentId, messageId },
        });
        if (this.__getGoalConfig()) {
          await beginGoalActivity({
            mastra: this.#mastra,
            agentId: workflowInput.agentId,
            resourceId,
            threadId,
            runId,
            requestContext: globalRunRegistry.get(runId)?.requestContext,
          });
        }
        try {
          return await this.executeWorkflow(runId, workflowInput);
        } finally {
          await stopGoalActivity({ agentId: workflowInput.agentId, runId });
        }
      })
      .catch(error => {
        this.emitErrorInBackground(runId, error);
      });
    const trackedEntry = globalRunRegistry.get(runId);
    if (trackedEntry) {
      trackedEntry.workflowExecution = workflowExecution;
    }

    // 5. Create cleanup function (cancels auto-cleanup timer if called)
    const cleanup = () => {
      if (autoCleanupTimer) {
        clearTimeout(autoCleanupTimer);
        autoCleanupTimer = null;
      }
      if (!cleanedUp) {
        streamCleanup();
        this.#cleanupBoundRun(runId, runtimeBindingId);
        cleanedUp = true;
      }
    };
    const abort = async (reason?: unknown) => {
      if (!abortController.signal.aborted) {
        abortController.abort(reason);
      }
      // Also stop the exact execution captured by this handle. Resolving the
      // binding at abort time could target a later execution that reused runId.
      await this.requestRemoteAbort(runId, runtimeBindingId);
    };

    return {
      output,
      get fullStream() {
        return output.fullStream as ReadableStream<any>;
      },
      runId,
      threadId,
      resourceId,
      cleanup,
      abort,
    };
  }

  /**
   * Resume a suspended workflow execution.
   */
  async resume(
    runId: string,
    resumeData: unknown,
    options?: DurableAgentResumeOptions<TOutput>,
  ): Promise<DurableAgentStreamResult<TOutput>> {
    if (
      typeof (options as DurableAgentStreamOptions<TOutput> | undefined)?.hooks?.beforeToolCall === 'function' ||
      typeof (options as DurableAgentStreamOptions<TOutput> | undefined)?.hooks?.afterToolCall === 'function'
    ) {
      throw new MastraError({
        id: 'DURABLE_AGENT_RESUME_TOOL_HOOKS_UNSUPPORTED',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text:
          'Durable tool hooks are fixed when a run starts and cannot be added or replaced on resume. ' +
          'Configure durable policy on the Agent or pass per-execution hooks to the initial stream or generate call.',
      });
    }

    resumeData = snapshotAgentExecutionValue(resumeData);
    options = options
      ? snapshotAgentExecutionOptions(options, [
          'runId',
          'toolCallId',
          'requestContext',
          'memory',
          'versions',
          '_resolvedDefaultOptions',
        ])
      : undefined;

    // Delegate to the idle-loop wrapper when `untilIdle` is set. Strip
    // `untilIdle` before passing to the wrapper so the inner agent.resume()
    // call (and subsequent agent.stream([]) continuations) don't recurse.
    if (options?.untilIdle) {
      const { untilIdle, ...rest } = options;
      const maxIdleMs = typeof untilIdle === 'object' ? untilIdle.maxIdleMs : undefined;
      let preflightedDefaultOptions: AgentExecutionOptions<TOutput> | undefined;
      if (this.requestContextSchema || this.#mastra?.getServer()?.fga) {
        // Resolve dynamic defaults exactly once per public call and preflight
        // with the merged options — mirrors stream({ untilIdle }); the inner
        // resume segment reuses the resolution via `_resolvedDefaultOptions`.
        preflightedDefaultOptions = (await this.getDefaultOptions({
          requestContext: rest.requestContext,
        })) as AgentExecutionOptions<TOutput>;
        const resolvedOptions = mergeAgentExecutionOptions(
          preflightedDefaultOptions as Record<string, any>,
          rest as Record<string, any>,
        ) as AgentExecutionOptions<TOutput>;
        // Actor is a per-call trust signal: an explicit value replaces the
        // default actor as a whole rather than field-merging with it.
        if (rest.actor !== undefined) resolvedOptions.actor = rest.actor;
        await this.#assertPublicAgentResumePreflight({
          requestContext: resolvedOptions.requestContext,
          memory: resolvedOptions.memory,
          runId,
          actor: resolvedOptions.actor,
        });
        // The wrapper's inner resume() re-runs the public entry for the SAME
        // runId; carry proof of the authorization above on the wrapper-owned
        // `_resolvedDefaultOptions` object (enumerable so it survives the
        // spreads in the idle loop, module-local Symbol so it cannot be forged
        // from outside) so the provider is not consulted twice per call.
        // Shallow-clone first: static `defaultOptions` configs are returned by
        // reference, and the marker must never be stamped onto shared agent
        // config.
        preflightedDefaultOptions = { ...preflightedDefaultOptions };
        Object.defineProperty(preflightedDefaultOptions, RESUME_PREFLIGHT_AUTHORIZED, {
          value: { runId },
          enumerable: true,
          configurable: true,
          writable: true,
        });
      }
      return runResumeDurableStreamUntilIdle<TOutput>(
        this as unknown as DurableAgent<any, any, TOutput>,
        runId,
        resumeData,
        { ...rest, maxIdleMs } as DurableAgentResumeOptions<TOutput> & { maxIdleMs?: number },
        {
          activeStreams: this.#activeStreamUntilIdle,
          bgManager: this.#mastra?.backgroundTaskManager,
          prepareContinuation:
            this.requestContextSchema || this.#mastra?.getServer()?.fga
              ? async resolvedOptions => {
                  await this.#assertPublicAgentResumePreflight({
                    requestContext: resolvedOptions.requestContext,
                    memory: resolvedOptions.memory,
                    actor: resolvedOptions.actor,
                  });
                  return resolvedOptions;
                }
              : undefined,
        },
        preflightedDefaultOptions,
      );
    }

    // Until-idle handoff: when the outer resume({ untilIdle }) branch already
    // fully authorized this public call, its marker rides the resolved-defaults
    // object the wrapper controls. Validate it against THIS runId and strip it
    // before the defaults are merged, so it can only ever skip the duplicate
    // provider authorization for the run it was minted for.
    const carriedResumeDefaults = options?._resolvedDefaultOptions as Record<PropertyKey, unknown> | undefined;
    const carriedResumePreflight = carriedResumeDefaults?.[RESUME_PREFLIGHT_AUTHORIZED] as
      | { runId?: string }
      | undefined;
    if (carriedResumeDefaults && carriedResumePreflight !== undefined) {
      delete carriedResumeDefaults[RESUME_PREFLIGHT_AUTHORIZED];
    }
    const resumeAlreadyAuthorized = carriedResumePreflight?.runId === runId;

    let entry = this.#runRegistry.get(runId);
    let priorExecution = entry?.workflowExecution;
    const warmMemoryInfo = entry ? this.#runRegistry.getMemoryInfo(runId) : undefined;
    const hasFga = Boolean(this.#mastra?.getServer()?.fga);
    const explicitRequestContext = options?.requestContext;
    const registryRequestContext = entry?.requestContext as RequestContext | undefined;
    const initialResumeRequestContext = entry
      ? mergeRegisteredRunRequestContext(
          explicitRequestContext,
          registryRequestContext,
          this.#durableRequestContextKeys,
        )
      : (explicitRequestContext ?? registryRequestContext);
    const contextResourceId = explicitRequestContext?.get(MASTRA_RESOURCE_ID_KEY);
    const contextThreadId = explicitRequestContext?.get(MASTRA_THREAD_ID_KEY);
    const requestedThread = options?.memory?.thread;
    const requestedThreadId = typeof requestedThread === 'string' ? requestedThread : requestedThread?.id;

    // Resolve execution options (dynamic defaults) exactly once and before
    // authorization, so the effective per-call actor — the explicit option or
    // a freshly resolved default, never a persisted one — is the identity that
    // is both authorized and forwarded to the workflow segment.
    const resolvedOptions = (await this.#resolveExecutionOptions({
      ...(options as DurableAgentStreamOptions<TOutput>),
      requestContext: initialResumeRequestContext as DurableAgentStreamOptions<TOutput>['requestContext'],
    })) as DurableAgentResumeOptions<TOutput>;

    // Warm resumes may authorize against the registered request context (the
    // run was admitted through this instance's own preflights). Cold resumes
    // with no explicit caller context fall back to the context the resolved
    // dynamic defaults just supplied — the same context the execution options
    // carry — instead of authorizing against an empty context. The reserved
    // owner ids above still come exclusively from the caller's own explicit
    // context: neither the registered nor the defaults-resolved context ever
    // vouches for a caller's thread ownership, so the fail-closed
    // AGENT_RESUME_OWNER_UNVERIFIED gate below is unaffected.
    const effectiveRequestContext =
      initialResumeRequestContext ?? (resolvedOptions.requestContext as RequestContext | undefined);

    // Fail closed on the caller's principal before revealing anything about
    // the run: with FGA configured, a caller with neither an authenticated
    // user nor a trusted actor is denied outright, without consulting the
    // provider or storage.
    await this.#assertPublicAgentResumePreflight({
      requestContext: effectiveRequestContext,
      memory: options?.memory,
      runId,
      actor: resolvedOptions.actor,
      authorize: false,
    });

    // A registry-warm run that never bound a thread or resource has no owner
    // tuple to verify — FGA authorization below remains its gate. Every other
    // resume (cold, thread-bound, or making an explicit memory claim) must
    // present caller-verified reserved ids.
    const isWarmThreadlessResume =
      entry !== undefined && !warmMemoryInfo?.threadId && !warmMemoryInfo?.resourceId && options?.memory === undefined;
    if (
      hasFga &&
      !isWarmThreadlessResume &&
      (typeof contextResourceId !== 'string' ||
        contextResourceId.trim().length === 0 ||
        typeof contextThreadId !== 'string' ||
        contextThreadId.trim().length === 0)
    ) {
      throw new MastraError({
        id: 'AGENT_RESUME_OWNER_UNVERIFIED',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" requires verified resource and thread ids before resuming run "${runId}".`,
        details: { agentName: this.name, runId },
      });
    }
    if (
      (typeof contextResourceId === 'string' &&
        options?.memory?.resource &&
        options.memory.resource !== contextResourceId) ||
      (typeof contextThreadId === 'string' && requestedThreadId && requestedThreadId !== contextThreadId)
    ) {
      throw new MastraError({
        id: 'DURABLE_AGENT_RESUME_OWNER_MISMATCH',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" cannot resume run "${runId}" from a different thread or resource.`,
        details: { agentName: this.name, runId },
      });
    }
    const callerMemory =
      typeof contextResourceId === 'string' && typeof contextThreadId === 'string'
        ? { resource: contextResourceId, thread: contextThreadId }
        : options?.memory;
    // Single FGA authorization for this resume call, with the effective
    // per-call actor and the warm owner tuple as authorization context. It
    // runs before the warm owner-tuple comparison so a denied caller never
    // learns whether their claim matched. When the until-idle wrapper's outer
    // branch already authorized this exact public call (validated marker
    // above), the provider is not consulted again — the preflight still runs
    // for request-context validation and the principal fail-closed check.
    await this.#assertPublicAgentResumePreflight({
      requestContext: effectiveRequestContext,
      memory:
        callerMemory ??
        (warmMemoryInfo?.threadId && warmMemoryInfo.resourceId
          ? { thread: warmMemoryInfo.threadId, resource: warmMemoryInfo.resourceId }
          : undefined),
      runId,
      snapshotMemoryInfo: warmMemoryInfo,
      actor: resolvedOptions.actor,
      ...(resumeAlreadyAuthorized ? { authorize: false } : {}),
    });
    if (
      (warmMemoryInfo?.resourceId &&
        (typeof contextResourceId === 'string' ? contextResourceId : options?.memory?.resource) &&
        (typeof contextResourceId === 'string' ? contextResourceId : options?.memory?.resource) !==
          warmMemoryInfo.resourceId) ||
      (warmMemoryInfo?.threadId &&
        (typeof contextThreadId === 'string' ? contextThreadId : requestedThreadId) &&
        (typeof contextThreadId === 'string' ? contextThreadId : requestedThreadId) !== warmMemoryInfo.threadId)
    ) {
      throw new MastraError({
        id: 'DURABLE_AGENT_RESUME_OWNER_MISMATCH',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" cannot resume run "${runId}" from a different thread or resource.`,
        details: { agentName: this.name, runId },
      });
    }
    // Fork (PF) ordering: the pinned-version storage read runs only after the
    // caller principal, reserved owner ids and FGA authorization above, so a
    // denied or unverified cold caller never triggers a workflow storage
    // lookup. The delegated fork re-runs its own public preflight.
    // A cold resume must resolve the agent definition the run actually
    // suspended on: cold rehydration rebuilds tools/model/instructions from
    // whatever `this` currently resolves to — which, for status selectors,
    // hot-switches to the latest publish mid-flight. Re-resolve to the pinned
    // id and delegate. An explicit exact version at the call site is an
    // operator escape hatch and wins over the pin, and forks already produced
    // by `resolveVersionedAgent` are left alone (they are either this very
    // delegation or an explicit server-side resolution). Warm resumes skip
    // the storage read; the rest of the cold rehydration (runtime registry
    // rebuild) stays inside the execution segment, after authorization.
    if (!this.#runRegistry.get(runId)) {
      try {
        const workflowsStore = await this.#mastra?.getStorage()?.getStore('workflows');
        const persisted = await workflowsStore?.getWorkflowRunById({
          runId,
          workflowName: DurableStepIds.AGENTIC_LOOP,
        });
        const snapshot =
          typeof persisted?.snapshot === 'string'
            ? (JSON.parse(persisted.snapshot) as WorkflowRunState)
            : persisted?.snapshot;
        const workflowInput = snapshot?.context?.input as DurableAgenticWorkflowInput | undefined;
        if (workflowInput?.__workflowKind === 'durable-agent') {
          const pinnedVersionId = workflowInput.agentVersionId;
          if (pinnedVersionId && this.#mastra && !this.__isStoredVersionApplied()) {
            const callSiteSelector = options?.versions?.agents?.[this.id];
            const hasExplicitVersion = !!callSiteSelector && 'versionId' in callSiteSelector;
            const currentVersionId = this.toRawConfig()?.resolvedVersionId as string | undefined;
            if (!hasExplicitVersion && pinnedVersionId !== currentVersionId) {
              try {
                const resolved = await this.#mastra.resolveVersionedAgent(this as unknown as Agent, {
                  versionId: pinnedVersionId,
                });
                if (resolved !== (this as unknown as Agent)) {
                  return (resolved as unknown as DurableAgent<TAgentId, TTools, TOutput>).resume(
                    runId,
                    resumeData,
                    options,
                  );
                }
              } catch (versionError) {
                // The pinned version may have been deleted while the run sat
                // suspended — resume on the current definition rather than
                // failing at the approver (mirrors Agent#execute's fallback).
                this.logger.warn(
                  'Failed to resolve pinned agent version for durable resume, using current definition',
                  {
                    agentId: this.id,
                    runId,
                    pinnedVersionId,
                    error: versionError,
                  },
                );
              }
            }
          }
        }
      } catch {
        // A malformed or unreadable snapshot falls through: the rehydration
        // in the execution segment owns validation and raises the proper
        // structured error.
      }
    }
    if (entry && !this.#activeRegistryPairIsValid(runId, entry)) entry = undefined;
    if (entry) this.#assertWarmResumeVersionSelectors(runId, entry, options ?? {});

    if (!entry) {
      await this.#rehydrateSuspendedRunRegistry(runId, options);
      entry = this.#runRegistry.get(runId);
    }
    priorExecution ??= entry?.workflowExecution;
    if (priorExecution) {
      await priorExecution.catch(() => {
        // The prior segment already emits its own error event.
      });
      // The prior segment's `executeWorkflow` promise is the native persisted
      // finish receipt (`run.start()` resolves on `workflows-finish` after
      // suspension persistence), so awaiting it is sufficient before the
      // authoritative status/ownership/pair reads below. Cold resumes read
      // storage as the authority with no wait.
    }
    const resolvedResumeLabel = await this.#resolveDurableResumeLabel(runId, options?.toolCallId);
    if (!entry || !this.#activeRegistryPairIsValid(runId, entry)) {
      await this.#rehydrateSuspendedRunRegistry(runId, options);
      entry = this.#runRegistry.get(runId);
    }
    if (!entry || !this.#activeRegistryPairIsValid(runId, entry)) {
      throw new MastraError({
        id: 'DURABLE_AGENT_RESUME_REGISTRY_REHYDRATION_FAILED',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.SYSTEM,
        text: `DurableAgent "${this.name}" could not restore runtime state for run "${runId}".`,
        details: { agentName: this.name, runId },
      });
    }
    const runtimeBindingId = entry.runtimeBindingId;
    const prevalidatedResumeLabel = resolvedResumeLabel.persisted ? resolvedResumeLabel.label : undefined;
    const memoryInfo = this.#runRegistry.getMemoryInfo(runId);

    const bindResumeRequestContext = (): RequestContext | undefined => {
      const context = mergeRegisteredRunRequestContext(
        options?.requestContext,
        entry?.requestContext as RequestContext | undefined,
        this.#durableRequestContextKeys,
      );
      if (entry) entry.requestContext = context;
      const boundGlobalEntry = getBoundRunRegistryEntry(runId, runtimeBindingId);
      if (boundGlobalEntry) boundGlobalEntry.requestContext = context;
      resolvedOptions.requestContext = context as DurableAgentStreamOptions<TOutput>['requestContext'];
      return context;
    };
    let resumeRequestContext = bindResumeRequestContext();

    // Bind the resumed segment to the registered thread/resource. Execution
    // options (defaults, callbacks, actor) were already resolved exactly once
    // before authorization; only patch in what rehydration just made
    // available: the registered memory tuple and — for cold resumes without an
    // explicit caller context — the rehydrated registry context, so runtime
    // registration matches the pre-restructure behavior.
    const registeredMemory = memoryInfo?.threadId
      ? ({
          ...resolvedOptions.memory,
          thread: memoryInfo.threadId,
          resource: memoryInfo.resourceId ?? resolvedOptions.memory?.resource,
        } as DurableAgentStreamOptions<TOutput>['memory'])
      : resolvedOptions.memory;
    resolvedOptions.memory = registeredMemory;

    // Install a fresh abort controller for the resumed segment. The original
    // controller is gone (the stream that owned it has already settled), so
    // we overwrite the registry slot. If the caller passed an external
    // signal, forward it onto the new internal controller.
    const abortController = new AbortController();
    if (resolvedOptions.abortSignal) {
      if (resolvedOptions.abortSignal.aborted) {
        abortController.abort((resolvedOptions.abortSignal as AbortSignal & { reason?: unknown }).reason);
      } else {
        resolvedOptions.abortSignal.addEventListener(
          'abort',
          () => abortController.abort((resolvedOptions.abortSignal as AbortSignal & { reason?: unknown }).reason),
          { once: true },
        );
      }
    }
    if (agentThreadStreamRuntime.isRunAborted(runId, this.getPubSub())) {
      abortController.abort();
    }
    // Re-arm the run-level execution budget for the resumed session (#21724).
    // Warm resumes read the original budget parked on the registry entry;
    // cold resumes restored it from the persisted workflow input during
    // prepare(). A caller-supplied modelSettings on the resume call wins.
    const resumeTotalMs = (resolvedOptions.modelSettings as { timeout?: { totalMs?: number } } | undefined)?.timeout
      ?.totalMs;
    if (resumeTotalMs !== undefined) {
      entry.timeoutTotalMs = resumeTotalMs;
    }
    this.#installAbortWithTotalTimeout(entry, abortController);
    const globalEntryForAbort = getBoundRunRegistryEntry(runId, runtimeBindingId);
    if (globalEntryForAbort) {
      globalEntryForAbort.abortController = abortController;
      globalEntryForAbort.abortSignal = entry.abortSignal;
    }

    // Track cleanup state to avoid double cleanup
    let cleanedUp = false;
    let autoCleanupTimer: ReturnType<typeof setTimeout> | null = null;
    // Assigned once the stream is created below so the shared performCleanup()
    // closure can unsubscribe the pubsub reader (via streamCleanup) from both
    // the auto-cleanup timer and the explicit cleanup() path — mirroring observe().
    let streamCleanup: (() => void) | undefined;

    // Single cleanup path for both the auto-cleanup timer and the explicit
    // cleanup(). Revokes continuation before unsubscribing the pubsub reader,
    // then tears down the registry entries and pubsub topic. Idempotent via
    // `cleanedUp`.
    const performCleanup = () => {
      if (autoCleanupTimer) {
        clearTimeout(autoCleanupTimer);
        autoCleanupTimer = null;
      }
      if (cleanedUp) return;

      agentThreadStreamRuntime.closeRunContinuation(output, this.getPubSub());
      streamCleanup?.();
      // Registry teardown stays keyed to the runtime binding observed when this
      // handle was created: a reused runId must never have a newer execution's
      // entry removed by an older observer.
      if (runtimeBindingId) this.#cleanupBoundRun(runId, runtimeBindingId);
      cleanedUp = true;
    };

    const scheduleAutoCleanup = () => {
      if (autoCleanupTimer || cleanedUp || this.#cleanupTimeoutMs === 0) return;
      autoCleanupTimer = setTimeout(performCleanup, this.#cleanupTimeoutMs);
    };

    const globalEntry = getBoundRunRegistryEntry(runId, runtimeBindingId);
    const resumeModel = globalEntry?.model as any;

    // Settle the prior segment before taking its event offset. Otherwise a late
    // suspension event can be replayed into the new segment and close it early.
    // Re-read the global registry rather than reusing the earlier capture: the
    // warm entry may have been swapped by the second rehydration above, and the
    // new segment's execution is only assigned after this offset read.
    priorExecution = globalRunRegistry.get(runId)?.workflowExecution;
    await priorExecution?.catch(() => {
      /* errors already handled by the prior segment */
    });

    // Skip events already broadcast by the original run (e.g. the SUSPENDED
    // chunk that paused it). Without this, a resume that closes on suspend
    // (resumeGenerate) would immediately close on the replayed SUSPENDED.
    const resumeOffset = await this.#getPubsubOffset(runId);
    if (!entry || !this.#activeRegistryPairIsValid(runId, entry)) {
      this.#throwRunIdConflict(runId);
    }

    // Suspended spans are terminal, so resume on fresh spans in the original
    // trace before constructing the adapter that parents per-chunk work.
    const origTraceId = entry.agentSpan?.traceId;
    const origSpanId = entry.agentSpan?.id;
    if (origTraceId && this.#mastra?.observability) {
      try {
        const ag = this.#wrappedAgent as Agent<string, any, any>;
        const rawConfig = typeof (ag as any).toRawConfig === 'function' ? (ag as any).toRawConfig() : undefined;
        const resolvedVersionId = rawConfig?.resolvedVersionId as string | undefined;
        const agentTracingPolicy = typeof ag.getTracingPolicy === 'function' ? ag.getTracingPolicy() : undefined;
        const resumeAgentSpan = getOrCreateSpan({
          type: SpanType.AGENT_RUN,
          name: `agent run: '${ag.id}' (resumed)`,
          entityType: EntityType.AGENT,
          entityId: ag.id,
          entityName: ag.name,
          metadata: {
            runId,
            resumed: true,
            ...(origSpanId ? { resumedFromSpanId: origSpanId } : {}),
            ...(resolvedVersionId ? { entityVersionId: resolvedVersionId } : {}),
          },
          tracingPolicy: agentTracingPolicy,
          tracingOptions: { traceId: origTraceId },
          requestContext: resolvedOptions.requestContext,
          mastra: this.#mastra,
        });
        const resumeModelSpan = resumeAgentSpan?.createChildSpan({
          type: SpanType.MODEL_GENERATION,
          name: `llm: '${resumeModel?.modelId ?? ''}'`,
          attributes: { model: resumeModel?.modelId, provider: resumeModel?.provider, streaming: true },
          metadata: { runId, resumed: true },
          requestContext: resolvedOptions.requestContext,
        });
        for (const registryEntry of [entry, getBoundRunRegistryEntry(runId, runtimeBindingId)]) {
          if (!registryEntry) continue;
          registryEntry.resumeAgentSpan = resumeAgentSpan;
          registryEntry.resumeModelSpan = resumeModelSpan;
          registryEntry.resumeAgentSpanData = resumeAgentSpan?.exportSpan();
          registryEntry.resumeModelSpanData = resumeModelSpan?.exportSpan();
        }
      } catch (error) {
        this.#mastra?.getLogger?.()?.warn?.(`[DurableAgent] Failed to open resume spans: ${error}`);
      }
    }
    const resumeSegmentSpan = entry.resumeAgentSpan ?? entry.agentSpan;

    // Same default-false semantics as the initial stream() path.
    const closeOnSuspend = (resolvedOptions as DurableAgentStreamOptions<TOutput>).closeOnSuspend ?? false;

    const {
      output,
      cleanup: createdStreamCleanup,
      ready,
    } = createDurableAgentStream<TOutput>({
      pubsub: this.pubsub,
      runId,
      messageId: crypto.randomUUID(),
      model: {
        modelId: resumeModel?.modelId,
        provider: resumeModel?.provider,
        version: 'v3',
      },
      threadId: memoryInfo?.threadId,
      resourceId: memoryInfo?.resourceId,
      offset: resumeOffset,
      onChunk: resolvedOptions.onChunk,
      experimentalTransform: resolvedOptions.experimentalTransform,
      hideSignals: resolvedOptions.hideSignals,
      onStepFinish: resolvedOptions.onStepFinish,
      onFinish: resolvedOptions.onFinish,
      onStreamFinished: scheduleAutoCleanup,
      onError: async error => {
        await resolvedOptions.onError?.(error);
        scheduleAutoCleanup();
      },
      onSuspended: resolvedOptions.onSuspended,
      closeOnSuspend,
      structuredOutput: entry.structuredOutput as any,
      outputProcessors: entry.outputProcessors,
      requestContext: resumeRequestContext,
      returnScorerData: options?.returnScorerData ?? entry.returnScorerData ?? resolvedOptions.returnScorerData,
      tracingContext: resumeSegmentSpan ? { currentSpan: resumeSegmentSpan } : undefined,
      messageList: globalEntry?.messageList ?? this.#runRegistry.getMessageList(runId),
    });
    streamCleanup = createdStreamCleanup;

    // Wait for subscription to be ready, then resume workflow
    const workflow = this.getWorkflow();
    const workflowExecution = ready
      .then(async () => {
        if (!entry || !this.#activeRegistryPairIsValid(runId, entry)) {
          await this.#rehydrateSuspendedRunRegistry(runId, options);
          entry = this.#runRegistry.get(runId);
        }
        const deferredResumeLabel = prevalidatedResumeLabel
          ? undefined
          : await this.#resolveDurableResumeLabel(runId, options?.toolCallId);
        const resumeLabel = prevalidatedResumeLabel ?? deferredResumeLabel?.label;
        if (!entry || !this.#activeRegistryPairIsValid(runId, entry)) {
          throw new MastraError({
            id: 'DURABLE_AGENT_RESUME_REGISTRY_REHYDRATION_FAILED',
            domain: ErrorDomain.AGENT,
            category: ErrorCategory.SYSTEM,
            text: `DurableAgent "${this.name}" lost runtime state before resuming run "${runId}".`,
            details: { agentName: this.name, runId },
          });
        }
        // Evented engine: make sure the workflow event workers are running
        // before resume — a resume in a fresh process (or after shutdown())
        // would otherwise publish events nobody consumes and hang.
        await this.ensureEngineWorkersStarted();
        resumeRequestContext = bindResumeRequestContext();
        const activeEntry = entry;
        const pinnedEntry = pinGlobalRunRegistryEntry(runId);
        if (pinnedEntry !== activeEntry) this.#throwRunIdConflict(runId);
        let result;
        try {
          const run = await workflow.createRun({
            runId,
            resourceId: memoryInfo?.resourceId,
            pubsub: this.pubsub,
          });
          if (this.__getGoalConfig()) {
            await beginGoalActivity({
              mastra: this.#mastra,
              agentId: this.id,
              resourceId: memoryInfo?.resourceId,
              threadId: memoryInfo?.threadId,
              runId,
              requestContext: resumeRequestContext,
            });
          }
          try {
            result = await run.resume({
              resumeData,
              label: resumeLabel,
              requestContext: resumeRequestContext,
              // Use the actor resolved for this resume call. A resumed segment
              // must never recover the initial actor from serialized options.
              actor: resolvedOptions.actor,
              ...createObservabilityContext({ currentSpan: activeEntry.resumeAgentSpan ?? activeEntry.agentSpan }),
            });
          } finally {
            await stopGoalActivity({ agentId: this.id, runId });
          }
        } finally {
          unpinGlobalRunRegistryEntry(runId, activeEntry.runtimeBindingId);
        }
        if (result?.status === 'failed') {
          const error = new Error((result as any).error?.message || 'Workflow resume failed');
          this.emitErrorInBackground(runId, error);
        }
        // The workflow onFinish hook owns terminal snapshot cleanup for
        // start, resume, and recovery, including failed terminals.
      })
      .catch(error => {
        this.emitErrorInBackground(runId, error);
      });
    const trackedResumeEntry = getBoundRunRegistryEntry(runId, runtimeBindingId);
    if (trackedResumeEntry) {
      trackedResumeEntry.workflowExecution = workflowExecution;
    }

    const resumeStreamOptions: AgentExecutionOptions<TOutput> = {
      ...resolvedOptions,
      runId,
    } as AgentExecutionOptions<TOutput>;
    // Registration is synchronous; the returned promise is the resumed
    // segment's terminal-delivery watcher and must not delay returning its
    // stream handle (especially when the segment re-suspends). continueRun
    // re-attaches this stream to the run's existing thread record so
    // subscribeToThread/sendMessage subscribers keep receiving events; a
    // fresh registerRun is only needed when no live record remains.
    const continued = agentThreadStreamRuntime.continueRun(
      this as unknown as Agent<any, any, any, any>,
      output,
      resumeStreamOptions,
      this.getPubSub(),
    );
    if (!continued) {
      // The prior segment's terminal delivery can lag its provider; let it park
      // before this same-run segment registers.
      const priorSegmentSettlement = agentThreadStreamRuntime.waitForSuspendedSegmentSettlement(
        runId,
        this.getPubSub(),
      );
      if (priorSegmentSettlement) await priorSegmentSettlement;
      // Fork (PF): a non-strict registration returns the segment's
      // terminal-delivery watcher; awaiting it would hold resume() until the
      // resumed segment finishes (or forever when it re-suspends).
      void agentThreadStreamRuntime.registerRun(
        this as unknown as Agent<any, any, any, any>,
        output,
        resumeStreamOptions,
        this.getPubSub(),
        closeOnSuspend ? undefined : { continuation: 'across-suspension' },
      );
    }

    // Route the explicit cleanup through the shared performCleanup() so it and
    // the auto-cleanup timer release the same resources.
    const cleanup = performCleanup;

    const abort = async (reason?: unknown) => {
      if (!abortController.signal.aborted) {
        abortController.abort(reason);
      }
      await this.requestRemoteAbort(runId, runtimeBindingId);
    };

    return {
      output,
      get fullStream() {
        return output.fullStream as ReadableStream<any>;
      },
      runId,
      threadId: memoryInfo?.threadId,
      resourceId: memoryInfo?.resourceId,
      cleanup,
      abort,
    };
  }

  /**
   * Recover a single durable run whose in-process agentic loop was orphaned by
   * a process restart. Streamable counterpart to
   * {@link DurableAgent.recoverActiveRuns} — where the bulk API only re-drives
   * the workflow and returns counts, `recover()` rebuilds the run's
   * non-serializable state (message list, model, tools, memory,
   * saveQueueManager, request context, agent span) from the persisted workflow
   * snapshot and returns a fresh {@link DurableAgentStreamResult} whose
   * `fullStream` observes the recovered run through pubsub.
   *
   * Because the rebuilt registry entry carries `memory` + `saveQueueManager`,
   * the durable agentic workflow's terminal step will flush new messages to
   * memory just like a fresh `stream()` call would. The single-run form is
   * useful when operators want to attach listeners to a specific recovered
   * run; for boot-time bulk recovery of every orphaned run, use
   * `recoverActiveRuns()`.
   *
   * @example
   * ```typescript
   * const { fullStream, output, cleanup } = await durableAgent.recover(runId, {
   *   onChunk: chunk => process.stdout.write(chunk.payload?.text ?? ''),
   * });
   * for await (const chunk of fullStream) {
   *   // ...
   * }
   * cleanup();
   * ```
   */
  async recover(
    runId: string,
    options?: DurableAgentRecoverOptions<TOutput>,
  ): Promise<DurableAgentStreamResult<TOutput>> {
    if (!this.#mastra) {
      throw new MastraError({
        id: 'DURABLE_AGENT_RECOVER_NO_MASTRA',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" recover() requires the agent to be registered on a Mastra instance.`,
        details: { agentName: this.name, runId },
      });
    }

    const workflowsStore = await this.#mastra.getStorage()?.getStore('workflows');
    if (!workflowsStore) {
      throw new MastraError({
        id: 'DURABLE_AGENT_RECOVER_NO_STORAGE',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text:
          `DurableAgent "${this.name}" recover() requires persistent storage to load the run snapshot. ` +
          `Register the agent on a Mastra instance with persistent storage (e.g. PostgreSQL, LibSQL).`,
        details: { agentName: this.name, runId },
      });
    }

    // 1. Validate the persisted durable-agent input before claiming ownership
    //    so obvious caller errors fail fast.
    let { workflowInput } = await this.#loadRecoverableSnapshot(workflowsStore, runId);

    // A crashed run that was executing a stored version must recover on
    // *that* version — rehydration rebuilds tools/model/instructions from
    // whatever `this` currently resolves to, which for status selectors
    // hot-switches to the latest publish (mirrors the resume() pin above).
    // Resolution happens BEFORE lease acquisition so we never delegate to a
    // fork while holding the lease; reading the pin from the pre-claim
    // snapshot is safe because `agentVersionId` is stamped once at
    // preparation and never mutated. Unlike resume(), recover options carry
    // no call-site version selector, so the pin always wins here — recovery
    // is unattended and has no operator escape hatch.
    const pinnedVersionId = workflowInput.agentVersionId;
    if (pinnedVersionId && this.#mastra && !this.__isStoredVersionApplied()) {
      const currentVersionId = this.toRawConfig()?.resolvedVersionId as string | undefined;
      if (pinnedVersionId !== currentVersionId) {
        try {
          const resolved = await this.#mastra.resolveVersionedAgent(this as unknown as Agent, {
            versionId: pinnedVersionId,
          });
          if (resolved !== (this as unknown as Agent)) {
            return (resolved as unknown as DurableAgent<TAgentId, TTools, TOutput>).recover(runId, options);
          }
        } catch (versionError) {
          // The pinned version may have been deleted while the run sat
          // crashed — recover on the current definition rather than failing
          // an unattended path (mirrors resume()'s deleted-pin fallback).
          this.logger.warn('Failed to resolve pinned agent version for durable recovery, using current definition', {
            agentId: this.id,
            runId,
            pinnedVersionId,
            error: versionError,
          });
        }
      }
    }

    // 2. Claim recovery ownership before resolving any live dependencies so a
    //    concurrent caller cannot finish first and leave this attempt using a
    //    stale snapshot.
    const abortController = new AbortController();
    if (options?.abortSignal) {
      if (options.abortSignal.aborted) {
        abortController.abort((options.abortSignal as AbortSignal & { reason?: unknown }).reason);
      } else {
        options.abortSignal.addEventListener(
          'abort',
          () => abortController.abort((options.abortSignal as AbortSignal & { reason?: unknown }).reason),
          { once: true },
        );
      }
    }
    const recoveryLease = await this.#acquireRecoveryLease(runId, abortController);

    let recoveryState: RehydratedRecoveryState;
    let finishPublishedBeforeCrash: boolean;
    try {
      // The lease RPC itself may have waited while an earlier owner completed.
      // Re-read after acquisition and recover from that authoritative snapshot,
      // never from the pre-claim copy.
      const loaded = await this.#loadRecoverableSnapshot(workflowsStore, runId);
      workflowInput = loaded.workflowInput;
      // map-final-output publishes FINISH before its result is saved, so a saved
      // success means FINISH went out before the crash. The default engine
      // continues after a finished step instead of re-running it, so FINISH is
      // never published again, and the recovered stream (subscribed from the
      // topic's current end) would wait for it forever. The evented engine
      // re-runs the step, which publishes FINISH itself. Check the resolved
      // engine: an EventedAgent can fall back to the default one. Once the
      // evented engine also continues after a finished step (COR-1354), drop
      // the engine check.
      finishPublishedBeforeCrash =
        this.resolveWorkflowEngine() === 'default' &&
        loaded.snapshot.context?.[MAP_FINAL_OUTPUT_STEP_ID]?.status === 'success';
      recoveryLease.assertOwned();
      recoveryState = await this.#rehydrateRecoveryState({
        runId,
        workflowInput,
        abortController,
        recoveryLease,
      });
    } catch (error) {
      await recoveryLease.release();
      throw error;
    }
    const { requestContext, threadId, resourceId, messageList, recoverAgentSpan, registryEntry } = recoveryState;
    const runtimeBindingId = registryEntry.runtimeBindingId;

    // 3. Cleanup plumbing (mirrors stream()/resume()).
    let cleanedUp = false;
    let autoCleanupTimer: ReturnType<typeof setTimeout> | null = null;
    // Assigned once the recovered stream is created below so the shared
    // performCleanup() closure can unsubscribe the pubsub reader (via
    // streamCleanup) from both the auto-cleanup timer and the explicit
    // cleanup() path — mirroring observe().
    let streamCleanup: (() => void) | undefined;
    const cleanupOwnedRegistryState = () => {
      // Identity- AND binding-scoped: this recovery may only tear down the exact
      // entry it registered, never a newer execution that reused the run ID.
      if (this.#runRegistry.get(runId) === registryEntry || getGlobalRunRegistryEntry(runId) === registryEntry) {
        this.#cleanupBoundRun(runId, runtimeBindingId);
      }
      cleanedUp = true;
    };
    // Single cleanup path for the auto-cleanup timer, the explicit cleanup(),
    // and the recovery error paths. Revokes continuation before unsubscribing
    // the pubsub reader, then tears down the owned registry entries. Idempotent
    // via `cleanedUp`.
    const performCleanup = () => {
      if (autoCleanupTimer) {
        clearTimeout(autoCleanupTimer);
        autoCleanupTimer = null;
      }
      if (cleanedUp) return;
      agentThreadStreamRuntime.closeRunContinuation(output, this.getPubSub());
      streamCleanup?.();
      cleanupOwnedRegistryState();
    };
    const scheduleAutoCleanup = () => {
      if (autoCleanupTimer || cleanedUp || this.#cleanupTimeoutMs === 0) return;
      autoCleanupTimer = setTimeout(performCleanup, this.#cleanupTimeoutMs);
    };

    let workflow: ReturnType<DurableAgent<TAgentId, TTools, TOutput>['getWorkflow']>;
    try {
      workflow = this.getWorkflow();
      recoveryLease.assertOwned();
      // Run-identity fence. This must throw straight out of `recover()`, before
      // any stream, thread registration or registry entry exists for this
      // attempt. Running it inside `#setupRecoveredStream` would route the
      // refusal through that method's catch -> `#reportRecoveryFailure` ->
      // `emitError(runId)`, which publishes a terminal ERROR on
      // `AGENT_STREAM_TOPIC(runId)` and ends the run's spans. That topic is
      // keyed by run ID only, not by runtime binding, so a *refused* duplicate
      // recovery would terminate the healthy in-flight execution that already
      // owns this run ID. Reusing a run ID must never clear or terminate
      // another execution's signal.
      this.#coordinateRegistryCleanup(runId, registryEntry);
      this.#assertNoRegistryCollision(runId);
    } catch (error) {
      await recoveryLease.release();
      throw error;
    }

    // 4. Register the reconstructed state and recovered stream only after
    //    claiming exclusive recovery ownership.
    const { stream, threadRegistration } = await this.#setupRecoveredStream({
      runId,
      workflowInput,
      requestContext,
      threadId,
      resourceId,
      messageList,
      registryEntry,
      options,
      scheduleAutoCleanup,
      recoveryLease,
    });
    const { output, cleanup: createdStreamCleanup, ready } = stream;
    streamCleanup = createdStreamCleanup;
    const recoveryPubsub = this.#createRecoveryFencedPubSub(recoveryLease);

    // 5. Re-drive the workflow from the persisted snapshot in the background
    //     and delete snapshot rows on non-suspended terminals (same contract
    //     as start()/resume()). Errors are also broadcast via `emitError` so
    //     observers on the pubsub topic see the failure. Callers who await
    //     the returned `workflowExecution` (e.g. `recoverActiveRuns()`) see
    //     the raw rejection so they can classify the run as failed.
    const workflowExecution = this.#raceRecoveryLease(ready, recoveryLease)
      .then(async () => {
        recoveryLease.assertOwned();
        await this.ensureEngineWorkersStarted();
        const run = await this.#raceRecoveryLease(
          workflow.createRun({ runId, resourceId, pubsub: recoveryPubsub }),
          recoveryLease,
        );
        recoveryLease.assertOwned();
        const result = await this.#raceRecoveryLease(
          run.restart({
            requestContext,
            ...createObservabilityContext({ currentSpan: recoverAgentSpan }),
          } as any),
          recoveryLease,
        );
        recoveryLease.assertOwned();
        // A crash after map-final-output succeeded can lose the FINISH event:
        // restart resumes from the persisted snapshot and will not re-run the
        // completed step, so republish its saved output here (the same payload
        // the lost FINISH carried).
        if (finishPublishedBeforeCrash && result?.status === 'success') {
          const { output: finalOutput, stepResult } = result.result;
          await emitFinishEvent(recoveryPubsub, runId, { output: finalOutput, stepResult });
          recoveryLease.assertOwned();
        }
        // Snapshot cleanup for every non-suspended terminal (success or failed)
        // is not inlined here: this fork routes it through the workflow's
        // `onFinish` hook (`onDurableWorkflowFinish` -> `deleteTerminalRunSnapshots`),
        // which fires once per terminal for start(), resume() and restart()
        // alike. Upstream calls `deleteRunSnapshots()` at each of those three
        // call sites instead; adding it back here would double-delete.
        if (result?.status === 'failed') {
          throw new Error((result as any).error?.message || 'Workflow recover failed');
        }
      })
      .then(async () => {
        // A renewal that was already in flight when restart settled still owns
        // the success decision. Classify it before exposing successful recovery.
        await recoveryLease.release();
        recoveryLease.assertOwned();
      })
      .catch(async error => {
        const leaseLossError = recoveryLease.getLossError();
        if (leaseLossError) {
          await threadRegistration?.rollback({ releaseLease: false });
          performCleanup();
        }
        const recoveryError = leaseLossError ?? error;
        const reported = await this.#reportRecoveryFailure(runId, recoveryError);
        if (!reported && !leaseLossError) {
          await threadRegistration?.rollback();
          performCleanup();
        }
        throw recoveryError;
      })
      .finally(() => recoveryLease.release());
    // Track the execution on the exact entry this recovery registered so a
    // graceful shutdown awaits this run without resolving the ID again.
    registryEntry.workflowExecution = workflowExecution;
    // Guard against unhandled rejection warnings for callers who don't await
    // `workflowExecution` (single-run `recover()` returns a stream, not the
    // workflow promise). Errors are already surfaced through `emitError` /
    // the stream's `onError` callback.
    workflowExecution.catch(() => {});

    // Route the explicit cleanup through the shared performCleanup() so it, the
    // auto-cleanup timer, and the recovery error paths release the same resources.
    const cleanup = performCleanup;

    const abort = async (reason?: unknown) => {
      if (!abortController.signal.aborted) {
        abortController.abort(reason);
      }
      await this.requestRemoteAbort(runId, runtimeBindingId);
    };

    const result: DurableAgentStreamResult<TOutput> & { [RECOVERY_WORKFLOW_EXECUTION]: Promise<void> } = {
      output,
      get fullStream() {
        return output.fullStream as ReadableStream<any>;
      },
      runId,
      threadId,
      resourceId,
      cleanup,
      abort,
      [RECOVERY_WORKFLOW_EXECUTION]: workflowExecution,
    };
    return result;
  }

  /**
   * Override the inherited `resumeStream()` so that callers using the base
   * `Agent` API (including `approveToolCall` / `declineToolCall`) are routed
   * through the durable `resume()` path instead of the regular Agent's
   * snapshot-based resume.
   *
   * Returns just the `MastraModelOutput` (matching the base Agent's return
   * type) while internally delegating to `this.resume()`. Hook overrides are
   * rejected because the suspended run remains bound to its initial policy.
   */
  override async resumeStream<OUTPUT = TOutput>(
    resumeData: unknown,
    streamOptions?: AgentExecutionOptionsBase<any> & {
      structuredOutput?: unknown;
      toolCallId?: string;
      model?: unknown;
    },
  ): Promise<MastraModelOutput<OUTPUT>> {
    const runId = streamOptions?.runId;
    if (!runId) {
      throw new Error('resumeStream() on DurableAgent requires a runId in streamOptions.');
    }
    const { runId: _runId, ...resumeOptions } = streamOptions;
    const result = await this.resume(runId, resumeData, {
      ...resumeOptions,
      // Close the stream when the workflow re-suspends so the caller's
      // `for await` loop terminates. Without this the stream stays open
      // indefinitely when the resumed turn hits another suspend point.
      closeOnSuspend: resumeOptions.closeOnSuspend ?? true,
    } as Parameters<DurableAgent<TAgentId, TTools, TOutput>['resume']>[2]);
    return result.output as unknown as MastraModelOutput<OUTPUT>;
  }

  /**
   * Override the inherited `approveToolCall()` to route through the durable
   * `resume()` path.
   */
  override async approveToolCall(
    options: { runId: string; toolCallId?: string } & Record<string, any>,
  ): Promise<MastraModelOutput<any>> {
    return this.resumeStream({ approved: true }, options);
  }

  /**
   * Override the inherited `declineToolCall()` to route through the durable
   * `resume()` path.
   */
  override async declineToolCall(
    options: { runId: string; toolCallId?: string; reason?: string } & Record<string, any>,
  ): Promise<MastraModelOutput<any>> {
    const { reason, ...resumeOptions } = options;
    return this.resumeStream({ approved: false, ...(reason !== undefined ? { reason } : {}) }, resumeOptions);
  }

  override async approveToolCallGenerate<OUTPUT = undefined>(
    options: AgentExecutionOptions<OUTPUT> & { runId: string; toolCallId?: string },
  ): Promise<Awaited<ReturnType<MastraModelOutput<OUTPUT>['getFullOutput']>>> {
    const { runId, ...resumeOptions } = options;
    return this.resumeGenerate(runId, { approved: true }, resumeOptions as any) as any;
  }

  override async declineToolCallGenerate<OUTPUT = undefined>(
    options: AgentExecutionOptions<OUTPUT> & { runId: string; toolCallId?: string; reason?: string },
  ): Promise<Awaited<ReturnType<MastraModelOutput<OUTPUT>['getFullOutput']>>> {
    const { runId, reason, ...resumeOptions } = options;
    return this.resumeGenerate(
      runId,
      { approved: false, ...(reason !== undefined ? { reason } : {}) },
      resumeOptions as any,
    ) as any;
  }

  /**
   * Generate a complete response from the agent using durable execution.
   *
   * Drains the underlying durable stream to completion and returns the same
   * {@link FullOutput} shape as non-durable `Agent.generate`. The underlying
   * workflow is identical to `stream()` — it just collects the final result
   * for callers that don't want to consume chunks themselves.
   *
   * This method intentionally re-implements the `stream()` setup rather than
   * delegating to `this.stream(...)` so that `prepareForDurableExecution` (and
   * downstream `convertTools`) receives `methodType: 'generate'`. Tool
   * factories that vary their `CoreTool` output based on the calling method
   * (e.g. `clientTools` vs server-side tools) rely on this signal — calling
   * `stream()` here would silently pass `methodType: 'stream'`.
   *
   * If the run suspends (e.g. tool approval or `suspend()` from a tool), the
   * returned output's `finishReason` will be `'suspended'` and
   * `suspendPayload` will be populated. Use {@link DurableAgent.resumeGenerate}
   * to continue.
   *
   * Note on suspend persistence: the workflow engine's `run.start()` only
   * resolves after the suspend snapshot is persisted, so awaiting
   * `workflowExecution` on suspend is sufficient for a subsequent
   * `resumeGenerate()` to find the snapshot.
   */
  // @ts-expect-error - Intentionally different signature for durable execution
  async generate(
    messages: MessageListInput,
    options?: DurableAgentStreamOptions<TOutput>,
  ): Promise<FullOutput<TOutput>> {
    messages = snapshotAgentExecutionValue(messages);
    options = options
      ? snapshotAgentExecutionOptions(options, ['runId', 'requestContext', 'memory', 'versions'])
      : undefined;
    const allocatedRunId = options?.runId ?? this.#allocateRunId();
    let releaseRunIdReservation: (() => void) | undefined;
    let preparation: PreparationResult<TOutput>;
    try {
      if (options?.runId !== undefined) {
        await this.#assertPublicAgentResumePreflight({
          requestContext: options.requestContext,
          memory: options.memory,
          runId: allocatedRunId,
          actor: options.actor,
        });
        releaseRunIdReservation = await this.#reserveRunId(allocatedRunId);
      }
      preparation = await prepareForDurableExecution<TOutput>({
        agent: this.#wrappedAgent as Agent<string, any, TOutput>,
        messages,
        options: options as AgentExecutionOptions<TOutput>,
        durableRequestContextKeys: this.#durableRequestContextKeys,
        runId: allocatedRunId,
        requestContext: options?.requestContext,
        logger: this.logger,
        mastra: this.#mastra,
        methodType: 'generate',
        durableAgentId: this.id,
        durableAgentName: this.name,
      });
      preparation.workflowInput.runtimeResolution = 'registry-required';
    } finally {
      releaseRunIdReservation?.();
    }

    const { runId, messageId, workflowInput, registryEntry, messageList, threadId, resourceId } = preparation;

    // 1a. Install the abort controller for this run. The controller is owned
    // by this DurableAgent instance; the result's abort() method flips it,
    // and the durable LLM-execution step reads `abortSignal` off the registry
    // to thread it into the model call + abort short-circuits. If the caller
    // also supplied an external signal, forward its abort to the internal
    // controller so either source can cancel the run.
    const abortController = new AbortController();
    if (options?.abortSignal) {
      if (options.abortSignal.aborted) {
        abortController.abort((options.abortSignal as AbortSignal & { reason?: unknown }).reason);
      } else {
        options.abortSignal.addEventListener(
          'abort',
          () => abortController.abort((options.abortSignal as AbortSignal & { reason?: unknown }).reason),
          { once: true },
        );
      }
    }
    if (agentThreadStreamRuntime.isRunAborted(runId, this.getPubSub())) {
      abortController.abort();
    }
    this.#installAbortWithTotalTimeout(registryEntry, abortController);

    // 2. Register the same capability object in both registries.
    registryEntry.messageList = messageList;
    this.#coordinateRegistryCleanup(runId, registryEntry);
    registerGlobalRunRegistryEntry(runId, registryEntry);
    this.#runRegistry.registerWithMessageList(runId, registryEntry, messageList, { threadId, resourceId });
    const runtimeBindingId = registryEntry.runtimeBindingId;

    // Track cleanup state to avoid double cleanup
    let cleanedUp = false;
    let autoCleanupTimer: ReturnType<typeof setTimeout> | null = null;

    // Schedule automatic registry cleanup after stream ends
    const scheduleAutoCleanup = () => {
      if (autoCleanupTimer || cleanedUp || this.#cleanupTimeoutMs === 0) return;
      autoCleanupTimer = setTimeout(() => {
        if (!cleanedUp) {
          this.#cleanupBoundRun(runId, runtimeBindingId);
          cleanedUp = true;
        }
      }, this.#cleanupTimeoutMs);
    };

    // 3. Create the durable agent stream (subscribes to pubsub)
    const {
      output,
      cleanup: streamCleanup,
      ready,
    } = createDurableAgentStream<TOutput>({
      pubsub: this.pubsub,
      runId,
      messageId,
      model: {
        modelId: workflowInput.modelConfig.modelId,
        provider: workflowInput.modelConfig.provider,
        version: 'v3',
      },
      threadId,
      resourceId,
      onChunk: options?.onChunk,
      experimentalTransform: options?.experimentalTransform,
      onStepFinish: options?.onStepFinish,
      onFinish: options?.onFinish,
      onStreamFinished: scheduleAutoCleanup,
      onError: async error => {
        await options?.onError?.(error);
        scheduleAutoCleanup();
      },
      onSuspended: options?.onSuspended,
      onAbort: async data => {
        try {
          await (options?.onAbort as ((event: any) => void | Promise<void>) | undefined)?.(data);
        } finally {
          scheduleAutoCleanup();
        }
      },
      // onIterationComplete is NOT forwarded here — the dowhile predicate
      // now calls it in-process from globalRunRegistry and honors its return
      // value ({ continue, feedback }). The pubsub ITERATION_COMPLETE event
      // still fires for external observability subscribers.
      closeOnSuspend: true,
      structuredOutput: registryEntry.structuredOutput as any,
      outputProcessors: registryEntry.outputProcessors,
      requestContext: registryEntry.requestContext,
      returnScorerData: workflowInput.options.returnScorerData,
      tracingContext: registryEntry.agentSpan ? { currentSpan: registryEntry.agentSpan } : undefined,
      messageList,
    });

    // 4. Wait for subscription to be ready, then execute workflow
    // This prevents race conditions where events are published before subscription
    const workflowExecution = ready
      .then(async () => {
        // Emit 'start' chunk before the workflow begins (matches regular agent's stream.ts).
        // Only the initial generate()/stream() path emits 'start'; resume() does not.
        await emitChunkEvent(this.pubsub, runId, {
          type: 'start',
          runId,
          from: ChunkFrom.AGENT,
          payload: { id: workflowInput.agentId, messageId },
        });
        if (this.__getGoalConfig()) {
          await beginGoalActivity({
            mastra: this.#mastra,
            agentId: workflowInput.agentId,
            resourceId,
            threadId,
            runId,
            requestContext: globalRunRegistry.get(runId)?.requestContext,
          });
        }
        try {
          return await this.executeWorkflow(runId, workflowInput);
        } finally {
          await stopGoalActivity({ agentId: workflowInput.agentId, runId });
        }
      })
      .catch(error => {
        this.emitErrorInBackground(runId, error);
      });
    const trackedEntry = globalRunRegistry.get(runId);
    if (trackedEntry) {
      trackedEntry.workflowExecution = workflowExecution;
    }

    // 5. Create cleanup function (cancels auto-cleanup timer if called)
    const cleanup = () => {
      if (autoCleanupTimer) {
        clearTimeout(autoCleanupTimer);
        autoCleanupTimer = null;
      }
      if (!cleanedUp) {
        agentThreadStreamRuntime.closeRunContinuation(output, this.getPubSub());
        streamCleanup();
        this.#cleanupBoundRun(runId, runtimeBindingId);
        cleanedUp = true;
      }
    };

    let suspended = false;
    try {
      const fullOutput = (await output.getFullOutput()) as FullOutput<TOutput>;
      if (fullOutput.error) {
        throw fullOutput.error;
      }
      suspended = fullOutput.finishReason === 'suspended';
      // On suspend, awaiting the workflow execution promise blocks until
      // `run.start()` returns, which happens after the suspend snapshot has
      // been persisted — so a later `resumeGenerate()` can find the snapshot.
      // Every engine's `executeWorkflow` promise is this native persisted
      // finish receipt.
      if (suspended) {
        await globalRunRegistry.get(runId)?.workflowExecution;
      }
      // Fall back to the stream-level runId if MastraModelOutput.runId wasn't
      // populated (no chunk surfaced before suspend).
      if (!fullOutput.runId) {
        (fullOutput as { runId?: string }).runId = runId;
      }
      return fullOutput;
    } finally {
      // Keep the registry entry alive on suspend so `resumeGenerate()` can
      // pick it up. Auto-cleanup is scheduled by FINISH/ERROR/ABORT paths.
      if (!suspended) {
        cleanup();
      }
    }
  }

  /**
   * Resume a suspended durable run and drain it to a single
   * {@link FullOutput}. Mirrors {@link Agent.resumeGenerate} on top of
   * {@link DurableAgent.resume}.
   *
   * Unlike `generate()`, this delegates to `resume()` because resume reads
   * its tools from the existing run-registry entry rather than running
   * `prepareForDurableExecution` again — there is no `methodType` to thread
   * through. The same `EventedAgent` caveat about fire-and-forget snapshot
   * persistence noted on `generate()` applies if the resumed turn suspends.
   */
  async resumeGenerate(
    runId: string,
    resumeData: unknown,
    options?: Parameters<DurableAgent<TAgentId, TTools, TOutput>['resume']>[2],
  ): Promise<FullOutput<TOutput>> {
    const result = await this.resume(runId, resumeData, {
      ...(options ?? {}),
      closeOnSuspend: true,
    } as Parameters<DurableAgent<TAgentId, TTools, TOutput>['resume']>[2]);
    let suspended = false;
    try {
      const fullOutput = (await result.output.getFullOutput()) as FullOutput<TOutput>;
      if (fullOutput.error) {
        throw fullOutput.error;
      }
      suspended = fullOutput.finishReason === 'suspended';
      if (suspended) {
        await globalRunRegistry.get(result.runId)?.workflowExecution;
      }
      if (!fullOutput.runId) {
        (fullOutput as { runId?: string }).runId = result.runId;
      }
      return fullOutput;
    } finally {
      if (!suspended) {
        result.cleanup();
      }
    }
  }

  /**
   * List durable agent runs currently reported as `running` in workflow
   * snapshot storage.
   *
   * A `running` snapshot is a durable agent run whose agentic loop was
   * mid-execution the last time the workflow engine persisted its state. On a
   * healthy process these transition to `suspended` (waiting on
   * tool approval / resume) or a terminal status. On a crashed / restarted
   * process they are orphaned in the `running` state with no in-process
   * driver — this is the discovery API used to enumerate them for recovery
   * (see {@link DurableAgent.recoverActiveRuns} and workflow `restart`).
   *
   * Requires persistent workflow storage. Filters `agentId` against the
   * persisted `DurableAgenticWorkflowInput.agentId`, so runs started by other
   * durable agents sharing the same storage are not surfaced.
   *
   * @example
   * ```typescript
   * const { runs } = await durableAgent.listActiveRuns({ resourceId });
   * for (const run of runs) {
   *   await durableAgent.recoverActiveRuns({ runId: run.runId });
   * }
   * ```
   */
  async listActiveRuns(options: DurableAgentListActiveRunsOptions = {}): Promise<DurableAgentListActiveRunsResult> {
    const { threadId, resourceId, fromDate, toDate, perPage, page } = options;

    if (perPage !== undefined && (!Number.isInteger(perPage) || perPage <= 0)) {
      throw new MastraError({
        id: 'DURABLE_AGENT_LIST_ACTIVE_RUNS_INVALID_PER_PAGE',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" listActiveRuns() requires perPage to be a positive integer.`,
        details: { agentName: this.name, perPage },
      });
    }
    if (page !== undefined && (!Number.isInteger(page) || page < 0)) {
      throw new MastraError({
        id: 'DURABLE_AGENT_LIST_ACTIVE_RUNS_INVALID_PAGE',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" listActiveRuns() requires page to be a non-negative integer.`,
        details: { agentName: this.name, page },
      });
    }

    const workflowsStore = await this.#mastra?.getStorage()?.getStore('workflows');

    if (!workflowsStore) {
      throw new MastraError({
        id: 'DURABLE_AGENT_LIST_ACTIVE_RUNS_NO_STORAGE',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text:
          `DurableAgent "${this.name}" listActiveRuns() requires storage to discover running runs. ` +
          `Register the agent on a Mastra instance with persistent storage (e.g. PostgreSQL, LibSQL).`,
        details: { agentName: this.name },
      });
    }

    // Filtering by agentId/threadId happens in application code because those
    // fields only exist inside each row's `snapshot` JSON — storage adapters
    // have no predicate for them. Fetch candidates in bounded batches so peak
    // memory is O(batch size) hydrated snapshots instead of every `running`
    // row's full snapshot at once (#21501). Only the small per-run summary is
    // retained across batches; each batch's snapshots are discarded before the
    // next fetch.
    const matchedRuns: DurableAgentActiveRun[] = [];
    for (let storagePage = 0; ; storagePage++) {
      const { runs, total: storageTotal } = await workflowsStore.listWorkflowRuns({
        workflowName: DurableStepIds.AGENTIC_LOOP,
        status: 'running',
        resourceId,
        fromDate,
        toDate,
        perPage: LIST_ACTIVE_RUNS_STORAGE_BATCH_SIZE,
        page: storagePage,
      });

      for (const run of runs) {
        let snapshot = run.snapshot;
        if (typeof snapshot === 'string') {
          try {
            snapshot = JSON.parse(snapshot) as WorkflowRunState;
          } catch {
            continue;
          }
        }
        if (snapshot?.status !== 'running') continue;

        // The persisted workflow input carries the owning agentId. Default-deny:
        // a snapshot without an input or whose agentId does not match this agent
        // is skipped so runs cannot leak across agents sharing the same storage.
        const input = snapshot.context?.input as
          | { agentId?: string; messageListState?: { memoryInfo?: { threadId?: string; resourceId?: string } } }
          | undefined;
        const runAgentId = input?.agentId;
        if (runAgentId !== this.id) continue;

        const memoryInfo = input?.messageListState?.memoryInfo;
        const runThreadId = memoryInfo?.threadId;
        const runResourceId = run.resourceId ?? memoryInfo?.resourceId;
        if (threadId && runThreadId !== threadId) continue;
        if (resourceId && runResourceId !== resourceId) continue;

        matchedRuns.push({
          runId: run.runId,
          status: 'running',
          threadId: runThreadId,
          resourceId: runResourceId,
          updatedAt: run.updatedAt,
        });
      }

      // A short batch means the last page. A batch larger than requested means
      // the adapter ignored pagination and returned everything in one call —
      // continuing would refetch the same rows forever.
      if (runs.length !== LIST_ACTIVE_RUNS_STORAGE_BATCH_SIZE) break;
      if ((storagePage + 1) * LIST_ACTIVE_RUNS_STORAGE_BATCH_SIZE >= storageTotal) break;
    }

    const total = matchedRuns.length;
    const paginatedRuns =
      perPage !== undefined && page !== undefined
        ? matchedRuns.slice(page * perPage, (page + 1) * perPage)
        : matchedRuns;

    return { runs: paginatedRuns, total };
  }

  /**
   * Bulk recover durable agent runs whose in-process agentic loop was orphaned
   * by a process restart. This is the recovery half of the discovery API
   * paired with {@link DurableAgent.listActiveRuns} and is the typical
   * boot-time hook.
   *
   * Each targeted run is delegated to {@link DurableAgent.recover}, which
   * rebuilds the run's non-serializable state (message list, model, memory,
   * save-queue manager, request context, agent span), re-subscribes to the
   * run's pubsub topic, and restarts the workflow in the background. Because
   * `recover()` registers `memory` + `saveQueueManager` on the run entry, the
   * durable agentic workflow's terminal step flushes new messages to memory
   * just like a fresh `stream()` call would.
   *
   * The per-run stream returned by `recover()` is discarded — this method
   * awaits each run's workflow settlement and reports summary counts instead
   * of surfacing live event streams. Callers who want to observe a specific
   * recovered run's events should use {@link DurableAgent.recover} directly
   * (or {@link DurableAgent.observe} with the returned `runId`).
   *
   * Failures are captured per-run so a single bad run does not block
   * recovery of the rest.
   *
   * @example
   * ```typescript
   * // Recover every orphaned run for this agent (typical boot-time hook).
   * const { recovered, succeeded, failed } = await durableAgent.recoverActiveRuns();
   * logger.info('Recovered durable agent runs', { succeeded, failed });
   *
   * // Recover a single run by ID.
   * await durableAgent.recoverActiveRuns({ runId });
   * ```
   */
  async recoverActiveRuns(
    options: DurableAgentRecoverActiveRunsOptions = {},
  ): Promise<DurableAgentRecoverActiveRunsResult> {
    const { runId, ...discoveryOptions } = options;

    let targetRunIds: string[];
    if (runId) {
      targetRunIds = [runId];
    } else {
      const { runs } = await this.listActiveRuns(discoveryOptions);
      targetRunIds = runs.map(r => r.runId);
    }

    const recovered: DurableAgentRecoveredRun[] = [];
    let succeeded = 0;
    let failed = 0;

    for (const targetRunId of targetRunIds) {
      let runError: Error | undefined;
      try {
        // Delegate to the single-run streamable recover path so each run
        // benefits from the rebuilt registry entry (message list, memory,
        // saveQueueManager, request context, agent span) and the pubsub
        // stream / terminal snapshot-cleanup contract stays identical to
        // `recover()`. We don't surface the per-run stream here — bulk
        // callers only care about counts — so we just await the workflow
        // execution promise that `recover()` parks on the registry entry,
        // capture any failure it surfaces via `onError`, and drop the
        // stream.
        const recoveredRun = await this.recover(targetRunId, {
          onError: ({ error }) => {
            runError = error instanceof Error ? error : new Error(String(error));
          },
        });
        try {
          await (
            recoveredRun as DurableAgentStreamResult<TOutput> & {
              [RECOVERY_WORKFLOW_EXECUTION]: Promise<void>;
            }
          )[RECOVERY_WORKFLOW_EXECUTION];
        } finally {
          recoveredRun.cleanup();
        }
        if (runError) throw runError;
        recovered.push({ runId: targetRunId, status: 'success' });
        succeeded++;
      } catch (error) {
        const err = runError ?? (error instanceof Error ? error : new Error(String(error)));
        recovered.push({ runId: targetRunId, status: 'failed', error: err });
        failed++;
        this.#mastra
          ?.getLogger?.()
          ?.error?.(`[DurableAgent] Failed to recover run ${targetRunId}: ${err.message}`, { error: err });
      }
    }

    return { recovered, succeeded, failed };
  }

  /**
   * Observe an existing stream.
   * Use this to reconnect to a stream after a network disconnection.
   *
   * To stop observing without affecting the run, call the returned `detach()`
   * or leave the `for await` loop over `fullStream` (break, return, or throw).
   * Both unsubscribe this observer only; the run keeps going and other
   * observers can still replay it. To stop observing when a request is
   * cancelled, wire its signal to `detach`:
   *
   * ```ts
   * const { fullStream, detach } = await agent.observe(runId, { offset });
   * req.signal.addEventListener('abort', detach, { once: true });
   * if (req.signal.aborted) detach();
   * for await (const chunk of fullStream) send(chunk);
   * ```
   *
   * **Warning:** The returned `cleanup()` function destroys the run's registry
   * entries and cached PubSub events (replay history), including for other
   * observers. Only call it when you are done with the run entirely. If the
   * workflow is suspended and you intend to resume later, do not call cleanup —
   * let the auto-cleanup timer handle it after FINISH/ERROR. Auto-cleanup does
   * not fire on SUSPENDED events.
   *
   * Pass `idleTimeoutMs` to bound how long the stream waits on a silent topic:
   * a durable run whose driving process crashed stops emitting chunks but never
   * publishes a terminal event, so without this `observe()` hangs forever on a
   * producerless topic. When the idle timeout fires, the optional `isAlive`
   * probe is consulted first — returning true (e.g. a live run-liveness
   * heartbeat, or a suspended HITL gate) re-arms the timer and keeps waiting,
   * while false/absent terminates the stream with an error chunk. Only an
   * `isAlive` that returns false schedules the run's full cleanup; a bare
   * `idleTimeoutMs` (no `isAlive`) only detaches this observer, so pass
   * `isAlive` if you rely on the timeout to reclaim a crashed run's state. Both
   * options are opt-in; omit them for the current unbounded behavior.
   */
  async observe(
    runId: string,
    options?: {
      offset?: number;
      idleTimeoutMs?: number;
      isAlive?: () => boolean | Promise<boolean>;
      onChunk?: (chunk: ChunkType<TOutput>) => void | Promise<void>;
      experimentalTransform?: MastraStreamTransformOptions<TOutput>;
      onStepFinish?: (result: AgentStepFinishEventData) => void | Promise<void>;
      onFinish?: MastraOnFinishCallback<TOutput>;
      onError?: ({ error }: { error: Error | string }) => void | Promise<void>;
      onSuspended?: (data: AgentSuspendedEventData) => void | Promise<void>;
    },
  ): Promise<Omit<DurableAgentStreamResult<TOutput>, 'runId'> & { runId: string; detach: () => void }> {
    const memoryInfo = this.#runRegistry.getMemoryInfo(runId);
    const runtimeBindingId =
      this.#runRegistry.get(runId)?.runtimeBindingId ?? getGlobalRunRegistryEntry(runId)?.runtimeBindingId;

    // Track cleanup state to avoid double cleanup
    let cleanedUp = false;
    let terminalCompleted = false;
    let abortPending = false;
    let cleanupRequested = false;
    let autoCleanupTimer: ReturnType<typeof setTimeout> | null = null;
    let streamCleanup: (() => void) | undefined;

    const performCleanup = () => {
      if (autoCleanupTimer) {
        clearTimeout(autoCleanupTimer);
        autoCleanupTimer = null;
      }
      if (cleanedUp) return;

      streamCleanup?.();
      // Registry teardown stays keyed to the runtime binding observed when this
      // handle was created: a reused runId must never have a newer execution's
      // entry removed by an older observer.
      if (runtimeBindingId) this.#cleanupBoundRun(runId, runtimeBindingId);
      cleanedUp = true;
    };

    const scheduleAutoCleanup = () => {
      if (autoCleanupTimer || cleanedUp || this.#cleanupTimeoutMs === 0) return;
      autoCleanupTimer = setTimeout(performCleanup, this.#cleanupTimeoutMs);
    };

    const completeTerminalLifecycle = () => {
      terminalCompleted = true;
      abortPending = false;
      if (cleanupRequested) {
        performCleanup();
      } else {
        scheduleAutoCleanup();
      }
    };

    const observedEntry = getBoundRunRegistryEntry(runId, runtimeBindingId) ?? this.#runRegistry.get(runId);
    const observedAgentSpan = observedEntry?.resumeAgentSpan ?? observedEntry?.agentSpan;

    const stream = createDurableAgentStream<TOutput>({
      pubsub: this.pubsub,
      runId,
      messageId: crypto.randomUUID(),
      model: {
        modelId: undefined,
        provider: undefined,
        version: 'v3',
      },
      threadId: memoryInfo?.threadId,
      resourceId: memoryInfo?.resourceId,
      offset: options?.offset,
      idleTimeoutMs: options?.idleTimeoutMs,
      isAlive: options?.isAlive,
      onChunk: options?.onChunk,
      experimentalTransform: options?.experimentalTransform,
      onStepFinish: options?.onStepFinish,
      onFinish: options?.onFinish,
      onStreamFinished: completeTerminalLifecycle,
      onError: async ({ error, runDead }) => {
        try {
          await options?.onError?.({ error });
        } finally {
          // A bare idle timeout (runDead === false) only means this observer
          // stopped hearing from the run — it may still be alive elsewhere, so
          // don't tear down its registry entry or replay history.
          if (runDead !== false) completeTerminalLifecycle();
        }
      },
      onAbort: completeTerminalLifecycle,
      onSuspended: options?.onSuspended,
      structuredOutput: this.#runRegistry.get(runId)?.structuredOutput as any,
      outputProcessors: this.#runRegistry.get(runId)?.outputProcessors,
      returnScorerData: this.#runRegistry.get(runId)?.returnScorerData,
      tracingContext: observedAgentSpan ? { currentSpan: observedAgentSpan } : undefined,
      messageList: globalRunRegistry.get(runId)?.messageList ?? this.#runRegistry.getMessageList(runId),
    });
    const { output, ready, detach } = stream;
    streamCleanup = stream.cleanup;

    // This output belongs to this observer alone, so a consumer that stops
    // reading fullStream (break/return/throw out of `for await`) should detach
    // the observer. MastraModelOutput fans out to several readers and only drops
    // the cancelled reader's listeners, so the cancel never reaches the pubsub
    // subscription — pass it through here.
    const baseFullStream = Object.getOwnPropertyDescriptor(Object.getPrototypeOf(output), 'fullStream')!.get!;
    Object.defineProperty(output, 'fullStream', {
      configurable: true,
      get() {
        const reader = (baseFullStream.call(output) as ReadableStream<ChunkType<TOutput>>).getReader();
        return new ReadableStream<ChunkType<TOutput>>({
          async pull(controller) {
            let result: ReadableStreamReadResult<ChunkType<TOutput>>;
            try {
              result = await reader.read();
            } catch (error) {
              detach();
              throw error;
            }
            if (result.done) {
              detach();
              controller.close();
            } else {
              controller.enqueue(result.value);
            }
          },
          cancel(reason) {
            detach();
            return reader.cancel(reason);
          },
        });
      },
    });

    // Wait for subscription to be ready
    await ready;

    // Subscription replay is asynchronous. Revalidate the exact entry after it
    // becomes ready so a run ID rebound during setup cannot hand this observer
    // another execution's run-scoped events.
    let stillObservingSameEntry = false;
    try {
      stillObservingSameEntry =
        (getBoundRunRegistryEntry(runId, runtimeBindingId) ?? this.#runRegistry.get(runId)) === observedEntry;
    } catch {
      // A rebound binding is normalized to the public conflict below.
    }
    if (!stillObservingSameEntry) {
      performCleanup();
      this.#throwRunIdConflict(runId);
    }

    const cleanup = () => {
      if (abortPending) {
        cleanupRequested = true;
        scheduleAutoCleanup();
        return;
      }
      performCleanup();
    };

    // observe() doesn't own the run's lifecycle, but the returned `abort` can
    // still stop the exact execution observed when this handle was created.
    // Never resolve the current binding/controller at click time: the run ID
    // may have been reused by then.
    const observedAbortController = observedEntry?.abortController ?? this.#runRegistry.get(runId)?.abortController;
    const abort = async (reason?: unknown) => {
      // A terminal event is still expected after an abort; hold cleanup until it
      // arrives so the observer keeps seeing the run's final chunks.
      abortPending = !terminalCompleted;
      if (observedAbortController && !observedAbortController.signal.aborted) {
        observedAbortController.abort(reason);
      }
      await this.requestRemoteAbort(runId, runtimeBindingId);
    };

    return {
      output,
      get fullStream() {
        return output.fullStream as ReadableStream<any>;
      },
      runId,
      threadId: memoryInfo?.threadId,
      resourceId: memoryInfo?.resourceId,
      cleanup,
      detach,
      abort,
    };
  }

  /**
   * Clear retained pubsub state for a run's topics (cached history and, for
   * persistent transports, the underlying stream). Fire-and-forget: the
   * `clearTopic` contract is best-effort and non-throwing.
   *
   * Clears both the agent stream topic and `workflow.events.v2.<runId>`.
   * The agent stream topic is cleared only here, on both engines. For the
   * workflow events topic the engines differ:
   * - Default engine (`DurableAgent`): no other cleanup exists — without
   *   this, CachingPubSub permanently orphans a no-TTL counter key per
   *   completed run.
   * - Evented engine (`EventedAgent`): the WorkflowEventProcessor also
   *   clears `workflow.events.v2.<runId>` via its own delayed,
   *   restart-guarded terminal cleanup. The two clears overlap safely:
   *   `clearTopic` is idempotent and clearing an empty topic is a no-op.
   *
   * This needs no restart guard of its own — the lifecycle facts are
   * engine-agnostic: cleanup timers arm only on terminal outcomes
   * (FINISH/ERROR/ABORT — never SUSPENDED), `resume()` rejects runs whose
   * snapshot isn't `suspended`, `untilIdle` continuations mint a fresh runId
   * per segment, and cross-process `recover()` can't race a dead process's
   * timer. Caller-supplied IDs are also rejected while either durable workflow
   * has a persisted row; shared persistence is required for supported
   * cross-process execution. Ephemeral no-storage runs are process-local and
   * the binding checks below fence their same-process reuse. No supported flow
   * re-engages a runId after its timer is armed. The one evented-only edge —
   * at-least-once redelivery writing to the workflow events topic after this
   * clear — is covered by the WEP's own cleanup, which reschedules deletion
   * on that run's terminal end.
   */
  #clearPubsubTopic(runId: string, runtimeBindingId: string): void {
    void this.pubsub.clearTopic(AGENT_STREAM_TOPIC(runId));
    void this.pubsub.clearTopic(AGENT_CONTROL_TOPIC(runId, runtimeBindingId));
    void this.pubsub.clearTopic(`workflow.events.v2.${runId}`);
  }

  /** Clear replay state only while this binding still owns the reused run ID. */
  #cleanupBoundRun(runId: string, runtimeBindingId: string): void {
    const cleanedLocal = this.#runRegistry.cleanupBound(runId, runtimeBindingId);
    const cleanedGlobal = deleteBoundRunRegistryEntry(runId, runtimeBindingId);
    if (cleanedLocal || cleanedGlobal) this.#clearPubsubTopic(runId, runtimeBindingId);
  }

  /**
   * Read the current number of cached events for this run's stream topic.
   * Used by `resume()` as the subscription offset so we don't re-deliver
   * events emitted by the original run (notably the SUSPENDED chunk that
   * paused it).
   */
  async #getPubsubOffset(runId: string): Promise<number> {
    const pubsub = this.pubsub as PubSub & {
      getHistory?: (topic: string) => Promise<unknown[]>;
    };
    if (typeof pubsub.getHistory !== 'function') return 0;
    try {
      const history = await pubsub.getHistory(AGENT_STREAM_TOPIC(runId));
      return Array.isArray(history) ? history.length : 0;
    } catch {
      return 0;
    }
  }

  /**
   * Get the workflow instance for direct execution.
   * Lazily creates the workflow and registers Mastra on it (needed for
   * getAgentById in execution steps).
   */
  getWorkflow() {
    if (!this.#workflow) {
      this.warnOnRiskyPersistencePolicy();
      this.#workflow = this.createWorkflow();
      // Register mastra on the workflow so execution steps can access agents/tools.
      // DurableAgent goes through the normal Agent registration path (not the durable wrapper
      // path that calls addWorkflow), so the workflow isn't registered in Mastra's #workflows.
      // We set mastra directly here instead.
      if (this.#mastra) {
        this.#workflow.__registerMastra(this.#mastra);
        this.#workflow.__registerPrimitives({
          logger: this.#mastra.getLogger(),
          storage: this.#mastra.getStorage(),
        });
        // Evented engine: the WorkflowEventProcessor resolves workflows by id
        // from Mastra's registries, so the loop workflow must be discoverable
        // there. Register it as an (unscoped) internal workflow — it's a
        // per-agent singleton. Without this, every run's events would be
        // unresolvable and the run would hang. Uses the resolved engine so
        // this stays consistent with the workflow instance just created.
        if (this.resolveWorkflowEngine() === 'evented') {
          this.#mastra.__registerInternalWorkflow(this.#workflow);
        }
      }
    }
    return this.#workflow;
  }

  /**
   * One-time guardrail warnings for user-supplied `shouldPersistSnapshot`
   * policies. Probes the predicate with an empty `stepResults`, so predicates
   * that read `stepResults` may probe inaccurately — the warnings are
   * best-effort and never block execution.
   *
   * Subclasses that ignore the user predicate (EventedAgent) override this
   * with their own warning.
   *
   * @internal
   */
  protected warnOnRiskyPersistencePolicy(): void {
    if (this.#warnedPersistencePolicy) return;
    this.#warnedPersistencePolicy = true;
    const predicate = this.userShouldPersistSnapshot;
    if (!predicate) return;
    const probe = (workflowStatus: WorkflowRunStatus): boolean => {
      try {
        return predicate({ workflowStatus, stepResults: {} });
      } catch {
        // The predicate depends on data the probe can't fake — assume it
        // persists rather than emitting a false-positive warning.
        return true;
      }
    };
    if (!probe('suspended') || !probe('paused')) {
      this.guardrailLogger?.warn(
        `DurableAgent '${this.id}': the custom shouldPersistSnapshot policy does not persist 'suspended'/'paused' snapshots. ` +
          `Suspended runs cannot be resumed — human-in-the-loop flows will break.`,
      );
    }
    if (this.#mastra?.recoveryConfig?.durableAgents === 'auto' && !probe('running')) {
      this.guardrailLogger?.warn(
        `DurableAgent '${this.id}': recovery.durableAgents is 'auto' but the custom shouldPersistSnapshot policy does not persist 'running' snapshots. ` +
          `In-flight runs of this agent are invisible to crash recovery (listActiveRuns/recoverActiveRuns).`,
      );
    }
  }

  /**
   * Logger for the persistence-policy guardrail warnings. The durable
   * registration path rewires the *underlying* agent's logger but not the
   * wrapper's, so prefer the Mastra-configured logger (which also respects
   * `logger: false`) and fall back to the base logger for unregistered
   * agents.
   *
   * @internal
   */
  protected get guardrailLogger() {
    return this.#mastra?.getLogger() ?? this.logger;
  }

  /**
   * @deprecated Use `stream(messages, { untilIdle: true })` instead.
   *
   * Stream until all background tasks complete and the agent is idle.
   * Mirrors the regular Agent's streamUntilIdle but adapted for durable execution.
   */
  // @ts-expect-error - Intentionally different return type for durable execution
  override async streamUntilIdle<OUTPUT = TOutput>(
    messages: MessageListInput,
    streamOptions?: DurableAgentStreamOptions<OUTPUT> & { maxIdleMs?: number },
  ): Promise<DurableAgentStreamResult<OUTPUT>> {
    const { maxIdleMs, ...options } = streamOptions ?? {};
    return (this.stream as any)(messages, {
      ...options,
      untilIdle: maxIdleMs === undefined ? true : { maxIdleMs },
    }) as Promise<DurableAgentStreamResult<OUTPUT>>;
  }

  /**
   * Prepare for durable execution without starting it.
   */
  async prepare(
    messages: MessageListInput,
    options?: AgentExecutionOptions<TOutput>,
  ): Promise<DurableAgentPreparationResult> {
    messages = snapshotAgentExecutionValue(messages);
    options = options
      ? snapshotAgentExecutionOptions(options, [
          'runId',
          'toolCallId',
          'requestContext',
          'memory',
          'versions',
          '_resolvedDefaultOptions',
        ])
      : undefined;
    let releaseRunIdReservation: (() => void) | undefined;
    let preparation: PreparationResult<TOutput>;
    let cleanup = () => {};
    try {
      if (options?.runId !== undefined) {
        validateMaxSteps(options.maxSteps, this.logger);
        if (options.runId.trim().length === 0) {
          throw new Error('DurableAgent runId must be a non-empty string.');
        }
        await this.#assertPublicAgentResumePreflight({
          requestContext: options.requestContext,
          memory: options.memory,
          runId: options.runId,
          actor: options.actor,
        });
        releaseRunIdReservation = await this.#reserveRunId(options.runId);
      }
      preparation = await prepareForDurableExecution<TOutput>({
        agent: this.#wrappedAgent as Agent<string, any, TOutput>,
        durableAgentId: this.id,
        durableAgentName: this.name,
        messages,
        options,
        durableRequestContextKeys: this.#durableRequestContextKeys,
        runId: options?.runId,
        requestContext: options?.requestContext,
        logger: this.logger,
        mastra: this.#mastra,
      });
      preparation.workflowInput.runtimeResolution = 'registry-required';

      const privatePreparation = this.#createPrivatePreparation(preparation);
      privatePreparation.registryEntry.messageList = privatePreparation.messageList;
      this.#coordinateRegistryCleanup(privatePreparation.runId, privatePreparation.registryEntry);
      this.#preparedExecutions.set(privatePreparation.runId, {
        requestFingerprint: this.#preparedRequestFingerprint(messages, options),
        integrityFingerprint: this.#preparedExecutionIntegrityFingerprint(privatePreparation),
        preparation: privatePreparation,
      });
      registerGlobalRunRegistryEntry(privatePreparation.runId, privatePreparation.registryEntry);
      this.#runRegistry.registerWithMessageList(
        privatePreparation.runId,
        privatePreparation.registryEntry,
        privatePreparation.messageList,
        {
          threadId: privatePreparation.threadId,
          resourceId: privatePreparation.resourceId,
        },
      );
      let cleanupRequested = false;
      cleanup = () => {
        if (cleanupRequested) return;
        const current = this.#preparedExecutions.get(privatePreparation.runId);
        if (current?.preparation !== privatePreparation) return;
        cleanupRequested = true;
        privatePreparation.registryEntry.cleanup?.();
      };
    } finally {
      releaseRunIdReservation?.();
    }

    return {
      runId: preparation.runId,
      messageId: preparation.messageId,
      workflowInput: preparation.workflowInput,
      threadId: preparation.threadId,
      resourceId: preparation.resourceId,
      cleanup,
    };
  }

  /**
   * Get the durable workflows required by this agent.
   * Called by Mastra during agent registration.
   * @internal
   */
  getDurableWorkflows() {
    return [this.getWorkflow()];
  }

  /**
   * Set the Mastra instance.
   * Called by the durable agent registration path in addAgent().
   * Delegates to __registerMastra so the pubsub wiring and agent
   * registration happen regardless of which entry point is called first.
   * @internal
   */
  __setMastra(mastra: Mastra): void {
    this.__registerMastra(mastra);
  }

  /**
   * Register the Mastra instance.
   * Called by Mastra during agent registration (normal Agent path).
   *
   * Also wires mastra.pubsub as the inner pubsub (if the user didn't provide
   * a custom one), so that the OBSERVE_AGENT_STREAM_ROUTE handler can subscribe
   * to the same PubSub instance that this agent publishes to.
   * @internal
   */
  __registerMastra(mastra: Mastra): void {
    super.__registerMastra(mastra);
    this.#mastra = mastra;
    // Also set on wrapped agent
    this.#wrappedAgent.__registerMastra(mastra);

    // Wire mastra.pubsub as the inner pubsub if user didn't provide a custom one.
    // This must happen before CachingPubSub initialization.
    if (!this.#hasCustomPubsub && !this.#cachingPubsub) {
      this.#innerPubsub = mastra.pubsub;
    }

    // If the CachingPubSub was already built (lazy init ran before
    // registration), it was constructed without a source — wire it now so the
    // cache follows the bus the evented engine publishes on. Intentionally
    // done for custom-pubsub agents too: the custom pubsub stays the local
    // transport, but the cache must still observe `mastra.pubsub` or
    // engine-published stream events never reach it.
    if (this.#cachingPubsub instanceof CachingPubSub) {
      this.#cachingPubsub.__setSource(mastra.pubsub);
    }
  }
}
