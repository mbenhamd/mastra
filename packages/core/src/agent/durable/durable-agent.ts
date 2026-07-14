import type { MastraServerCache } from '../../cache/base';
import { InMemoryServerCache } from '../../cache/inmemory';
import { MastraError, ErrorCategory, ErrorDomain } from '../../error';
import { CachingPubSub } from '../../events/caching-pubsub';
import { EventEmitterPubSub } from '../../events/event-emitter';
import type { PubSub } from '../../events/pubsub';
import { validateMaxSteps } from '../../llm/model/max-steps';
import type { MastraLanguageModel } from '../../llm/model/shared.types';
import type { Mastra } from '../../mastra';
import {
  MASTRA_RESOURCE_ID_KEY,
  MASTRA_THREAD_ID_KEY,
  MASTRA_VERSIONS_KEY,
  RequestContext,
  mergeVersionOverrides,
} from '../../request-context';
import type { VersionOverrides } from '../../request-context';
import type { MastraModelOutput } from '../../stream/base/output';
import type { ChunkType } from '../../stream/types';
import type { WorkflowFinishCallbackResult } from '../../workflows/types';
import { Agent } from '../agent';
import type { AgentListSuspendedRunsOptions, AgentListSuspendedRunsResult, AgentRunToolCall } from '../agent';
import type { AgentExecutionOptions } from '../agent.types';
import { snapshotAgentExecutionOptions, snapshotAgentExecutionValue } from '../execution-snapshot';
import type { MessageListInput } from '../message-list';
import { MessageList } from '../message-list';
import { stableStringify } from '../message-list/cache/stable-stringify';
import { SaveQueueManager } from '../save-queue';
import type { AgentMemoryOption, ToolsInput } from '../types';

import { AGENT_STREAM_TOPIC, DurableStepIds } from './constants';
import { runDurableStreamUntilIdle } from './durable-stream-until-idle';
import { prepareForDurableExecution } from './preparation';
import type { PreparationResult } from './preparation';
import {
  clearPinnedRunRegistryEntry,
  ExtendedRunRegistry,
  getGlobalRunRegistryEntry,
  globalRunRegistry,
  pinGlobalRunRegistryEntry,
  unpinGlobalRunRegistryEntry,
} from './run-registry';
import { createDurableAgentStream, emitErrorEvent } from './stream-adapter';
import type {
  AgentFinishEventData,
  AgentStepFinishEventData,
  AgentSuspendedEventData,
  DurableAgenticWorkflowInput,
  RunRegistryEntry,
} from './types';
import {
  createRuntimeDependencyFingerprint,
  serializeModelConfig,
  serializeModelList,
  serializeToolsMetadata,
} from './utils/serialize-state';
import { createDurableAgenticWorkflow } from './workflows';

const pendingDurableRunIds = new Set<string>();

/**
 * Options for DurableAgent.stream()
 */
export interface DurableAgentStreamOptions<OUTPUT = undefined> {
  /** Custom instructions that override the agent's default instructions for this execution */
  instructions?: AgentExecutionOptions<OUTPUT>['instructions'];
  /** Additional context messages to provide to the agent */
  context?: AgentExecutionOptions<OUTPUT>['context'];
  /** Memory configuration for conversation persistence and retrieval */
  memory?: AgentExecutionOptions<OUTPUT>['memory'];
  /** Unique identifier for this execution run */
  runId?: string;
  /** Request Context containing dynamic configuration and state */
  requestContext?: AgentExecutionOptions<OUTPUT>['requestContext'];
  /** Maximum number of steps to run */
  maxSteps?: number;
  /** Additional tool sets that can be used for this execution */
  toolsets?: AgentExecutionOptions<OUTPUT>['toolsets'];
  /** Client-side tools available during execution */
  clientTools?: AgentExecutionOptions<OUTPUT>['clientTools'];
  /** Tool selection strategy */
  toolChoice?: AgentExecutionOptions<OUTPUT>['toolChoice'];
  /** Tool names enabled for this execution */
  activeTools?: AgentExecutionOptions<OUTPUT>['activeTools'];
  /** Model-specific settings like temperature */
  modelSettings?: AgentExecutionOptions<OUTPUT>['modelSettings'];
  /** Require approval for all tool calls */
  requireToolApproval?: boolean;
  /** Automatically resume suspended tools */
  autoResumeSuspendedTools?: boolean;
  /** Maximum number of tool calls to execute concurrently */
  toolCallConcurrency?: number;
  /** Whether to include raw chunks in the stream output */
  includeRawChunks?: boolean;
  /** Maximum processor retries */
  maxProcessorRetries?: number;
  /** Structured output configuration */
  structuredOutput?: AgentExecutionOptions<OUTPUT>['structuredOutput'];
  /** Version overrides for sub-agent delegation */
  versions?: AgentExecutionOptions<OUTPUT>['versions'];
  /** Callback when chunk is received */
  onChunk?: (chunk: ChunkType<OUTPUT>) => void | Promise<void>;
  /** Callback when step finishes */
  onStepFinish?: (result: AgentStepFinishEventData) => void | Promise<void>;
  /** Callback when execution finishes */
  onFinish?: (result: AgentFinishEventData) => void | Promise<void>;
  /** Callback on error */
  onError?: (error: Error) => void | Promise<void>;
  /** Callback when workflow suspends (e.g., for tool approval) */
  onSuspended?: (data: AgentSuspendedEventData) => void | Promise<void>;
  /** When true, the in-loop background task check step skips waiting (streamUntilIdle sets this) */
  _skipBgTaskWait?: boolean;
}

/** Runtime callbacks plus caller identity used to resume a durable segment. */
export interface DurableAgentResumeOptions<OUTPUT = undefined> {
  onChunk?: (chunk: ChunkType<OUTPUT>) => void | Promise<void>;
  onStepFinish?: (result: AgentStepFinishEventData) => void | Promise<void>;
  onFinish?: (result: AgentFinishEventData) => void | Promise<void>;
  onError?: (error: Error) => void | Promise<void>;
  onSuspended?: (data: AgentSuspendedEventData) => void | Promise<void>;
  /** Required for ownership verification when recovering after process loss. */
  requestContext?: AgentExecutionOptions<OUTPUT>['requestContext'];
  /** Thread/resource target required for cold suspended-run recovery. */
  memory?: AgentExecutionOptions<OUTPUT>['memory'];
  /** Version overrides must exactly match the selectors persisted by the suspended run. */
  versions?: AgentExecutionOptions<OUTPUT>['versions'];
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
export class DurableAgent<
  TAgentId extends string = string,
  TTools extends ToolsInput = ToolsInput,
  TOutput = undefined,
> extends Agent<TAgentId, TTools, TOutput> {
  /** The wrapped agent */
  readonly #wrappedAgent: Agent<TAgentId, TTools, TOutput>;

  /** Registry for per-run non-serializable state */
  readonly #runRegistry: ExtendedRunRegistry;

  /** The durable workflow for agent execution */
  #workflow: ReturnType<typeof createDurableAgenticWorkflow> | null = null;

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

  /**
   * Create a new DurableAgent that wraps an existing Agent
   */
  constructor(config: DurableAgentConfig<TAgentId, TTools, TOutput>) {
    const { agent, id: idOverride, name: nameOverride, pubsub, cache, maxSteps, cleanupTimeoutMs } = config;

    // Use provided id/name or fall back to agent.id/agent.name
    const agentId = idOverride ?? agent.id;
    const agentName = nameOverride ?? agent.name ?? agent.id;

    // Call Agent constructor with minimal config - we delegate to the wrapped agent
    super({
      id: agentId as TAgentId,
      name: agentName,
      // Delegate to wrapped agent's instructions
      instructions: ({ requestContext }) => agent.getInstructions({ requestContext }),
      // We need to provide model to satisfy the base class, but we'll delegate to wrapped agent
      model: (() => {
        validateMaxSteps(maxSteps);
        return (agent as any).__model ?? agent.getModel();
      })(),
    });

    this.#wrappedAgent = agent;
    this.#runRegistry = new ExtendedRunRegistry();
    this.#maxSteps = maxSteps;
    this.#hasCustomPubsub = !!pubsub;
    this.#innerPubsub = pubsub ?? new EventEmitterPubSub();
    this.#cacheConfig = cache;
    this.#cleanupTimeoutMs = cleanupTimeoutMs ?? 30_000;
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
    } else {
      // Resolve cache: user-provided > mastra's cache > default InMemoryServerCache
      const resolvedCache = this.#cacheConfig ?? this.#mastra?.serverCache ?? new InMemoryServerCache();
      this.#resolvedCache = resolvedCache;
      this.#cachingPubsub = new CachingPubSub(this.#innerPubsub, resolvedCache);
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

  // Delegate Agent methods to wrapped agent
  override getModel(options?: any) {
    return this.#wrappedAgent.getModel(options);
  }

  override getInstructions(options?: any) {
    return this.#wrappedAgent.getInstructions(options);
  }

  override listTools(options?: any) {
    return this.#wrappedAgent.listTools(options);
  }

  override getMemory() {
    return this.#wrappedAgent.getMemory();
  }

  override getVoice() {
    return this.#wrappedAgent.getVoice();
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
   * Execute the durable workflow.
   *
   * Subclasses override this method to customize how the workflow is executed:
   * - DurableAgent (this): Runs the workflow directly via createRun + start
   * - EventedAgent: Uses run.startAsync() for fire-and-forget execution
   * - InngestAgent: Uses inngest.send() to trigger Inngest function
   *
   * @param runId - The unique run ID
   * @param workflowInput - The serialized workflow input
   * @internal
   */
  protected async executeWorkflow(runId: string, workflowInput: DurableAgenticWorkflowInput): Promise<void> {
    const workflow = this.getWorkflow();
    const requestContext = pinGlobalRunRegistryEntry(runId)?.requestContext;

    try {
      const run = await workflow.createRun({
        runId,
        resourceId: workflowInput.state?.resourceId,
        pubsub: this.pubsub,
      });
      await run.start({ inputData: workflowInput, requestContext });
    } finally {
      unpinGlobalRunRegistryEntry(runId);
    }
  }

  /**
   * Handle completion of the outer durable workflow.
   *
   * The workflow lifecycle owns terminal handling for start(), startAsync(),
   * and later resume() calls.
   * @internal
   */
  protected async onDurableWorkflowFinish(result: WorkflowFinishCallbackResult): Promise<void> {
    if (['running', 'suspended', 'waiting', 'pending', 'paused'].includes(result.status)) return;

    try {
      await this.deleteTerminalRunSnapshots(result.runId);
    } catch (error) {
      this.logger.warn(`[DurableAgent] Failed to clean terminal workflow snapshots for run ${result.runId}`, {
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
      },
      this.logger,
    );
  }

  /**
   * Emit an error event to pubsub.
   *
   * @param runId - The run ID
   * @param error - The error to emit
   * @internal
   */
  protected async emitError(runId: string, error: Error): Promise<void> {
    await emitErrorEvent(this.pubsub, runId, error);
  }

  /** Best-effort deletion of the outer and nested snapshots for a terminal run. */
  protected async deleteTerminalRunSnapshots(runId: string): Promise<void> {
    const workflowsStore = await this.#mastra?.getStorage()?.getStore('workflows');
    const deleteOrTombstone = async (workflowName: string, deletion: () => Promise<void>) => {
      try {
        await deletion();
        return;
      } catch {
        // Retry once for transient driver failures before replacing any stale
        // suspended row with a terminal tombstone.
        try {
          await deletion();
          return;
        } catch (cause) {
          if (workflowsStore) {
            try {
              await workflowsStore.persistWorkflowSnapshot({
                workflowName,
                runId,
                snapshot: { status: 'success', context: {}, resumeLabels: {} } as any,
              });
              return;
            } catch {
              // Surface the original deletion failure to logging below.
            }
          }
          throw cause;
        }
      }
    };
    const deletions = await Promise.allSettled([
      deleteOrTombstone(DurableStepIds.AGENTIC_LOOP, () => this.getWorkflow().deleteWorkflowRunById(runId)),
      deleteOrTombstone(DurableStepIds.AGENTIC_EXECUTION, async () => {
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
      requestContext: preparation.registryEntry.requestContext
        ? snapshotAgentExecutionValue(preparation.registryEntry.requestContext)
        : undefined,
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
        this.#runRegistry.cleanup(runId);
        clearPinnedRunRegistryEntry(runId);
        globalRunRegistry.delete(runId);
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
    runId: string;
    snapshotMemoryInfo?: { threadId?: string; resourceId?: string };
  }): Promise<void> {
    await this.#wrappedAgent.__assertAgentResumePreflight({
      ...options,
      agentId: this.id,
      agentName: this.name,
    });
  }

  #durableSnapshotPairIsConsistent(
    runId: string,
    outerRun: any,
    outerSnapshot: any,
    nestedRun: any,
    nestedSnapshot: any,
  ): boolean {
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

  async #resolveDurableResumeLabel(
    runId: string,
    requestedToolCallId?: string,
  ): Promise<{ label?: string; persisted: boolean }> {
    const workflowsStore = await this.#mastra?.getStorage()?.getStore('workflows');
    if (!workflowsStore) return { label: requestedToolCallId, persisted: false };
    let persisted;
    let nestedPersisted;
    try {
      [persisted, nestedPersisted] = await Promise.all([
        workflowsStore.getWorkflowRunById({
          workflowName: DurableStepIds.AGENTIC_LOOP,
          runId,
        }),
        workflowsStore.getWorkflowRunById({
          workflowName: DurableStepIds.AGENTIC_EXECUTION,
          runId,
        }),
      ]);
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
    let nestedSnapshot: any = nestedPersisted?.snapshot;
    if (typeof nestedSnapshot === 'string') {
      try {
        nestedSnapshot = JSON.parse(nestedSnapshot);
      } catch {
        throw new MastraError({
          id: 'DURABLE_AGENT_RESUME_INVALID_SNAPSHOT',
          domain: ErrorDomain.AGENT,
          category: ErrorCategory.SYSTEM,
          text: `DurableAgent "${this.name}" found an invalid nested snapshot for run "${runId}".`,
          details: { agentName: this.name, runId },
        });
      }
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
    options: Pick<DurableAgentResumeOptions<TOutput>, 'requestContext' | 'memory' | 'versions'> = {},
  ): Promise<void> {
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
    let nestedPersisted;
    try {
      [persisted, nestedPersisted] = await Promise.all([
        workflowsStore.getWorkflowRunById({
          workflowName: DurableStepIds.AGENTIC_LOOP,
          runId,
        }),
        workflowsStore.getWorkflowRunById({
          workflowName: DurableStepIds.AGENTIC_EXECUTION,
          runId,
        }),
      ]);
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
    let nestedSnapshot: any = nestedPersisted?.snapshot;
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
    const requestContext = options.requestContext ?? new RequestContext();
    const requestVersions = requestContext.get(MASTRA_VERSIONS_KEY) as VersionOverrides | undefined;
    let mergedVersions = mergeVersionOverrides(this.#mastra?.getVersionOverrides(), workflowInput.versions);
    mergedVersions = mergeVersionOverrides(mergedVersions, requestVersions);
    if (options.versions) {
      mergedVersions = mergeVersionOverrides(mergedVersions, options.versions);
    }
    if (mergedVersions) {
      requestContext.set(MASTRA_VERSIONS_KEY, mergedVersions);
    }
    const resourceIdFromContext = requestContext.get(MASTRA_RESOURCE_ID_KEY);
    const hasFga = Boolean(this.#mastra?.getServer()?.fga);
    const callerResourceId = resourceIdFromContext ?? (hasFga ? undefined : options.memory?.resource);
    const requestedThread = options.memory?.thread;
    const memoryThreadId = typeof requestedThread === 'string' ? requestedThread : requestedThread?.id;
    const threadIdFromContext = requestContext.get(MASTRA_THREAD_ID_KEY);
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
    const [tools, model, modelList, memory, workspace, inputProcessors, outputProcessors, errorProcessors] =
      await Promise.all([
        this.#wrappedAgent.getToolsForExecution({
          threadId,
          resourceId,
          runId,
          requestContext,
          memoryConfig,
          autoResumeSuspendedTools: workflowInput.options?.autoResumeSuspendedTools,
          agentId: this.id,
          agentName: this.name,
        }),
        this.#wrappedAgent.getModel({ requestContext }),
        this.#wrappedAgent.getModelList(requestContext),
        this.#wrappedAgent.getMemory({ requestContext }),
        this.#wrappedAgent.getWorkspace({ requestContext }),
        this.#wrappedAgent.listInputProcessors(requestContext),
        this.#wrappedAgent.listOutputProcessors(requestContext),
        this.#wrappedAgent.listErrorProcessors(requestContext),
      ]);
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
    const registryEntry = {
      agentId: this.id,
      threadId,
      resourceId,
      tools,
      model: model as MastraLanguageModel,
      modelList: modelList?.map(entry => ({
        id: entry.id,
        model: entry.model,
        maxRetries: entry.maxRetries ?? 0,
        enabled: entry.enabled ?? true,
      })),
      memory,
      saveQueueManager,
      workspace,
      requestContext: snapshotAgentExecutionValue(requestContext),
      versions: workflowInput.versions ? structuredClone(workflowInput.versions) : undefined,
      inputProcessors,
      outputProcessors,
      errorProcessors,
      processorStates: new Map(),
      backgroundTaskManager: this.#mastra?.backgroundTaskManager,
      backgroundTasksConfig: this.#wrappedAgent.getBackgroundTasksConfig(),
      messageList,
      cleanup: () => {},
    };
    this.#coordinateRegistryCleanup(runId, registryEntry);
    this.#assertNoRegistryCollision(runId);
    this.#runRegistry.registerWithMessageList(runId, registryEntry, messageList, { threadId, resourceId });
    globalRunRegistry.set(runId, registryEntry);
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

      const nestedRun = nestedRunsById.get(run.runId);
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
      ? snapshotAgentExecutionOptions(options, ['runId', 'toolCallId', 'requestContext', 'memory', 'versions'])
      : undefined;
    let releaseRunIdReservation: (() => void) | undefined;
    let reusedPreparedExecution = false;
    // 1. Prepare for durable execution (non-durable phase)
    let preparation: PreparationResult<TOutput> | undefined;
    try {
      if (options?.runId !== undefined) {
        validateMaxSteps(options.maxSteps, this.logger);
        if (options.runId.trim().length === 0) {
          throw new Error('DurableAgent runId must be a non-empty string.');
        }
        const prepared = this.#preparedExecutions.get(options.runId)?.preparation;
        if (prepared) {
          const snapshotMemoryInfo = { threadId: prepared.threadId, resourceId: prepared.resourceId };
          await this.#assertPublicAgentResumePreflight({
            requestContext: options.requestContext,
            memory: options.memory,
            runId: options.runId,
            snapshotMemoryInfo,
          });
          const requestFingerprint = this.#preparedRequestFingerprint(messages, options);
          const matchedPreparation = this.#getPreparedExecution(options.runId, requestFingerprint);
          if (!matchedPreparation) this.#throwRunIdConflict(options.runId);
          preparation = matchedPreparation;
          releaseRunIdReservation = await this.#reserveRunId(options.runId, {
            agentId: preparation.registryEntry.agentId,
            threadId: preparation.threadId,
            resourceId: preparation.resourceId,
          });
          await this.#assertPublicAgentResumePreflight({
            requestContext: options.requestContext,
            memory: options.memory,
            runId: options.runId,
            snapshotMemoryInfo,
          });
          if (this.#getPreparedExecution(options.runId, requestFingerprint) !== preparation) {
            this.#throwRunIdConflict(options.runId);
          }
          reusedPreparedExecution = true;
        } else {
          await this.#assertPublicAgentResumePreflight({
            requestContext: options.requestContext,
            memory: options.memory,
            runId: options.runId,
          });
          releaseRunIdReservation = await this.#reserveRunId(options.runId);
        }
      }

      preparation ??= await prepareForDurableExecution<TOutput>({
        agent: this.#wrappedAgent as Agent<string, any, TOutput>,
        durableAgentId: this.id,
        durableAgentName: this.name,
        messages,
        options: options as AgentExecutionOptions<TOutput>,
        runId: options?.runId,
        requestContext: options?.requestContext,
        logger: this.logger,
        mastra: this.#mastra,
      });
      preparation.workflowInput.runtimeResolution = 'registry-required';
      const { runId, registryEntry, messageList, threadId, resourceId } = preparation;

      // 2. Register non-serializable state (both local and global registries)
      if (reusedPreparedExecution) {
        this.#consumePreparedExecution(runId);
      } else {
        registryEntry.messageList = messageList;
        registryEntry.requestContext = registryEntry.requestContext
          ? snapshotAgentExecutionValue(registryEntry.requestContext)
          : undefined;
        this.#coordinateRegistryCleanup(runId, registryEntry);
        this.#runRegistry.registerWithMessageList(runId, registryEntry, messageList, { threadId, resourceId });
        globalRunRegistry.set(runId, registryEntry);
      }
    } finally {
      releaseRunIdReservation?.();
    }

    const { runId, messageId, workflowInput, registryEntry, threadId, resourceId } = preparation;

    // Track cleanup state to avoid double cleanup
    let cleanedUp = false;
    let autoCleanupTimer: ReturnType<typeof setTimeout> | null = null;

    // Schedule automatic registry cleanup after stream ends
    const scheduleAutoCleanup = () => {
      if (autoCleanupTimer || cleanedUp || this.#cleanupTimeoutMs === 0) return;
      autoCleanupTimer = setTimeout(() => {
        if (!cleanedUp) {
          this.#runRegistry.cleanup(runId);
          globalRunRegistry.delete(runId);
          this.#clearPubsubTopic(runId);
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
      onStepFinish: options?.onStepFinish,
      onFinish: async result => {
        await options?.onFinish?.(result);
        scheduleAutoCleanup();
      },
      onError: async error => {
        await options?.onError?.(error);
        scheduleAutoCleanup();
      },
      onSuspended: options?.onSuspended,
    });

    // 4. Wait for subscription to be ready, then execute workflow
    // This prevents race conditions where events are published before subscription
    const workflowExecution = ready
      .then(() => this.executeWorkflow(runId, workflowInput))
      .catch(error => {
        void this.emitError(runId, error);
      });
    registryEntry.workflowExecution = workflowExecution;
    const globalEntry = getGlobalRunRegistryEntry(runId);
    if (globalEntry) globalEntry.workflowExecution = workflowExecution;

    // 5. Create cleanup function (cancels auto-cleanup timer if called)
    const cleanup = () => {
      if (autoCleanupTimer) {
        clearTimeout(autoCleanupTimer);
        autoCleanupTimer = null;
      }
      if (!cleanedUp) {
        streamCleanup();
        this.#runRegistry.cleanup(runId);
        globalRunRegistry.delete(runId);
        this.#clearPubsubTopic(runId);
        cleanedUp = true;
      }
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
    resumeData = snapshotAgentExecutionValue(resumeData);
    options = options
      ? snapshotAgentExecutionOptions(options, ['runId', 'toolCallId', 'requestContext', 'memory', 'versions'])
      : undefined;
    let entry = this.#runRegistry.get(runId);
    let priorExecution = entry?.workflowExecution;
    const warmMemoryInfo = entry ? this.#runRegistry.getMemoryInfo(runId) : undefined;
    const hasFga = Boolean(this.#mastra?.getServer()?.fga);
    const explicitRequestContext = options?.requestContext;
    const contextResourceId = explicitRequestContext?.get(MASTRA_RESOURCE_ID_KEY);
    const contextThreadId = explicitRequestContext?.get(MASTRA_THREAD_ID_KEY);
    const requestedThread = options?.memory?.thread;
    const requestedThreadId = typeof requestedThread === 'string' ? requestedThread : requestedThread?.id;
    if (
      hasFga &&
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
    await this.#assertPublicAgentResumePreflight({
      requestContext: explicitRequestContext,
      memory: callerMemory,
      runId,
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
    await this.#assertPublicAgentResumePreflight({
      requestContext: explicitRequestContext,
      memory:
        callerMemory ??
        (warmMemoryInfo?.threadId && warmMemoryInfo.resourceId
          ? { thread: warmMemoryInfo.threadId, resource: warmMemoryInfo.resourceId }
          : undefined),
      runId,
      snapshotMemoryInfo: warmMemoryInfo,
    });
    if (entry && !this.#activeRegistryPairIsValid(runId, entry)) entry = undefined;
    if (entry) this.#assertWarmResumeVersionSelectors(runId, entry, options ?? {});

    let resolvedResumeLabel: { label?: string; persisted: boolean } | undefined;
    if (!entry) {
      await this.#rehydrateSuspendedRunRegistry(runId, options);
      entry = this.#runRegistry.get(runId);
    }
    priorExecution ??= entry?.workflowExecution;
    if (priorExecution) {
      await priorExecution.catch(() => {
        // The prior segment already emits its own error event.
      });
    }
    resolvedResumeLabel = await this.#resolveDurableResumeLabel(runId, options?.toolCallId);
    if (!entry) {
      throw new MastraError({
        id: 'DURABLE_AGENT_RESUME_REGISTRY_REHYDRATION_FAILED',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.SYSTEM,
        text: `DurableAgent "${this.name}" could not restore runtime state for run "${runId}".`,
        details: { agentName: this.name, runId },
      });
    }
    if (!this.#activeRegistryPairIsValid(runId, entry)) {
      await this.#rehydrateSuspendedRunRegistry(runId, options);
      entry = this.#runRegistry.get(runId);
      if (!entry || !this.#activeRegistryPairIsValid(runId, entry)) {
        throw new MastraError({
          id: 'DURABLE_AGENT_RESUME_REGISTRY_REHYDRATION_FAILED',
          domain: ErrorDomain.AGENT,
          category: ErrorCategory.SYSTEM,
          text: `DurableAgent "${this.name}" could not restore runtime state for run "${runId}".`,
          details: { agentName: this.name, runId },
        });
      }
    }
    const prevalidatedResumeLabel = resolvedResumeLabel.persisted ? resolvedResumeLabel.label : undefined;

    const memoryInfo = this.#runRegistry.getMemoryInfo(runId);
    const resumeRequestContext = options?.requestContext ?? entry.requestContext;
    const typedResumeRequestContext = resumeRequestContext as RequestContext | undefined;

    // Track cleanup state to avoid double cleanup
    let cleanedUp = false;
    let autoCleanupTimer: ReturnType<typeof setTimeout> | null = null;

    const scheduleAutoCleanup = () => {
      if (autoCleanupTimer || cleanedUp || this.#cleanupTimeoutMs === 0) return;
      autoCleanupTimer = setTimeout(() => {
        if (!cleanedUp) {
          this.#runRegistry.cleanup(runId);
          globalRunRegistry.delete(runId);
          this.#clearPubsubTopic(runId);
          cleanedUp = true;
        }
      }, this.#cleanupTimeoutMs);
    };

    const globalEntry = getGlobalRunRegistryEntry(runId);
    const resumeModel = globalEntry?.model as any;

    const {
      output,
      cleanup: streamCleanup,
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
      onChunk: options?.onChunk,
      onStepFinish: options?.onStepFinish,
      onFinish: async result => {
        await options?.onFinish?.(result);
        scheduleAutoCleanup();
      },
      onError: async error => {
        await options?.onError?.(error);
        scheduleAutoCleanup();
      },
      onSuspended: options?.onSuspended,
    });

    // Wait for subscription to be ready, then resume workflow
    const workflow = this.getWorkflow();
    const requestContext = typedResumeRequestContext;
    const workflowExecution = ready
      .then(async () => {
        if (!entry || !this.#activeRegistryPairIsValid(runId, entry)) {
          await this.#rehydrateSuspendedRunRegistry(runId, options);
          entry = this.#runRegistry.get(runId);
          if (!entry || !this.#activeRegistryPairIsValid(runId, entry)) {
            throw new MastraError({
              id: 'DURABLE_AGENT_RESUME_REGISTRY_REHYDRATION_FAILED',
              domain: ErrorDomain.AGENT,
              category: ErrorCategory.SYSTEM,
              text: `DurableAgent "${this.name}" could not restore runtime state for run "${runId}".`,
              details: { agentName: this.name, runId },
            });
          }
        }
        if (priorExecution) {
          await priorExecution.catch(() => {
            // The prior segment already emits its own error event.
          });
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
        const pinnedEntry = pinGlobalRunRegistryEntry(runId);
        if (pinnedEntry !== entry) this.#throwRunIdConflict(runId);
        try {
          const run = await workflow.createRun({ runId, resourceId: memoryInfo?.resourceId, pubsub: this.pubsub });
          await run.resume({ resumeData, label: resumeLabel, requestContext });
        } finally {
          unpinGlobalRunRegistryEntry(runId);
        }
      })
      .catch(error => {
        void this.emitError(runId, error);
      });
    entry.workflowExecution = workflowExecution;
    const resumedGlobalEntry = getGlobalRunRegistryEntry(runId);
    if (resumedGlobalEntry) resumedGlobalEntry.workflowExecution = workflowExecution;

    const cleanup = () => {
      if (autoCleanupTimer) {
        clearTimeout(autoCleanupTimer);
        autoCleanupTimer = null;
      }
      if (!cleanedUp) {
        streamCleanup();
        this.#runRegistry.cleanup(runId);
        globalRunRegistry.delete(runId);
        this.#clearPubsubTopic(runId);
        cleanedUp = true;
      }
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
    };
  }

  /** Route the base Agent approval helpers through the durable workflow. */
  // @ts-expect-error - DurableAgent exposes the durable workflow callback contract instead of the base loop callbacks.
  override async resumeStream(
    resumeData: any,
    streamOptions?: DurableAgentResumeOptions<TOutput> & { runId: string },
  ): Promise<MastraModelOutput<TOutput>> {
    const runId = streamOptions?.runId;
    if (!runId) {
      throw new MastraError({
        id: 'DURABLE_AGENT_RESUME_RUN_ID_REQUIRED',
        domain: ErrorDomain.AGENT,
        category: ErrorCategory.USER,
        text: `DurableAgent "${this.name}" resumeStream() requires runId.`,
        details: { agentName: this.name },
      });
    }
    const result = await this.resume(runId, resumeData, {
      onChunk: streamOptions?.onChunk,
      onStepFinish: streamOptions?.onStepFinish,
      onFinish: streamOptions?.onFinish,
      onError: streamOptions?.onError,
      onSuspended: streamOptions?.onSuspended,
      requestContext: streamOptions?.requestContext,
      memory: streamOptions?.memory,
      versions: streamOptions?.versions,
      toolCallId: streamOptions?.toolCallId,
    });
    return result.output;
  }

  /**
   * Observe an existing stream.
   * Use this to reconnect to a stream after a network disconnection.
   *
   * **Warning:** The returned `cleanup()` function destroys the run's registry
   * entries and cached PubSub events. Only call it when you are done with the
   * run entirely. If the workflow is suspended and you intend to resume later,
   * do not call cleanup — let the auto-cleanup timer handle it after
   * FINISH/ERROR. Auto-cleanup does not fire on SUSPENDED events.
   */
  async observe(
    runId: string,
    options?: {
      offset?: number;
      onChunk?: (chunk: ChunkType<TOutput>) => void | Promise<void>;
      onStepFinish?: (result: AgentStepFinishEventData) => void | Promise<void>;
      onFinish?: (result: AgentFinishEventData) => void | Promise<void>;
      onError?: (error: Error) => void | Promise<void>;
      onSuspended?: (data: AgentSuspendedEventData) => void | Promise<void>;
    },
  ): Promise<Omit<DurableAgentStreamResult<TOutput>, 'runId'> & { runId: string }> {
    const memoryInfo = this.#runRegistry.getMemoryInfo(runId);

    // Track cleanup state to avoid double cleanup
    let cleanedUp = false;
    let autoCleanupTimer: ReturnType<typeof setTimeout> | null = null;

    const scheduleAutoCleanup = () => {
      if (autoCleanupTimer || cleanedUp || this.#cleanupTimeoutMs === 0) return;
      autoCleanupTimer = setTimeout(() => {
        if (!cleanedUp) {
          this.#runRegistry.cleanup(runId);
          globalRunRegistry.delete(runId);
          this.#clearPubsubTopic(runId);
          cleanedUp = true;
        }
      }, this.#cleanupTimeoutMs);
    };

    const {
      output,
      cleanup: streamCleanup,
      ready,
    } = createDurableAgentStream<TOutput>({
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
      onChunk: options?.onChunk,
      onStepFinish: options?.onStepFinish,
      onFinish: async result => {
        await options?.onFinish?.(result);
        scheduleAutoCleanup();
      },
      onError: async error => {
        await options?.onError?.(error);
        scheduleAutoCleanup();
      },
      onSuspended: options?.onSuspended,
    });

    // Wait for subscription to be ready
    await ready;

    const cleanup = () => {
      if (autoCleanupTimer) {
        clearTimeout(autoCleanupTimer);
        autoCleanupTimer = null;
      }
      if (!cleanedUp) {
        streamCleanup();
        this.#runRegistry.cleanup(runId);
        globalRunRegistry.delete(runId);
        this.#clearPubsubTopic(runId);
        cleanedUp = true;
      }
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
    };
  }

  /**
   * Clear cached pubsub events for a run's topic.
   * Only effective when pubsub supports clearTopic (e.g. CachingPubSub).
   */
  #clearPubsubTopic(runId: string): void {
    const pubsub = this.pubsub;
    if ('clearTopic' in pubsub && typeof (pubsub as any).clearTopic === 'function') {
      void (pubsub as any).clearTopic(AGENT_STREAM_TOPIC(runId));
    }
  }

  /**
   * Get the workflow instance for direct execution.
   * Lazily creates the workflow and registers Mastra on it (needed for
   * getAgentById in execution steps).
   */
  getWorkflow() {
    if (!this.#workflow) {
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
      }
    }
    return this.#workflow;
  }

  /**
   * Stream until all background tasks complete and the agent is idle.
   * Mirrors the regular Agent's streamUntilIdle but adapted for durable execution.
   */
  // @ts-expect-error - Intentionally different return type for durable execution
  override async streamUntilIdle<OUTPUT = TOutput>(
    messages: MessageListInput,
    streamOptions?: DurableAgentStreamOptions<OUTPUT> & { maxIdleMs?: number },
  ): Promise<DurableAgentStreamResult<OUTPUT>> {
    return runDurableStreamUntilIdle<OUTPUT>(
      this as unknown as DurableAgent<any, any, OUTPUT>,
      messages,
      streamOptions,
      {
        activeStreams: this.#activeStreamUntilIdle,
        bgManager: this.#mastra?.backgroundTaskManager,
      },
    );
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
      ? snapshotAgentExecutionOptions(options, ['runId', 'toolCallId', 'requestContext', 'memory', 'versions'])
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
        });
        releaseRunIdReservation = await this.#reserveRunId(options.runId);
      }
      preparation = await prepareForDurableExecution<TOutput>({
        agent: this.#wrappedAgent as Agent<string, any, TOutput>,
        durableAgentId: this.id,
        durableAgentName: this.name,
        messages,
        options,
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
      this.#runRegistry.registerWithMessageList(
        privatePreparation.runId,
        privatePreparation.registryEntry,
        privatePreparation.messageList,
        {
          threadId: privatePreparation.threadId,
          resourceId: privatePreparation.resourceId,
        },
      );
      globalRunRegistry.set(privatePreparation.runId, privatePreparation.registryEntry);
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
  }
}
