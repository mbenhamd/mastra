/**
 * EventedAgent - A durable agent that uses fire-and-forget execution.
 *
 * EventedAgent extends DurableAgent and overrides the execution strategy to use
 * fire-and-forget execution: the workflow run is started without awaiting it.
 *
 * Unlike DurableAgent which runs the workflow synchronously, EventedAgent:
 * 1. Uses an un-awaited start() for non-blocking execution
 * 2. Fire-and-forget pattern - execution starts and returns immediately
 * 3. Events are streamed via pubsub as the workflow executes
 */

import { createObservabilityContext } from '../../observability';
import type { ShouldPersistSnapshotFn, WorkflowFinishCallbackResult } from '../../workflows/types';
import type { ToolsInput } from '../types';

import { DurableAgent } from './durable-agent';
import type { DurableAgentConfig } from './durable-agent';
import { getGlobalRunRegistryEntry, pinGlobalRunRegistryEntry, unpinGlobalRunRegistryEntry } from './run-registry';
import type { DurableAgenticWorkflowInput } from './types';
import { defaultShouldPersistSnapshot } from './workflows/create-durable-agentic-workflow';

/**
 * Configuration for EventedAgent - wraps an existing Agent with fire-and-forget execution
 */
export interface EventedAgentConfig<
  TAgentId extends string = string,
  TTools extends ToolsInput = ToolsInput,
  TOutput = undefined,
> extends DurableAgentConfig<TAgentId, TTools, TOutput> {}

/**
 * EventedAgent extends DurableAgent to use fire-and-forget execution.
 *
 * This agent type uses the built-in evented workflow engine, which is useful when:
 * - You don't need an external execution engine (like Inngest)
 * - You want fire-and-forget execution with pubsub streaming
 * - You need resumable streams with event caching
 * - You need runs to survive process death: an active run can be picked up by
 *   a fresh process over the same storage via `recover(runId)` /
 *   `recoverActiveRuns()` (running step state is persisted before execution)
 *
 * The key difference from DurableAgent is the execution strategy:
 * - DurableAgent: Runs the workflow synchronously via createRun + start
 * - EventedAgent: Starts the run without awaiting it (fire-and-forget); steps
 *   execute via events on `mastra.pubsub`, so any worker sharing that bus can
 *   process them. The agent's stream follows `mastra.pubsub`, so streaming,
 *   suspend/resume, and finish events work even when the agent was constructed
 *   with its own pubsub.
 *
 * Register the EventedAgent on a `Mastra` instance (with storage) to get
 * evented execution — the evented engine needs the host's pubsub, storage,
 * and event workers. Without a host, the agent falls back to the default
 * in-process engine (with a warning), preserving the released behavior of a
 * standalone evented agent. Recovery entry points (`recover`,
 * `listActiveRuns`, `recoverActiveRuns`) still require a host and throw
 * directly without one.
 *
 * @example
 * ```typescript
 * import { Mastra } from '@mastra/core';
 * import { Agent } from '@mastra/core/agent';
 * import { EventedAgent } from '@mastra/core/agent/durable';
 *
 * const agent = new Agent({
 *   id: 'my-agent',
 *   instructions: 'You are a helpful assistant',
 *   model: openai('gpt-4'),
 * });
 *
 * const eventedAgent = new EventedAgent({ agent });
 *
 * // Register on a Mastra host to get evented execution (a hostless agent
 * // falls back to the default in-process engine).
 * const mastra = new Mastra({
 *   agents: { myAgent: eventedAgent },
 *   storage: new LibSQLStore({ url: 'file:mastra.db' }),
 * });
 *
 * const { output, runId, cleanup } = await eventedAgent.stream('Hello!');
 * const text = await output.text;
 * cleanup();
 *
 * // After a crash/restart, a fresh process over the same storage can resume:
 * // await eventedAgent.recoverActiveRuns();
 * ```
 */
export class EventedAgent<
  TAgentId extends string = string,
  TTools extends ToolsInput = ToolsInput,
  TOutput = undefined,
> extends DurableAgent<TAgentId, TTools, TOutput> {
  /**
   * Create a new EventedAgent that wraps an existing Agent
   */
  constructor(config: EventedAgentConfig<TAgentId, TTools, TOutput>) {
    super(config);
  }

  /**
   * EventedAgent runs the durable agentic loop on the evented execution
   * engine (pubsub + WorkflowEventProcessor) instead of the default
   * in-process engine.
   * @internal
   */
  protected override get workflowEngine(): 'default' | 'evented' {
    return 'evented';
  }

  /**
   * EventedAgent owns its snapshot-persistence policy and always persists the
   * full set (`pending | paused | suspended | running`), ignoring any
   * user-supplied `shouldPersistSnapshot`.
   *
   * This is structural, not a default: the evented engine's initial `running`
   * write creates the base snapshot row that later suspend-merges build on,
   * and the fire-and-forget model coordinates workers through storage rather
   * than in-process state. Skipping `running` would break suspension and
   * multi-worker coordination.
   *
   * @internal
   */
  protected override resolveShouldPersistSnapshot(): ShouldPersistSnapshotFn {
    return defaultShouldPersistSnapshot;
  }

  /**
   * EventedAgent ignores user-supplied persistence policies (see
   * {@link EventedAgent.resolveShouldPersistSnapshot}), so instead of probing
   * the predicate, warn that it has no effect.
   *
   * @internal
   */
  protected override warnOnRiskyPersistencePolicy(): void {
    if (this.userShouldPersistSnapshot) {
      this.guardrailLogger?.warn(
        `EventedAgent '${this.id}': ignoring the shouldPersistSnapshot option. ` +
          `The evented engine requires the full snapshot set (pending|paused|suspended|running) — ` +
          `the initial 'running' write creates the base row that suspend-merges and multi-worker coordination build on.`,
      );
    }
  }

  /**
   * Execute the durable workflow against the restored evented engine.
   *
   * Awaits the native `run.start()` promise, which resolves on
   * `workflows-finish` after the engine persists suspension (`updateWorkflowState`
   * and pruning precede `workflow.suspend`). The caller already runs
   * `executeWorkflow()` in the background (see `DurableAgent.stream()`), so the
   * public stream stays asynchronous while this promise is the persisted-finish
   * receipt `resume()` / `generate()` wait on. Cleans up snapshots when the
   * background promise reaches a non-suspended terminal status.
   *
   * @param runId - The unique run ID
   * @param workflowInput - The serialized workflow input
   * @internal
   */
  protected override async executeWorkflow(runId: string, workflowInput: DurableAgenticWorkflowInput): Promise<void> {
    try {
      const workflow = this.getWorkflow();
      const entry = pinGlobalRunRegistryEntry(runId);
      // The evented engine executes via pubsub events consumed by in-process
      // workers — without them `run.start()` never resolves (it waits on the
      // `workflows-finish` topic). Idempotent; a no-op when the server has
      // already booted the workers.
      await this.ensureEngineWorkersStarted();
      // Populate the run row's resourceId column so storage-level resource
      // filters (listSuspendedRuns / listActiveRuns) can narrow the query.
      const memoryInfo = (
        workflowInput.messageListState as { memoryInfo?: { threadId?: string; resourceId?: string } } | undefined
      )?.memoryInfo;
      // Note: unlike the default engine, evented `createRun` accepts no
      // `pubsub` — the engine always publishes on `mastra.pubsub` so any
      // worker in a fleet can execute a step. The agent's stream still sees
      // those events because its CachingPubSub follows `mastra.pubsub` as its
      // source (wired at registration; see #ensurePubsubInitialized).
      //
      // On the hostless fallback (default engine, see resolveWorkflowEngine)
      // there is no `mastra.pubsub`: the default engine publishes on the
      // pubsub handed to `createRun`, so pass the agent's own transport —
      // otherwise the caller's stream never sees a single event and hangs.
      // This mirrors the previously shipped standalone behavior.
      const run = await workflow.createRun({
        runId,
        resourceId: workflowInput.state?.resourceId ?? memoryInfo?.resourceId,
        ...(this.resolveWorkflowEngine() === 'default' ? { pubsub: this.pubsubInternal } : {}),
      });
      // The caller already runs executeWorkflow() in the background. Await the
      // native `run.start()` promise so this settles only after the engine's
      // persisted finish receipt (`workflows-finish`, emitted after suspension
      // persistence). Unlike `startAsync()` — which returns only `{ runId }` —
      // `start()` subscribes before dispatch and resolves with the settled
      // result. Preserve the caller's request context and parent the workflow
      // segment under the AGENT_RUN span created during durable preparation.
      await run.start({
        inputData: workflowInput,
        requestContext: entry?.requestContext,
        actor: workflowInput.options?.actor,
        ...createObservabilityContext({ currentSpan: entry?.agentSpan }),
      });
      // A 'failed'/'tripwire' terminal is surfaced as an ERROR event by the
      // workflow `onFinish` hook (`onDurableWorkflowFinish`), exactly once per
      // terminal on every engine; emitting here too would double-publish it.
      // Terminal snapshot cleanup is owned by the workflow `onFinish` hook
      // (`onDurableWorkflowFinish` -> `deleteTerminalRunSnapshots`), which fires
      // once per terminal on every engine (fork routing; see DurableAgent.recover).
      // Deleting here as well would double-delete.
    } catch (error) {
      unpinGlobalRunRegistryEntry(runId, workflowInput.runtimeBindingId);
      // Await delivery so this promise still settles after the error event, but
      // a pubsub that refuses the publish during shutdown must not turn the
      // run's own failure into a rejection (#24071).
      await this.emitError(runId, error instanceof Error ? error : new Error(String(error))).catch(publishError => {
        this.logger.warn(`Failed to publish error event for run ${runId}`, { runId, error: publishError });
      });
    }
  }

  protected override async onDurableWorkflowFinish(result: WorkflowFinishCallbackResult): Promise<void> {
    // Capture the exact pin identity before terminal cleanup can remove the
    // registry entry. Older/custom workflow callbacks may omit getInitData().
    let runtimeBindingId = getGlobalRunRegistryEntry(result.runId)?.runtimeBindingId;
    if (typeof result.getInitData === 'function') {
      runtimeBindingId =
        (result.getInitData() as DurableAgenticWorkflowInput | undefined)?.runtimeBindingId ?? runtimeBindingId;
    }
    try {
      await super.onDurableWorkflowFinish(result);
    } finally {
      unpinGlobalRunRegistryEntry(result.runId, runtimeBindingId);
    }
  }
}

/**
 * Check if an object is an EventedAgent class instance
 */
export function isEventedAgentClass(obj: any): obj is EventedAgent {
  return obj instanceof EventedAgent;
}
