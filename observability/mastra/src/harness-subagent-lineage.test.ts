/**
 * Harness subagent request-lineage integration.
 *
 * This crosses the real supervisor Agent -> spawn_subagent -> child Agent
 * provider pipeline and inspects completed MODEL_INFERENCE spans from the real
 * TestExporter. The models are deterministic provider mocks; Harness, Agent,
 * observability, and span serialization are production implementations.
 */
import { MockLanguageModelV2, convertArrayToReadableStream } from '@internal/ai-sdk-v5/test';
import { Agent } from '@mastra/core/agent';
import { Harness } from '@mastra/core/harness/v1';
import { Mastra } from '@mastra/core/mastra';
import { MockMemory } from '@mastra/core/memory';
import { SpanType } from '@mastra/core/observability';
import { InMemoryStore } from '@mastra/core/storage';
import { describe, expect, it, vi } from 'vitest';

import { Observability } from './default';
import { TestExporter } from './exporters';

const usage = { inputTokens: 4, outputTokens: 2, totalTokens: 6 };

function toolCallModel(options: {
  provider: string;
  modelId: string;
  responseId: string;
  toolCallId: string;
  toolName: string;
  input: object;
}) {
  const doStream = vi.fn(async () => ({
    rawCall: { rawPrompt: null, rawSettings: {} },
    warnings: [],
    stream: convertArrayToReadableStream([
      { type: 'stream-start', warnings: [] },
      { type: 'response-metadata', id: options.responseId, modelId: options.modelId, timestamp: new Date(0) },
      {
        type: 'tool-call',
        toolCallType: 'function',
        toolCallId: options.toolCallId,
        toolName: options.toolName,
        input: JSON.stringify(options.input),
        providerExecuted: false,
      },
      { type: 'finish', finishReason: 'tool-calls', usage },
    ]),
  }));
  return {
    doStream,
    model: new MockLanguageModelV2({
      provider: options.provider,
      modelId: options.modelId,
      doStream,
    }),
  };
}

async function settle(exporter: TestExporter, maxMs = 2_000): Promise<void> {
  let previousCount = -1;
  for (let waited = 0; waited < maxMs; waited += 20) {
    const count = exporter.getAllSpans().length;
    if (count > 0 && count === previousCount) return;
    previousCount = count;
    await new Promise(resolve => setTimeout(resolve, 20));
  }
}

describe('Harness subagent provider lineage (real exporter)', () => {
  it('joins supervisor and child inference spans with only the approved opaque app key', async () => {
    const lineageKey = 'turnCorrelationId';
    const lineageValue = 'opaque-turn-5049';
    const functionalRequestId = 'FUNCTIONAL_REQUEST_ID_CANARY';
    const userIdCanary = 'PRIVATE_USER_ID_CANARY';
    const contentCanary = 'PRIVATE_INSTRUCTION_CONTENT_CANARY';
    const exporter = new TestExporter();

    const rootProvider = toolCallModel({
      provider: 'root-provider',
      modelId: 'root-model',
      responseId: 'root-response',
      toolCallId: 'spawn-child',
      toolName: 'spawn_subagent',
      input: {
        agentType: 'worker',
        task: 'Return the verified child outcome.',
        delivery: 'final',
      },
    });
    const childProvider = toolCallModel({
      provider: 'child-provider',
      modelId: 'child-model',
      responseId: 'child-response',
      toolCallId: 'report-child-outcome',
      toolName: 'report_subagent_outcome',
      input: {
        outcome: 'completed',
        summary: 'child lineage verified',
        evidence: [{ kind: 'analysis', description: 'Deterministic provider fixture completed.' }],
      },
    });

    const storage = new InMemoryStore();
    const rootAgent = new Agent({
      id: 'root-agent',
      name: 'root-agent',
      instructions: 'Delegate once.',
      model: rootProvider.model,
      memory: new MockMemory({ storage }),
    });
    const childAgent = new Agent({
      id: 'child-agent',
      name: 'child-agent',
      instructions: 'Report the completed outcome.',
      model: childProvider.model,
      memory: new MockMemory({ storage }),
    });
    const mastra = new Mastra({
      agents: { 'root-agent': rootAgent, 'child-agent': childAgent } as any,
      storage,
      observability: new Observability({
        configs: {
          test: {
            serviceName: 'harness-subagent-lineage-it',
            exporters: [exporter],
            requestContextKeys: [lineageKey],
          },
        },
      }),
    });
    const harness = new Harness({
      mastra,
      modes: [
        { id: 'root-mode', agentId: 'root-agent' },
        { id: 'child-mode', agentId: 'child-agent' },
      ],
      defaultModeId: 'root-mode',
      subagents: {
        inheritRequestContextAppKeys: [lineageKey],
        maxDepth: 1,
        types: {
          worker: {
            agentId: 'child-agent',
            modeId: 'child-mode',
            description: 'Deterministic lineage worker',
            defaultModelId: 'child-model',
          },
        },
      },
    });

    try {
      const session = await harness.session({ resourceId: 'lineage-user', threadId: { fresh: true } });
      const result = await session.message({
        content: 'Delegate the lineage check.',
        requestContext: {
          app: {
            [lineageKey]: lineageValue,
            'papersflow.requestId': functionalRequestId,
            'papersflow.userId': userIdCanary,
            instructions: contentCanary,
          },
        },
      });
      expect(result.text).toBe('child lineage verified');
      expect(rootProvider.doStream).toHaveBeenCalledTimes(1);
      expect(childProvider.doStream).toHaveBeenCalledTimes(1);

      await settle(exporter);
      const inferences = exporter.getSpansByType(SpanType.MODEL_INFERENCE);
      expect(inferences).toHaveLength(2);
      expect(new Set(inferences.map(span => span.attributes?.model))).toEqual(new Set(['root-model', 'child-model']));
      for (const inference of inferences) {
        expect(inference.requestContext?.[lineageKey]).toBe(lineageValue);
        expect(inference.metadata?.[lineageKey]).toBe(lineageValue);
        const exportedCorrelation = JSON.stringify({
          requestContext: inference.requestContext,
          metadata: inference.metadata,
        });
        expect(exportedCorrelation).not.toContain('papersflow.requestId');
        expect(exportedCorrelation).not.toContain(functionalRequestId);
        expect(exportedCorrelation).not.toContain('papersflow.userId');
        expect(exportedCorrelation).not.toContain(userIdCanary);
        expect(exportedCorrelation).not.toContain(contentCanary);
      }
      expect(exporter.getIncompleteSpans()).toHaveLength(0);
    } finally {
      await harness.shutdown();
    }
  });

  it('runs an inline subagent inside the parent trace, under its spawn_subagent tool call', async () => {
    const requestId = 'parent-turn-request-5258';
    const exporter = new TestExporter();
    const rootProvider = toolCallModel({
      provider: 'root-provider',
      modelId: 'root-model',
      responseId: 'root-response',
      toolCallId: 'spawn-child',
      toolName: 'spawn_subagent',
      input: { agentType: 'worker', task: 'Return the verified child outcome.', delivery: 'final' },
    });
    const childProvider = toolCallModel({
      provider: 'child-provider',
      modelId: 'child-model',
      responseId: 'child-response',
      toolCallId: 'report-child-outcome',
      toolName: 'report_subagent_outcome',
      input: {
        outcome: 'completed',
        summary: 'child trace verified',
        evidence: [{ kind: 'analysis', description: 'Deterministic provider fixture completed.' }],
      },
    });
    const storage = new InMemoryStore();
    const mastra = new Mastra({
      agents: {
        // Run-level trace metadata belongs to the parent turn only; the child
        // agent declares none of its own.
        'root-agent': new Agent({
          id: 'root-agent',
          name: 'root-agent',
          instructions: 'Delegate once.',
          model: rootProvider.model,
          memory: new MockMemory({ storage }),
          defaultOptions: { tracingOptions: { metadata: { requestId } } },
        }),
        'child-agent': new Agent({
          id: 'child-agent',
          name: 'child-agent',
          instructions: 'Report the completed outcome.',
          model: childProvider.model,
          memory: new MockMemory({ storage }),
        }),
      } as any,
      storage,
      observability: new Observability({
        configs: { test: { serviceName: 'harness-subagent-trace-it', exporters: [exporter] } },
      }),
    });
    const harness = new Harness({
      mastra,
      modes: [
        { id: 'root-mode', agentId: 'root-agent' },
        { id: 'child-mode', agentId: 'child-agent' },
      ],
      defaultModeId: 'root-mode',
      subagents: {
        maxDepth: 1,
        types: {
          worker: {
            agentId: 'child-agent',
            modeId: 'child-mode',
            description: 'Deterministic trace worker',
            defaultModelId: 'child-model',
          },
        },
      },
    });

    try {
      const session = await harness.session({ resourceId: 'trace-user', threadId: { fresh: true } });
      const result = await session.message({ content: 'Delegate the trace check.' });
      expect(result.text).toBe('child trace verified');

      await settle(exporter);
      const agentRuns = exporter.getSpansByType(SpanType.AGENT_RUN);
      const rootRun = agentRuns.find(span => span.entityId === 'root-agent');
      const childRun = agentRuns.find(span => span.entityId === 'child-agent');
      const spawnCall = exporter
        .getSpansByType(SpanType.TOOL_CALL)
        .find(span => span.entityName === 'spawn_subagent' || span.name.includes('spawn_subagent'));
      expect(rootRun).toBeDefined();
      expect(childRun).toBeDefined();
      expect(spawnCall).toBeDefined();
      expect(childRun!.traceId).toBe(rootRun!.traceId);
      expect(childRun!.parentSpanId).toBe(spawnCall!.id);
      // The child keeps its own run identity; only the turn's metadata is inherited.
      expect(childRun!.metadata?.runId).toBeDefined();
      expect(childRun!.metadata?.runId).not.toBe(rootRun!.metadata?.runId);

      const generations = exporter.getSpansByType(SpanType.MODEL_GENERATION);
      expect(new Set(generations.map(span => span.attributes?.model))).toEqual(new Set(['root-model', 'child-model']));
      for (const generation of generations) {
        expect(generation.traceId).toBe(rootRun!.traceId);
        expect(generation.metadata?.requestId).toBe(requestId);
      }
      expect(exporter.getIncompleteSpans()).toHaveLength(0);
    } finally {
      await harness.shutdown();
    }
  });

  /**
   * A parent turn that plans one task and durably delegates it to a child whose
   * provider call waits for `releaseChild()`, so the child generates only after
   * the parent turn has finished.
   */
  async function startDelegation(requestId: string) {
    const exporter = new TestExporter();
    const createdTaskIds: string[] = [];
    let releaseChild!: () => void;
    const childReleased = new Promise<void>(resolve => {
      releaseChild = resolve;
    });
    const streamOf = (chunks: unknown[]) => ({
      rawCall: { rawPrompt: null, rawSettings: {} },
      warnings: [],
      stream: convertArrayToReadableStream(chunks as any[]),
    });
    const toolCallChunks = (toolCallId: string, toolName: string, input: object) => [
      { type: 'stream-start', warnings: [] },
      { type: 'response-metadata', id: `id-${toolCallId}`, modelId: 'root-model', timestamp: new Date(0) },
      { type: 'tool-call', toolCallType: 'function', toolCallId, toolName, input: JSON.stringify(input) },
      { type: 'finish', finishReason: 'tool-calls', usage },
    ];
    let rootCall = 0;
    const rootModel = new MockLanguageModelV2({
      provider: 'root-provider',
      modelId: 'root-model',
      doStream: async () => {
        rootCall += 1;
        if (rootCall === 1) return streamOf(toolCallChunks('add-task', 'task_add', { content: 'Delegate this' }));
        if (rootCall === 2) {
          return streamOf(
            toolCallChunks('delegate-task', 'task_delegate', { taskId: createdTaskIds[0], agentType: 'worker' }),
          );
        }
        return streamOf([
          { type: 'stream-start', warnings: [] },
          { type: 'response-metadata', id: 'root-final', modelId: 'root-model', timestamp: new Date(0) },
          { type: 'text-start', id: 'text-1' },
          { type: 'text-delta', id: 'text-1', delta: 'Delegated.' },
          { type: 'text-end', id: 'text-1' },
          { type: 'finish', finishReason: 'stop', usage },
        ]);
      },
    });
    const childProvider = toolCallModel({
      provider: 'child-provider',
      modelId: 'child-model',
      responseId: 'child-response',
      toolCallId: 'report-child-outcome',
      toolName: 'report_subagent_outcome',
      input: {
        outcome: 'completed',
        summary: 'delegated trace verified',
        evidence: [{ kind: 'analysis', description: 'Deterministic provider fixture completed.' }],
      },
    });
    const childModel = new MockLanguageModelV2({
      provider: 'child-provider',
      modelId: 'child-model',
      doStream: async options => {
        await childReleased;
        return childProvider.doStream(options as never);
      },
    });

    const storage = new InMemoryStore();
    const mastra = new Mastra({
      agents: {
        'root-agent': new Agent({
          id: 'root-agent',
          name: 'root-agent',
          instructions: 'Plan and delegate.',
          model: rootModel,
          memory: new MockMemory({ storage }),
          defaultOptions: { tracingOptions: { metadata: { requestId }, hideInput: true } },
        }),
        'child-agent': new Agent({
          id: 'child-agent',
          name: 'child-agent',
          instructions: 'Report the completed outcome.',
          model: childModel,
          memory: new MockMemory({ storage }),
        }),
      } as any,
      storage,
      observability: new Observability({
        configs: { test: { serviceName: 'harness-subagent-delegation-trace-it', exporters: [exporter] } },
      }),
    });
    const harness = new Harness({
      mastra,
      modes: [
        { id: 'root-mode', agentId: 'root-agent' },
        { id: 'child-mode', agentId: 'child-agent' },
      ],
      defaultModeId: 'root-mode',
      subagents: {
        maxDepth: 1,
        types: {
          worker: {
            agentId: 'child-agent',
            modeId: 'child-mode',
            description: 'Deterministic delegated trace worker',
            defaultModelId: 'child-model',
          },
        },
      },
    });
    const session = await harness.session({ resourceId: 'delegation-trace-user', threadId: { fresh: true } });
    session.subscribe(event => {
      const output = (event as { output?: { taskId?: string } }).output;
      if (event.type === 'tool_end' && (event as { toolName?: string }).toolName === 'task_add' && output?.taskId) {
        createdTaskIds.push(output.taskId);
      }
    });
    return { exporter, harness, session, childProvider, releaseChild };
  }

  /**
   * The detached delegation drive is not part of `session.waitForIdle()` (the
   * parent turn does not wait for it), so wait for the drive's own end
   * marker: the delegation span ends only after settlement.
   */
  async function endedDelegationSpan(exporter: TestExporter) {
    const deadline = Date.now() + 3_000;
    for (;;) {
      const delegation = exporter
        .getSpansByType(SpanType.GENERIC)
        .find(span => span.name === "subagent delegation: 'worker'" && span.endTime !== undefined);
      if (delegation !== undefined) return delegation;
      if (Date.now() > deadline) throw new Error('the delegation span did not end');
      await new Promise(resolve => setTimeout(resolve, 20));
    }
  }

  it('joins a durable delegation to the parent trace and its trace settings without tying it to the parent turn', async () => {
    const requestId = 'parent-turn-request-5258-delegated';
    const { exporter, harness, session, childProvider, releaseChild } = await startDelegation(requestId);
    try {
      const result = await session.message({ content: 'Delegate the work.' });
      expect(result.text).toBe('Delegated.');
      const rootRun = exporter.getSpansByType(SpanType.AGENT_RUN).find(span => span.entityId === 'root-agent');
      expect(rootRun?.endTime).toBeDefined();
      expect(childProvider.doStream).not.toHaveBeenCalled();

      releaseChild();
      const delegation = await endedDelegationSpan(exporter);
      await settle(exporter);

      const delegateCall = exporter
        .getSpansByType(SpanType.TOOL_CALL)
        .find(span => span.entityName === 'task_delegate' || span.name.includes('task_delegate'));
      const childRun = exporter.getSpansByType(SpanType.AGENT_RUN).find(span => span.entityId === 'child-agent');
      expect(delegateCall).toBeDefined();
      expect(childRun).toBeDefined();
      expect(delegation.traceId).toBe(rootRun!.traceId);
      expect(delegation.parentSpanId).toBe(delegateCall!.id);
      expect(delegation.output).toEqual({ outcome: 'completed', settlement: 'settled' });
      expect(delegation.errorInfo).toBeUndefined();
      expect(childRun!.traceId).toBe(rootRun!.traceId);
      expect(childRun!.parentSpanId).toBe(delegation.id);
      expect(childRun!.metadata?.runId).toBeDefined();
      expect(childRun!.metadata?.runId).not.toBe(rootRun!.metadata?.runId);

      const childGeneration = exporter
        .getSpansByType(SpanType.MODEL_GENERATION)
        .find(span => span.attributes?.model === 'child-model');
      expect(childGeneration!.traceId).toBe(rootRun!.traceId);
      expect(childGeneration!.metadata?.requestId).toBe(requestId);
      expect(childGeneration!.metadata?.runId).toBe(childRun!.metadata?.runId);
      // The parent's trace-level settings hold across the delegation boundary.
      expect(childGeneration!.input).toBeUndefined();
      expect(childRun!.input).toBeUndefined();
      expect(exporter.getIncompleteSpans()).toHaveLength(0);
    } finally {
      releaseChild();
      await harness.shutdown();
    }
  });

  it('ends the delegation span as an error when the drive cannot settle the delegation', async () => {
    const { exporter, harness, session, releaseChild } = await startDelegation('parent-turn-request-5258-unsettled');
    // The child completes, but the fenced plan-task reconciliation does not
    // commit, so the delegation stays unsettled for restart reconciliation.
    const reconcile = vi
      .spyOn(session as unknown as { _reconcileDelegatedTask: () => Promise<boolean> }, '_reconcileDelegatedTask')
      .mockResolvedValue(false);
    try {
      await session.message({ content: 'Delegate the work.' });
      releaseChild();
      const delegation = await endedDelegationSpan(exporter);

      expect(reconcile).toHaveBeenCalled();
      expect(delegation.output).toEqual({ outcome: 'completed', settlement: 'not_reconciled' });
      expect(delegation.errorInfo?.message).toBe('delegation was not settled: not_reconciled');
    } finally {
      releaseChild();
      reconcile.mockRestore();
      await harness.shutdown();
    }
  });
});
