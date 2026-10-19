import type { LanguageModelV2 } from '@ai-sdk/provider-v5';
import { MockLanguageModelV2, convertArrayToReadableStream } from '@internal/ai-sdk-v5/test';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { EventEmitterPubSub } from '../../../events/event-emitter';
import { Mastra } from '../../../mastra';
import { RequestContext } from '../../../request-context';
import { InMemoryStore } from '../../../storage';
import type { WorkflowFinishCallbackResult } from '../../../workflows/types';
import { Agent } from '../../agent';
import { EventedAgent } from '../evented-agent';
import { getExecutionClaim } from '../execution-fence';
import { getGlobalRunRegistryEntry, globalRunRegistry } from '../run-registry';

function createTextModel(text: string) {
  return new MockLanguageModelV2({
    doStream: async () => ({
      stream: convertArrayToReadableStream([
        { type: 'stream-start', warnings: [] },
        { type: 'response-metadata', id: 'id-0', modelId: '__AI_SDK_OPENAI_MODEL_REALTIME__', timestamp: new Date(0) },
        { type: 'text-start', id: 'text-1' },
        { type: 'text-delta', id: 'text-1', delta: text },
        { type: 'text-end', id: 'text-1' },
        {
          type: 'finish',
          finishReason: 'stop',
          usage: { inputTokens: 10, outputTokens: 20, totalTokens: 30 },
        },
      ]),
      rawCall: { rawPrompt: null, rawSettings: {} },
      warnings: [],
    }),
  });
}

describe('EventedAgent requestContext forwarding', () => {
  const pubsubs: EventEmitterPubSub[] = [];

  afterEach(async () => {
    await Promise.all(pubsubs.splice(0).map(pubsub => pubsub.close()));
  });

  it('passes requestContext to fire-and-forget workflow execution', async () => {
    // Upstream re-enabled the evented engine, whose `startAsync()` returns only
    // `{ runId }`; EventedAgent awaits the native `start()` receipt instead.
    const startAsync = vi.fn(async () => ({ status: 'success' }));
    const createRun = vi.fn(async () => ({ start: startAsync }));
    const emitError = vi.fn(async () => undefined);
    const pubsub = new EventEmitterPubSub();
    pubsubs.push(pubsub);
    const baseAgent = new Agent({
      id: 'evented-request-context-agent',
      name: 'Evented Request Context Agent',
      instructions: 'Test requestContext',
      model: createTextModel('Hello!') as LanguageModelV2,
    });
    const eventedAgent = new (class extends EventedAgent {
      protected override emitError(runId: string, error: Error) {
        return emitError(runId, error);
      }

      override getWorkflow() {
        return { createRun } as unknown as ReturnType<EventedAgent['getWorkflow']>;
      }
    })({ agent: baseAgent, pubsub });

    const requestContext = new RequestContext();
    requestContext.set('tenantId', 'tenant-123');

    const { runId, cleanup } = await eventedAgent.stream('Hello', {
      requestContext,
      memory: { thread: 'thread-123', resource: 'resource-123' },
    });
    try {
      await vi.waitFor(() => expect(startAsync).toHaveBeenCalledTimes(1));
      expect(createRun).toHaveBeenCalledWith(
        expect.objectContaining({
          resourceId: 'resource-123',
        }),
      );
      expect(startAsync).toHaveBeenCalledWith(
        expect.objectContaining({
          inputData: expect.any(Object),
          requestContext: expect.any(RequestContext),
        }),
      );
      // PF-5928 adaptation (upstream #26418): the forwarded context is the run's
      // own copy, carrying the caller's entries plus the execution claim, so
      // the caller's context is never mutated.
      const forwarded = (startAsync.mock.calls[0] as unknown as [{ requestContext: RequestContext }])[0]
        .requestContext;
      expect(forwarded.get('tenantId')).toBe('tenant-123');
      expect(getExecutionClaim(forwarded, runId)).toMatchObject({ executionId: expect.any(String) });
      expect(getExecutionClaim(requestContext, runId)).toBeUndefined();
      expect(emitError).not.toHaveBeenCalled();
    } finally {
      cleanup();
    }
  });

  it('deletes terminal snapshots once the segment settles a terminal status but preserves suspended runs', async () => {
    // PF-5928 adaptation (upstream #26418/#26420): terminal writes are the
    // segment's settlement, made only while its execution fence still owns
    // the run, instead of the fork's workflow onFinish hook (which held no
    // fence). The hook keeps pin bookkeeping only. Suspended runs keep their
    // snapshots, and a failing delete never suppresses the ERROR event.
    const statuses: Record<string, { status: string; error?: { message: string } }> = {
      'evented-suspended-run': { status: 'suspended' },
      'evented-success-run': { status: 'success' },
      'evented-failed-run': { status: 'failed', error: { message: 'Workflow execution failed' } },
    };
    const createRun = vi.fn(async ({ runId }: { runId: string }) => ({ start: vi.fn(async () => statuses[runId]) }));
    const deleteTerminalRunSnapshots = vi.fn(async (_runId: string) => undefined);
    const emitError = vi.fn(async (_runId: string, _error: Error) => undefined);
    const pubsub = new EventEmitterPubSub();
    pubsubs.push(pubsub);
    const baseAgent = new Agent({
      id: 'evented-terminal-cleanup-agent',
      name: 'Evented Terminal Cleanup Agent',
      instructions: 'Test terminal cleanup',
      model: createTextModel('Done') as LanguageModelV2,
    });
    const eventedAgent = new (class extends EventedAgent {
      protected override deleteTerminalRunSnapshots(runId: string) {
        return deleteTerminalRunSnapshots(runId);
      }

      protected override emitError(runId: string, error: Error) {
        return emitError(runId, error);
      }

      finish(result: Pick<WorkflowFinishCallbackResult, 'runId' | 'status'>) {
        return this.onDurableWorkflowFinish(result as WorkflowFinishCallbackResult);
      }

      override getWorkflow() {
        return { createRun } as unknown as ReturnType<EventedAgent['getWorkflow']>;
      }
    })({ agent: baseAgent, pubsub });

    const suspended = await eventedAgent.stream('Suspend', { runId: 'evented-suspended-run' });
    try {
      await vi.waitFor(() => expect(createRun).toHaveBeenCalledTimes(1));
      await eventedAgent.finish({ runId: suspended.runId, status: 'suspended' });
      // The lifecycle hook alone never deletes, even for a terminal status.
      await eventedAgent.finish({ runId: suspended.runId, status: 'success' });
      await new Promise(resolve => setTimeout(resolve, 20));
      expect(deleteTerminalRunSnapshots).not.toHaveBeenCalled();
    } finally {
      suspended.cleanup();
    }

    const succeeded = await eventedAgent.stream('Finish', { runId: 'evented-success-run' });
    try {
      await vi.waitFor(() => expect(deleteTerminalRunSnapshots).toHaveBeenCalledWith(succeeded.runId));
      expect(deleteTerminalRunSnapshots).toHaveBeenCalledTimes(1);
      expect(emitError).not.toHaveBeenCalled();
    } finally {
      succeeded.cleanup();
    }

    deleteTerminalRunSnapshots.mockRejectedValueOnce(new Error('storage unavailable'));
    const failed = await eventedAgent.stream('Fail', { runId: 'evented-failed-run' });
    try {
      await vi.waitFor(() =>
        expect(emitError).toHaveBeenCalledWith(
          failed.runId,
          expect.objectContaining({ message: 'Workflow execution failed' }),
        ),
      );
      await vi.waitFor(() => expect(deleteTerminalRunSnapshots).toHaveBeenCalledWith(failed.runId));
      expect(emitError).toHaveBeenCalledTimes(1);
    } finally {
      failed.cleanup();
    }
  });

  it('preserves terminal snapshot status when deletion retries fail', async () => {
    const storage = new InMemoryStore();
    const baseAgent = new Agent({
      id: 'evented-terminal-delete-failure-agent',
      name: 'Evented Terminal Delete Failure Agent',
      instructions: 'Test terminal cleanup failure',
      model: createTextModel('Done') as LanguageModelV2,
    });
    const outerDelete = vi.fn().mockRejectedValue(new Error('outer delete unavailable'));
    const pubsub = new EventEmitterPubSub();
    pubsubs.push(pubsub);
    const eventedAgent = new (class extends EventedAgent {
      cleanupTerminal(runId: string) {
        return this.deleteTerminalRunSnapshots(runId);
      }

      override getWorkflow() {
        return { deleteWorkflowRunById: outerDelete } as unknown as ReturnType<EventedAgent['getWorkflow']>;
      }
    })({ agent: baseAgent, pubsub });
    eventedAgent.__setMastra(new Mastra({ storage, logger: false }));
    const workflows = (await storage.getStore('workflows'))!;
    const runId = 'terminal-delete-failure-run';
    await workflows.persistWorkflowSnapshot({
      workflowName: 'durable-agentic-loop',
      runId,
      snapshot: { status: 'failed', context: { failure: true }, resumeLabels: {} } as any,
    });
    await workflows.persistWorkflowSnapshot({
      workflowName: 'durable-agentic-execution',
      runId,
      snapshot: { status: 'tripwire', context: { tripwire: true }, resumeLabels: {} } as any,
    });
    const persistSnapshot = vi.spyOn(workflows, 'persistWorkflowSnapshot');
    const nestedDelete = vi
      .spyOn(workflows, 'deleteWorkflowRunById')
      .mockRejectedValue(new Error('nested delete unavailable'));

    await eventedAgent.cleanupTerminal(runId);

    expect(outerDelete).toHaveBeenCalledTimes(2);
    expect(nestedDelete).toHaveBeenCalledTimes(2);
    expect(persistSnapshot).not.toHaveBeenCalled();
    expect(await workflows.getWorkflowRunById({ workflowName: 'durable-agentic-loop', runId })).toMatchObject({
      snapshot: { status: 'failed', context: { failure: true } },
    });
    expect(await workflows.getWorkflowRunById({ workflowName: 'durable-agentic-execution', runId })).toMatchObject({
      snapshot: { status: 'tripwire', context: { tripwire: true } },
    });
  });

  it('reports rejected background execution once and releases its runtime pin', async () => {
    const executionError = new Error('background execution failed before lifecycle');
    const startAsync = vi.fn(async () => {
      throw executionError;
    });
    const createRun = vi.fn(async () => ({ start: startAsync }));
    const emitError = vi.fn(async () => undefined);
    const pubsub = new EventEmitterPubSub();
    pubsubs.push(pubsub);
    const baseAgent = new Agent({
      id: 'evented-rejected-execution-agent',
      name: 'Evented Rejected Execution Agent',
      instructions: 'Test background execution rejection',
      model: createTextModel('Done') as LanguageModelV2,
    });
    const eventedAgent = new (class extends EventedAgent {
      protected override emitError(runId: string, error: Error) {
        return emitError(runId, error);
      }

      override getWorkflow() {
        return { createRun } as unknown as ReturnType<EventedAgent['getWorkflow']>;
      }
    })({ agent: baseAgent, pubsub });

    const { runId, cleanup } = await eventedAgent.stream('Fail', { runId: 'evented-rejected-execution-run' });
    try {
      await vi.waitFor(() => expect(emitError).toHaveBeenCalledTimes(1));
      expect(emitError).toHaveBeenCalledWith(runId, executionError);
      expect(getGlobalRunRegistryEntry(runId)).toBeDefined();
      globalRunRegistry.delete(runId);
      expect(getGlobalRunRegistryEntry(runId)).toBeUndefined();
    } finally {
      cleanup();
    }
  });

  it('runs terminal cleanup through the real startAsync workflow lifecycle', async () => {
    const deleteTerminalRunSnapshots = vi.fn(async () => undefined);
    const pubsub = new EventEmitterPubSub();
    pubsubs.push(pubsub);
    const baseAgent = new Agent({
      id: 'evented-native-lifecycle-agent',
      name: 'Evented Native Lifecycle Agent',
      instructions: 'Complete successfully',
      model: createTextModel('Done') as LanguageModelV2,
    });
    const eventedAgent = new (class extends EventedAgent {
      protected override deleteTerminalRunSnapshots(runId: string) {
        return deleteTerminalRunSnapshots(runId);
      }
    })({ agent: baseAgent, pubsub });

    const { runId, cleanup } = await eventedAgent.stream('Finish', { runId: 'evented-native-lifecycle-run' });
    try {
      await vi.waitFor(() => expect(deleteTerminalRunSnapshots).toHaveBeenCalledWith(runId));
    } finally {
      cleanup();
    }
  });
});
