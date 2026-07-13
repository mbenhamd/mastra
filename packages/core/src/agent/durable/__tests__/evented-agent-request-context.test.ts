import type { LanguageModelV2 } from '@ai-sdk/provider-v5';
import { MockLanguageModelV2, convertArrayToReadableStream } from '@internal/ai-sdk-v5/test';
import { afterEach, describe, expect, it, vi } from 'vitest';

import { EventEmitterPubSub } from '../../../events/event-emitter';
import { RequestContext } from '../../../request-context';
import type { WorkflowFinishCallbackResult } from '../../../workflows/types';
import { Agent } from '../../agent';
import { EventedAgent } from '../evented-agent';
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
    const startAsync = vi.fn(async () => ({ runId: 'evented-request-context-run', execution: Promise.resolve({}) }));
    const createRun = vi.fn(async () => ({ startAsync }));
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

    const { cleanup } = await eventedAgent.stream('Hello', {
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
          requestContext,
        }),
      );
      expect(emitError).not.toHaveBeenCalled();
    } finally {
      cleanup();
    }
  });

  it('deletes terminal snapshots from the native workflow lifecycle but preserves suspended runs', async () => {
    const startAsync = vi.fn(async () => ({ runId: 'evented-terminal-run', execution: Promise.resolve({}) }));
    const createRun = vi.fn(async () => ({ startAsync }));
    const deleteTerminalRunSnapshots = vi.fn(async () => undefined);
    const emitError = vi.fn(async () => undefined);
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

    const { runId, cleanup } = await eventedAgent.stream('Finish', { runId: 'evented-terminal-run' });
    try {
      await vi.waitFor(() => expect(startAsync).toHaveBeenCalledTimes(1));
      expect(deleteTerminalRunSnapshots).not.toHaveBeenCalled();
      await eventedAgent.finish({ runId, status: 'suspended' });
      expect(deleteTerminalRunSnapshots).not.toHaveBeenCalled();
      await eventedAgent.finish({ runId, status: 'success' });
      expect(deleteTerminalRunSnapshots).toHaveBeenCalledWith(runId);
      deleteTerminalRunSnapshots.mockRejectedValueOnce(new Error('storage unavailable'));
      await eventedAgent.finish({ runId, status: 'failed' });
      expect(emitError).toHaveBeenCalledWith(runId, expect.objectContaining({ message: 'Workflow execution failed' }));
    } finally {
      cleanup();
    }
  });

  it('reports rejected background execution once and releases its runtime pin', async () => {
    const executionError = new Error('background execution failed before lifecycle');
    const startAsync = vi.fn(async () => ({
      runId: 'evented-rejected-execution-run',
      execution: Promise.reject(executionError),
    }));
    const createRun = vi.fn(async () => ({ startAsync }));
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
