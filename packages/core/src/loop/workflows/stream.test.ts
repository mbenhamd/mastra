import { beforeEach, describe, expect, it, vi } from 'vitest';
import { MessageList } from '../../agent/message-list';
import { ConsoleLogger } from '../../logger';
import { Mastra } from '../../mastra';
import type { Processor, ProcessorStreamWriter } from '../../processors';
import { InMemoryStore } from '../../storage';
import { createEmptyWorkflowSnapshot } from '../../storage/workflow-snapshot';
import { ChunkFrom } from '../../stream/types';
import type { ChunkType } from '../../stream/types';

// Capture the outputWriter passed to createAgenticLoopWorkflow so we can
// invoke it directly in tests without spinning up a real agentic loop.
let capturedOutputWriter: ((chunk: ChunkType, options?: { messageId?: string }) => Promise<void>) | undefined;
let capturedCreateRunArgs: any;
const parentDeleteMock = vi.fn<(runId: string) => Promise<void>>().mockResolvedValue(undefined);

vi.mock('./agentic-loop', () => ({
  createAgenticLoopWorkflow: (params: any) => {
    capturedOutputWriter = params.outputWriter;

    return {
      id: 'agentic-loop',
      __markInternal: vi.fn(),
      __registerMastra: vi.fn(),
      __registerPrimitives: vi.fn(),
      deleteWorkflowRunById: parentDeleteMock,
      createRun: vi.fn().mockImplementation(async (args: any) => {
        capturedCreateRunArgs = args;
        return {
          start: vi.fn().mockImplementation(async () => {
            // Simulate the agentic loop emitting a data-* chunk
            await capturedOutputWriter!(
              {
                type: 'data-moderation',
                id: 'moderation-1',
                data: { flagged: true },
                runId: 'run-1',
                from: ChunkFrom.AGENT,
              } as ChunkType,
              { messageId: 'rotated-msg' },
            );

            return {
              status: 'success',
              result: {
                output: { steps: [], usage: { inputTokens: 0, outputTokens: 0, totalTokens: 0 } },
                stepResult: { reason: 'stop', warnings: [], isContinued: false },
                metadata: {},
                messages: { nonUser: [], all: [] },
              },
            };
          }),
        };
      }),
    };
  },
}));

const { workflowLoopStream } = await import('./stream');

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (error?: unknown) => void;
  const promise = new Promise<T>((res, rej) => {
    resolve = res;
    reject = rej;
  });
  return { promise, resolve, reject };
}

async function waitForSignal(signal: () => boolean) {
  for (let attempt = 0; attempt < 100 && !signal(); attempt++) {
    await new Promise<void>(resolve => setImmediate(resolve));
  }
  expect(signal()).toBe(true);
}

describe('workflowLoopStream', () => {
  beforeEach(() => {
    parentDeleteMock.mockReset().mockResolvedValue(undefined);
  });
  it('should pass a defined writer to output processors when processing data-* chunks', async () => {
    let receivedWriter: ProcessorStreamWriter | undefined;

    const processor: Processor = {
      id: 'writer-capture',
      name: 'Writer Capture',
      processDataParts: true,
      processOutputStream: async ({ part, writer }) => {
        receivedWriter = writer;
        return part;
      },
    };

    const messageList = new MessageList({ threadId: 'test-thread' });

    const stream = workflowLoopStream({
      messageId: 'msg-1',
      runId: 'run-1',
      startTimestamp: Date.now(),
      agentId: 'test-agent',
      messageList,
      models: [{ model: {} as any, toolChoice: undefined }],
      outputProcessors: [processor],
      _internal: {},
      streamState: { serialize: () => ({}), deserialize: () => {} },
      methodType: 'stream',
    });

    // Consume the stream
    const reader = stream.getReader();
    const chunks: ChunkType[] = [];
    while (true) {
      const { done, value } = await reader.read();
      if (value) chunks.push(value);
      if (done) break;
    }

    // The processor should have received a defined writer
    expect(receivedWriter).toBeDefined();
    expect(typeof receivedWriter!.custom).toBe('function');

    // Verify the data-* chunk was emitted and persisted with the supplied response message id.
    const dataChunk = chunks.find(c => c.type === 'data-moderation');
    expect(dataChunk).toBeDefined();
    expect(messageList.get.response.db().map(message => message.id)).toEqual(['rotated-msg']);
    expect(messageList.get.response.db()[0]?.content.parts).toEqual([
      expect.objectContaining({ type: 'data-moderation', id: 'moderation-1', data: { flagged: true } }),
    ]);
  });

  it('should forward resourceId from _internal to createRun()', async () => {
    const messageList = new MessageList({ threadId: 'test-thread' });

    const stream = workflowLoopStream({
      messageId: 'msg-2',
      runId: 'run-2',
      startTimestamp: Date.now(),
      agentId: 'test-agent',
      messageList,
      models: [{ model: {} as any, toolChoice: undefined }],
      _internal: { resourceId: 'user-abc-123' },
      streamState: { serialize: () => ({}), deserialize: () => {} },
      methodType: 'stream',
    });

    // Consume the stream to trigger createRun
    const reader = stream.getReader();
    while (true) {
      const { done } = await reader.read();
      if (done) break;
    }

    expect(capturedCreateRunArgs).toBeDefined();
    expect(capturedCreateRunArgs.resourceId).toBe('user-abc-123');
  });

  it('starts parent and retained child snapshot deletes before the finish barrier', async () => {
    const mastra = new Mastra({ logger: false, storage: new InMemoryStore() });
    const workflowsStore = (await mastra.getStorage()!.getStore('workflows'))!;
    const runId = 'run-cleanup';
    const nestedRunId = 'wfn:v1:nested-cleanup';
    const wrapperDelete = deferred<void>();
    const parentDelete = deferred<void>();
    const childDelete = deferred<void>();
    const started: string[] = [];
    await workflowsStore.persistWorkflowSnapshot({
      workflowName: 'agentic-loop',
      runId,
      snapshot: {
        ...createEmptyWorkflowSnapshot(runId),
        status: 'suspended',
        context: {
          executionWorkflow: {
            status: 'suspended',
            metadata: { nestedRunId },
          },
        } as any,
      },
    });
    const readSnapshot = workflowsStore.getWorkflowRunById.bind(workflowsStore);
    let lookupCompleted = false;
    const lookup = vi.spyOn(workflowsStore, 'getWorkflowRunById').mockImplementation(async args => {
      const row = await readSnapshot(args);
      lookupCompleted = true;
      return row;
    });
    const deleteWorkflowRunById = vi.spyOn(workflowsStore, 'deleteWorkflowRunById').mockImplementation(async args => {
      expect(lookupCompleted).toBe(true);
      started.push(`${args.workflowName}:${args.runId}`);
      await (args.runId === runId ? parentDelete.promise : childDelete.promise);
    });

    parentDeleteMock.mockImplementation(async () => {
      expect(lookupCompleted).toBe(true);
      started.push('parent-wrapper');
      await wrapperDelete.promise;
    });
    const stream = workflowLoopStream({
      mastra,
      messageId: 'msg-cleanup',
      runId,
      startTimestamp: Date.now(),
      agentId: 'test-agent',
      messageList: new MessageList({ threadId: 'test-thread' }),
      models: [{ model: {} as any, toolChoice: undefined }],
      _internal: {},
      streamState: { serialize: () => ({}), deserialize: () => {} },
      methodType: 'stream',
    });

    const chunks: ChunkType[] = [];
    let ended = false;
    const consume = (async () => {
      for await (const chunk of stream) chunks.push(chunk);
      ended = true;
    })();

    try {
      await waitForSignal(() => started.includes('parent-wrapper'));
      expect(lookup).toHaveBeenCalledExactlyOnceWith({ runId, workflowName: 'agentic-loop' });
      await waitForSignal(() => started.includes(`executionWorkflow:${runId}`));
      await waitForSignal(() => started.includes(`executionWorkflow:${nestedRunId}`));
      expect(started).toEqual(['parent-wrapper', `executionWorkflow:${runId}`, `executionWorkflow:${nestedRunId}`]);
      expect(chunks.some(chunk => chunk.type === 'finish')).toBe(false);
      expect(ended).toBe(false);
      wrapperDelete.resolve();
      parentDelete.resolve();
      await new Promise<void>(resolve => setImmediate(resolve));
      expect(chunks.some(chunk => chunk.type === 'finish')).toBe(false);
      expect(ended).toBe(false);
      childDelete.resolve();
      await consume;
      expect(chunks.filter(chunk => chunk.type === 'finish')).toHaveLength(1);
      expect(ended).toBe(true);
      expect(parentDeleteMock).toHaveBeenCalledExactlyOnceWith(runId);
      expect(deleteWorkflowRunById).toHaveBeenCalledTimes(2);
    } finally {
      wrapperDelete.resolve();
      parentDelete.resolve();
      childDelete.resolve();
      await consume;
      await mastra.shutdown();
    }
  });

  it('preserves deletion warnings and waits for the remaining child after partial failure', async () => {
    const mastra = new Mastra({ logger: false, storage: new InMemoryStore() });
    const workflowsStore = (await mastra.getStorage()!.getStore('workflows'))!;
    const runId = 'run-partial-cleanup';
    const nestedRunId = 'wfn:v1:nested-partial';
    const wrapperDelete = deferred<void>();
    const parentDelete = deferred<void>();
    const childDelete = deferred<void>();
    const started: string[] = [];
    await workflowsStore.persistWorkflowSnapshot({
      workflowName: 'agentic-loop',
      runId,
      snapshot: {
        ...createEmptyWorkflowSnapshot(runId),
        status: 'suspended',
        context: { executionWorkflow: { status: 'suspended', metadata: { nestedRunId } } } as any,
      },
    });
    await workflowsStore.persistWorkflowSnapshot({
      workflowName: 'executionWorkflow',
      runId: nestedRunId,
      snapshot: createEmptyWorkflowSnapshot(nestedRunId),
    });
    const deleteWorkflowRunById = vi.spyOn(workflowsStore, 'deleteWorkflowRunById').mockImplementation(async args => {
      started.push(`${args.workflowName}:${args.runId}`);
      await (args.runId === runId ? parentDelete.promise : childDelete.promise);
    });
    const logger = new ConsoleLogger({ level: 'error' });
    const warn = vi.spyOn(logger, 'warn').mockImplementation(() => {});
    const wrapperError = new Error('wrapper delete failed');
    const parentError = new Error('parent execution delete failed');
    parentDeleteMock.mockImplementation(async () => {
      started.push('parent-wrapper');
      await wrapperDelete.promise;
    });
    const stream = workflowLoopStream({
      mastra,
      messageId: 'msg-partial-cleanup',
      runId,
      startTimestamp: Date.now(),
      agentId: 'test-agent',
      messageList: new MessageList({ threadId: 'test-thread' }),
      models: [{ model: {} as any, toolChoice: undefined }],
      _internal: {},
      streamState: { serialize: () => ({}), deserialize: () => {} },
      methodType: 'stream',
      logger,
    });
    const chunks: ChunkType[] = [];
    let ended = false;
    const consume = (async () => {
      for await (const chunk of stream) chunks.push(chunk);
      ended = true;
    })();
    try {
      await waitForSignal(() => started.length === 3);
      expect(chunks.some(chunk => chunk.type === 'finish')).toBe(false);
      expect(ended).toBe(false);
      expect(parentDeleteMock).toHaveBeenCalledExactlyOnceWith(runId);
      expect(deleteWorkflowRunById).toHaveBeenCalledTimes(2);
      wrapperDelete.reject(wrapperError);
      parentDelete.reject(parentError);
      await waitForSignal(() => warn.mock.calls.length === 2);
      expect(warn).toHaveBeenCalledWith('Failed to delete agentic-loop snapshot after terminal state', {
        runId,
        error: wrapperError,
      });
      expect(warn).toHaveBeenCalledWith('Failed to delete nested agent execution snapshot after terminal state', {
        runId,
        executionRunId: runId,
        error: parentError,
      });
      expect(chunks.some(chunk => chunk.type === 'finish')).toBe(false);
      expect(ended).toBe(false);
      childDelete.resolve();
      await consume;
      expect(chunks.filter(chunk => chunk.type === 'finish')).toHaveLength(1);
      expect(ended).toBe(true);
    } finally {
      wrapperDelete.resolve();
      parentDelete.resolve();
      childDelete.resolve();
      await consume;
      await mastra.shutdown();
    }

    expect(started).toEqual(['parent-wrapper', `executionWorkflow:${runId}`, `executionWorkflow:${nestedRunId}`]);
  });
});
