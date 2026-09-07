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
const { deleteAgenticLoopRun } = vi.hoisted(() => ({ deleteAgenticLoopRun: vi.fn() }));

vi.mock('./agentic-loop', () => ({
  createAgenticLoopWorkflow: (params: any) => {
    capturedOutputWriter = params.outputWriter;

    return {
      id: 'agentic-loop',
      __markInternal: vi.fn(),
      __registerMastra: vi.fn(),
      __registerPrimitives: vi.fn(),
      deleteWorkflowRunById: deleteAgenticLoopRun,
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

function deferred() {
  let resolve!: () => void;
  const promise = new Promise<void>(r => {
    resolve = r;
  });
  return { promise, resolve };
}

describe('workflowLoopStream', () => {
  beforeEach(() => {
    deleteAgenticLoopRun.mockReset().mockResolvedValue(undefined);
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

  it('starts independent snapshot deletions after lookup and waits for the parent before finish', async () => {
    const mastra = new Mastra({ logger: false, storage: new InMemoryStore() });
    const workflowsStore = (await mastra.getStorage()!.getStore('workflows'))!;
    const runId = 'run-cleanup';
    const nestedRunId = 'wfn:v1:nested-cleanup';
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
    for (const executionRunId of [runId, nestedRunId]) {
      await workflowsStore.persistWorkflowSnapshot({
        workflowName: 'executionWorkflow',
        runId: executionRunId,
        snapshot: createEmptyWorkflowSnapshot(executionRunId),
      });
    }
    const lookup = deferred();
    const parentDeletion = deferred();
    const childDeletions = deferred();
    const getRun = workflowsStore.getWorkflowRunById.bind(workflowsStore);
    const deleteRun = workflowsStore.deleteWorkflowRunById.bind(workflowsStore);
    const lookupSpy = vi.spyOn(workflowsStore, 'getWorkflowRunById').mockImplementation(async args => {
      await lookup.promise;
      return getRun(args);
    });
    deleteAgenticLoopRun.mockImplementation(() => parentDeletion.promise);
    const deleteWorkflowRunById = vi.spyOn(workflowsStore, 'deleteWorkflowRunById').mockImplementation(async args => {
      await childDeletions.promise;
      await deleteRun(args);
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
      await vi.waitFor(() => expect(lookupSpy).toHaveBeenCalledOnce());
      expect(deleteAgenticLoopRun).not.toHaveBeenCalled();
      expect(deleteWorkflowRunById).not.toHaveBeenCalled();
      lookup.resolve();
      await vi.waitFor(() => expect(deleteWorkflowRunById).toHaveBeenCalledTimes(2));
      expect(deleteAgenticLoopRun).toHaveBeenCalledExactlyOnceWith(runId);
      expect(deleteWorkflowRunById.mock.calls).toEqual([
        [{ workflowName: 'executionWorkflow', runId }],
        [{ workflowName: 'executionWorkflow', runId: nestedRunId }],
      ]);
      expect(chunks.some(chunk => chunk.type === 'finish')).toBe(false);
      expect(ended).toBe(false);
      childDeletions.resolve();
      await vi.waitFor(async () => {
        expect(await getRun({ workflowName: 'executionWorkflow', runId: nestedRunId })).toBeNull();
      });
      expect(chunks.some(chunk => chunk.type === 'finish')).toBe(false);
      expect(ended).toBe(false);
      parentDeletion.resolve();
      await consume;
      expect(chunks.filter(chunk => chunk.type === 'finish')).toHaveLength(1);
      expect(chunks.some(chunk => chunk.type === 'error')).toBe(false);
      expect(ended).toBe(true);
      expect(lookupSpy).toHaveBeenCalledExactlyOnceWith({ runId, workflowName: 'agentic-loop' });
      expect(deleteAgenticLoopRun).toHaveBeenCalledExactlyOnceWith(runId);
      expect(deleteWorkflowRunById).toHaveBeenCalledTimes(2);
    } finally {
      lookup.resolve();
      parentDeletion.resolve();
      childDeletions.resolve();
      await consume;
      await mastra.shutdown();
    }
  });

  it('attempts every snapshot deletion after failures and waits for the remaining child before finish', async () => {
    const mastra = new Mastra({ logger: false, storage: new InMemoryStore() });
    const workflowsStore = (await mastra.getStorage()!.getStore('workflows'))!;
    const runId = 'run-cleanup-failure';
    const nestedRunId = 'wfn:v1:nested-cleanup-failure';
    await workflowsStore.persistWorkflowSnapshot({
      workflowName: 'agentic-loop',
      runId,
      snapshot: {
        ...createEmptyWorkflowSnapshot(runId),
        status: 'suspended',
        context: {
          executionWorkflow: { status: 'suspended', metadata: { nestedRunId } },
        } as any,
      },
    });
    const parentError = new Error('parent deletion failed');
    const childError = new Error('child deletion failed');
    const remainingChild = deferred();
    deleteAgenticLoopRun.mockRejectedValue(parentError);
    const deleteWorkflowRunById = vi.spyOn(workflowsStore, 'deleteWorkflowRunById').mockImplementation(async args => {
      if (args.runId === runId) throw childError;
      await remainingChild.promise;
    });
    const logger = new ConsoleLogger({ level: 'error' });
    const warn = vi.spyOn(logger, 'warn').mockImplementation(() => {});
    const stream = workflowLoopStream({
      mastra,
      logger,
      messageId: 'msg-cleanup-failure',
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
      await vi.waitFor(() => expect(deleteWorkflowRunById).toHaveBeenCalledTimes(2));
      expect(deleteAgenticLoopRun).toHaveBeenCalledExactlyOnceWith(runId);
      expect(deleteWorkflowRunById.mock.calls).toEqual([
        [{ workflowName: 'executionWorkflow', runId }],
        [{ workflowName: 'executionWorkflow', runId: nestedRunId }],
      ]);
      expect(warn).toHaveBeenCalledWith('Failed to delete agentic-loop snapshot after terminal state', {
        runId,
        error: parentError,
      });
      expect(warn).toHaveBeenCalledWith('Failed to delete nested agent execution snapshot after terminal state', {
        runId,
        executionRunId: runId,
        error: childError,
      });
      expect(chunks.some(chunk => chunk.type === 'finish')).toBe(false);
      expect(ended).toBe(false);
      remainingChild.resolve();
      await consume;
      expect(chunks.filter(chunk => chunk.type === 'finish')).toHaveLength(1);
      expect(chunks.some(chunk => chunk.type === 'error')).toBe(false);
      expect(ended).toBe(true);
      expect(deleteAgenticLoopRun).toHaveBeenCalledExactlyOnceWith(runId);
      expect(deleteWorkflowRunById).toHaveBeenCalledTimes(2);
    } finally {
      remainingChild.resolve();
      await consume;
      await mastra.shutdown();
    }
  });
});
