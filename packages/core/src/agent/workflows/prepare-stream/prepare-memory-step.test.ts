import { describe, expect, it, vi } from 'vitest';

import { createRunScope } from '../../../mastra/run-scope';
import { RequestContext } from '../../../request-context';
import { createPrepareMemoryStep } from './prepare-memory-step';
import { MESSAGE_LIST_KEY } from './run-scope-keys';

describe('prepare memory step source-write fencing', () => {
  it('restores the serialized source guard before a resumed thread save', async () => {
    const guard = { recordId: 'record-1', threadId: 'thread-1', resourceId: 'resource-1' } as const;
    const savedGuards: unknown[] = [];
    const existingThread = {
      id: 'thread-1',
      resourceId: 'resource-1',
      title: 'existing',
      metadata: { persisted: true },
      createdAt: new Date('2026-01-01T00:00:00.000Z'),
      updatedAt: new Date('2026-01-01T00:00:00.000Z'),
    };
    const memory = {
      getThreadById: vi.fn().mockResolvedValue(existingThread),
      getMergedThreadConfig: vi.fn().mockReturnValue({}),
      storage: { getStore: vi.fn().mockResolvedValue({ supportsObservationalMemorySourceWriteGuards: true }) },
      saveThread: vi.fn(async ({ thread, observationalMemorySourceWriteGuard }) => {
        savedGuards.push(observationalMemorySourceWriteGuard);
        return thread;
      }),
    } as any;
    const runScope = createRunScope();
    const options = { messages: [] } as any;
    const step = createPrepareMemoryStep({
      capabilities: {
        agentName: 'resume-test-agent',
        logger: { trackException: vi.fn() },
        generateMessageId: () => 'message-1',
        runInputProcessors: vi.fn(),
      } as any,
      options,
      threadFromArgs: { ...existingThread, metadata: { resumed: true } },
      resourceId: 'resource-1',
      runId: 'run-1',
      requestContext: new RequestContext(),
      methodType: 'stream',
      instructions: 'resume',
      memory,
      resumeContext: {
        snapshot: {
          context: {
            suspended: {
              status: 'suspended',
              suspendPayload: {
                __streamState: {
                  messageList: {
                    memoryInfo: { observationalMemorySourceWriteGuard: guard },
                  },
                },
              },
            },
          },
        },
      },
      isResume: true,
      runScope,
    });

    await step.execute({
      runId: 'run-1',
      resourceId: 'resource-1',
      workflowId: 'workflow-1',
      mastra: undefined as any,
      requestContext: new RequestContext(),
      inputData: {},
      state: undefined,
      setState: vi.fn(),
      retryCount: 0,
      getInitData: vi.fn(),
      getStepResult: vi.fn(),
      suspend: vi.fn(),
      bail: vi.fn(),
      abort: vi.fn(),
      engine: undefined as any,
      abortSignal: new AbortController().signal,
      writer: undefined as any,
    } as any);

    expect(savedGuards).toEqual([guard]);
    expect(runScope.getOrThrow(MESSAGE_LIST_KEY).serialize().memoryInfo).toMatchObject({
      observationalMemorySourceWriteGuard: guard,
    });
  });
});
