import { describe, expect, it, vi } from 'vitest';
import { RequestContext } from '../../../request-context';
import { PUBSUB_SYMBOL } from '../../../workflows/constants';
import { MessageList } from '../../message-list';
import { SaveQueueManager } from '../../save-queue';
import { globalRunRegistry } from '../run-registry';
import { createDurableAgenticWorkflow } from './create-durable-agentic-workflow';

describe('createDurableAgenticWorkflow terminal result finalization', () => {
  const workflow = createDurableAgenticWorkflow() as any;
  // `map-final-output` is declared with `.map()`, so the execution graph holds a
  // `{ type: 'mapping' }` entry that carries no `.step`. The runnable step exists only
  // in `workflow.steps`, where `.map()` registers it via `createMappingStep`; that step
  // dispatches through the same `runMappingEntry` the evented engine uses, so invoking
  // it here exercises the real finalization path.
  const finalOutputEntry = { step: workflow.steps['map-final-output'] };

  it('resolves the map-final-output step this suite drives', () => {
    // Guard the lookup itself: if `.map()` ever registers its runnable step elsewhere,
    // fail here once and legibly instead of throwing an undefined-read in all 14 cases.
    expect(finalOutputEntry.step).toBeDefined();
  });

  function createRecoverableTerminalState(runId: string, runtimeBindingId: string, threadExists = true) {
    return {
      runId,
      runtimeBindingId,
      agentId: 'terminal-agent',
      accumulatedSteps: [{}],
      accumulatedUsage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
      messageListState: new MessageList().serialize(),
      messageId: 'terminal-message',
      state: {
        memoryConfigured: true,
        threadId: 'terminal-thread',
        resourceId: 'terminal-resource',
        threadExists,
        observationalMemory: false,
        memoryConfig: {},
      },
      lastStepResult: { reason: 'tool-calls', warnings: [], isContinued: false },
      terminalToolResult: {
        status: 'success',
        items: [
          {
            toolName: 'answer',
            toolCallId: 'call-terminal',
            status: 'success',
            value: { answer: 'recoverable answer' },
          },
        ],
      },
    };
  }

  function finalizerParams(state: ReturnType<typeof createRecoverableTerminalState>, published: unknown[]) {
    return {
      inputData: state,
      getInitData: () => ({
        runId: state.runId,
        runtimeBindingId: state.runtimeBindingId,
        agentId: state.agentId,
        state: state.state,
      }),
      [PUBSUB_SYMBOL]: {
        async publish(_topic: string, event: unknown) {
          published.push(event);
        },
      },
    };
  }

  it.each([
    ['empty items', { status: 'success', items: [] }],
    [
      'oversized value',
      {
        status: 'success',
        items: [
          {
            toolName: 'answer',
            toolCallId: 'call-oversized',
            status: 'success',
            value: { text: 'x'.repeat(64 * 1024) },
          },
        ],
      },
    ],
  ])('rejects a replayed %s terminal payload before processors, memory, or publication', async (_label, terminal) => {
    const runId = `run-invalid-terminal-${_label}`;
    const runtimeBindingId = `binding-invalid-terminal-${_label}`;
    const processOutputResult = vi.fn();
    const flushMessages = vi.fn();
    const published: unknown[] = [];
    globalRunRegistry.set(runId, {
      runtimeBindingId,
      outputProcessors: [{ id: 'must-not-run', processOutputResult }],
      inputProcessors: [],
      errorProcessors: [],
      processorStates: new Map(),
      saveQueueManager: { flushMessages },
    } as any);
    const state = {
      runId,
      runtimeBindingId,
      agentId: 'invalid-terminal-agent',
      accumulatedSteps: [{}],
      accumulatedUsage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
      messageListState: new MessageList().serialize(),
      messageId: 'invalid-terminal-message',
      state: {
        memoryConfigured: true,
        threadId: 'invalid-terminal-thread',
        resourceId: 'invalid-terminal-resource',
        threadExists: true,
        observationalMemory: false,
        memoryConfig: {},
      },
      lastStepResult: { reason: 'tool-calls', warnings: [], isContinued: false },
      terminalToolResult: terminal,
    };

    try {
      await expect(
        finalOutputEntry.step.execute({
          inputData: state,
          getInitData: () => ({
            runId,
            runtimeBindingId,
            agentId: state.agentId,
            state: state.state,
          }),
          [PUBSUB_SYMBOL]: {
            async publish(_topic: string, event: unknown) {
              published.push(event);
            },
          },
        }),
      ).rejects.toThrow(/Terminal tool result|byte limit/);
    } finally {
      globalRunRegistry.delete(runId);
    }

    expect(processOutputResult).not.toHaveBeenCalled();
    expect(flushMessages).not.toHaveBeenCalled();
    expect(published).toEqual([]);
  });

  it('does not publish a terminal answer when an output processor fails', async () => {
    const runId = 'run-terminal-processor-failure';
    const runtimeBindingId = 'binding-terminal-processor-failure';
    const state = createRecoverableTerminalState(runId, runtimeBindingId);
    const flushMessages = vi.fn();
    const published: unknown[] = [];
    globalRunRegistry.set(runId, {
      runtimeBindingId,
      memory: {},
      saveQueueManager: { flushMessages },
      outputProcessors: [
        {
          id: 'failing-terminal-processor',
          terminalToolResultPolicy: 'pass-through',
          processOutputResult: vi.fn().mockRejectedValue(new Error('processor persistence failed')),
        },
      ],
      inputProcessors: [],
      errorProcessors: [],
      processorStates: new Map(),
    } as any);

    try {
      await expect(finalOutputEntry.step.execute(finalizerParams(state, published))).rejects.toThrow(
        'processor persistence failed',
      );
    } finally {
      globalRunRegistry.delete(runId);
    }

    expect(flushMessages).not.toHaveBeenCalled();
    expect(published).toEqual([]);
  });

  it.each(['delete', 'mutate', 'duplicate'] as const)(
    'does not publish when a final processor %ss the authoritative terminal part',
    async mode => {
      const runId = `run-terminal-processor-${mode}`;
      const runtimeBindingId = `binding-terminal-processor-${mode}`;
      const state = createRecoverableTerminalState(runId, runtimeBindingId);
      state.state.memoryConfigured = false;
      const published: unknown[] = [];
      const processOutputResult = vi.fn(({ messages }: { messages: any[] }) =>
        messages.map(message => ({
          ...message,
          content: {
            ...message.content,
            parts: message.content.parts.flatMap((part: any) => {
              if (part.type !== 'data-terminal-tool-result') return [part];
              if (mode === 'delete') return [];
              if (mode === 'duplicate') return [part, structuredClone(part)];
              return [
                {
                  ...part,
                  data: {
                    ...part.data,
                    items: part.data.items.map((item: any) => ({ ...item, value: { answer: 'changed' } })),
                  },
                },
              ];
            }),
          },
        })),
      );
      globalRunRegistry.set(runId, {
        runtimeBindingId,
        outputProcessors: [
          {
            id: `invalidating-terminal-owner-${mode}`,
            terminalToolResultPolicy: 'pass-through',
            terminalToolResultPersistence: 'owner',
            processOutputResult,
          },
        ],
        inputProcessors: [],
        errorProcessors: [],
        processorStates: new Map(),
      } as any);

      try {
        await expect(finalOutputEntry.step.execute(finalizerParams(state, published))).rejects.toThrow(
          /retain exactly one|changed the terminal tool result/,
        );
      } finally {
        globalRunRegistry.delete(runId);
      }

      expect(processOutputResult).toHaveBeenCalledOnce();
      expect(published).toEqual([]);
    },
  );

  it.each([
    ['existing', true],
    ['new', false],
  ])(
    'does not publish a terminal answer when configured memory is missing for an %s thread',
    async (_label, exists) => {
      const runId = `run-terminal-missing-memory-${_label}`;
      const runtimeBindingId = `binding-terminal-missing-memory-${_label}`;
      const state = createRecoverableTerminalState(runId, runtimeBindingId, exists);
      const published: unknown[] = [];
      globalRunRegistry.set(runId, {
        runtimeBindingId,
        outputProcessors: [],
        inputProcessors: [],
        errorProcessors: [],
        processorStates: new Map(),
      } as any);

      try {
        await expect(finalOutputEntry.step.execute(finalizerParams(state, published))).rejects.toThrow(
          'configured memory could not be resolved',
        );
      } finally {
        globalRunRegistry.delete(runId);
      }

      expect(published).toEqual([]);
    },
  );

  it('delivers without persistence when thread identifiers are supplied but no memory backend was configured', async () => {
    const runId = 'run-terminal-no-memory';
    const runtimeBindingId = 'binding-terminal-no-memory';
    const state = createRecoverableTerminalState(runId, runtimeBindingId, false);
    state.state.memoryConfigured = false;
    const published: any[] = [];
    globalRunRegistry.set(runId, {
      runtimeBindingId,
      outputProcessors: [],
      inputProcessors: [],
      errorProcessors: [],
      processorStates: new Map(),
    } as any);

    try {
      await finalOutputEntry.step.execute(finalizerParams(state, published));
    } finally {
      globalRunRegistry.delete(runId);
    }

    expect(published.filter(event => event.type === 'finish' && event.data?.terminalToolResult)).toHaveLength(1);
  });

  it('finalizes an active memory turn on its existing list with the exact child read-only context', async () => {
    const runId = 'run-terminal-active-memory-context';
    const runtimeBindingId = 'binding-terminal-active-memory-context';
    const state = createRecoverableTerminalState(runId, runtimeBindingId);
    state.state.memoryConfig = { readOnly: true };
    const activeMessageList = new MessageList({
      threadId: state.state.threadId,
      resourceId: state.state.resourceId,
    });
    const processOutputResult = vi.fn(async ({ messageList, requestContext }) => {
      expect(messageList).toBe(activeMessageList);
      expect(requestContext.get('MastraMemory')).toEqual({
        thread: { id: 'terminal-thread' },
        resourceId: 'terminal-resource',
        memoryConfig: { readOnly: true },
      });
      return messageList;
    });
    const published: any[] = [];
    globalRunRegistry.set(runId, {
      runtimeBindingId,
      messageList: activeMessageList,
      outputProcessors: [
        {
          id: 'active-memory-owner',
          terminalToolResultPolicy: 'pass-through',
          terminalToolResultPersistence: 'owner',
          processOutputResult,
        },
      ],
      inputProcessors: [],
      errorProcessors: [],
      processorStates: new Map(),
    } as any);
    const parentContext = new RequestContext();
    parentContext.set('MastraMemory', {
      thread: { id: 'parent-thread' },
      resourceId: 'parent-resource',
      memoryConfig: { readOnly: false },
    });

    try {
      await finalOutputEntry.step.execute({
        ...finalizerParams(state, published),
        requestContext: parentContext,
      });
    } finally {
      globalRunRegistry.delete(runId);
    }

    expect(processOutputResult).toHaveBeenCalledOnce();
    expect(
      activeMessageList.get.response
        .db()
        .flatMap(message => message.content.parts)
        .filter(part => part.type === 'data-terminal-tool-result'),
    ).toHaveLength(1);
    expect(published.filter(event => event.type === 'finish' && event.data?.terminalToolResult)).toHaveLength(1);
  });

  it('creates and persists a new memory thread before publishing its terminal answer', async () => {
    const runId = 'run-terminal-new-thread-success';
    const runtimeBindingId = 'binding-terminal-new-thread-success';
    const state = createRecoverableTerminalState(runId, runtimeBindingId, false);
    const published: any[] = [];
    const createThread = vi.fn().mockResolvedValue(undefined);
    const flushMessagesStrict = vi.fn().mockResolvedValue(undefined);
    globalRunRegistry.set(runId, {
      runtimeBindingId,
      memory: { createThread },
      saveQueueManager: { flushMessagesStrict },
      outputProcessors: [],
      inputProcessors: [],
      errorProcessors: [],
      processorStates: new Map(),
    } as any);

    try {
      await finalOutputEntry.step.execute(finalizerParams(state, published));
    } finally {
      globalRunRegistry.delete(runId);
    }

    expect(createThread).toHaveBeenCalledOnce();
    expect(createThread).toHaveBeenCalledWith(
      expect.objectContaining({ threadId: 'terminal-thread', resourceId: 'terminal-resource' }),
    );
    expect(flushMessagesStrict).toHaveBeenCalledOnce();
    expect(published.filter(event => event.type === 'finish' && event.data?.terminalToolResult)).toHaveLength(1);
  });

  it.each([
    ['existing', true],
    ['new', false],
  ])('does not publish a terminal answer when an %s thread memory flush fails', async (_label, exists) => {
    const runId = `run-terminal-memory-failure-${_label}`;
    const runtimeBindingId = `binding-terminal-memory-failure-${_label}`;
    const state = createRecoverableTerminalState(runId, runtimeBindingId, exists);
    const published: unknown[] = [];
    const flushMessagesStrict = vi.fn().mockRejectedValue(new Error('memory flush failed'));
    const createThread = vi.fn().mockResolvedValue(undefined);
    globalRunRegistry.set(runId, {
      runtimeBindingId,
      memory: { createThread },
      saveQueueManager: { flushMessagesStrict },
      outputProcessors: [],
      inputProcessors: [],
      errorProcessors: [],
      processorStates: new Map(),
    } as any);

    try {
      await expect(finalOutputEntry.step.execute(finalizerParams(state, published))).rejects.toThrow(
        'memory flush failed',
      );
    } finally {
      globalRunRegistry.delete(runId);
    }

    expect(flushMessagesStrict).toHaveBeenCalledOnce();
    expect(createThread).toHaveBeenCalledTimes(exists ? 0 : 1);
    expect(published).toEqual([]);
  });

  it('retries a real strict memory flush and publishes the terminal answer only after one durable save', async () => {
    const runId = 'run-terminal-real-save-retry';
    const runtimeBindingId = 'binding-terminal-real-save-retry';
    const state = createRecoverableTerminalState(runId, runtimeBindingId);
    const persisted: any[] = [];
    let attempts = 0;
    const memory = {
      createThread: vi.fn(),
      saveMessages: vi.fn(async ({ messages }: { messages: any[] }) => {
        attempts++;
        if (attempts === 1) throw new Error('transient memory outage');
        persisted.push(...messages);
      }),
    };
    const published: any[] = [];
    globalRunRegistry.set(runId, {
      runtimeBindingId,
      memory,
      saveQueueManager: new SaveQueueManager({ memory: memory as any }),
      outputProcessors: [],
      inputProcessors: [],
      errorProcessors: [],
      processorStates: new Map(),
    } as any);

    try {
      await expect(finalOutputEntry.step.execute(finalizerParams(state, published))).rejects.toThrow(
        'transient memory outage',
      );
      expect(published).toEqual([]);

      await finalOutputEntry.step.execute(finalizerParams(state, published));
    } finally {
      globalRunRegistry.delete(runId);
    }

    expect(memory.saveMessages).toHaveBeenCalledTimes(2);
    expect(persisted).toHaveLength(1);
    expect(persisted[0]?.content?.parts?.filter((part: any) => part.type === 'data-terminal-tool-result')).toHaveLength(
      1,
    );
    expect(published.filter(event => event.type === 'finish' && event.data?.terminalToolResult)).toHaveLength(1);
  });
});
