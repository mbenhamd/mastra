import { describe, expect, it } from 'vitest';

import { toHarnessDisplayStateSnapshotV1 } from './display-state';
import type { SessionDisplayState } from './session';

function makeDisplayState(overrides: Partial<SessionDisplayState> = {}): SessionDisplayState {
  return {
    sessionId: 'session-1',
    threadId: 'thread-1',
    resourceId: 'resource-1',
    lifecycleState: 'live',
    modeId: 'default',
    modelId: '__GATEWAY_OPENAI_MODEL_BASE__',
    createdAt: 1,
    lastActivityAt: 2,
    isRunning: false,
    activeTools: {},
    toolInputBuffers: {},
    activeSubagents: {},
    assistantDrafts: {},
    tokenUsage: { promptTokens: 0, completionTokens: 0, totalTokens: 0 },
    pending: null,
    queueDepth: 0,
    ...overrides,
  };
}

describe('toHarnessDisplayStateSnapshotV1', () => {
  it('adds a versioned JSON-safe display-state envelope', () => {
    const snapshot = toHarnessDisplayStateSnapshotV1(makeDisplayState());

    expect(snapshot).toMatchObject({
      version: 1,
      sessionId: 'session-1',
      threadId: 'thread-1',
      resourceId: 'resource-1',
      lifecycleState: 'live',
      modeId: 'default',
      modelId: '__GATEWAY_OPENAI_MODEL_BASE__',
      isRunning: false,
      activeTools: {},
      toolInputBuffers: {},
      activeSubagents: {},
      assistantDrafts: {},
      pending: null,
      queueDepth: 0,
    });
  });

  it('serializes unknown active tool and pending payload values to JsonValue', () => {
    const date = new Date('2026-05-23T05:00:00.000Z');
    const circular: Record<string, unknown> = { ok: true };
    circular.self = circular;
    const protoKey = JSON.parse('{"__proto__":{"polluted":true}}') as Record<string, unknown>;
    const serializable = {
      toJSON() {
        return { href: 'https://mastra.ai/harness' };
      },
    };

    const snapshot = toHarnessDisplayStateSnapshotV1(
      makeDisplayState({
        activeTools: {
          tool_1: {
            toolCallId: 'tool_1',
            toolName: 'inspect',
            args: {
              date,
              count: 3n,
              badNumber: Number.POSITIVE_INFINITY,
              nested: circular,
              protoKey,
              serializable,
              omitted: undefined,
            },
            startedAt: 11,
          },
        },
        pending: {
          kind: 'question',
          itemId: 'question-1',
          runId: 'run-1',
          toolCallId: 'tool_1',
          source: 'parent',
          requestedAt: 12,
          payload: {
            question: 'Pick one',
            requestedAt: date,
            count: 4n,
          },
        } as SessionDisplayState['pending'],
      }),
    );

    const args = snapshot.activeTools.tool_1?.args as Record<string, unknown>;
    expect(args).toMatchObject({
      date: '2026-05-23T05:00:00.000Z',
      count: '3',
      badNumber: null,
      nested: { ok: true, self: null },
      serializable: { href: 'https://mastra.ai/harness' },
    });
    expect(Object.prototype.hasOwnProperty.call(args, 'protoKey')).toBe(true);
    const encodedProtoKey = args.protoKey as Record<string, unknown>;
    expect(Object.prototype.hasOwnProperty.call(encodedProtoKey, '__proto__')).toBe(true);
    expect(encodedProtoKey.__proto__).toEqual({ polluted: true });
    expect((encodedProtoKey as any).polluted).toBeUndefined();
    expect(snapshot.pending?.payload).toEqual({
      question: 'Pick one',
      requestedAt: '2026-05-23T05:00:00.000Z',
      count: '4',
    });
    expect((snapshot.pending as unknown as Record<string, unknown>).runtimeDependencies).toBeUndefined();
  });

  it('preserves active subagents, tool buffers, token usage, queue, run, and goal fields', () => {
    const snapshot = toHarnessDisplayStateSnapshotV1(
      makeDisplayState({
        parentSessionId: 'parent-1',
        isRunning: true,
        currentRunId: 'run-1',
        currentMessageId: 'message-1',
        currentTraceId: 'trace-1',
        currentQueuedItemId: 'queue-1',
        activeSubagents: {
          tool_2: {
            subagentSessionId: 'child-1',
            agentType: 'research',
            task: 'scan',
            parentToolCallId: 'tool_2',
            startedAt: 20,
          },
        },
        toolInputBuffers: {
          tool_1: { toolName: 'shell', text: 'npm test' },
        },
        assistantDrafts: {
          run_1: {
            runId: 'run_1',
            sessionId: 'session-1',
            resourceId: 'resource-1',
            threadId: 'thread-1',
            messageId: 'message-1',
            text: 'partial answer',
            reasoningText: 'thinking',
            status: 'streaming',
            startedAt: 21,
            updatedAt: 22,
          },
        },
        tokenUsage: { promptTokens: 2, completionTokens: 3, totalTokens: 5 },
        queueDepth: 1,
        goal: {
          id: 'goal-1',
          objective: 'finish',
          status: 'active',
          turnsUsed: 1,
          maxTurns: 4,
          judgeModelId: '__GATEWAY_OPENAI_MODEL_MINI__',
          createdAt: 30,
        },
      }),
    );

    expect(snapshot.parentSessionId).toBe('parent-1');
    expect(snapshot.currentRunId).toBe('run-1');
    expect(snapshot.currentMessageId).toBe('message-1');
    expect(snapshot.currentTraceId).toBe('trace-1');
    expect(snapshot.currentQueuedItemId).toBe('queue-1');
    expect(snapshot.activeSubagents.tool_2).toEqual({
      subagentSessionId: 'child-1',
      agentType: 'research',
      task: 'scan',
      parentToolCallId: 'tool_2',
      startedAt: 20,
    });
    expect(snapshot.toolInputBuffers.tool_1).toEqual({ toolName: 'shell', text: 'npm test' });
    expect(snapshot.assistantDrafts.run_1).toEqual({
      runId: 'run_1',
      sessionId: 'session-1',
      resourceId: 'resource-1',
      threadId: 'thread-1',
      messageId: 'message-1',
      text: 'partial answer',
      reasoningText: 'thinking',
      status: 'streaming',
      startedAt: 21,
      updatedAt: 22,
    });
    expect(snapshot.tokenUsage).toEqual({ promptTokens: 2, completionTokens: 3, totalTokens: 5 });
    expect(snapshot.queueDepth).toBe(1);
    expect(snapshot.goal).toMatchObject({ id: 'goal-1', status: 'active' });
  });

  it('copies assistant drafts so consumers cannot mutate source state', () => {
    const assistantDrafts: NonNullable<SessionDisplayState['assistantDrafts']> = {
      run_1: {
        runId: 'run_1',
        sessionId: 'session-1',
        resourceId: 'resource-1',
        threadId: 'thread-1',
        text: 'hello',
        status: 'streaming',
        startedAt: 1,
        updatedAt: 2,
      },
    };
    const snapshot = toHarnessDisplayStateSnapshotV1(makeDisplayState({ assistantDrafts }));

    snapshot.assistantDrafts.run_1!.text = 'mutated';
    expect(assistantDrafts.run_1!.text).toBe('hello');
  });

  it('carries the bounded plan-task summary (counts/byStatus/inProgressTaskIds/rootCount), copied not shared', () => {
    const planTasks = {
      total: 4,
      byStatus: { in_progress: 2, pending: 2 } as Record<string, number>,
      inProgressTaskIds: ['task-a', 'task-r'],
      rootCount: 2,
    };
    const snapshot = toHarnessDisplayStateSnapshotV1(makeDisplayState({ planTasks }));

    expect(snapshot.planTasks).toEqual({
      total: 4,
      byStatus: { in_progress: 2, pending: 2 },
      inProgressTaskIds: ['task-a', 'task-r'],
      rootCount: 2,
    });
    // Defensive copy: mutating the snapshot must not reach back into the source.
    snapshot.planTasks!.inProgressTaskIds.push('leak');
    snapshot.planTasks!.byStatus.pending = 99;
    expect(planTasks.inProgressTaskIds).toEqual(['task-a', 'task-r']);
    expect(planTasks.byStatus.pending).toBe(2);
    // Bounded shape — no full tree embedded.
    expect(Object.keys(snapshot.planTasks!).sort()).toEqual(['byStatus', 'inProgressTaskIds', 'rootCount', 'total']);
  });

  it('omits planTasks when the session has not observed a plan tree', () => {
    const snapshot = toHarnessDisplayStateSnapshotV1(makeDisplayState());
    expect(snapshot.planTasks).toBeUndefined();
  });
});
