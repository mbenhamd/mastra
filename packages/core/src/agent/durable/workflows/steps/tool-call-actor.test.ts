import { describe, expect, it, vi } from 'vitest';
import type { MCPToolExecutionContext } from '../../../../tools';
import { createToolCallIdentityDigest } from '../../../tool-call-identity';
import { globalRunRegistry } from '../../run-registry';
import { createDurableToolCallStep } from './tool-call';

vi.mock('../../utils/resolve-runtime', async () => ({
  restoreRequestContext: (
    await vi.importActual<typeof import('../../utils/resolve-runtime')>('../../utils/resolve-runtime')
  ).restoreRequestContext,
  resolveTool: vi.fn(),
  toolApprovalRequirement: vi.fn().mockResolvedValue({ required: false, reasons: [] }),
  rebuildRunToolsFromMastra: vi.fn().mockResolvedValue(undefined),
}));

const RUNTIME_BINDING_ID = 'durable-tool-context-binding';

describe('durable tool-call context forwarding', () => {
  it('uses the current workflow-segment actor instead of the initial actor', async () => {
    const runId = 'durable-tool-actor-run';
    const execute = vi.fn().mockResolvedValue('ok');
    const initialActor = { actorKind: 'system' as const, sourceWorkflow: 'initial-run' };
    const resumeActor = { actorKind: 'system' as const, sourceWorkflow: 'approval-resume' };
    globalRunRegistry.set(runId, { runtimeBindingId: RUNTIME_BINDING_ID, tools: { secureTool: { execute } } } as any);

    try {
      await (createDurableToolCallStep() as any).execute({
        inputData: {
          toolCallId: 'call-1',
          toolName: 'secureTool',
          args: { query: 'mastra' },
        },
        mastra: { getLogger: () => undefined },
        suspend: vi.fn(),
        actor: resumeActor,
        getInitData: () => ({
          runId,
          runtimeBindingId: RUNTIME_BINDING_ID,
          agentId: 'agent-1',
          options: { actor: initialActor },
          state: {},
        }),
      });

      expect(execute).toHaveBeenCalledWith({ query: 'mastra' }, expect.objectContaining({ actor: resumeActor }));
    } finally {
      globalRunRegistry.delete(runId);
    }
  });

  it.each([
    ['false', false],
    ['zero', 0],
    ['an empty string', ''],
  ])(
    'retires both persisted representations of only the selected delegated suspension for %s',
    async (_label, resumeData) => {
      const runId = 'durable-tool-resume-run';
      const execute = vi.fn().mockResolvedValue('ok');
      const message = {
        id: 'assistant-suspended',
        role: 'assistant',
        createdAt: new Date(0),
        content: {
          format: 2,
          metadata: {
            suspendedTools: {
              'call-a': {
                toolCallId: 'call-a',
                toolName: 'workflow-sub',
                delegatedRunId: 'inner-a',
              },
              'call-b': {
                toolCallId: 'call-b',
                toolName: 'workflow-sub',
                delegatedRunId: 'inner-b',
                // Fork's durable resume-evidence envelope: the step
                // authenticates a fresh-turn model resume against the
                // persisted suspension record (hasMatchingFreshTurnIdentity)
                // before executing. Without these fields the fence rejects
                // the resume as unauthenticated.
                version: 1,
                type: 'suspension',
                stepId: 'durable-tool-call',
                originRunId: 'inner-b',
                runId: 'inner-b',
                iterationCount: 0,
                identityDigest: createToolCallIdentityDigest({
                  toolCallId: 'call-b',
                  toolName: 'workflow-sub',
                  args: {},
                }),
              },
            },
          },
          parts: [
            {
              type: 'data-tool-call-suspended',
              data: { toolCallId: 'call-a', toolName: 'workflow-sub', runId: 'inner-a' },
            },
            {
              type: 'data-tool-call-suspended',
              data: { toolCallId: 'call-b', toolName: 'workflow-sub', runId: 'inner-b' },
            },
          ],
        },
      };
      const messageList = {
        add: vi.fn(),
        get: {
          response: { db: () => [message] },
          all: { db: () => [message] },
        },
      };
      globalRunRegistry.set(runId, {
        runtimeBindingId: RUNTIME_BINDING_ID,
        tools: { 'workflow-sub': { execute } },
        messageList,
      } as any);

      try {
        await (createDurableToolCallStep() as any).execute({
          inputData: {
            toolCallId: 'resume-call',
            toolName: 'workflow-sub',
            args: {
              resumeData,
              suspendedToolCallId: 'call-b',
              suspendedToolRunId: 'inner-b',
            },
          },
          mastra: { getLogger: () => undefined },
          suspend: vi.fn(),
          getInitData: () => ({
            runId,
            runtimeBindingId: RUNTIME_BINDING_ID,
            agentId: 'agent-1',
            options: {},
            state: {},
          }),
        });

        expect(execute).toHaveBeenCalledWith(
          expect.objectContaining({ suspendedToolRunId: 'inner-b' }),
          expect.objectContaining({ resumeData }),
        );
        expect(message.content.metadata.suspendedTools).toEqual({
          'call-a': expect.objectContaining({ delegatedRunId: 'inner-a' }),
        });
        expect(message.content.parts).toEqual([
          expect.objectContaining({ data: expect.objectContaining({ toolCallId: 'call-a' }) }),
          expect.objectContaining({ data: expect.objectContaining({ toolCallId: 'call-b', resumed: true }) }),
        ]);
        expect(messageList.add).toHaveBeenCalledWith([message], 'response');
      } finally {
        globalRunRegistry.delete(runId);
      }
    },
  );

  it('treats null model resume data as framework-driven and ignores model identity claims', async () => {
    const runId = 'durable-null-resume-run';
    const execute = vi.fn().mockResolvedValue('ok');
    const message = {
      id: 'assistant-suspended',
      role: 'assistant',
      createdAt: new Date(0),
      content: {
        format: 2,
        metadata: {
          suspendedTools: {
            'call-a': { toolCallId: 'call-a', toolName: 'workflow-sub', delegatedRunId: 'inner-a' },
            'call-b': { toolCallId: 'call-b', toolName: 'workflow-sub', delegatedRunId: 'inner-b' },
          },
        },
        parts: [
          {
            type: 'data-tool-call-suspended',
            data: { toolCallId: 'call-a', toolName: 'workflow-sub', runId: 'inner-a' },
          },
          {
            type: 'data-tool-call-suspended',
            data: { toolCallId: 'call-b', toolName: 'workflow-sub', runId: 'inner-b' },
          },
        ],
      },
    };
    const messageList = {
      add: vi.fn(),
      get: {
        response: { db: () => [message] },
        all: { db: () => [message] },
      },
    };
    globalRunRegistry.set(runId, {
      runtimeBindingId: RUNTIME_BINDING_ID,
      tools: { 'workflow-sub': { execute } },
      messageList,
    } as any);

    try {
      await (createDurableToolCallStep() as any).execute({
        inputData: {
          toolCallId: 'call-a',
          toolName: 'workflow-sub',
          args: {
            resumeData: null,
            suspendedToolCallId: 'call-b',
            suspendedToolRunId: 'inner-b',
          },
        },
        resumeData: { approved: true },
        // Fork's durable resume-evidence envelope for the workflow-owned
        // resume leg (hasMatchingWorkflowIdentity): authenticates call-a
        // against this run before the framework path resolves the inner
        // suspended run from the persisted state.
        suspendData: {
          version: 1,
          type: 'suspension',
          stepId: 'durable-tool-call',
          toolCallId: 'call-a',
          toolName: 'workflow-sub',
          runId: 'durable-null-resume-run',
          iterationCount: 0,
          identityDigest: createToolCallIdentityDigest({ toolCallId: 'call-a', toolName: 'workflow-sub', args: {} }),
          suspendedToolRunId: 'inner-a',
        },
        mastra: { getLogger: () => undefined },
        suspend: vi.fn(),
        getInitData: () => ({
          runId,
          runtimeBindingId: RUNTIME_BINDING_ID,
          agentId: 'agent-1',
          options: {},
          state: {},
        }),
      });

      expect(execute).toHaveBeenCalledWith(
        expect.objectContaining({ suspendedToolRunId: 'inner-a' }),
        expect.objectContaining({ resumeData: { approved: true } }),
      );
      expect(message.content.metadata.suspendedTools).toEqual({
        'call-b': expect.objectContaining({ delegatedRunId: 'inner-b' }),
      });
      expect(message.content.parts).toEqual([
        expect.objectContaining({ data: expect.objectContaining({ toolCallId: 'call-a', resumed: true }) }),
        expect.objectContaining({ data: expect.objectContaining({ toolCallId: 'call-b' }) }),
      ]);
    } finally {
      globalRunRegistry.delete(runId);
    }
  });

  it('forwards MCP protocol context from the run registry', async () => {
    const runId = 'durable-tool-mcp-run';
    const execute = vi.fn().mockResolvedValue('ok');
    const mcp: MCPToolExecutionContext = {
      extra: {
        signal: new AbortController().signal,
        requestId: 'request-1',
        sendNotification: vi.fn(),
        sendRequest: vi.fn(),
      },
      elicitation: { sendRequest: vi.fn() },
    };
    globalRunRegistry.set(runId, {
      runtimeBindingId: RUNTIME_BINDING_ID,
      tools: { secureTool: { execute } },
      mcp,
    } as any);

    try {
      await (createDurableToolCallStep() as any).execute({
        inputData: {
          toolCallId: 'call-1',
          toolName: 'secureTool',
          args: { query: 'mastra' },
        },
        mastra: { getLogger: () => undefined },
        suspend: vi.fn(),
        getInitData: () => ({
          runId,
          runtimeBindingId: RUNTIME_BINDING_ID,
          agentId: 'agent-1',
          options: {},
          state: {},
        }),
      });

      expect(execute).toHaveBeenCalledWith({ query: 'mastra' }, expect.objectContaining({ mcp }));
    } finally {
      globalRunRegistry.delete(runId);
    }
  });
});
