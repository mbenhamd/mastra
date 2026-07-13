/**
 * Storage-backed suspended-run discovery.
 *
 * `getActiveThreadRunId()` is backed by an in-memory map, so it returns
 * `undefined` after a server restart or on a different instance. These tests
 * cover the durable path: `agent.listSuspendedRuns()` discovers suspended runs from
 * workflow snapshot storage, and `sendToolApproval()` falls back to it when
 * the in-memory map has no entry — making HITL approvals survive restarts.
 */
import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import type { IFGAProvider } from '../../auth/ee/interfaces/fga';
import { Mastra } from '../../mastra';
import { MASTRA_RESOURCE_ID_KEY, MASTRA_THREAD_ID_KEY, RequestContext } from '../../request-context';
import { InMemoryStore } from '../../storage';
import { createTool } from '../../tools';
import type { WorkflowRunState } from '../../workflows/types';
import { Agent } from '../agent';
import { agentThreadStreamRuntime } from '../thread-stream-runtime';
import { convertArrayToReadableStream, MockLanguageModelV2 } from './mock-model';

const mockFindUser = vi.fn().mockImplementation(async (data: { name: string }) => {
  return { name: data.name, email: 'dero@mail.com' };
});

function createFindUserTool() {
  return createTool({
    id: 'Find user tool',
    description: 'Returns the name and email of a user',
    inputSchema: z.object({ name: z.string() }),
    requireApproval: true,
    execute: async input => {
      return mockFindUser(input);
    },
  });
}

function createMockModel({
  toolCallOnFirstCall = true,
  toolCallId = 'call-1',
  toolName = 'findUserTool',
}: { toolCallOnFirstCall?: boolean; toolCallId?: string; toolName?: string } = {}) {
  let callCount = 0;
  return new MockLanguageModelV2({
    doStream: async () => {
      callCount++;
      if (toolCallOnFirstCall && callCount === 1) {
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: convertArrayToReadableStream([
            { type: 'stream-start', warnings: [] },
            { type: 'response-metadata', id: 'id-0', modelId: 'mock-model-id', timestamp: new Date(0) },
            {
              type: 'tool-call',
              toolCallId,
              toolName,
              input: '{"name":"Dero Israel"}',
              providerExecuted: false,
            },
            {
              type: 'finish',
              finishReason: 'tool-calls',
              usage: { inputTokens: 10, outputTokens: 20, totalTokens: 30 },
            },
          ]),
        };
      }
      return {
        rawCall: { rawPrompt: null, rawSettings: {} },
        warnings: [],
        stream: convertArrayToReadableStream([
          { type: 'stream-start', warnings: [] },
          { type: 'response-metadata', id: 'id-1', modelId: 'mock-model-id', timestamp: new Date(0) },
          { type: 'text-start', id: 'text-1' },
          { type: 'text-delta', id: 'text-1', delta: 'User found' },
          { type: 'text-end', id: 'text-1' },
          {
            type: 'finish',
            finishReason: 'stop',
            usage: { inputTokens: 10, outputTokens: 20, totalTokens: 30 },
          },
        ]),
      };
    },
  });
}

/**
 * Emits two parallel tool calls on the first model turn, then a final text
 * answer. When both tools require approval the loop forces sequential
 * (concurrency 1) execution, so the calls suspend one at a time.
 */
function createParallelToolCallsModel() {
  let callCount = 0;
  return new MockLanguageModelV2({
    doStream: async () => {
      callCount++;
      if (callCount === 1) {
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: convertArrayToReadableStream([
            { type: 'stream-start', warnings: [] },
            { type: 'response-metadata', id: 'id-0', modelId: 'mock-model-id', timestamp: new Date(0) },
            {
              type: 'tool-call',
              toolCallId: 'call-A',
              toolName: 'toolA',
              input: '{"name":"A"}',
              providerExecuted: false,
            },
            {
              type: 'tool-call',
              toolCallId: 'call-B',
              toolName: 'toolB',
              input: '{"name":"B"}',
              providerExecuted: false,
            },
            {
              type: 'finish',
              finishReason: 'tool-calls',
              usage: { inputTokens: 10, outputTokens: 20, totalTokens: 30 },
            },
          ]),
        };
      }
      return {
        rawCall: { rawPrompt: null, rawSettings: {} },
        warnings: [],
        stream: convertArrayToReadableStream([
          { type: 'stream-start', warnings: [] },
          { type: 'response-metadata', id: 'id-final', modelId: 'mock-model-id', timestamp: new Date(0) },
          { type: 'text-start', id: 'text-1' },
          { type: 'text-delta', id: 'text-1', delta: 'done' },
          { type: 'text-end', id: 'text-1' },
          { type: 'finish', finishReason: 'stop', usage: { inputTokens: 10, outputTokens: 20, totalTokens: 30 } },
        ]),
      };
    },
  });
}

function createApprovalTool(id: string) {
  return createTool({
    id,
    description: id,
    inputSchema: z.object({ name: z.string() }),
    requireApproval: true,
    execute: async input => input,
  });
}

/**
 * A model that always delegates by emitting a single tool call to a named
 * sub-agent. Used to build multi-level supervisor → sub-agent chains: each
 * delegating agent re-suspends its own loop when the agent it called suspends.
 */
function createDelegationModel({ toolName, toolCallId }: { toolName: string; toolCallId: string }) {
  return new MockLanguageModelV2({
    doStream: async () => ({
      rawCall: { rawPrompt: null, rawSettings: {} },
      warnings: [],
      stream: convertArrayToReadableStream([
        { type: 'stream-start', warnings: [] },
        { type: 'response-metadata', id: `${toolCallId}-0`, modelId: 'mock-model-id', timestamp: new Date(0) },
        {
          type: 'tool-call',
          toolCallId,
          toolName,
          input: '{"message":"Find Dero Israel"}',
          providerExecuted: false,
        },
        { type: 'finish', finishReason: 'tool-calls', usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 } },
      ]),
    }),
  });
}

function createSuspendedSetup({
  storage = new InMemoryStore(),
  toolCallOnFirstCall = true,
  toolCallId,
  fgaProvider,
  requestContextSchema,
}: {
  storage?: InMemoryStore;
  toolCallOnFirstCall?: boolean;
  toolCallId?: string;
  fgaProvider?: IFGAProvider;
  requestContextSchema?: any;
} = {}) {
  const agent = new Agent({
    id: 'user-agent',
    name: 'User Agent',
    instructions: 'You find users.',
    model: createMockModel({ toolCallOnFirstCall, toolCallId }),
    tools: { findUserTool: createFindUserTool() },
    requestContextSchema,
  });

  const mastra = new Mastra({
    agents: { agent },
    logger: false,
    storage,
    server: fgaProvider ? ({ fga: fgaProvider } as any) : undefined,
  });

  return { agent, mastra, storage };
}

function createFGAProvider(): IFGAProvider {
  return {
    check: vi.fn().mockResolvedValue(true),
    require: vi.fn().mockResolvedValue(undefined),
    filterAccessible: vi.fn(),
  };
}

async function suspendRun(agent: Agent, threadId: string, resourceId: string) {
  const stream = await agent.stream('Find the user with name - Dero Israel', {
    requireToolApproval: true,
    memory: { thread: threadId, resource: resourceId },
  });

  let toolCallId = '';
  for await (const chunk of stream.fullStream) {
    if (chunk.type === 'tool-call-approval') {
      toolCallId = chunk.payload.toolCallId;
    }
  }
  expect(toolCallId).toBeTruthy();
  return { runId: stream.runId, toolCallId };
}

async function persistConflictingSuspendedStepOwnership(
  storage: InMemoryStore,
  runId: string,
  conflicting: { threadId: string; resourceId: string },
) {
  const workflowsStore = (await storage.getStore('workflows'))!;
  const run = await workflowsStore.getWorkflowRunById({ runId, workflowName: 'agentic-loop' });
  expect(run).not.toBeNull();
  const snapshot = run!.snapshot as WorkflowRunState;
  const suspendedStep = Object.values(snapshot.context).find(step => step?.status === 'suspended');
  expect(suspendedStep?.suspendPayload?.__streamState).toBeDefined();
  const streamState = suspendedStep!.suspendPayload!.__streamState;
  snapshot.context['conflicting-ownership-step'] = {
    ...suspendedStep,
    suspendPayload: {
      ...suspendedStep!.suspendPayload,
      __streamState: {
        ...streamState,
        messageList: {
          ...streamState.messageList,
          memoryInfo: {
            ...streamState.messageList.memoryInfo,
            ...conflicting,
          },
        },
      },
    },
  } as any;
  await workflowsStore.persistWorkflowSnapshot({
    workflowName: 'agentic-loop',
    runId,
    resourceId: run!.resourceId,
    snapshot,
  });
}

afterEach(() => {
  mockFindUser.mockClear();
});

// The loop workflows pick their engine from MASTRA_EVENTED_EXECUTION at
// creation time (per stream call), so discovery must work against rows
// persisted by both the default (direct) engine and the evented engine.
describe.each([
  { engine: 'default', evented: false },
  { engine: 'evented', evented: true },
])('suspended-run discovery ($engine engine)', ({ evented }) => {
  beforeAll(() => {
    if (evented) vi.stubEnv('MASTRA_EVENTED_EXECUTION', 'true');
  });

  afterAll(() => {
    if (evented) vi.unstubAllEnvs();
  });

  describe('agent.listSuspendedRuns()', () => {
    it('returns suspended runs with thread, resource, and tool-call info', async () => {
      const { agent } = createSuspendedSetup();
      const { runId, toolCallId } = await suspendRun(agent, 'thread-1', 'resource-1');

      const { runs, total } = await agent.listSuspendedRuns();
      expect(total).toBe(1);
      expect(runs).toHaveLength(1);
      expect(runs[0]).toEqual({
        runId,
        status: 'suspended',
        workflowName: 'agentic-loop',
        threadId: 'thread-1',
        resourceId: 'resource-1',
        suspendedAt: expect.any(Date),
        toolCalls: [
          {
            toolCallId,
            toolName: 'findUserTool',
            args: { name: 'Dero Israel' },
            requiresApproval: true,
          },
        ],
      });
    }, 30000);

    it('filters by threadId and resourceId', async () => {
      const { agent } = createSuspendedSetup();
      const { runId } = await suspendRun(agent, 'thread-1', 'resource-1');

      expect((await agent.listSuspendedRuns({ threadId: 'thread-1' })).runs).toHaveLength(1);
      expect((await agent.listSuspendedRuns({ threadId: 'other-thread' })).runs).toHaveLength(0);
      expect((await agent.listSuspendedRuns({ resourceId: 'resource-1' })).runs).toHaveLength(1);
      expect((await agent.listSuspendedRuns({ resourceId: 'other-resource' })).runs).toHaveLength(0);

      const scoped = await agent.listSuspendedRuns({ threadId: 'thread-1', resourceId: 'resource-1' });
      expect(scoped.runs.map(run => run.runId)).toEqual([runId]);
      expect(scoped.total).toBe(1);
    }, 30000);

    it('paginates with perPage/page while keeping total accurate', async () => {
      // The mock model only tool-calls on its first invocation, so suspend each
      // run from a fresh agent sharing the same storage.
      const storage = new InMemoryStore();
      await suspendRun(createSuspendedSetup({ storage }).agent, 'thread-1', 'resource-1');
      await suspendRun(createSuspendedSetup({ storage }).agent, 'thread-2', 'resource-1');
      const { agent } = createSuspendedSetup({ storage });
      await suspendRun(agent, 'thread-3', 'resource-1');
      const workflowsStore = (await storage.getStore('workflows'))!;
      const listWorkflowRuns = vi.spyOn(workflowsStore, 'listWorkflowRuns');

      const pageOne = await agent.listSuspendedRuns({ resourceId: 'resource-1', perPage: 2, page: 0 });
      expect(pageOne.total).toBe(3);
      expect(pageOne.runs).toHaveLength(2);

      const pageTwo = await agent.listSuspendedRuns({ resourceId: 'resource-1', perPage: 2, page: 1 });
      expect(pageTwo.total).toBe(3);
      expect(pageTwo.runs).toHaveLength(1);

      const pageOneIds = pageOne.runs.map(run => run.runId);
      const pageTwoIds = pageTwo.runs.map(run => run.runId);
      expect(new Set([...pageOneIds, ...pageTwoIds]).size).toBe(3);

      // Without both perPage and page, all matching runs are returned.
      expect((await agent.listSuspendedRuns({ resourceId: 'resource-1', perPage: 2 })).runs).toHaveLength(3);
      expect(listWorkflowRuns).toHaveBeenCalledWith(
        expect.objectContaining({ perPage: false, resourceId: 'resource-1' }),
      );
    }, 30000);

    it('only returns runs owned by the listing agent', async () => {
      const storage = new InMemoryStore();
      const agentA = new Agent({
        id: 'agent-a',
        name: 'Agent A',
        instructions: 'You find users.',
        model: createMockModel(),
        tools: { findUserTool: createFindUserTool() },
      });
      const agentB = new Agent({
        id: 'agent-b',
        name: 'Agent B',
        instructions: 'You find users.',
        model: createMockModel(),
        tools: { findUserTool: createFindUserTool() },
      });
      new Mastra({ agents: { agentA, agentB }, logger: false, storage });

      // Both agents suspend for the same resource (distinct threads — a thread
      // only allows one active run at a time).
      const { runId: runA } = await suspendRun(agentA, 'thread-a', 'shared-resource');
      const { runId: runB } = await suspendRun(agentB, 'thread-b', 'shared-resource');

      const listedByA = await agentA.listSuspendedRuns({ resourceId: 'shared-resource' });
      expect(listedByA.runs.map(run => run.runId)).toEqual([runA]);
      expect(listedByA.total).toBe(1);

      const listedByB = await agentB.listSuspendedRuns({ resourceId: 'shared-resource' });
      expect(listedByB.runs.map(run => run.runId)).toEqual([runB]);
      expect(listedByB.total).toBe(1);

      await expect(
        agentB.approveToolCall({
          runId: runA,
          memory: { thread: 'thread-a', resource: 'shared-resource' },
        }),
      ).rejects.toMatchObject({ id: 'AGENT_RESUME_AGENT_MISMATCH' });
      await expect(
        agentA.approveToolCall({
          runId: runA,
          memory: { thread: 'thread-a', resource: 'other-resource' },
        }),
      ).rejects.toMatchObject({ id: 'AGENT_RESUME_OWNER_MISMATCH' });
    }, 30000);

    it('hides snapshots without an owning agent id from every agent (default-deny)', async () => {
      const storage = new InMemoryStore();
      const agentA = new Agent({
        id: 'agent-a',
        name: 'Agent A',
        instructions: 'You find users.',
        model: createMockModel(),
        tools: { findUserTool: createFindUserTool() },
      });
      const agentB = new Agent({
        id: 'agent-b',
        name: 'Agent B',
        instructions: 'You find users.',
        model: createMockModel(),
        tools: { findUserTool: createFindUserTool() },
      });
      new Mastra({ agents: { agentA, agentB }, logger: false, storage });

      const { runId } = await suspendRun(agentA, 'thread-a', 'shared-resource');

      // Simulate a legacy snapshot persisted before __agentId was introduced by
      // stripping it from every suspended step's payload, then re-persisting.
      const workflowsStore = (await storage.getStore('workflows'))!;
      const run = await workflowsStore.getWorkflowRunById({ runId, workflowName: 'agentic-loop' });
      expect(run).not.toBeNull();
      const snapshot = run!.snapshot as WorkflowRunState;
      for (const key in snapshot.context) {
        const step = snapshot.context[key];
        if (step?.status === 'suspended' && step.suspendPayload) {
          delete (step.suspendPayload as Record<string, unknown>).__agentId;
        }
      }
      await workflowsStore.persistWorkflowSnapshot({
        workflowName: 'agentic-loop',
        runId,
        resourceId: 'shared-resource',
        snapshot,
      });

      // A snapshot with no owning agent id must not leak to any agent.
      expect((await agentA.listSuspendedRuns({ resourceId: 'shared-resource' })).total).toBe(0);
      expect((await agentB.listSuspendedRuns({ resourceId: 'shared-resource' })).total).toBe(0);

      await expect(
        agentA.approveToolCall({
          runId,
          memory: { thread: 'thread-a', resource: 'shared-resource' },
        }),
      ).rejects.toMatchObject({ id: 'AGENT_RESUME_AGENT_MISMATCH' });
    }, 30000);

    it('fails closed when suspended steps disagree on thread or resource ownership', async () => {
      const { agent, storage } = createSuspendedSetup();
      const { runId } = await suspendRun(agent, 'thread-a', 'resource-a');
      await persistConflictingSuspendedStepOwnership(storage, runId, {
        threadId: 'thread-b',
        resourceId: 'resource-b',
      });

      expect((await agent.listSuspendedRuns()).runs).toHaveLength(0);
      const options = {
        runId,
        memory: { thread: 'thread-a', resource: 'resource-a' },
      };
      await expect(agent.resumeStream({}, options)).rejects.toMatchObject({
        id: 'AGENT_RESUME_SNAPSHOT_OWNER_CONFLICT',
      });
      await expect(agent.resumeGenerate({}, options)).rejects.toMatchObject({
        id: 'AGENT_RESUME_SNAPSHOT_OWNER_CONFLICT',
      });
    }, 30000);

    it('fails closed when one of several suspended tool steps is missing ownership metadata', async () => {
      const { agent, storage } = createSuspendedSetup();
      const { runId } = await suspendRun(agent, 'thread-a', 'resource-a');
      const workflowsStore = (await storage.getStore('workflows'))!;
      const run = await workflowsStore.getWorkflowRunById({ runId, workflowName: 'agentic-loop' });
      const snapshot = run!.snapshot as WorkflowRunState;
      const [stepId, suspendedStep] = Object.entries(snapshot.context).find(
        ([, step]) => step?.status === 'suspended',
      )!;
      const unownedStepId = `${stepId}-unowned`;
      snapshot.context[unownedStepId] = structuredClone(suspendedStep) as any;
      const unownedPayload = snapshot.context[unownedStepId]!.suspendPayload as any;
      delete unownedPayload.__agentId;
      unownedPayload.requireToolApproval.toolCallId = 'unowned-call';
      snapshot.resumeLabels!['unowned-call'] = { stepId: unownedStepId } as any;
      await workflowsStore.persistWorkflowSnapshot({
        workflowName: 'agentic-loop',
        runId,
        resourceId: 'resource-a',
        snapshot,
      });

      expect((await agent.listSuspendedRuns({ resourceId: 'resource-a' })).total).toBe(0);
      await expect(
        agent.resumeStream(
          { approved: true },
          { runId, toolCallId: 'unowned-call', memory: { thread: 'thread-a', resource: 'resource-a' } },
        ),
      ).rejects.toMatchObject({ id: 'AGENT_RESUME_AGENT_MISMATCH' });
    }, 30000);

    it('rejects a persisted payload toolCallId that disagrees with its resume label', async () => {
      const { agent, storage } = createSuspendedSetup();
      const { runId } = await suspendRun(agent, 'thread-a', 'resource-a');
      const workflowsStore = (await storage.getStore('workflows'))!;
      const run = await workflowsStore.getWorkflowRunById({ runId, workflowName: 'agentic-loop' });
      const snapshot = run!.snapshot as WorkflowRunState;
      const suspendedStep = Object.values(snapshot.context).find(step => step?.status === 'suspended')!;
      (suspendedStep.suspendPayload as any).requireToolApproval.toolCallId = 'tampered-call';
      await workflowsStore.persistWorkflowSnapshot({
        workflowName: 'agentic-loop',
        runId,
        resourceId: 'resource-a',
        snapshot,
      });

      expect((await agent.listSuspendedRuns({ resourceId: 'resource-a' })).total).toBe(0);
      await expect(
        agent.resumeStream(
          { approved: true },
          { runId, toolCallId: 'tampered-call', memory: { thread: 'thread-a', resource: 'resource-a' } },
        ),
      ).rejects.toMatchObject({ id: 'AGENT_RESUME_LABEL_CONFLICT' });
    }, 30000);

    it('fails closed when the workflow row and embedded snapshot disagree on resource ownership', async () => {
      const { agent, storage } = createSuspendedSetup();
      const { runId } = await suspendRun(agent, 'thread-a', 'resource-a');
      const workflowsStore = (await storage.getStore('workflows'))!;
      const run = await workflowsStore.getWorkflowRunById({ runId, workflowName: 'agentic-loop' });
      await workflowsStore.persistWorkflowSnapshot({
        workflowName: 'agentic-loop',
        runId,
        resourceId: 'conflicting-resource',
        snapshot: run!.snapshot,
      });

      expect((await agent.listSuspendedRuns()).runs).toHaveLength(0);
      const options = {
        runId,
        memory: { thread: 'thread-a', resource: 'resource-a' },
      };
      await expect(agent.resumeStream({}, options)).rejects.toMatchObject({
        id: 'AGENT_RESUME_SNAPSHOT_OWNER_CONFLICT',
      });
      await expect(agent.resumeGenerate({}, options)).rejects.toMatchObject({
        id: 'AGENT_RESUME_SNAPSHOT_OWNER_CONFLICT',
      });
    }, 30000);

    it('rejects invalid pagination inputs', async () => {
      const { agent } = createSuspendedSetup();

      await expect(agent.listSuspendedRuns({ perPage: 0 })).rejects.toMatchObject({
        id: 'AGENT_LIST_SUSPENDED_RUNS_INVALID_PER_PAGE',
      });
      await expect(agent.listSuspendedRuns({ perPage: 1.5 })).rejects.toMatchObject({
        id: 'AGENT_LIST_SUSPENDED_RUNS_INVALID_PER_PAGE',
      });
      await expect(agent.listSuspendedRuns({ page: -1 })).rejects.toMatchObject({
        id: 'AGENT_LIST_SUSPENDED_RUNS_INVALID_PAGE',
      });
      await expect(agent.listSuspendedRuns({ page: 0.5 })).rejects.toMatchObject({
        id: 'AGENT_LIST_SUSPENDED_RUNS_INVALID_PAGE',
      });
    }, 30000);

    it('filters by fromDate and toDate', async () => {
      const { agent } = createSuspendedSetup();
      await suspendRun(agent, 'thread-1', 'resource-1');

      const past = new Date(Date.now() - 60_000);
      const future = new Date(Date.now() + 60_000);

      expect((await agent.listSuspendedRuns({ fromDate: past })).runs).toHaveLength(1);
      expect((await agent.listSuspendedRuns({ fromDate: future })).runs).toHaveLength(0);
      expect((await agent.listSuspendedRuns({ toDate: future })).runs).toHaveLength(1);
      expect((await agent.listSuspendedRuns({ toDate: past })).runs).toHaveLength(0);
    }, 30000);

    it('discovers suspend()-style suspensions with their suspend payload', async () => {
      const getUserTool = createTool({
        id: 'Get user tool',
        description: 'Returns a user, suspends to ask for the name',
        inputSchema: z.object({ name: z.string() }),
        suspendSchema: z.object({ message: z.string() }),
        resumeSchema: z.object({ name: z.string() }),
        execute: async (_input, context) => {
          if (!context?.agent?.resumeData) {
            return await context?.agent?.suspend({ message: 'Please provide the name of the user' });
          }
          return { name: context.agent.resumeData.name, email: 'dero@mail.com' };
        },
      });

      const agent = new Agent({
        id: 'suspending-agent',
        name: 'Suspending Agent',
        instructions: 'You find users.',
        model: createMockModel({ toolName: 'getUserTool' }),
        tools: { getUserTool },
      });
      new Mastra({ agents: { agent }, logger: false, storage: new InMemoryStore() });

      const stream = await agent.stream('Find the user', {
        memory: { thread: 'thread-1', resource: 'resource-1' },
      });
      for await (const _chunk of stream.fullStream) {
        // consume until the run suspends
      }

      const { runs } = await agent.listSuspendedRuns({ threadId: 'thread-1' });
      expect(runs).toHaveLength(1);
      expect(runs[0]!.runId).toBe(stream.runId);
      expect(runs[0]!.toolCalls).toEqual([
        expect.objectContaining({
          requiresApproval: false,
          suspendPayload: expect.objectContaining({ message: 'Please provide the name of the user' }),
        }),
      ]);
    }, 30000);

    it('reports the toolCallId for suspend()-parked tool calls', async () => {
      const askUserTool = createTool({
        id: 'Ask user tool',
        description: 'Asks the user for the name',
        inputSchema: z.object({ name: z.string() }),
        suspendSchema: z.object({ question: z.string() }),
        resumeSchema: z.object({ name: z.string() }),
        execute: async (_input, context) => {
          if (!context?.agent?.resumeData) {
            return await context?.agent?.suspend({ question: 'Which user?' });
          }
          return { name: context.agent.resumeData.name, email: 'dero@mail.com' };
        },
      });

      const agent = new Agent({
        id: 'suspending-agent',
        name: 'Suspending Agent',
        instructions: 'You find users.',
        model: createMockModel({ toolName: 'askUserTool' }),
        tools: { askUserTool },
      });
      new Mastra({ agents: { agent }, logger: false, storage: new InMemoryStore() });

      const stream = await agent.stream('Find the user', {
        memory: { thread: 'thread-1', resource: 'resource-1' },
      });
      let suspendedToolCallId = '';
      for await (const chunk of stream.fullStream) {
        if (chunk.type === 'tool-call-suspended') {
          suspendedToolCallId = chunk.payload.toolCallId;
        }
      }
      expect(suspendedToolCallId).toBeTruthy();

      // The id the stream chunk carries must survive into discovery, otherwise
      // sendToolApproval({ toolCallId }) can never match the run it names.
      const { runs } = await agent.listSuspendedRuns({ threadId: 'thread-1' });
      expect(runs).toHaveLength(1);
      expect(runs[0]!.toolCalls).toEqual([
        expect.objectContaining({
          toolCallId: suspendedToolCallId,
          toolName: 'askUserTool',
          requiresApproval: false,
        }),
      ]);
    }, 30000);

    it('recovers the toolCallId from resume labels for suspend()-parked snapshots persisted without one', async () => {
      const askUserTool = createTool({
        id: 'Ask user tool',
        description: 'Asks the user for the name',
        inputSchema: z.object({ name: z.string() }),
        suspendSchema: z.object({ question: z.string() }),
        resumeSchema: z.object({ name: z.string() }),
        execute: async (_input, context) => {
          if (!context?.agent?.resumeData) {
            return await context?.agent?.suspend({ question: 'Which user?' });
          }
          return { name: context.agent.resumeData.name, email: 'dero@mail.com' };
        },
      });

      const storage = new InMemoryStore();
      const agent = new Agent({
        id: 'suspending-agent',
        name: 'Suspending Agent',
        instructions: 'You find users.',
        model: createMockModel({ toolName: 'askUserTool' }),
        tools: { askUserTool },
      });
      new Mastra({ agents: { agent }, logger: false, storage });

      const stream = await agent.stream('Find the user', {
        memory: { thread: 'thread-1', resource: 'resource-1' },
      });
      let suspendedToolCallId = '';
      for await (const chunk of stream.fullStream) {
        if (chunk.type === 'tool-call-suspended') {
          suspendedToolCallId = chunk.payload.toolCallId;
        }
      }
      expect(suspendedToolCallId).toBeTruthy();

      // Simulate a snapshot persisted before the suspend payload carried
      // toolCallId: back then the id only survived as the workflow resume
      // label (resumeLabels[toolCallId] = { stepId }).
      const workflowsStore = (await storage.getStore('workflows'))!;
      const run = await workflowsStore.getWorkflowRunById({ runId: stream.runId, workflowName: 'agentic-loop' });
      expect(run).not.toBeNull();
      const snapshot = run!.snapshot as WorkflowRunState;
      for (const key in snapshot.context) {
        const step = snapshot.context[key];
        if (step?.status === 'suspended' && step.suspendPayload) {
          delete (step.suspendPayload as Record<string, unknown>).toolCallId;
        }
      }
      await workflowsStore.persistWorkflowSnapshot({
        workflowName: 'agentic-loop',
        runId: stream.runId,
        resourceId: 'resource-1',
        snapshot,
      });

      const { runs } = await agent.listSuspendedRuns({ threadId: 'thread-1' });
      expect(runs).toHaveLength(1);
      expect(runs[0]!.toolCalls[0]!.toolCallId).toBe(suspendedToolCallId);
    }, 30000);

    it('returns an empty list once the run is resumed and completes', async () => {
      const { agent } = createSuspendedSetup();
      const { runId, toolCallId } = await suspendRun(agent, 'thread-1', 'resource-1');

      const resumeStream = await agent.approveToolCall({
        runId,
        toolCallId,
        memory: { thread: 'thread-1', resource: 'resource-1' },
      });
      for await (const _chunk of resumeStream.fullStream) {
        // consume
      }

      expect((await agent.listSuspendedRuns()).runs).toHaveLength(0);
    }, 30000);

    it('drains parallel approval-requiring tool calls one suspension at a time', async () => {
      // The model requests two tool calls in the same turn. Because both
      // require approval, the loop runs them sequentially, so each call
      // suspends on its own — discovery always reports exactly the tool call
      // that is currently blocking (and therefore resumable), never a stale or
      // half-executed sibling.
      const agent = new Agent({
        id: 'user-agent',
        name: 'User Agent',
        instructions: 'You call tools.',
        model: createParallelToolCallsModel(),
        tools: { toolA: createApprovalTool('toolA'), toolB: createApprovalTool('toolB') },
      });
      new Mastra({ agents: { agent }, logger: false, storage: new InMemoryStore() });

      const stream = await agent.stream('do both', {
        requireToolApproval: true,
        memory: { thread: 'thread-par', resource: 'resource-par' },
      });
      for await (const _chunk of stream.fullStream) {
        // consume until the first suspension
      }

      // First suspension: only the first tool call is parked and discoverable.
      const first = await agent.listSuspendedRuns({ threadId: 'thread-par' });
      expect(first.runs).toHaveLength(1);
      expect(first.runs[0]!.runId).toBe(stream.runId);
      expect(first.runs[0]!.toolCalls).toEqual([
        { toolCallId: 'call-A', toolName: 'toolA', args: { name: 'A' }, requiresApproval: true },
      ]);

      // Approve the first call; the loop resumes and re-suspends on the second.
      const afterFirst = await agent.approveToolCall({
        runId: stream.runId,
        toolCallId: 'call-A',
        memory: { thread: 'thread-par', resource: 'resource-par' },
      });
      for await (const _chunk of afterFirst.fullStream) {
        // consume until the next suspension
      }

      const second = await agent.listSuspendedRuns({ threadId: 'thread-par' });
      expect(second.runs).toHaveLength(1);
      expect(second.runs[0]!.runId).toBe(stream.runId);
      expect(second.runs[0]!.toolCalls).toEqual([
        { toolCallId: 'call-B', toolName: 'toolB', args: { name: 'B' }, requiresApproval: true },
      ]);

      // Approve the second call; the run completes and is no longer discoverable.
      const afterSecond = await agent.approveToolCall({
        runId: stream.runId,
        toolCallId: 'call-B',
        memory: { thread: 'thread-par', resource: 'resource-par' },
      });
      for await (const _chunk of afterSecond.fullStream) {
        // consume to completion
      }

      expect((await agent.listSuspendedRuns({ threadId: 'thread-par' })).runs).toHaveLength(0);
    }, 30000);

    it('scopes nested supervisor/subagent suspensions by threadId to the resumable outer run', async () => {
      const subAgent = new Agent({
        id: 'billing-agent',
        name: 'Billing Agent',
        instructions: 'You handle billing.',
        model: createMockModel(),
        tools: { findUserTool: createFindUserTool() },
      });
      const supervisorModel = new MockLanguageModelV2({
        doStream: async () => ({
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: convertArrayToReadableStream([
            { type: 'stream-start', warnings: [] },
            { type: 'response-metadata', id: 'sup-0', modelId: 'mock-model-id', timestamp: new Date(0) },
            {
              type: 'tool-call',
              toolCallId: 'sup-call-1',
              toolName: 'agent-billing-agent',
              input: '{"message":"Find Dero Israel"}',
              providerExecuted: false,
            },
            { type: 'finish', finishReason: 'tool-calls', usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 } },
          ]),
        }),
      });
      const supervisor = new Agent({
        id: 'support-agent',
        name: 'Support Agent',
        instructions: 'You delegate to billing.',
        model: supervisorModel,
        agents: { 'billing-agent': subAgent },
      });
      new Mastra({ agents: { supervisor, subAgent }, logger: false, storage: new InMemoryStore() });

      const stream = await supervisor.stream('Find the user Dero Israel via billing', {
        memory: { thread: 'thread-1', resource: 'resource-1' },
      });
      for await (const _chunk of stream.fullStream) {
        // consume until suspension
      }

      // Both the supervisor's outer run and the subagent's inner run persist
      // suspended agentic-loop rows, but snapshots carry the owning agent's id:
      // the supervisor only sees its own resumable outer run...
      const allRuns = await supervisor.listSuspendedRuns();
      expect(allRuns.runs.map(run => run.runId)).toEqual([stream.runId]);

      // ...while the subagent sees its inner run.
      const subAgentRuns = await subAgent.listSuspendedRuns();
      expect(subAgentRuns.runs).toHaveLength(1);
      expect(subAgentRuns.runs[0]!.runId).not.toBe(stream.runId);

      const scoped = await supervisor.listSuspendedRuns({ threadId: 'thread-1', resourceId: 'resource-1' });
      expect(scoped.runs.map(run => run.runId)).toEqual([stream.runId]);
      expect(scoped.runs[0]!.toolCalls).toEqual([
        {
          toolCallId: 'sup-call-1',
          toolName: 'agent-billing-agent',
          args: { message: 'Find Dero Israel' },
          requiresApproval: true,
        },
      ]);
    }, 30000);

    it('scopes deep (3-level) supervisor → mid → leaf suspensions to each agent owner', async () => {
      // leaf has the real requireApproval tool; mid delegates to leaf;
      // supervisor delegates to mid. A suspension at the leaf bubbles up,
      // re-suspending mid's and the supervisor's delegating tool calls in turn,
      // producing three chained suspended agentic-loop rows.
      const leaf = new Agent({
        id: 'leaf-agent',
        name: 'Leaf Agent',
        instructions: 'You find users.',
        model: createMockModel(),
        tools: { findUserTool: createFindUserTool() },
      });
      const mid = new Agent({
        id: 'mid-agent',
        name: 'Mid Agent',
        instructions: 'You delegate to leaf.',
        model: createDelegationModel({ toolName: 'agent-leaf-agent', toolCallId: 'mid-call-1' }),
        agents: { 'leaf-agent': leaf },
      });
      const supervisor = new Agent({
        id: 'supervisor-agent',
        name: 'Supervisor Agent',
        instructions: 'You delegate to mid.',
        model: createDelegationModel({ toolName: 'agent-mid-agent', toolCallId: 'sup-call-1' }),
        agents: { 'mid-agent': mid },
      });
      new Mastra({ agents: { supervisor, mid, leaf }, logger: false, storage: new InMemoryStore() });

      const stream = await supervisor.stream('Find the user Dero Israel', {
        memory: { thread: 'thread-1', resource: 'resource-1' },
      });
      for await (const _chunk of stream.fullStream) {
        // consume until suspension
      }

      // Each agent in the chain sees only its own suspended run.
      const supervisorRuns = await supervisor.listSuspendedRuns();
      expect(supervisorRuns.runs.map(run => run.runId)).toEqual([stream.runId]);
      // The supervisor's resumable run shows the delegation call to mid, not the
      // real approval tool (which lives on the leaf's run).
      expect(supervisorRuns.runs[0]!.toolCalls).toEqual([
        {
          toolCallId: 'sup-call-1',
          toolName: 'agent-mid-agent',
          args: { message: 'Find Dero Israel' },
          requiresApproval: true,
        },
      ]);

      const midRuns = await mid.listSuspendedRuns();
      expect(midRuns.runs).toHaveLength(1);
      expect(midRuns.runs[0]!.runId).not.toBe(stream.runId);
      expect(midRuns.runs[0]!.toolCalls[0]!.toolName).toBe('agent-leaf-agent');

      const leafRuns = await leaf.listSuspendedRuns();
      expect(leafRuns.runs).toHaveLength(1);
      // Only the innermost (leaf) run surfaces the actual approval tool + args.
      expect(leafRuns.runs[0]!.toolCalls).toEqual([
        {
          toolCallId: expect.any(String),
          toolName: 'findUserTool',
          args: { name: 'Dero Israel' },
          requiresApproval: true,
        },
      ]);

      // All three runs are distinct rows.
      const allRunIds = [supervisorRuns.runs[0]!.runId, midRuns.runs[0]!.runId, leafRuns.runs[0]!.runId];
      expect(new Set(allRunIds).size).toBe(3);
    }, 30000);

    it('fails explicitly for a standalone agent without workflow storage', async () => {
      const agent = new Agent({
        id: 'no-storage-agent',
        name: 'No Storage Agent',
        instructions: 'You find users.',
        model: createMockModel(),
        tools: { findUserTool: createFindUserTool() },
      });

      await expect(agent.listSuspendedRuns()).rejects.toMatchObject({
        id: 'AGENT_LIST_SUSPENDED_RUNS_NO_STORAGE',
      });
    }, 30000);

    it('validates a required request context before scanning storage', async () => {
      const storage = new InMemoryStore();
      const { agent } = createSuspendedSetup({
        storage,
        requestContextSchema: z.object({ principal: z.string() }),
      });
      const workflows = (await storage.getStore('workflows'))!;
      const listWorkflowRuns = vi.spyOn(workflows, 'listWorkflowRuns');

      await expect(agent.listSuspendedRuns()).rejects.toMatchObject({
        id: 'AGENT_REQUEST_CONTEXT_VALIDATION_FAILED',
      });
      expect(listWorkflowRuns).not.toHaveBeenCalled();
    });

    it.each(['', '   '])('rejects an empty FGA resource scope before scanning storage (%j)', async resourceId => {
      const storage = new InMemoryStore();
      const fgaProvider = createFGAProvider();
      const { agent } = createSuspendedSetup({ storage, fgaProvider });
      const workflows = (await storage.getStore('workflows'))!;
      const listWorkflowRuns = vi.spyOn(workflows, 'listWorkflowRuns');
      const requestContext = new RequestContext([
        ['user', { id: 'user-1' }],
        [MASTRA_RESOURCE_ID_KEY, resourceId],
      ]);

      await expect(agent.listSuspendedRuns({ requestContext })).rejects.toMatchObject({
        id: 'AGENT_LIST_SUSPENDED_RUNS_OWNER_UNVERIFIED',
      });
      expect(fgaProvider.require).not.toHaveBeenCalled();
      expect(listWorkflowRuns).not.toHaveBeenCalled();
    });
  });

  describe('agent resume fail-closed boundaries', () => {
    it('rejects terminal snapshots even when suspended-looking context remains', async () => {
      const { agent, storage } = createSuspendedSetup();
      const { runId } = await suspendRun(agent, 'thread-terminal', 'resource-terminal');
      const workflows = (await storage.getStore('workflows'))!;
      const run = await workflows.getWorkflowRunById({ workflowName: 'agentic-loop', runId });
      const snapshot = run!.snapshot as WorkflowRunState;
      snapshot.status = 'success';
      await workflows.persistWorkflowSnapshot({
        workflowName: 'agentic-loop',
        runId,
        resourceId: 'resource-terminal',
        snapshot,
      });
      const options = {
        runId,
        memory: { thread: 'thread-terminal', resource: 'resource-terminal' },
      };

      await expect(agent.resumeStream({ approved: true }, options)).rejects.toMatchObject({
        id: 'AGENT_RESUME_RUN_NOT_SUSPENDED',
      });
      await expect(agent.resumeGenerate({ approved: true }, options)).rejects.toMatchObject({
        id: 'AGENT_RESUME_RUN_NOT_SUSPENDED',
      });
      expect(mockFindUser).not.toHaveBeenCalled();
    });

    it('snapshots explicit resume identity before awaiting workflow storage', async () => {
      const storage = new InMemoryStore();
      const suspended = await suspendRun(createSuspendedSetup({ storage }).agent, 'thread-owner-b', 'resource-owner-b');
      const fgaProvider = createFGAProvider();
      const restarted = createSuspendedSetup({ storage, toolCallOnFirstCall: false, fgaProvider });
      const workflows = (await storage.getStore('workflows'))!;
      const getWorkflowRunById = workflows.getWorkflowRunById.bind(workflows);
      let releaseStorage!: () => void;
      const storageGate = new Promise<void>(resolve => {
        releaseStorage = resolve;
      });
      vi.spyOn(workflows, 'getWorkflowRunById').mockImplementationOnce(async args => {
        await storageGate;
        return getWorkflowRunById(args);
      });
      const requestContext = new RequestContext([
        ['user', { id: 'user-a' }],
        [MASTRA_RESOURCE_ID_KEY, 'resource-owner-a'],
        [MASTRA_THREAD_ID_KEY, 'thread-owner-a'],
      ]);
      const memory = { thread: 'thread-owner-a', resource: 'resource-owner-a' };
      const options = {
        runId: suspended.runId,
        toolCallId: suspended.toolCallId,
        get requestContext() {
          return requestContext;
        },
        get memory() {
          return memory;
        },
      };

      const resuming = restarted.agent.resumeStream({ approved: true }, options);
      const rejection = expect(resuming).rejects.toMatchObject({ id: 'AGENT_RESUME_OWNER_MISMATCH' });
      await vi.waitFor(() => expect(fgaProvider.require).toHaveBeenCalledTimes(1));
      requestContext.set('user', { id: 'user-b' });
      requestContext.set(MASTRA_RESOURCE_ID_KEY, 'resource-owner-b');
      requestContext.set(MASTRA_THREAD_ID_KEY, 'thread-owner-b');
      memory.resource = 'resource-owner-b';
      memory.thread = 'thread-owner-b';
      releaseStorage();

      await rejection;
      expect(fgaProvider.require).toHaveBeenCalledTimes(1);
      expect(mockFindUser).not.toHaveBeenCalled();
    });

    it('snapshots a class-instance resume options bag before awaiting workflow storage', async () => {
      const storage = new InMemoryStore();
      const suspended = await suspendRun(createSuspendedSetup({ storage }).agent, 'thread-owner-b', 'resource-owner-b');
      const fgaProvider = createFGAProvider();
      const restarted = createSuspendedSetup({ storage, toolCallOnFirstCall: false, fgaProvider });
      const workflows = (await storage.getStore('workflows'))!;
      const getWorkflowRunById = workflows.getWorkflowRunById.bind(workflows);
      let releaseStorage!: () => void;
      const storageGate = new Promise<void>(resolve => {
        releaseStorage = resolve;
      });
      vi.spyOn(workflows, 'getWorkflowRunById').mockImplementationOnce(async args => {
        await storageGate;
        return getWorkflowRunById(args);
      });
      const requestContext = new RequestContext([
        ['user', { id: 'user-a' }],
        [MASTRA_RESOURCE_ID_KEY, 'resource-owner-a'],
        [MASTRA_THREAD_ID_KEY, 'thread-owner-a'],
      ]);
      const memory = { thread: 'thread-owner-a', resource: 'resource-owner-a' };
      class ResumeOptions {
        constructor(
          readonly runId: string,
          readonly toolCallId: string,
        ) {}

        get requestContext() {
          return requestContext;
        }

        get memory() {
          return memory;
        }
      }
      const options = new ResumeOptions(suspended.runId, suspended.toolCallId);

      const resuming = restarted.agent.resumeStream({ approved: true }, options);
      const rejection = expect(resuming).rejects.toMatchObject({ id: 'AGENT_RESUME_OWNER_MISMATCH' });
      await vi.waitFor(() => expect(fgaProvider.require).toHaveBeenCalledTimes(1));
      requestContext.set('user', { id: 'user-b' });
      requestContext.set(MASTRA_RESOURCE_ID_KEY, 'resource-owner-b');
      requestContext.set(MASTRA_THREAD_ID_KEY, 'thread-owner-b');
      memory.resource = 'resource-owner-b';
      memory.thread = 'thread-owner-b';
      releaseStorage();

      await rejection;
      expect(fgaProvider.require).toHaveBeenCalledTimes(1);
      expect(mockFindUser).not.toHaveBeenCalled();
    });

    it('reauthorizes the exact resume owner immediately before stream execution', async () => {
      const storage = new InMemoryStore();
      const suspended = await suspendRun(createSuspendedSetup({ storage }).agent, 'thread-revoked', 'resource-revoked');
      const fgaProvider = createFGAProvider();
      const revoked = new Error('resume authorization revoked');
      vi.mocked(fgaProvider.require).mockResolvedValueOnce(undefined).mockRejectedValueOnce(revoked);
      const restarted = createSuspendedSetup({ storage, toolCallOnFirstCall: false, fgaProvider });
      const requestContext = new RequestContext([
        ['user', { id: 'user-1' }],
        [MASTRA_RESOURCE_ID_KEY, 'resource-revoked'],
        [MASTRA_THREAD_ID_KEY, 'thread-revoked'],
      ]);

      await expect(
        restarted.agent.resumeStream(
          { approved: true },
          {
            runId: suspended.runId,
            toolCallId: suspended.toolCallId,
            requestContext,
            memory: { thread: 'thread-revoked', resource: 'resource-revoked' },
          },
        ),
      ).rejects.toBe(revoked);
      expect(fgaProvider.require).toHaveBeenCalledTimes(2);
      expect(mockFindUser).not.toHaveBeenCalled();
    });

    it('reauthorizes after waiting for another agent to release the resume thread', async () => {
      const storage = new InMemoryStore();
      const suspended = await suspendRun(createSuspendedSetup({ storage }).agent, 'thread-wait', 'resource-wait');
      const fgaProvider = createFGAProvider();
      const revoked = new Error('resume authorization revoked while waiting');
      vi.mocked(fgaProvider.require).mockResolvedValueOnce(undefined).mockRejectedValueOnce(revoked);
      const restarted = createSuspendedSetup({ storage, toolCallOnFirstCall: false, fgaProvider });
      const blocker = new Agent({
        id: 'resume-thread-blocker',
        name: 'Resume thread blocker',
        instructions: 'Hold the thread while the suspended run waits.',
        model: createMockModel({ toolCallOnFirstCall: false }),
      });
      let releaseBlocker!: () => void;
      const blockerFinished = new Promise<void>(resolve => {
        releaseBlocker = resolve;
      });
      const blockerRunId = `resume-thread-blocker-run-${evented ? 'evented' : 'default'}`;
      const blockerCompletion = agentThreadStreamRuntime.registerRun(
        blocker as any,
        {
          runId: blockerRunId,
          status: 'running',
          fullStream: (async function* () {})(),
          _waitUntilFinished: () => blockerFinished,
        } as any,
        {
          runId: blockerRunId,
          memory: { thread: 'thread-wait', resource: 'resource-wait' },
        } as any,
        restarted.agent.getPubSub(),
      );
      const requestContext = new RequestContext([
        ['user', { id: 'user-1' }],
        [MASTRA_RESOURCE_ID_KEY, 'resource-wait'],
        [MASTRA_THREAD_ID_KEY, 'thread-wait'],
      ]);

      const resuming = restarted.agent.resumeStream(
        { approved: true },
        {
          runId: suspended.runId,
          toolCallId: suspended.toolCallId,
          requestContext,
          memory: { thread: 'thread-wait', resource: 'resource-wait' },
        },
      );
      const rejectedWith = resuming.then(
        () => new Error('Expected the parked resume to be rejected'),
        error => error,
      );
      let blockerReleased = false;
      try {
        await vi.waitFor(() => expect(fgaProvider.require).toHaveBeenCalledTimes(1));
        await new Promise(resolve => setTimeout(resolve, 0));
        expect(fgaProvider.require).toHaveBeenCalledTimes(1);
        expect(mockFindUser).not.toHaveBeenCalled();

        releaseBlocker();
        blockerReleased = true;
        await blockerCompletion;
        expect(await rejectedWith).toBe(revoked);
        expect(fgaProvider.require).toHaveBeenCalledTimes(2);
        expect(mockFindUser).not.toHaveBeenCalled();
      } finally {
        if (!blockerReleased) releaseBlocker();
        await blockerCompletion;
      }
    });

    it('reauthorizes the exact resume owner immediately before generate execution', async () => {
      const storage = new InMemoryStore();
      const suspended = await suspendRun(
        createSuspendedSetup({ storage }).agent,
        'thread-generate',
        'resource-generate',
      );
      const fgaProvider = createFGAProvider();
      const revoked = new Error('resume authorization revoked');
      vi.mocked(fgaProvider.require).mockResolvedValueOnce(undefined).mockRejectedValueOnce(revoked);
      const restarted = createSuspendedSetup({ storage, toolCallOnFirstCall: false, fgaProvider });
      const requestContext = new RequestContext([
        ['user', { id: 'user-1' }],
        [MASTRA_RESOURCE_ID_KEY, 'resource-generate'],
        [MASTRA_THREAD_ID_KEY, 'thread-generate'],
      ]);

      await expect(
        restarted.agent.resumeGenerate(
          { approved: true },
          {
            runId: suspended.runId,
            toolCallId: suspended.toolCallId,
            requestContext,
            memory: { thread: 'thread-generate', resource: 'resource-generate' },
          },
        ),
      ).rejects.toBe(revoked);
      expect(fgaProvider.require).toHaveBeenCalledTimes(2);
      expect(mockFindUser).not.toHaveBeenCalled();
    });
  });

  describe('agent.sendToolApproval() storage fallback', () => {
    it('passes authenticated request context through cold FGA discovery and resume', async () => {
      const storage = new InMemoryStore();
      const suspended = await suspendRun(createSuspendedSetup({ storage }).agent, 'thread-1', 'resource-1');
      const fgaProvider = createFGAProvider();
      const restarted = createSuspendedSetup({ storage, toolCallOnFirstCall: false, fgaProvider });
      const requestContext = new RequestContext([
        ['user', { id: 'user-1' }],
        [MASTRA_RESOURCE_ID_KEY, 'resource-1'],
        [MASTRA_THREAD_ID_KEY, 'thread-1'],
      ]);

      await expect(
        restarted.agent.sendToolApproval({
          threadId: 'thread-1',
          resourceId: 'resource-1',
          toolCallId: suspended.toolCallId,
          approved: false,
          requestContext,
        }),
      ).resolves.toEqual({ accepted: true, runId: suspended.runId, toolCallId: suspended.toolCallId });
      expect(fgaProvider.require).toHaveBeenCalled();
    });

    it('rejects conflicting message and resume-data continuation modes', async () => {
      const { agent } = createSuspendedSetup();
      await expect(
        agent.sendToolApproval({
          threadId: 'thread-1',
          resourceId: 'resource-1',
          toolCallId: 'call-1',
          approved: true,
          messages: [{ role: 'user', content: 'continue' }],
          resumeData: { name: 'Dero Israel' },
        }),
      ).rejects.toMatchObject({ id: 'AGENT_SEND_TOOL_APPROVAL_CONFLICTING_CONTINUATION_INPUT' });
    });

    it('approves a suspended run after a simulated restart (in-memory state lost)', async () => {
      const storage = new InMemoryStore();
      const { agent } = createSuspendedSetup({ storage });
      const { runId, toolCallId } = await suspendRun(agent, 'thread-1', 'resource-1');

      // Simulate a server restart: a fresh Agent + Mastra process sharing the
      // same storage. The in-memory thread-run map is empty, but the suspended
      // snapshot is still in storage.
      const { agent: restartedAgent } = createSuspendedSetup({ storage, toolCallOnFirstCall: false });
      expect(restartedAgent.getActiveThreadRunId({ threadId: 'thread-1', resourceId: 'resource-1' })).toBeUndefined();

      const result = await restartedAgent.sendToolApproval({
        threadId: 'thread-1',
        resourceId: 'resource-1',
        toolCallId,
        approved: true,
      });
      expect(result).toEqual({ accepted: true, runId, toolCallId });

      // The resumed run executes the approved tool and runs to completion,
      // leaving no suspended rows behind.
      await vi.waitFor(
        async () => {
          expect(mockFindUser).toHaveBeenCalledWith(expect.objectContaining({ name: 'Dero Israel' }));
          expect((await restartedAgent.listSuspendedRuns({ resourceId: 'resource-1' })).runs).toHaveLength(0);
        },
        { timeout: 10000 },
      );
    }, 30000);

    it('matches a suspend()-parked run by toolCallId after a simulated restart', async () => {
      const resumedTool = vi.fn();
      const makeAskUserAgent = (toolCallOnFirstCall: boolean) => {
        const askUserTool = createTool({
          id: 'Ask user tool',
          description: 'Asks the user for the name',
          inputSchema: z.object({ name: z.string() }),
          suspendSchema: z.object({ question: z.string() }),
          resumeSchema: z.object({ name: z.string() }),
          execute: async (_input, context) => {
            if (!context?.agent?.resumeData) {
              return await context?.agent?.suspend({ question: 'Which user?' });
            }
            resumedTool(context.agent.resumeData);
            return { name: context.agent.resumeData.name, email: 'dero@mail.com' };
          },
        });
        const agent = new Agent({
          id: 'suspending-agent',
          name: 'Suspending Agent',
          instructions: 'You find users.',
          model: createMockModel({ toolName: 'askUserTool', toolCallOnFirstCall }),
          tools: { askUserTool },
        });
        const mastra = new Mastra({ agents: { agent }, logger: false, storage });
        return { agent, mastra };
      };

      const storage = new InMemoryStore();
      const { agent } = makeAskUserAgent(true);
      const stream = await agent.stream('Find the user', {
        memory: { thread: 'thread-1', resource: 'resource-1' },
      });
      let suspendedToolCallId = '';
      for await (const chunk of stream.fullStream) {
        if (chunk.type === 'tool-call-suspended') {
          suspendedToolCallId = chunk.payload.toolCallId;
        }
      }
      expect(suspendedToolCallId).toBeTruthy();

      // Fresh process: the run must be resolved from storage, and the id taken
      // from the tool-call-suspended chunk has to match the discovered run.
      const { agent: restartedAgent } = makeAskUserAgent(false);
      expect(restartedAgent.getActiveThreadRunId({ threadId: 'thread-1', resourceId: 'resource-1' })).toBeUndefined();

      const result = await restartedAgent.sendToolApproval({
        threadId: 'thread-1',
        resourceId: 'resource-1',
        toolCallId: suspendedToolCallId,
        approved: true,
        resumeData: { name: 'Dero Israel' },
      });
      expect(result).toEqual({ accepted: true, runId: stream.runId, toolCallId: suspendedToolCallId });

      await vi.waitFor(
        async () => {
          expect(resumedTool).toHaveBeenCalledWith(expect.objectContaining({ name: 'Dero Israel' }));
          expect((await restartedAgent.listSuspendedRuns({ resourceId: 'resource-1' })).runs).toHaveLength(0);
        },
        { timeout: 10000 },
      );
    }, 30000);

    it('declines a suspended run after a simulated restart', async () => {
      const storage = new InMemoryStore();
      const { agent } = createSuspendedSetup({ storage });
      const { runId } = await suspendRun(agent, 'thread-1', 'resource-1');

      const { agent: restartedAgent } = createSuspendedSetup({ storage, toolCallOnFirstCall: false });

      const result = await restartedAgent.sendToolApproval({
        threadId: 'thread-1',
        resourceId: 'resource-1',
        approved: false,
      });
      expect(result.runId).toBe(runId);

      await vi.waitFor(
        async () => {
          expect((await restartedAgent.listSuspendedRuns({ resourceId: 'resource-1' })).runs).toHaveLength(0);
        },
        { timeout: 10000 },
      );
      expect(mockFindUser).not.toHaveBeenCalled();
    }, 30000);

    it('throws on ambiguous suspended runs and disambiguates by toolCallId', async () => {
      const storage = new InMemoryStore();
      await suspendRun(createSuspendedSetup({ storage, toolCallId: 'call-a' }).agent, 'thread-1', 'resource-1');
      const second = await suspendRun(
        createSuspendedSetup({ storage, toolCallId: 'call-b' }).agent,
        'thread-1',
        'resource-1',
      );

      // Fresh process: two suspended runs match the thread and no toolCallId
      // is provided, so the fallback cannot pick one.
      const { agent: restartedAgent } = createSuspendedSetup({ storage, toolCallOnFirstCall: false });
      await expect(
        restartedAgent.sendToolApproval({
          threadId: 'thread-1',
          resourceId: 'resource-1',
          approved: true,
        }),
      ).rejects.toMatchObject({
        id: 'AGENT_SEND_TOOL_APPROVAL_AMBIGUOUS_SUSPENDED_CALLS',
      });

      // Passing the toolCallId narrows the match to a single run.
      const result = await restartedAgent.sendToolApproval({
        threadId: 'thread-1',
        resourceId: 'resource-1',
        toolCallId: second.toolCallId,
        approved: true,
      });
      expect(result).toEqual({ accepted: true, runId: second.runId, toolCallId: second.toolCallId });
    }, 30000);

    it('rejects an exact cold call while another run owns the same thread', async () => {
      const storage = new InMemoryStore();
      const cold = await suspendRun(
        createSuspendedSetup({ storage, toolCallId: 'cold-call' }).agent,
        'thread-1',
        'resource-1',
      );
      const restarted = createSuspendedSetup({ storage, toolCallId: 'warm-call' });
      const warm = await suspendRun(restarted.agent, 'thread-1', 'resource-1');
      expect(restarted.agent.getActiveThreadRunId({ threadId: 'thread-1', resourceId: 'resource-1' })).toBe(warm.runId);

      await expect(
        restarted.agent.sendToolApproval({
          threadId: 'thread-1',
          resourceId: 'resource-1',
          toolCallId: cold.toolCallId,
          approved: false,
        }),
      ).rejects.toMatchObject({ id: 'AGENT_SEND_TOOL_APPROVAL_ACTIVE_RUN_CONFLICT' });
    }, 30000);

    it('rejects duplicate exact ids across warm and cold runs', async () => {
      const storage = new InMemoryStore();
      await suspendRun(createSuspendedSetup({ storage, toolCallId: 'duplicate-call' }).agent, 'thread-1', 'resource-1');
      const restarted = createSuspendedSetup({ storage, toolCallId: 'duplicate-call' });
      await suspendRun(restarted.agent, 'thread-1', 'resource-1');

      await expect(
        restarted.agent.sendToolApproval({
          threadId: 'thread-1',
          resourceId: 'resource-1',
          toolCallId: 'duplicate-call',
          approved: true,
        }),
      ).rejects.toMatchObject({ id: 'AGENT_SEND_TOOL_APPROVAL_AMBIGUOUS_SUSPENDED_CALLS' });
    }, 30000);

    it('throws when no active or suspended run exists for the thread', async () => {
      const { agent } = createSuspendedSetup();

      await expect(
        agent.sendToolApproval({
          threadId: 'thread-without-run',
          resourceId: 'resource-1',
          approved: true,
        }),
      ).rejects.toMatchObject({
        id: 'AGENT_SEND_TOOL_APPROVAL_NO_ACTIVE_THREAD_RUN',
      });
    }, 30000);

    it('surfaces storage failures instead of reporting "no suspended run"', async () => {
      const storage = new InMemoryStore();
      const { agent } = createSuspendedSetup({ storage });

      const workflowsStore = (await storage.getStore('workflows'))!;
      vi.spyOn(workflowsStore, 'listWorkflowRuns').mockRejectedValue(new Error('storage outage'));

      await expect(
        agent.sendToolApproval({
          threadId: 'thread-1',
          resourceId: 'resource-1',
          approved: true,
        }),
      ).rejects.toThrow('storage outage');
    }, 30000);
  });
});
