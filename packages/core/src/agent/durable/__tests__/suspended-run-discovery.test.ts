import type { LanguageModelV2 } from '@ai-sdk/provider-v5';
import { MockLanguageModelV2, convertArrayToReadableStream } from '@internal/ai-sdk-v5/test';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import type { IFGAProvider } from '../../../auth/ee/interfaces/fga';
import { EventEmitterPubSub } from '../../../events/event-emitter';
import { Mastra } from '../../../mastra';
import {
  MASTRA_RESOURCE_ID_KEY,
  MASTRA_THREAD_ID_KEY,
  MASTRA_VERSIONS_KEY,
  RequestContext,
} from '../../../request-context';
import { InMemoryStore } from '../../../storage';
import { createTool } from '../../../tools';
import type { WorkflowRunState } from '../../../workflows/types';
import { Agent } from '../../agent';
import { createDurableAgent } from '../create-durable-agent';
import {
  getGlobalRunRegistryEntry,
  globalRunRegistry,
  pinGlobalRunRegistryEntry,
  unpinGlobalRunRegistryEntry,
} from '../run-registry';
import { resolveRuntimeDependencies } from '../utils/resolve-runtime';
import { serializeToolsMetadata } from '../utils/serialize-state';

const openPubsubs: EventEmitterPubSub[] = [];
let durableRunSequence = 0;

afterEach(async () => {
  vi.unstubAllEnvs();
  await Promise.all(openPubsubs.splice(0).map(pubsub => pubsub.close()));
});

function toolCallModel(toolCallId = 'durable-call-1') {
  return new MockLanguageModelV2({
    doStream: async () => ({
      rawCall: { rawPrompt: null, rawSettings: {} },
      warnings: [],
      stream: convertArrayToReadableStream([
        { type: 'stream-start', warnings: [] },
        { type: 'response-metadata', id: 'tool-turn', modelId: 'mock-model', timestamp: new Date(0) },
        {
          type: 'tool-call',
          toolCallType: 'function',
          toolCallId,
          toolName: 'protectedTool',
          input: JSON.stringify({ value: 'persisted' }),
          providerExecuted: false,
        },
        {
          type: 'finish',
          finishReason: 'tool-calls',
          usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
        },
      ]),
    }),
  });
}

function textModel() {
  return new MockLanguageModelV2({
    doStream: async () => ({
      rawCall: { rawPrompt: null, rawSettings: {} },
      warnings: [],
      stream: convertArrayToReadableStream([
        { type: 'stream-start', warnings: [] },
        { type: 'response-metadata', id: 'text-turn', modelId: 'mock-model', timestamp: new Date(0) },
        { type: 'text-start', id: 'text-1' },
        { type: 'text-delta', id: 'text-1', delta: 'done' },
        { type: 'text-end', id: 'text-1' },
        {
          type: 'finish',
          finishReason: 'stop',
          usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
        },
      ]),
    }),
  });
}

function twoApprovalCallsModel() {
  let call = 0;
  return new MockLanguageModelV2({
    doStream: async () => {
      call++;
      if (call <= 2) {
        return {
          rawCall: { rawPrompt: null, rawSettings: {} },
          warnings: [],
          stream: convertArrayToReadableStream([
            { type: 'stream-start', warnings: [] },
            { type: 'response-metadata', id: `tool-turn-${call}`, modelId: 'mock-model', timestamp: new Date(0) },
            {
              type: 'tool-call',
              toolCallType: 'function',
              toolCallId: `durable-call-${call}`,
              toolName: 'protectedTool',
              input: JSON.stringify({ value: `value-${call}` }),
              providerExecuted: false,
            },
            {
              type: 'finish',
              finishReason: 'tool-calls',
              usage: { inputTokens: 1, outputTokens: 1, totalTokens: 2 },
            },
          ]),
        };
      }
      return textModel().doStream({} as any);
    },
  });
}

function createSetup({
  storage,
  model,
  agentId = 'durable-discovery-agent',
  durableAgentId,
  durableAgentName,
  execute = vi.fn().mockResolvedValue({ ok: true }),
  includeTool = true,
  fgaProvider,
  requestContextSchema,
  inputProcessors,
  toolOptions,
}: {
  storage: InMemoryStore;
  model: MockLanguageModelV2;
  agentId?: string;
  durableAgentId?: string;
  durableAgentName?: string;
  execute?: any;
  includeTool?: boolean;
  fgaProvider?: IFGAProvider;
  requestContextSchema?: any;
  inputProcessors?: any[];
  toolOptions?: Record<string, any>;
}) {
  const tool = createTool({
    id: 'protectedTool',
    description: 'Requires an explicit approval',
    inputSchema: z.object({ value: z.string() }),
    requireApproval: true,
    ...toolOptions,
    execute,
  });
  const baseAgent = new Agent({
    id: agentId,
    name: agentId,
    instructions: 'Use the protected tool.',
    model: model as LanguageModelV2,
    tools: includeTool ? { protectedTool: tool } : {},
    requestContextSchema,
    inputProcessors,
  });
  const pubsub = new EventEmitterPubSub();
  openPubsubs.push(pubsub);
  const agent = createDurableAgent({
    agent: baseAgent,
    id: durableAgentId,
    name: durableAgentName,
    pubsub,
    cleanupTimeoutMs: 0,
  });
  new Mastra({
    logger: false,
    storage,
    agents: { [agent.id]: agent as any },
    server: fgaProvider ? ({ fga: fgaProvider } as any) : undefined,
  });
  return { agent, baseAgent, execute };
}

function createFGAProvider(): IFGAProvider {
  return {
    check: vi.fn().mockResolvedValue(true),
    require: vi.fn().mockResolvedValue(undefined),
    filterAccessible: vi.fn(),
  };
}

async function persistSuspendedRun(storage: InMemoryStore, toolCallId = 'durable-call-1', versions?: any) {
  const first = createSetup({ storage, model: toolCallModel(toolCallId) });
  const result = await first.agent.stream('run it', {
    runId: `run-${toolCallId}-${++durableRunSequence}`,
    requireToolApproval: true,
    memory: { thread: 'thread-1', resource: 'resource-1' },
    versions,
  });
  await vi.waitFor(async () => {
    expect((await first.agent.listSuspendedRuns({ resourceId: 'resource-1' })).runs).toHaveLength(1);
  });
  result.cleanup();
  return { runId: result.runId, toolCallId };
}

describe('DurableAgent suspended-run discovery', () => {
  it('keeps a caller-provided runId during prepare for explicit rehydration', async () => {
    const setup = createSetup({ storage: new InMemoryStore(), model: textModel() });
    await expect(setup.agent.prepare('prepare', { runId: 'fixed-durable-run' })).resolves.toMatchObject({
      runId: 'fixed-durable-run',
    });
  });

  it('uses the public durable-agent identity for prepared and recovered runs', async () => {
    const storage = new InMemoryStore();
    const initial = createSetup({
      storage,
      model: textModel(),
      agentId: 'wrapped-agent-id',
      durableAgentId: 'public-durable-id',
      durableAgentName: 'Public durable name',
    });
    const prepared = await initial.agent.prepare('prepared', { runId: 'overridden-id-prepared-run' });
    expect(prepared.workflowInput).toMatchObject({
      agentId: 'public-durable-id',
      agentName: 'Public durable name',
    });
    expect(getGlobalRunRegistryEntry(prepared.runId)?.agentId).toBe('public-durable-id');
    const preparedStream = await initial.agent.stream('prepared', { runId: prepared.runId });
    await preparedStream.output.consumeStream();
    preparedStream.cleanup();

    const suspending = createSetup({
      storage,
      model: toolCallModel('overridden-id-call'),
      agentId: 'wrapped-agent-id',
      durableAgentId: 'public-durable-id',
      durableAgentName: 'Public durable name',
    });
    const suspended = await suspending.agent.stream('suspend', {
      runId: 'overridden-id-suspended-run',
      requireToolApproval: true,
      memory: { thread: 'thread-1', resource: 'resource-1' },
    });
    await vi.waitFor(async () => {
      expect((await suspending.agent.listSuspendedRuns({ resourceId: 'resource-1' })).total).toBe(1);
    });
    suspended.cleanup();

    const execute = vi.fn().mockResolvedValue({ ok: true });
    const restarted = createSetup({
      storage,
      model: textModel(),
      agentId: 'wrapped-agent-id',
      durableAgentId: 'public-durable-id',
      durableAgentName: 'Public durable name',
      execute,
    });
    await expect(restarted.agent.listSuspendedRuns({ resourceId: 'resource-1' })).resolves.toMatchObject({
      total: 1,
      runs: [{ runId: suspended.runId }],
    });
    const resumed = await restarted.agent.resume(
      suspended.runId,
      { approved: true },
      {
        toolCallId: 'overridden-id-call',
        memory: { thread: 'thread-1', resource: 'resource-1' },
      },
    );
    await resumed.output.consumeStream();
    await vi.waitFor(() => expect(execute).toHaveBeenCalledOnce());
    resumed.cleanup();
  });

  it('binds a prepared runId to the exact request and consumes preparation only once', async () => {
    const processInput = vi.fn(({ messages }) => messages);
    const setup = createSetup({
      storage: new InMemoryStore(),
      model: textModel(),
      inputProcessors: [{ id: 'prepared-request-processor', processInput }],
    });
    const runId = 'prepared-request-binding';
    const preparedHandoff = await setup.agent.prepare('prepared message', {
      runId,
      memory: { thread: 'thread-1', resource: 'resource-1' },
    });
    expect(processInput).toHaveBeenCalledTimes(1);

    await expect(
      setup.agent.stream('substituted message', {
        runId,
        memory: { thread: 'thread-1', resource: 'resource-1' },
      }),
    ).rejects.toMatchObject({ id: 'DURABLE_AGENT_RUN_ID_CONFLICT' });
    expect(processInput).toHaveBeenCalledTimes(1);

    const started = await setup.agent.stream('prepared message', {
      runId,
      memory: { thread: 'thread-1', resource: 'resource-1' },
    });
    expect(processInput).toHaveBeenCalledTimes(1);
    preparedHandoff.cleanup();
    preparedHandoff.cleanup();
    await expect(
      setup.agent.stream('prepared message', {
        runId,
        memory: { thread: 'thread-1', resource: 'resource-1' },
      }),
    ).rejects.toMatchObject({ id: 'DURABLE_AGENT_RUN_ID_CONFLICT' });
    await started.output.consumeStream();
    started.cleanup();
  });

  it('releases an abandoned preparation through an idempotent public cleanup', async () => {
    const setup = createSetup({ storage: new InMemoryStore(), model: textModel() });
    const runId = 'abandoned-prepared-handoff';
    const abandoned = await setup.agent.prepare('first', { runId });
    abandoned.cleanup();
    abandoned.cleanup();

    const replacement = await setup.agent.prepare('second', { runId });
    replacement.cleanup();
  });

  it('keeps the one-time prepared handoff private from caller mutation', async () => {
    const setup = createSetup({ storage: new InMemoryStore(), model: toolCallModel('private-handoff-call') });
    const runId = 'private-prepared-handoff';
    const prepared = await setup.agent.prepare('run it', {
      runId,
      requireToolApproval: true,
      memory: { thread: 'thread-1', resource: 'resource-1' },
    });

    (prepared.workflowInput.options as any).requireToolApproval = false;
    expect(prepared).not.toHaveProperty('registryEntry');

    const started = await setup.agent.stream('run it', {
      runId,
      requireToolApproval: true,
      memory: { thread: 'thread-1', resource: 'resource-1' },
    });
    await vi.waitFor(async () => {
      expect((await setup.agent.listSuspendedRuns({ resourceId: 'resource-1' })).total).toBe(1);
    });
    started.cleanup();
  });

  it('snapshots mutable messages before asynchronous preparation begins', async () => {
    let releaseProcessor!: () => void;
    const processorGate = new Promise<void>(resolve => {
      releaseProcessor = resolve;
    });
    const processInput = vi.fn(async ({ messages }) => {
      await processorGate;
      return messages;
    });
    const setup = createSetup({
      storage: new InMemoryStore(),
      model: textModel(),
      inputProcessors: [{ id: 'delayed-message-processor', processInput }],
    });
    const messages = [{ role: 'user', content: 'original message' }] as any;
    const preparing = setup.agent.prepare(messages, { runId: 'mutable-preparation-messages' });
    messages[0].content = 'substituted message';
    releaseProcessor();
    const prepared = await preparing;

    const started = await setup.agent.stream([{ role: 'user', content: 'original message' }] as any, {
      runId: prepared.runId,
    });
    started.cleanup();
    prepared.cleanup();
  });

  it('rejects same-owner substitution of the global prepared runtime entry', async () => {
    const setup = createSetup({ storage: new InMemoryStore(), model: textModel() });
    const runId = 'prepared-global-substitution';
    const prepared = await setup.agent.prepare('prepared message', { runId });
    const original = globalRunRegistry.get(runId)!;
    globalRunRegistry.set(runId, { ...original, model: toolCallModel('substituted-model-call') as any });

    await expect(setup.agent.stream('prepared message', { runId })).rejects.toMatchObject({
      id: 'DURABLE_AGENT_RUN_ID_CONFLICT',
    });
    prepared.cleanup();
  });

  it('reruns authorization before consuming a prepared handoff', async () => {
    const fgaProvider = createFGAProvider();
    const setup = createSetup({ storage: new InMemoryStore(), model: textModel(), fgaProvider });
    const requestContext = new RequestContext([
      ['user', { id: 'user-1' }],
      [MASTRA_RESOURCE_ID_KEY, 'resource-1'],
      [MASTRA_THREAD_ID_KEY, 'thread-1'],
    ]);
    const runId = 'prepared-fga-recheck';
    await setup.agent.prepare('prepared message', {
      runId,
      requestContext,
      memory: { thread: 'thread-1', resource: 'resource-1' },
    });

    vi.mocked(fgaProvider.require).mockRejectedValueOnce(new Error('authorization revoked'));
    await expect(
      setup.agent.stream('prepared message', {
        runId,
        requestContext,
        memory: { thread: 'thread-1', resource: 'resource-1' },
      }),
    ).rejects.toThrow('authorization revoked');

    vi.mocked(fgaProvider.require).mockResolvedValue(undefined);
    const started = await setup.agent.stream('prepared message', {
      runId,
      requestContext,
      memory: { thread: 'thread-1', resource: 'resource-1' },
    });
    await started.output.consumeStream();
    started.cleanup();
  });

  it('binds hidden and symbol-valued request context fields exactly', async () => {
    const setup = createSetup({ storage: new InMemoryStore(), model: textModel() });
    const adminPolicy = {};
    Object.defineProperty(adminPolicy, 'role', { value: 'admin', enumerable: false });
    const userPolicy = {};
    Object.defineProperty(userPolicy, 'role', { value: 'user', enumerable: false });
    const preparedContext = new RequestContext([
      ['policy', adminPolicy],
      ['scope', Symbol('same-description')],
    ]);
    const substitutedContext = new RequestContext([
      ['policy', userPolicy],
      ['scope', Symbol('same-description')],
    ]);
    const runId = 'prepared-hidden-context';
    await setup.agent.prepare('prepared message', { runId, requestContext: preparedContext });

    await expect(
      setup.agent.stream('prepared message', { runId, requestContext: substitutedContext }),
    ).rejects.toMatchObject({ id: 'DURABLE_AGENT_RUN_ID_CONFLICT' });
    globalRunRegistry.delete(runId);
  });

  it('binds custom properties on special request values exactly', async () => {
    const setup = createSetup({ storage: new InMemoryStore(), model: textModel() });
    const makeValues = (role: string) => {
      const array: any[] = [];
      const date = new Date('2026-01-01T00:00:00.000Z');
      const map = new Map([['scope', 'reports']]);
      const set = new Set(['reports']);
      for (const value of [array, date, map, set]) {
        Object.defineProperty(value, 'role', { value: role, enumerable: false });
      }
      return { array, date, map, set };
    };
    const preparedContext = new RequestContext([['policy', makeValues('admin')]]);
    const substitutedContext = new RequestContext([['policy', makeValues('user')]]);
    const runId = 'prepared-special-value-properties';
    await setup.agent.prepare('prepared message', { runId, requestContext: preparedContext });

    await expect(
      setup.agent.stream('prepared message', { runId, requestContext: substitutedContext }),
    ).rejects.toMatchObject({ id: 'DURABLE_AGENT_RUN_ID_CONFLICT' });
    globalRunRegistry.delete(runId);
  });

  it('binds undefined, sparse arrays, and Date aliasing exactly', async () => {
    const setup = createSetup({ storage: new InMemoryStore(), model: textModel() });
    const sharedDate = new Date('2026-01-01T00:00:00.000Z');
    const preparedContext = new RequestContext([
      ['nullable', undefined],
      ['sparse', [, 'value']],
      ['dates', { first: sharedDate, second: sharedDate }],
    ]);
    const substitutedContext = new RequestContext([
      ['nullable', null],
      ['sparse', [undefined, 'value']],
      [
        'dates',
        {
          first: new Date('2026-01-01T00:00:00.000Z'),
          second: new Date('2026-01-01T00:00:00.000Z'),
        },
      ],
    ]);
    const runId = 'prepared-primitive-alias-binding';
    await setup.agent.prepare('prepared message', { runId, requestContext: preparedContext });

    await expect(
      setup.agent.stream('prepared message', { runId, requestContext: substitutedContext }),
    ).rejects.toMatchObject({ id: 'DURABLE_AGENT_RUN_ID_CONFLICT' });
    globalRunRegistry.delete(runId);
  });

  it('releases local prepared state when the global registry evicts the handoff', async () => {
    const setup = createSetup({ storage: new InMemoryStore(), model: textModel() });
    const runId = 'evicted-prepared-handoff';
    await setup.agent.prepare('first', { runId });
    globalRunRegistry.delete(runId);

    await expect(setup.agent.prepare('second', { runId })).resolves.toMatchObject({ runId });
    globalRunRegistry.delete(runId);
  });

  it('cold-rehydrates a suspended run after its global runtime entry is evicted', async () => {
    const storage = new InMemoryStore();
    const setup = createSetup({ storage, model: toolCallModel('evicted-runtime-call') });
    const started = await setup.agent.stream('run it', {
      runId: 'evicted-runtime-run',
      requireToolApproval: true,
      memory: { thread: 'thread-1', resource: 'resource-1' },
    });
    await vi.waitFor(async () => {
      expect((await setup.agent.listSuspendedRuns({ resourceId: 'resource-1' })).total).toBe(1);
    });
    const getTools = vi.spyOn(setup.baseAgent, 'getToolsForExecution');
    globalRunRegistry.delete(started.runId);

    const resumed = await setup.agent.resume(
      started.runId,
      { approved: false },
      { memory: { thread: 'thread-1', resource: 'resource-1' } },
    );
    expect(getTools).toHaveBeenCalledOnce();
    resumed.cleanup();
    started.cleanup();
  });

  it('fails closed when a workflow step has no validated runtime registry entry', async () => {
    const setup = createSetup({ storage: new InMemoryStore(), model: textModel() });
    const prepared = await setup.agent.prepare('prepared message', { runId: 'missing-runtime-step' });
    prepared.cleanup();

    await expect(
      resolveRuntimeDependencies({
        runId: prepared.runId,
        agentId: 'durable-discovery-agent',
        input: prepared.workflowInput,
      }),
    ).rejects.toMatchObject({ id: 'DURABLE_AGENT_RUNTIME_REGISTRY_MISSING' });
  });

  it('pins an active runtime capability across TTL or capacity eviction', async () => {
    const setup = createSetup({ storage: new InMemoryStore(), model: textModel() });
    const prepared = await setup.agent.prepare('prepared message', { runId: 'pinned-runtime-step' });
    const entry = globalRunRegistry.get(prepared.runId)!;

    expect(pinGlobalRunRegistryEntry(prepared.runId)).toBe(entry);
    expect(pinGlobalRunRegistryEntry(prepared.runId)).toBe(entry);
    globalRunRegistry.delete(prepared.runId);
    expect(getGlobalRunRegistryEntry(prepared.runId)).toBe(entry);
    unpinGlobalRunRegistryEntry(prepared.runId);
    expect(getGlobalRunRegistryEntry(prepared.runId)).toBe(entry);
    expect(globalRunRegistry.get(prepared.runId)).toBeUndefined();
    unpinGlobalRunRegistryEntry(prepared.runId);
    expect(globalRunRegistry.get(prepared.runId)).toBe(entry);
    prepared.cleanup();
  });

  it('rejects a colliding run id while the original runtime is pinned outside the TTL cache', async () => {
    const first = createSetup({ storage: new InMemoryStore(), model: textModel(), agentId: 'first-pinned-agent' });
    const prepared = await first.agent.prepare('prepared message', { runId: 'pinned-runtime-collision' });
    const entry = globalRunRegistry.get(prepared.runId)!;
    expect(pinGlobalRunRegistryEntry(prepared.runId)).toBe(entry);
    globalRunRegistry.delete(prepared.runId);

    const second = createSetup({ storage: new InMemoryStore(), model: textModel(), agentId: 'second-pinned-agent' });
    await expect(second.agent.prepare('different run', { runId: prepared.runId })).rejects.toMatchObject({
      id: 'DURABLE_AGENT_RUN_ID_CONFLICT',
    });

    unpinGlobalRunRegistryEntry(prepared.runId);
    prepared.cleanup();
  });

  it('rejects a prepared handoff evicted during the awaited runId reservation', async () => {
    const storage = new InMemoryStore();
    const setup = createSetup({ storage, model: textModel() });
    const runId = 'evicted-during-reservation';
    await setup.agent.prepare('prepared message', { runId });
    const workflows = (await storage.getStore('workflows'))!;
    const getWorkflowRunById = workflows.getWorkflowRunById.bind(workflows);
    let evicted = false;
    vi.spyOn(workflows, 'getWorkflowRunById').mockImplementation(input => {
      if (!evicted) {
        evicted = true;
        globalRunRegistry.delete(runId);
      }
      return getWorkflowRunById(input);
    });

    await expect(setup.agent.stream('prepared message', { runId })).rejects.toMatchObject({
      id: 'DURABLE_AGENT_RUN_ID_CONFLICT',
    });
  });

  it('rejects a prepared handoff cleaned during the awaited runId reservation', async () => {
    const storage = new InMemoryStore();
    const setup = createSetup({ storage, model: textModel() });
    const runId = 'cleaned-during-reservation';
    const prepared = await setup.agent.prepare('prepared message', { runId });
    const workflows = (await storage.getStore('workflows'))!;
    const getWorkflowRunById = workflows.getWorkflowRunById.bind(workflows);
    let cleaned = false;
    vi.spyOn(workflows, 'getWorkflowRunById').mockImplementation(input => {
      if (!cleaned) {
        cleaned = true;
        prepared.cleanup();
      }
      return getWorkflowRunById(input);
    });

    await expect(setup.agent.stream('prepared message', { runId })).rejects.toMatchObject({
      id: 'DURABLE_AGENT_RUN_ID_CONFLICT',
    });
    await expect(setup.agent.prepare('replacement', { runId })).resolves.toMatchObject({ runId });
    globalRunRegistry.delete(runId);
  });

  it('classifies missing storage, missing runs, and malformed cold snapshots', async () => {
    const baseAgent = new Agent({
      id: 'durable-discovery-agent',
      instructions: 'Test.',
      model: textModel() as LanguageModelV2,
    });
    const unregistered = createDurableAgent({ agent: baseAgent });
    await expect(
      unregistered.resume(
        'missing',
        { approved: true },
        {
          memory: { thread: 'thread-1', resource: 'resource-1' },
        },
      ),
    ).rejects.toMatchObject({ id: 'DURABLE_AGENT_RESUME_NO_STORAGE' });

    const storage = new InMemoryStore();
    const registered = createSetup({ storage, model: textModel() });
    await expect(
      registered.agent.resume(
        'missing',
        { approved: true },
        {
          memory: { thread: 'thread-1', resource: 'resource-1' },
        },
      ),
    ).rejects.toMatchObject({ id: 'DURABLE_AGENT_RESUME_SNAPSHOT_NOT_FOUND' });

    const workflows = (await storage.getStore('workflows'))!;
    await workflows.persistWorkflowSnapshot({
      workflowName: 'durable-agentic-loop',
      runId: 'invalid-json',
      resourceId: 'resource-1',
      snapshot: '{' as any,
    });
    await expect(
      registered.agent.resume(
        'invalid-json',
        { approved: true },
        {
          memory: { thread: 'thread-1', resource: 'resource-1' },
        },
      ),
    ).rejects.toMatchObject({ id: 'DURABLE_AGENT_RESUME_INVALID_SNAPSHOT' });

    await workflows.persistWorkflowSnapshot({
      workflowName: 'durable-agentic-loop',
      runId: 'not-suspended',
      resourceId: 'resource-1',
      snapshot: { status: 'success', context: {} } as any,
    });
    await expect(
      registered.agent.resume(
        'not-suspended',
        { approved: true },
        {
          memory: { thread: 'thread-1', resource: 'resource-1' },
        },
      ),
    ).rejects.toMatchObject({ id: 'DURABLE_AGENT_RESUME_INVALID_SNAPSHOT' });
  });

  it.each([
    { engine: 'default', evented: false },
    { engine: 'evented', evented: true },
  ])('serializes immediate resume behind the $engine engine suspension snapshot', async ({ evented }) => {
    if (evented) vi.stubEnv('MASTRA_EVENTED_EXECUTION', 'true');
    const storage = new InMemoryStore();
    const execute = vi.fn().mockResolvedValue({ ok: true });
    const setup = createSetup({ storage, model: toolCallModel(), execute });
    const runId = `immediate-${evented ? 'evented' : 'default'}`;
    let resumeResult: ReturnType<typeof setup.agent.resume> | undefined;

    const started = await setup.agent.stream('run it', {
      runId,
      requireToolApproval: true,
      memory: { thread: 'thread-1', resource: 'resource-1' },
      onSuspended: () => {
        resumeResult = setup.agent.resume(
          runId,
          { approved: true },
          {
            memory: { thread: 'thread-1', resource: 'resource-1' },
          },
        );
      },
    });

    await vi.waitFor(() => expect(resumeResult).toBeDefined());
    const resumed = await resumeResult!;
    await vi.waitFor(() => expect(execute).toHaveBeenCalled());
    resumed.cleanup();
    started.cleanup();
  });

  it('snapshots resume data before waiting for the prior workflow segment', async () => {
    const storage = new InMemoryStore();
    const execute = vi.fn().mockResolvedValue({ ok: true });
    const setup = createSetup({ storage, model: toolCallModel('immutable-resume-data-call'), execute });
    const started = await setup.agent.stream('run it', {
      runId: 'immutable-resume-data-run',
      requireToolApproval: true,
      memory: { thread: 'thread-1', resource: 'resource-1' },
    });
    await vi.waitFor(async () => {
      expect((await setup.agent.listSuspendedRuns({ resourceId: 'resource-1' })).total).toBe(1);
    });

    let releasePriorSegment!: () => void;
    const priorSegment = new Promise<void>(resolve => {
      releasePriorSegment = resolve;
    });
    globalRunRegistry.get(started.runId)!.workflowExecution = priorSegment;
    const decision = { approved: true };
    const policy = { role: 'admin' };
    const resumeRequestContext = new RequestContext([['policy', policy]]);
    const resuming = setup.agent.resume(started.runId, decision, {
      toolCallId: 'immutable-resume-data-call',
      memory: { thread: 'thread-1', resource: 'resource-1' },
      requestContext: resumeRequestContext,
    });
    decision.approved = false;
    policy.role = 'user';
    resumeRequestContext.set('policy', { role: 'substituted' });
    releasePriorSegment();
    const resumed = await resuming;

    await vi.waitFor(() => expect(execute).toHaveBeenCalledOnce());
    expect(execute.mock.calls[0]![1].requestContext.get('policy')).toEqual({ role: 'admin' });
    resumed.cleanup();
    started.cleanup();
  });

  it('classifies an owner-verified terminal durable snapshot as not suspended', async () => {
    const storage = new InMemoryStore();
    const { runId } = await persistSuspendedRun(storage, 'terminal-classification-call');
    const workflows = (await storage.getStore('workflows'))!;
    const persisted = await workflows.getWorkflowRunById({ workflowName: 'durable-agentic-loop', runId });
    const snapshot = persisted!.snapshot as WorkflowRunState;
    snapshot.status = 'success';
    await workflows.persistWorkflowSnapshot({
      workflowName: 'durable-agentic-loop',
      runId,
      resourceId: 'resource-1',
      snapshot,
    });

    const restarted = createSetup({ storage, model: textModel() });
    await expect(
      restarted.agent.resume(
        runId,
        { approved: true },
        {
          memory: { thread: 'thread-1', resource: 'resource-1' },
        },
      ),
    ).rejects.toMatchObject({ id: 'DURABLE_AGENT_RESUME_RUN_NOT_SUSPENDED' });
  });

  it('lists durable snapshots with exact ownership and tool-call identity', async () => {
    const storage = new InMemoryStore();
    const { runId, toolCallId } = await persistSuspendedRun(storage);
    const restarted = createSetup({ storage, model: textModel() });

    const result = await restarted.agent.listSuspendedRuns({
      threadId: 'thread-1',
      resourceId: 'resource-1',
    });
    expect(result).toMatchObject({
      total: 1,
      runs: [
        {
          runId,
          workflowName: 'durable-agentic-loop',
          threadId: 'thread-1',
          resourceId: 'resource-1',
          toolCalls: [
            {
              toolCallId,
              toolName: 'protectedTool',
              args: { value: 'persisted' },
              requiresApproval: true,
            },
          ],
        },
      ],
    });
    expect((await restarted.agent.listSuspendedRuns({ threadId: 'other' })).total).toBe(0);
    expect((await restarted.agent.listSuspendedRuns({ resourceId: 'other' })).total).toBe(0);
  });

  it('rehydrates the runtime registry and approves after process loss', async () => {
    const storage = new InMemoryStore();
    const execute = vi.fn().mockResolvedValue({ ok: true });
    const { runId, toolCallId } = await persistSuspendedRun(storage);
    const restarted = createSetup({ storage, model: textModel(), execute });

    await expect(
      restarted.agent.sendToolApproval({
        threadId: 'thread-1',
        resourceId: 'resource-1',
        toolCallId,
        approved: true,
      }),
    ).resolves.toEqual({ accepted: true, runId, toolCallId });

    await vi.waitFor(() => expect(execute).toHaveBeenCalledWith({ value: 'persisted' }, expect.anything()));
    await vi.waitFor(async () => {
      expect((await restarted.agent.listSuspendedRuns({ resourceId: 'resource-1' })).total).toBe(0);
    });
    const workflows = (await storage.getStore('workflows'))!;
    expect(await workflows.getWorkflowRunById({ workflowName: 'durable-agentic-loop', runId })).toBeNull();
    expect(await workflows.getWorkflowRunById({ workflowName: 'durable-agentic-execution', runId })).toBeNull();
  });

  it('declines after process loss without executing the tool', async () => {
    const storage = new InMemoryStore();
    const execute = vi.fn().mockResolvedValue({ ok: true });
    const { runId, toolCallId } = await persistSuspendedRun(storage);
    const restarted = createSetup({ storage, model: textModel(), execute });

    await expect(
      restarted.agent.sendToolApproval({
        threadId: 'thread-1',
        resourceId: 'resource-1',
        toolCallId,
        approved: false,
      }),
    ).resolves.toEqual({ accepted: true, runId, toolCallId });
    await vi.waitFor(async () => {
      expect((await restarted.agent.listSuspendedRuns({ resourceId: 'resource-1' })).total).toBe(0);
    });
    expect(execute).not.toHaveBeenCalled();
  });

  it('rejects ambiguous durable runs and resumes only the exact toolCallId', async () => {
    const storage = new InMemoryStore();
    const initial = createSetup({ storage, model: twoApprovalCallsModel() });
    const first = await initial.agent.stream('first', {
      runId: 'ambiguous-run-a',
      requireToolApproval: true,
      memory: { thread: 'thread-1', resource: 'resource-1' },
    });
    await vi.waitFor(async () => {
      expect((await initial.agent.listSuspendedRuns({ resourceId: 'resource-1' })).runs).toHaveLength(1);
    });
    first.cleanup();
    const second = await initial.agent.stream('second', {
      runId: 'ambiguous-run-b',
      requireToolApproval: true,
      memory: { thread: 'thread-1', resource: 'resource-1' },
    });
    await vi.waitFor(async () => {
      expect((await initial.agent.listSuspendedRuns({ resourceId: 'resource-1' })).runs).toHaveLength(2);
    });
    second.cleanup();
    const execute = vi.fn().mockResolvedValue({ ok: true });
    const restarted = createSetup({ storage, model: textModel(), execute });
    expect((await restarted.agent.listSuspendedRuns({ resourceId: 'resource-1' })).runs).toHaveLength(2);

    await expect(
      restarted.agent.sendToolApproval({
        threadId: 'thread-1',
        resourceId: 'resource-1',
        approved: true,
      }),
    ).rejects.toMatchObject({ id: 'AGENT_SEND_TOOL_APPROVAL_AMBIGUOUS_SUSPENDED_CALLS' });

    await expect(
      restarted.agent.sendToolApproval({
        threadId: 'thread-1',
        resourceId: 'resource-1',
        toolCallId: 'durable-call-2',
        approved: true,
      }),
    ).resolves.toMatchObject({ runId: second.runId, toolCallId: 'durable-call-2' });
    await vi.waitFor(() => expect(execute).toHaveBeenCalledTimes(1));
    expect((await restarted.agent.listSuspendedRuns({ resourceId: 'resource-1' })).runs).toHaveLength(1);
  });

  it('retains durable snapshots when an approved run suspends on the next exact call', async () => {
    const storage = new InMemoryStore();
    const setup = createSetup({ storage, model: twoApprovalCallsModel() });
    const started = await setup.agent.stream('run both', {
      requireToolApproval: true,
      memory: { thread: 'thread-1', resource: 'resource-1' },
    });
    await vi.waitFor(async () => {
      expect(
        (await setup.agent.listSuspendedRuns({ resourceId: 'resource-1' })).runs[0]?.toolCalls[0]?.toolCallId,
      ).toBe('durable-call-1');
    });

    let secondSuspension: unknown;
    await setup.agent.approveToolCall({
      runId: started.runId,
      toolCallId: 'durable-call-1',
      memory: { thread: 'thread-1', resource: 'resource-1' },
      onSuspended: data => {
        secondSuspension = data;
      },
    });
    await vi.waitFor(() => expect(secondSuspension).toMatchObject({ toolCallId: 'durable-call-2' }));
    await vi.waitFor(async () => {
      expect(
        (await setup.agent.listSuspendedRuns({ resourceId: 'resource-1' })).runs[0]?.toolCalls[0]?.toolCallId,
      ).toBe('durable-call-2');
    });
    const workflows = (await storage.getStore('workflows'))!;
    expect(
      await workflows.getWorkflowRunById({ workflowName: 'durable-agentic-loop', runId: started.runId }),
    ).not.toBeNull();
    started.cleanup();

    const restarted = createSetup({ storage, model: textModel() });
    await expect(
      restarted.agent.declineToolCall({
        runId: started.runId,
        toolCallId: 'durable-call-2',
        memory: { thread: 'thread-1', resource: 'resource-1' },
      }),
    ).resolves.toBeDefined();
    await vi.waitFor(async () => {
      expect((await restarted.agent.listSuspendedRuns({ resourceId: 'resource-1' })).total).toBe(0);
    });
  });

  it('recovers a suspend()-parked durable call with custom resume data after process loss', async () => {
    const storage = new InMemoryStore();
    const resumedTool = vi.fn();
    const createSuspendingSetup = (model: MockLanguageModelV2) => {
      const tool = createTool({
        id: 'protectedTool',
        description: 'Suspends for user input',
        inputSchema: z.object({ value: z.string() }),
        suspendSchema: z.object({ question: z.string() }),
        resumeSchema: z.object({ answer: z.string() }),
        execute: async (_input, context) => {
          if (!context?.agent?.resumeData) {
            return context?.agent?.suspend({ question: 'Continue?' });
          }
          resumedTool(context.agent.resumeData);
          return { answer: context.agent.resumeData.answer };
        },
      });
      const baseAgent = new Agent({
        id: 'durable-suspend-agent',
        instructions: 'Use the tool.',
        model: model as LanguageModelV2,
        tools: { protectedTool: tool },
      });
      const pubsub = new EventEmitterPubSub();
      openPubsubs.push(pubsub);
      const agent = createDurableAgent({ agent: baseAgent, pubsub, cleanupTimeoutMs: 0 });
      new Mastra({ logger: false, storage, agents: { 'durable-suspend-agent': agent as any } });
      return agent;
    };

    const first = createSuspendingSetup(toolCallModel('durable-suspend-call'));
    const started = await first.stream('run it', {
      memory: { thread: 'thread-1', resource: 'resource-1' },
    });
    await vi.waitFor(async () => {
      expect((await first.listSuspendedRuns({ resourceId: 'resource-1' })).runs[0]?.toolCalls).toEqual([
        expect.objectContaining({
          toolCallId: 'durable-suspend-call',
          requiresApproval: false,
        }),
      ]);
    });
    started.cleanup();

    const restarted = createSuspendingSetup(textModel());
    await restarted.sendToolApproval({
      threadId: 'thread-1',
      resourceId: 'resource-1',
      toolCallId: 'durable-suspend-call',
      approved: true,
      resumeData: { answer: 'continue' },
    });
    await vi.waitFor(() => expect(resumedTool).toHaveBeenCalledWith({ answer: 'continue' }));
    await vi.waitFor(async () => {
      expect((await restarted.listSuspendedRuns({ resourceId: 'resource-1' })).total).toBe(0);
    });
  });

  it('fails closed for another agent or resource during cold recovery', async () => {
    const storage = new InMemoryStore();
    const { runId } = await persistSuspendedRun(storage);
    const wrongAgent = createSetup({ storage, model: textModel(), agentId: 'other-agent' });
    expect((await wrongAgent.agent.listSuspendedRuns({ resourceId: 'resource-1' })).total).toBe(0);
    await expect(
      wrongAgent.agent.resume(runId, { approved: true }, { memory: { thread: 'thread-1', resource: 'resource-1' } }),
    ).rejects.toMatchObject({ id: 'DURABLE_AGENT_RESUME_AGENT_MISMATCH' });

    const owner = createSetup({ storage, model: textModel() });
    await expect(
      owner.agent.resume(runId, { approved: true }, { memory: { thread: 'thread-1', resource: 'other' } }),
    ).rejects.toMatchObject({ id: 'DURABLE_AGENT_RESUME_OWNER_MISMATCH' });

    const forgedContext = new RequestContext([[MASTRA_RESOURCE_ID_KEY, 'other']]);
    await expect(
      owner.agent.resume(
        runId,
        { approved: true },
        {
          requestContext: forgedContext,
          memory: { thread: 'thread-1', resource: 'resource-1' },
        },
      ),
    ).rejects.toMatchObject({ id: 'DURABLE_AGENT_RESUME_OWNER_MISMATCH' });
  });

  it('fails closed when the durable row and embedded snapshot disagree on ownership', async () => {
    const storage = new InMemoryStore();
    const { runId } = await persistSuspendedRun(storage);
    const workflows = (await storage.getStore('workflows'))!;
    const persisted = await workflows.getWorkflowRunById({ workflowName: 'durable-agentic-loop', runId });
    await workflows.persistWorkflowSnapshot({
      workflowName: 'durable-agentic-loop',
      runId,
      resourceId: 'conflicting-resource',
      snapshot: persisted!.snapshot,
    });

    const restarted = createSetup({ storage, model: textModel() });
    expect((await restarted.agent.listSuspendedRuns()).total).toBe(0);
    await expect(
      restarted.agent.resume(runId, { approved: true }, { memory: { thread: 'thread-1', resource: 'resource-1' } }),
    ).rejects.toMatchObject({ id: 'DURABLE_AGENT_RESUME_SNAPSHOT_OWNER_CONFLICT' });
  });

  it('rejects inconsistent outer and nested durable snapshots before cold execution', async () => {
    const storage = new InMemoryStore();
    const execute = vi.fn().mockResolvedValue({ ok: true });
    const { runId } = await persistSuspendedRun(storage, 'pair-conflict-call');
    const workflows = (await storage.getStore('workflows'))!;
    const nested = await workflows.getWorkflowRunById({ workflowName: 'durable-agentic-execution', runId });
    const nestedSnapshot = nested!.snapshot as WorkflowRunState;
    (nestedSnapshot.context.input as any).state.resourceId = 'other-resource';
    await workflows.persistWorkflowSnapshot({
      workflowName: 'durable-agentic-execution',
      runId,
      resourceId: 'resource-1',
      snapshot: nestedSnapshot,
    });

    const restarted = createSetup({ storage, model: textModel(), execute });
    expect((await restarted.agent.listSuspendedRuns({ resourceId: 'resource-1' })).total).toBe(0);
    await expect(
      restarted.agent.resume(
        runId,
        { approved: true },
        {
          toolCallId: 'pair-conflict-call',
          memory: { thread: 'thread-1', resource: 'resource-1' },
        },
      ),
    ).rejects.toMatchObject({ id: 'DURABLE_AGENT_RESUME_SNAPSHOT_PAIR_CONFLICT' });
    expect(execute).not.toHaveBeenCalled();
  });

  it('rejects nested durable snapshots with different execution options', async () => {
    const storage = new InMemoryStore();
    const execute = vi.fn().mockResolvedValue({ ok: true });
    const { runId } = await persistSuspendedRun(storage, 'pair-options-conflict-call');
    const workflows = (await storage.getStore('workflows'))!;
    const nested = await workflows.getWorkflowRunById({ workflowName: 'durable-agentic-execution', runId });
    const nestedSnapshot = nested!.snapshot as WorkflowRunState;
    (nestedSnapshot.context.input as any).options.requireToolApproval = false;
    (nestedSnapshot.context.input as any).options.activeTools = [];
    await workflows.persistWorkflowSnapshot({
      workflowName: 'durable-agentic-execution',
      runId,
      resourceId: 'resource-1',
      snapshot: nestedSnapshot,
    });

    const restarted = createSetup({ storage, model: textModel(), execute });
    expect((await restarted.agent.listSuspendedRuns({ resourceId: 'resource-1' })).total).toBe(0);
    await expect(
      restarted.agent.resume(
        runId,
        { approved: true },
        {
          toolCallId: 'pair-options-conflict-call',
          memory: { thread: 'thread-1', resource: 'resource-1' },
        },
      ),
    ).rejects.toMatchObject({ id: 'DURABLE_AGENT_RESUME_SNAPSHOT_PAIR_CONFLICT' });
    expect(execute).not.toHaveBeenCalled();
  });

  it('rejects an inconsistent nested snapshot while the runtime registry is still warm', async () => {
    const storage = new InMemoryStore();
    const execute = vi.fn().mockResolvedValue({ ok: true });
    const setup = createSetup({ storage, model: toolCallModel('warm-pair-conflict-call'), execute });
    const started = await setup.agent.stream('run it', {
      runId: 'warm-pair-conflict-run',
      requireToolApproval: true,
      memory: { thread: 'thread-1', resource: 'resource-1' },
    });
    await vi.waitFor(async () => {
      expect((await setup.agent.listSuspendedRuns({ resourceId: 'resource-1' })).total).toBe(1);
    });

    const workflows = (await storage.getStore('workflows'))!;
    const nested = await workflows.getWorkflowRunById({
      workflowName: 'durable-agentic-execution',
      runId: started.runId,
    });
    const nestedSnapshot = nested!.snapshot as WorkflowRunState;
    (nestedSnapshot.context.input as any).options.activeTools = [];
    await workflows.persistWorkflowSnapshot({
      workflowName: 'durable-agentic-execution',
      runId: started.runId,
      resourceId: 'resource-1',
      snapshot: nestedSnapshot,
    });

    await expect(
      setup.agent.resume(
        started.runId,
        { approved: true },
        {
          toolCallId: 'warm-pair-conflict-call',
          memory: { thread: 'thread-1', resource: 'resource-1' },
        },
      ),
    ).rejects.toMatchObject({ id: 'DURABLE_AGENT_RESUME_SNAPSHOT_PAIR_CONFLICT' });
    expect(execute).not.toHaveBeenCalled();
    started.cleanup();
  });

  it('requires FGA-verified reserved identity for durable discovery and cold recovery', async () => {
    const storage = new InMemoryStore();
    const { runId, toolCallId } = await persistSuspendedRun(storage);
    const fgaProvider = createFGAProvider();
    const restarted = createSetup({ storage, model: textModel(), fgaProvider });
    const workflows = (await storage.getStore('workflows'))!;
    const listWorkflowRuns = vi.spyOn(workflows, 'listWorkflowRuns');

    const verifiedContext = new RequestContext([
      ['user', { id: 'user-1' }],
      [MASTRA_RESOURCE_ID_KEY, 'resource-1'],
      [MASTRA_THREAD_ID_KEY, 'thread-1'],
    ]);
    await expect(restarted.agent.listSuspendedRuns({ requestContext: verifiedContext })).resolves.toMatchObject({
      total: 1,
    });
    expect(fgaProvider.require).toHaveBeenCalled();
    expect(listWorkflowRuns).toHaveBeenCalledWith(expect.objectContaining({ resourceId: 'resource-1' }));

    listWorkflowRuns.mockClear();
    const missingReservedIdentity = new RequestContext([['user', { id: 'user-1' }]]);
    await expect(restarted.agent.listSuspendedRuns({ requestContext: missingReservedIdentity })).rejects.toMatchObject({
      id: 'AGENT_LIST_SUSPENDED_RUNS_OWNER_UNVERIFIED',
    });
    vi.mocked(fgaProvider.require).mockClear();
    for (const resourceId of ['', '   ']) {
      const emptyReservedIdentity = new RequestContext([
        ['user', { id: 'user-1' }],
        [MASTRA_RESOURCE_ID_KEY, resourceId],
      ]);
      await expect(restarted.agent.listSuspendedRuns({ requestContext: emptyReservedIdentity })).rejects.toMatchObject({
        id: 'AGENT_LIST_SUSPENDED_RUNS_OWNER_UNVERIFIED',
      });
    }
    expect(fgaProvider.require).not.toHaveBeenCalled();
    await expect(
      restarted.agent.listSuspendedRuns({ requestContext: verifiedContext, resourceId: 'other-resource' }),
    ).rejects.toMatchObject({ id: 'AGENT_LIST_SUSPENDED_RUNS_OWNER_MISMATCH' });
    expect(listWorkflowRuns).not.toHaveBeenCalled();

    const getStore = vi.spyOn(storage, 'getStore');
    await expect(
      restarted.agent.resume(
        runId,
        { approved: false },
        {
          requestContext: missingReservedIdentity,
          memory: { thread: 'thread-1', resource: 'resource-1' },
        },
      ),
    ).rejects.toMatchObject({ id: 'AGENT_RESUME_OWNER_UNVERIFIED' });
    expect(getStore).not.toHaveBeenCalled();

    getStore.mockClear();
    const missingVerifiedThread = new RequestContext([
      ['user', { id: 'user-1' }],
      [MASTRA_RESOURCE_ID_KEY, 'resource-1'],
    ]);
    await expect(
      restarted.agent.resume(runId, { approved: false }, { requestContext: missingVerifiedThread }),
    ).rejects.toMatchObject({ id: 'AGENT_RESUME_OWNER_UNVERIFIED' });
    expect(getStore).not.toHaveBeenCalled();

    for (const [resourceId, threadId] of [
      ['   ', 'thread-1'],
      ['resource-1', '   '],
    ] as const) {
      const whitespaceReservedIdentity = new RequestContext([
        ['user', { id: 'user-1' }],
        [MASTRA_RESOURCE_ID_KEY, resourceId],
        [MASTRA_THREAD_ID_KEY, threadId],
      ]);
      await expect(
        restarted.agent.resume(
          runId,
          { approved: false },
          {
            requestContext: whitespaceReservedIdentity,
            memory: { thread: 'thread-1', resource: 'resource-1' },
          },
        ),
      ).rejects.toMatchObject({ id: 'AGENT_RESUME_OWNER_UNVERIFIED' });
    }
    expect(getStore).not.toHaveBeenCalled();

    await expect(
      restarted.agent.sendToolApproval({
        threadId: 'thread-1',
        resourceId: 'resource-1',
        toolCallId,
        approved: false,
        requestContext: verifiedContext,
      }),
    ).resolves.toEqual({ accepted: true, runId, toolCallId });
    expect(fgaProvider.require).toHaveBeenCalledWith(
      { id: 'user-1' },
      expect.objectContaining({
        resource: { type: 'agent', id: 'durable-discovery-agent' },
        permission: 'agents:execute',
      }),
    );
  });

  it('authorizes the public durable-agent identity when it overrides the wrapped agent id', async () => {
    const storage = new InMemoryStore();
    const fgaProvider = createFGAProvider();
    const requestContext = new RequestContext([
      ['user', { id: 'user-1' }],
      [MASTRA_RESOURCE_ID_KEY, 'resource-1'],
      [MASTRA_THREAD_ID_KEY, 'thread-1'],
    ]);
    const setup = createSetup({
      storage,
      model: toolCallModel('public-fga-call'),
      agentId: 'wrapped-agent-id',
      durableAgentId: 'public-durable-id',
      durableAgentName: 'Public durable name',
      fgaProvider,
    });
    const expectPublicAgentAuthorization = () => {
      expect(fgaProvider.require).toHaveBeenCalledWith(
        { id: 'user-1' },
        expect.objectContaining({
          resource: { type: 'agent', id: 'public-durable-id' },
          permission: 'agents:execute',
          context: expect.objectContaining({
            metadata: expect.objectContaining({
              agentId: 'public-durable-id',
              agentName: 'Public durable name',
            }),
          }),
        }),
      );
    };

    const started = await setup.agent.stream('run it', {
      runId: 'public-fga-run',
      requireToolApproval: true,
      requestContext,
      memory: { thread: 'thread-1', resource: 'resource-1' },
    });
    expectPublicAgentAuthorization();
    await vi.waitFor(async () => {
      expect((await setup.agent.listSuspendedRuns({ requestContext })).total).toBe(1);
    });
    expectPublicAgentAuthorization();
    started.cleanup();

    vi.mocked(fgaProvider.require).mockClear();
    const execute = vi.fn().mockResolvedValue({ ok: true });
    const restarted = createSetup({
      storage,
      model: textModel(),
      agentId: 'wrapped-agent-id',
      durableAgentId: 'public-durable-id',
      durableAgentName: 'Public durable name',
      fgaProvider,
      execute,
    });
    const resumed = await restarted.agent.resume(
      'public-fga-run',
      { approved: true },
      {
        requestContext,
        toolCallId: 'public-fga-call',
        memory: { thread: 'thread-1', resource: 'resource-1' },
      },
    );
    await resumed.output.consumeStream();
    expectPublicAgentAuthorization();
    expect(fgaProvider.require).toHaveBeenCalledWith(
      { id: 'user-1' },
      expect.objectContaining({
        resource: { type: 'tool', id: 'public-durable-id:protectedTool' },
        permission: 'tools:execute',
      }),
    );
    expect(fgaProvider.require).not.toHaveBeenCalledWith(
      { id: 'user-1' },
      expect.objectContaining({ resource: { type: 'tool', id: 'wrapped-agent-id:protectedTool' } }),
    );
    expect(execute).toHaveBeenCalledOnce();
    resumed.cleanup();
  });

  it('validates a required request context before durable storage discovery', async () => {
    const storage = new InMemoryStore();
    const setup = createSetup({
      storage,
      model: textModel(),
      requestContextSchema: z.object({ principal: z.string() }),
    });
    const workflows = (await storage.getStore('workflows'))!;
    const listWorkflowRuns = vi.spyOn(workflows, 'listWorkflowRuns');

    await expect(setup.agent.listSuspendedRuns()).rejects.toMatchObject({
      id: 'AGENT_REQUEST_CONTEXT_VALIDATION_FAILED',
    });
    expect(listWorkflowRuns).not.toHaveBeenCalled();

    const getStore = vi.spyOn(storage, 'getStore');
    await expect(
      setup.agent.resume(
        'schema-invalid-run',
        { approved: false },
        { memory: { thread: 'thread-1', resource: 'resource-1' } },
      ),
    ).rejects.toMatchObject({ id: 'AGENT_REQUEST_CONTEXT_VALIDATION_FAILED' });
    expect(getStore).not.toHaveBeenCalled();
  });

  it('rejects denied cold durable callers before any workflow storage lookup', async () => {
    const storage = new InMemoryStore();
    const { runId } = await persistSuspendedRun(storage, 'denied-fga-call');
    const fgaProvider = createFGAProvider();
    vi.mocked(fgaProvider.require).mockRejectedValue(new Error('denied'));
    const restarted = createSetup({ storage, model: textModel(), fgaProvider });
    const getStore = vi.spyOn(storage, 'getStore');
    const requestContext = new RequestContext([
      ['user', { id: 'user-1' }],
      [MASTRA_RESOURCE_ID_KEY, 'resource-1'],
      [MASTRA_THREAD_ID_KEY, 'thread-1'],
    ]);

    await expect(
      restarted.agent.resume(
        runId,
        { approved: false },
        {
          requestContext,
          memory: { thread: 'thread-1', resource: 'resource-1' },
        },
      ),
    ).rejects.toThrow('denied');
    expect(getStore).not.toHaveBeenCalled();
  });

  it('requires the current FGA caller before reading a warm durable snapshot', async () => {
    const storage = new InMemoryStore();
    const fgaProvider = createFGAProvider();
    const setup = createSetup({ storage, model: toolCallModel('warm-fga-call'), fgaProvider });
    const requestContext = new RequestContext([
      ['user', { id: 'user-1' }],
      [MASTRA_RESOURCE_ID_KEY, 'resource-1'],
      [MASTRA_THREAD_ID_KEY, 'thread-1'],
    ]);
    const started = await setup.agent.stream('run it', {
      runId: 'warm-fga-run',
      requireToolApproval: true,
      requestContext,
      memory: { thread: 'body-thread', resource: 'body-resource' },
    });
    await vi.waitFor(async () => {
      expect((await setup.agent.listSuspendedRuns({ requestContext })).total).toBe(1);
    });
    const workflows = (await storage.getStore('workflows'))!;
    const readRun = vi.spyOn(workflows, 'getWorkflowRunById');

    await expect(
      setup.agent.approveToolCall({ runId: started.runId, toolCallId: 'warm-fga-call' }),
    ).rejects.toMatchObject({ id: 'AGENT_RESUME_OWNER_UNVERIFIED' });
    expect(readRun).not.toHaveBeenCalled();
    started.cleanup();
  });

  it('authorizes a warm FGA caller before revealing an owner-tuple mismatch', async () => {
    const storage = new InMemoryStore();
    const fgaProvider = createFGAProvider();
    const setup = createSetup({ storage, model: toolCallModel('warm-fga-owner-call'), fgaProvider });
    const ownerContext = new RequestContext([
      ['user', { id: 'owner' }],
      [MASTRA_RESOURCE_ID_KEY, 'resource-1'],
      [MASTRA_THREAD_ID_KEY, 'thread-1'],
    ]);
    const started = await setup.agent.stream('run it', {
      runId: 'warm-fga-owner-run',
      requireToolApproval: true,
      requestContext: ownerContext,
      memory: { thread: 'thread-1', resource: 'resource-1' },
    });
    await vi.waitFor(async () => {
      expect((await setup.agent.listSuspendedRuns({ requestContext: ownerContext })).total).toBe(1);
    });

    vi.mocked(fgaProvider.require).mockRejectedValueOnce(new Error('denied before owner comparison'));
    const callerContext = new RequestContext([
      ['user', { id: 'caller' }],
      [MASTRA_RESOURCE_ID_KEY, 'other-resource'],
      [MASTRA_THREAD_ID_KEY, 'other-thread'],
    ]);
    await expect(
      setup.agent.resume(
        started.runId,
        { approved: false },
        {
          requestContext: callerContext,
          memory: { thread: 'other-thread', resource: 'other-resource' },
        },
      ),
    ).rejects.toThrow('denied before owner comparison');
    started.cleanup();
  });

  it('cold-rehydrates when the runtime registry is evicted during resume preflight', async () => {
    const storage = new InMemoryStore();
    const fgaProvider = createFGAProvider();
    const setup = createSetup({ storage, model: toolCallModel('preflight-eviction-call'), fgaProvider });
    const requestContext = new RequestContext([
      ['user', { id: 'owner' }],
      [MASTRA_RESOURCE_ID_KEY, 'resource-1'],
      [MASTRA_THREAD_ID_KEY, 'thread-1'],
    ]);
    const started = await setup.agent.stream('run it', {
      runId: 'preflight-eviction-run',
      requireToolApproval: true,
      requestContext,
      memory: { thread: 'thread-1', resource: 'resource-1' },
    });
    await vi.waitFor(async () => {
      expect((await setup.agent.listSuspendedRuns({ requestContext })).total).toBe(1);
    });
    const getTools = vi.spyOn(setup.baseAgent, 'getToolsForExecution');
    vi.mocked(fgaProvider.require).mockImplementationOnce(async () => {
      globalRunRegistry.delete(started.runId);
    });

    const resumed = await setup.agent.resume(
      started.runId,
      { approved: false },
      {
        requestContext,
        memory: { thread: 'thread-1', resource: 'resource-1' },
      },
    );
    expect(getTools).toHaveBeenCalledOnce();
    resumed.cleanup();
    started.cleanup();
  });

  it('does not resume a durable call when the explicit toolCallId is wrong', async () => {
    const storage = new InMemoryStore();
    const execute = vi.fn().mockResolvedValue({ ok: true });
    const { runId } = await persistSuspendedRun(storage);
    const restarted = createSetup({ storage, model: textModel(), execute });

    await expect(
      restarted.agent.approveToolCall({
        runId,
        toolCallId: 'wrong-call',
        memory: { thread: 'thread-1', resource: 'resource-1' },
      }),
    ).rejects.toMatchObject({ id: 'DURABLE_AGENT_RESUME_TOOL_CALL_MISMATCH' });
    expect(execute).not.toHaveBeenCalled();
  });

  it('rejects a wrong warm toolCallId synchronously instead of reporting acceptance', async () => {
    const storage = new InMemoryStore();
    const execute = vi.fn().mockResolvedValue({ ok: true });
    const setup = createSetup({ storage, model: toolCallModel(), execute });
    const started = await setup.agent.stream('run it', {
      runId: 'warm-wrong-call',
      requireToolApproval: true,
      memory: { thread: 'thread-1', resource: 'resource-1' },
    });
    await vi.waitFor(async () => {
      expect((await setup.agent.listSuspendedRuns({ resourceId: 'resource-1' })).total).toBe(1);
    });

    await expect(
      setup.agent.sendToolApproval({
        threadId: 'thread-1',
        resourceId: 'resource-1',
        toolCallId: 'wrong-call',
        approved: true,
      }),
    ).rejects.toMatchObject({ id: 'AGENT_SEND_TOOL_APPROVAL_NO_ACTIVE_THREAD_RUN' });
    expect(execute).not.toHaveBeenCalled();
    started.cleanup();
  });

  it('recovers a legacy toolCallId from the unique resume label', async () => {
    const storage = new InMemoryStore();
    const { runId, toolCallId } = await persistSuspendedRun(storage);
    const workflows = (await storage.getStore('workflows'))!;
    const persisted = await workflows.getWorkflowRunById({ workflowName: 'durable-agentic-loop', runId });
    const snapshot = persisted!.snapshot as WorkflowRunState;
    for (const step of Object.values(snapshot.context)) {
      if (step?.status === 'suspended' && step.suspendPayload) {
        delete (step.suspendPayload as Record<string, unknown>).toolCallId;
        const approval = (step.suspendPayload as any).requireToolApproval;
        if (approval) delete approval.toolCallId;
      }
    }
    await workflows.persistWorkflowSnapshot({
      workflowName: 'durable-agentic-loop',
      runId,
      resourceId: 'resource-1',
      snapshot,
    });

    const restarted = createSetup({ storage, model: textModel() });
    const [run] = (await restarted.agent.listSuspendedRuns({ resourceId: 'resource-1' })).runs;
    expect(run?.toolCalls[0]?.toolCallId).toBe(toolCallId);
  });

  it('refuses cold recovery when the suspended tool is no longer available', async () => {
    const storage = new InMemoryStore();
    const { runId } = await persistSuspendedRun(storage);
    const restarted = createSetup({ storage, model: textModel(), includeTool: false });

    await expect(
      restarted.agent.resume(
        runId,
        { approved: true },
        {
          memory: { thread: 'thread-1', resource: 'resource-1' },
        },
      ),
    ).rejects.toMatchObject({ id: 'DURABLE_AGENT_RESUME_TOOL_NOT_FOUND' });
  });

  it('keeps terminal snapshot deletion best-effort', async () => {
    const storage = new InMemoryStore();
    const execute = vi.fn().mockResolvedValue({ ok: true });
    const { toolCallId } = await persistSuspendedRun(storage);
    const workflows = (await storage.getStore('workflows'))!;
    const originalDelete = workflows.deleteWorkflowRunById.bind(workflows);
    const deleteRun = vi.spyOn(workflows, 'deleteWorkflowRunById').mockImplementation(call => {
      if (call.workflowName === 'durable-agentic-loop') return Promise.reject(new Error('delete failed'));
      return originalDelete(call);
    });
    const restarted = createSetup({ storage, model: textModel(), execute });

    await expect(
      restarted.agent.sendToolApproval({
        threadId: 'thread-1',
        resourceId: 'resource-1',
        toolCallId,
        approved: true,
      }),
    ).resolves.toMatchObject({ accepted: true });
    await vi.waitFor(() => expect(execute).toHaveBeenCalled());
    await vi.waitFor(() => expect(deleteRun).toHaveBeenCalledTimes(3));
    expect(deleteRun.mock.calls.map(([call]) => call.workflowName)).toEqual(
      expect.arrayContaining(['durable-agentic-loop', 'durable-agentic-execution']),
    );
    await vi.waitFor(async () => {
      expect((await restarted.agent.listSuspendedRuns({ resourceId: 'resource-1' })).total).toBe(0);
    });
    await expect(
      restarted.agent.sendToolApproval({
        threadId: 'thread-1',
        resourceId: 'resource-1',
        toolCallId,
        approved: true,
      }),
    ).rejects.toMatchObject({ id: 'AGENT_SEND_TOOL_APPROVAL_NO_ACTIVE_THREAD_RUN' });
    expect(execute).toHaveBeenCalledTimes(1);
  });

  it('rejects changed tool implementations before cold execution', async () => {
    const storage = new InMemoryStore();
    const initial = createSetup({
      storage,
      model: toolCallModel('tool-binding-call'),
      execute: async () => ({ implementation: 'v1' }),
    });
    const started = await initial.agent.stream('run it', {
      runId: 'tool-binding-run',
      requireToolApproval: true,
      memory: { thread: 'thread-1', resource: 'resource-1' },
    });
    await vi.waitFor(async () => {
      expect((await initial.agent.listSuspendedRuns({ resourceId: 'resource-1' })).total).toBe(1);
    });
    started.cleanup();

    const restartedExecute = vi.fn(async () => ({ implementation: 'v2' }));
    const restarted = createSetup({ storage, model: textModel(), execute: restartedExecute });
    await expect(
      restarted.agent.resume(
        'tool-binding-run',
        { approved: true },
        {
          toolCallId: 'tool-binding-call',
          memory: { thread: 'thread-1', resource: 'resource-1' },
        },
      ),
    ).rejects.toMatchObject({ id: 'DURABLE_AGENT_RESUME_TOOL_BINDING_MISMATCH' });
    expect(restartedExecute).not.toHaveBeenCalled();
  });

  it('rejects changed execution-affecting tool configuration before cold execution', async () => {
    const storage = new InMemoryStore();
    const execute = vi.fn(async () => ({ ok: true }));
    const initial = createSetup({
      storage,
      model: toolCallModel('tool-config-binding-call'),
      execute,
      toolOptions: {
        background: { enabled: false, timeoutMs: 1_000 },
        providerOptions: { openai: { strict: false } },
      },
    });
    const started = await initial.agent.stream('run it', {
      runId: 'tool-config-binding-run',
      requireToolApproval: true,
      memory: { thread: 'thread-1', resource: 'resource-1' },
    });
    await vi.waitFor(async () => {
      expect((await initial.agent.listSuspendedRuns({ resourceId: 'resource-1' })).total).toBe(1);
    });
    started.cleanup();

    const restarted = createSetup({
      storage,
      model: textModel(),
      execute,
      toolOptions: {
        background: { enabled: true, timeoutMs: 1_000 },
        providerOptions: { openai: { strict: false } },
      },
    });
    await expect(
      restarted.agent.resume(
        'tool-config-binding-run',
        { approved: true },
        {
          toolCallId: 'tool-config-binding-call',
          memory: { thread: 'thread-1', resource: 'resource-1' },
        },
      ),
    ).rejects.toMatchObject({ id: 'DURABLE_AGENT_RESUME_TOOL_BINDING_MISMATCH' });
    expect(execute).not.toHaveBeenCalled();
  });

  it('accepts semantically equivalent tool schemas with different property order', async () => {
    const storage = new InMemoryStore();
    const execute = vi.fn(async () => ({ ok: true }));
    const initial = createSetup({
      storage,
      model: toolCallModel('canonical-schema-call'),
      execute,
      toolOptions: {
        inputSchema: z.object({ value: z.string(), count: z.number() }),
      },
    });
    const started = await initial.agent.stream('run it', {
      runId: 'canonical-schema-run',
      requireToolApproval: true,
      memory: { thread: 'thread-1', resource: 'resource-1' },
    });
    await vi.waitFor(async () => {
      expect((await initial.agent.listSuspendedRuns({ resourceId: 'resource-1' })).total).toBe(1);
    });
    started.cleanup();

    const restarted = createSetup({
      storage,
      model: textModel(),
      execute,
      toolOptions: {
        inputSchema: z.object({ count: z.number(), value: z.string() }),
      },
    });
    const [initialTools, restartedTools] = await Promise.all([
      initial.baseAgent.getToolsForExecution({}),
      restarted.baseAgent.getToolsForExecution({}),
    ]);
    expect(serializeToolsMetadata(restartedTools)).toEqual(serializeToolsMetadata(initialTools));
    const resumed = await restarted.agent.resume(
      'canonical-schema-run',
      { approved: false },
      {
        toolCallId: 'canonical-schema-call',
        memory: { thread: 'thread-1', resource: 'resource-1' },
      },
    );
    await resumed.output.consumeStream();
    resumed.cleanup();
    expect(execute).not.toHaveBeenCalled();
  });

  it('rejects cold recovery when request-local processor state cannot be restored', async () => {
    const storage = new InMemoryStore();
    const initial = createSetup({
      storage,
      model: toolCallModel('processor-call'),
      inputProcessors: [{ id: 'stateful', processInput: ({ messages }: any) => messages }],
    });
    const started = await initial.agent.stream('run it', {
      runId: 'processor-run',
      requireToolApproval: true,
      memory: { thread: 'thread-1', resource: 'resource-1' },
    });
    await vi.waitFor(async () => {
      expect((await initial.agent.listSuspendedRuns({ resourceId: 'resource-1' })).total).toBe(1);
    });
    started.cleanup();

    const restarted = createSetup({
      storage,
      model: textModel(),
      inputProcessors: [{ id: 'stateful', processInput: ({ messages }: any) => messages }],
    });
    await expect(
      restarted.agent.resume(
        'processor-run',
        { approved: false },
        { memory: { thread: 'thread-1', resource: 'resource-1' } },
      ),
    ).rejects.toMatchObject({ id: 'DURABLE_AGENT_RESUME_PROCESSOR_STATE_UNRECOVERABLE' });
  });

  it('rejects cold recovery when processors are added after suspension', async () => {
    const storage = new InMemoryStore();
    const { runId } = await persistSuspendedRun(storage, 'added-processor-call');
    const restarted = createSetup({
      storage,
      model: textModel(),
      inputProcessors: [{ id: 'added', processInput: ({ messages }: any) => messages }],
    });
    await expect(
      restarted.agent.resume(runId, { approved: false }, { memory: { thread: 'thread-1', resource: 'resource-1' } }),
    ).rejects.toMatchObject({ id: 'DURABLE_AGENT_RESUME_PROCESSOR_STATE_UNRECOVERABLE' });
  });

  it('rejects active, persisted, and concurrent runId collisions without replacement', async () => {
    const storage = new InMemoryStore();
    const first = createSetup({ storage, model: textModel(), agentId: 'collision-agent-a' });
    await first.agent.prepare('first', {
      runId: 'active-collision',
      memory: { thread: 'thread-a', resource: 'resource-a' },
    });
    const other = createSetup({ storage, model: textModel(), agentId: 'collision-agent-b' });
    await expect(
      other.agent.prepare('other', {
        runId: 'active-collision',
        memory: { thread: 'thread-b', resource: 'resource-b' },
      }),
    ).rejects.toMatchObject({ id: 'DURABLE_AGENT_RUN_ID_CONFLICT' });

    const persisted = await persistSuspendedRun(storage, 'persisted-collision-call');
    await expect(
      other.agent.prepare('other', {
        runId: persisted.runId,
        memory: { thread: 'thread-b', resource: 'resource-b' },
      }),
    ).rejects.toMatchObject({ id: 'DURABLE_AGENT_RUN_ID_CONFLICT' });

    const processInput = vi.fn(({ messages }) => messages);
    const concurrent = createSetup({
      storage: new InMemoryStore(),
      model: textModel(),
      agentId: 'concurrent-agent',
      inputProcessors: [{ id: 'concurrent-side-effect', processInput }],
    });
    const attempts = await Promise.allSettled([
      concurrent.agent.prepare('one', { runId: 'concurrent-collision' }),
      concurrent.agent.prepare('two', { runId: 'concurrent-collision' }),
    ]);
    expect(attempts.filter(result => result.status === 'fulfilled')).toHaveLength(1);
    expect(attempts.filter(result => result.status === 'rejected')).toHaveLength(1);
    expect(processInput).toHaveBeenCalledTimes(1);
    await expect(concurrent.agent.prepare('empty', { runId: '' })).rejects.toThrow('non-empty string');
    await expect(concurrent.agent.prepare('blank', { runId: '   ' })).rejects.toThrow('non-empty string');
  });

  it('rejects version selectors that differ from the suspended run', async () => {
    const storage = new InMemoryStore();
    const { runId } = await persistSuspendedRun(storage);
    const restarted = createSetup({ storage, model: textModel() });

    await expect(
      restarted.agent.resume(
        runId,
        { approved: false },
        {
          memory: { thread: 'thread-1', resource: 'resource-1' },
          versions: { agents: { 'durable-discovery-agent': { versionId: 'v2' } } },
        },
      ),
    ).rejects.toMatchObject({ id: 'DURABLE_AGENT_RESUME_VERSION_MISMATCH' });
  });

  it('rejects changed call-site and request-context version selectors on warm recovery', async () => {
    const storage = new InMemoryStore();
    const originalVersions = { agents: { 'durable-discovery-agent': { versionId: 'v1' } } };
    const changedVersions = { agents: { 'durable-discovery-agent': { versionId: 'v2' } } };
    const setup = createSetup({ storage, model: toolCallModel('warm-version-call') });
    const started = await setup.agent.stream('run it', {
      runId: 'warm-version-run',
      requireToolApproval: true,
      memory: { thread: 'thread-1', resource: 'resource-1' },
      versions: originalVersions,
    });
    await vi.waitFor(async () => {
      expect((await setup.agent.listSuspendedRuns({ resourceId: 'resource-1' })).total).toBe(1);
    });

    await expect(
      setup.agent.resume(
        started.runId,
        { approved: false },
        {
          memory: { thread: 'thread-1', resource: 'resource-1' },
          versions: changedVersions,
        },
      ),
    ).rejects.toMatchObject({ id: 'DURABLE_AGENT_RESUME_VERSION_MISMATCH' });

    const changedContext = new RequestContext([[MASTRA_VERSIONS_KEY, changedVersions]]);
    await expect(
      setup.agent.resume(
        started.runId,
        { approved: false },
        {
          requestContext: changedContext,
          memory: { thread: 'thread-1', resource: 'resource-1' },
        },
      ),
    ).rejects.toMatchObject({ id: 'DURABLE_AGENT_RESUME_VERSION_MISMATCH' });
    started.cleanup();
  });

  it('isolates warm recovery from caller mutation of the original request context', async () => {
    const storage = new InMemoryStore();
    const originalVersions = { agents: { 'durable-discovery-agent': { versionId: 'v1' } } } as const;
    const requestContext = new RequestContext();
    const setup = createSetup({ storage, model: toolCallModel('warm-version-context-mutation') });
    const started = await setup.agent.stream('run it', {
      runId: 'warm-version-context-mutation-run',
      requireToolApproval: true,
      requestContext,
      memory: { thread: 'thread-1', resource: 'resource-1' },
      versions: originalVersions,
    });
    await vi.waitFor(async () => {
      expect((await setup.agent.listSuspendedRuns({ resourceId: 'resource-1' })).total).toBe(1);
    });

    requestContext.set(MASTRA_VERSIONS_KEY, {
      agents: { 'durable-discovery-agent': { versionId: 'v2' } },
    });
    const resumed = await setup.agent.resume(
      started.runId,
      { approved: false },
      { memory: { thread: 'thread-1', resource: 'resource-1' } },
    );
    resumed.cleanup();
    started.cleanup();
  });

  it('reuses the persisted version selectors while resolving cold runtime dependencies', async () => {
    const storage = new InMemoryStore();
    const versions = { agents: { 'durable-discovery-agent': { versionId: 'v2' } } };
    const { runId } = await persistSuspendedRun(storage, 'durable-call-versioned', versions);
    const restarted = createSetup({ storage, model: textModel() });
    const getTools = vi.spyOn(restarted.baseAgent, 'getToolsForExecution');

    const resumed = await restarted.agent.resume(
      runId,
      { approved: false },
      {
        memory: { thread: 'thread-1', resource: 'resource-1' },
      },
    );
    expect(getTools).toHaveBeenCalledOnce();
    expect(getTools.mock.calls[0]![0].requestContext?.get(MASTRA_VERSIONS_KEY)).toEqual({
      agents: { 'durable-discovery-agent': { versionId: 'v2' } },
    });
    resumed.cleanup();
  });

  it('batch-loads outer and nested driver rows and surfaces storage failures', async () => {
    const storage = new InMemoryStore();
    const restarted = createSetup({ storage, model: textModel() });
    const workflows = (await storage.getStore('workflows'))!;
    const list = vi.spyOn(workflows, 'listWorkflowRuns');
    const getById = vi.spyOn(workflows, 'getWorkflowRunById');
    await restarted.agent.listSuspendedRuns({ perPage: 2, page: 0 });
    expect(list).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({ workflowName: 'durable-agentic-loop', perPage: false }),
    );
    expect(list).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({ workflowName: 'durable-agentic-execution', perPage: false }),
    );
    expect(getById).not.toHaveBeenCalled();

    list.mockRejectedValueOnce(new Error('storage unavailable'));
    await expect(restarted.agent.listSuspendedRuns()).rejects.toThrow('storage unavailable');
  });
});
