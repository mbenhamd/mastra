import { afterEach, describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { BACKGROUND_WORK_CONTEXT } from '../../../../processors/background-work-signals';
import { PUBSUB_SYMBOL } from '../../../../workflows/constants';
import { createToolCallIdentityDigest } from '../../../tool-call-identity';
import { ON_BEFORE_TOOL_EXECUTION_KEY, TOOL_PERMISSION_DENIED_ERROR_NAME } from '../../../tool-permission-prefilter';
import { globalRunRegistry } from '../../run-registry';
import { createDurableToolCallStep } from './tool-call';

vi.mock('../../../../workflows', () => ({
  createStep: (config: unknown) => config,
}));

vi.mock('../../../../background-tasks/create', () => ({
  createBackgroundTask: vi.fn(),
}));

vi.mock('../../../../background-tasks/resolve-config', () => ({
  resolveBackgroundConfig: vi.fn(),
}));

vi.mock('../../utils/resolve-runtime', async () => ({
  restoreRequestContext: (
    await vi.importActual<typeof import('../../utils/resolve-runtime')>('../../utils/resolve-runtime')
  ).restoreRequestContext,
  resolveTool: vi.fn(),
  toolApprovalRequirement: vi.fn().mockResolvedValue({ required: false, reasons: [] }),
  toolRequiresApproval: vi.fn().mockResolvedValue(false),
  rebuildRunToolsFromMastra: vi.fn().mockResolvedValue(undefined),
}));

vi.mock('../../stream-adapter', () => ({
  emitChunkEvent: vi.fn().mockResolvedValue(undefined),
  emitSuspendedEvent: vi.fn().mockResolvedValue(undefined),
}));

const { createBackgroundTask } = await import('../../../../background-tasks/create');
const { resolveBackgroundConfig } = await import('../../../../background-tasks/resolve-config');
const { emitChunkEvent } = await import('../../stream-adapter');
const { resolveTool: _resolveTool } = await import('../../utils/resolve-runtime');

const RUN_ID = 'run-bg-1';
const RUNTIME_BINDING_ID = 'binding-bg-1';
const AGENT_ID = 'agent-1';
const TOOL_NAME = 'research';
const TOOL_CALL_ID = 'call-1';

function mockPubsub() {
  return { publish: vi.fn(), subscribe: vi.fn(), unsubscribe: vi.fn(), flush: vi.fn() };
}

function baseInput() {
  return {
    toolCallId: TOOL_CALL_ID,
    toolName: TOOL_NAME,
    args: { topic: 'quantum' },
  };
}

function makeInitData(overrides: Record<string, any> = {}) {
  return {
    runId: RUN_ID,
    runtimeBindingId: RUNTIME_BINDING_ID,
    agentId: AGENT_ID,
    options: { requireToolApproval: false },
    state: {
      threadId: 'thread-1',
      resourceId: 'user-1',
      memoryConfig: undefined,
      threadExists: false,
    },
    ...overrides,
  };
}

function makeMessageList() {
  // Upstream's mock-based double: the merged file carries upstream's tests
  // that stub `get.all.db` (stored suspension metadata) and assert on `add`
  // (transcript reconciliation), which the real MessageList cannot satisfy.
  // The recording/source-checker stubs serve the processToolResult runner
  // path (per-invocation data-part collection regression below).
  return {
    updateToolInvocation: vi.fn().mockReturnValue(true),
    updateMessageMetadataByToolCallId: vi.fn().mockReturnValue(true),
    add: vi.fn(),
    get: { all: { db: vi.fn().mockReturnValue([]) } },
    makeMessageSourceChecker: vi.fn().mockReturnValue(() => false),
    getAllSystemMessages: vi.fn().mockReturnValue([]),
    startRecording: vi.fn(),
    stopRecording: vi.fn().mockReturnValue([]),
  };
}

function makeSaveQueueManager() {
  return { flushMessages: vi.fn().mockResolvedValue(undefined) };
}

function setupRegistry(overrides: Record<string, any> = {}) {
  const messageList = makeMessageList();
  const saveQueueManager = makeSaveQueueManager();
  const bgManager = { config: {}, listTasks: vi.fn() };

  const entry = {
    runtimeBindingId: RUNTIME_BINDING_ID,
    tools: {
      [TOOL_NAME]: {
        execute: vi.fn().mockResolvedValue({ summary: 'done' }),
        backgroundConfig: { enabled: true },
      },
    },
    model: {} as any,
    backgroundTaskManager: bgManager,
    backgroundTasksConfig: { tools: { [TOOL_NAME]: true } },
    messageList,
    saveQueueManager,
    // Some tests install a live registry-held request context (fork's
    // onBeforeToolExecution hook transport test); declare it so the
    // assignment typechecks.
    requestContext: undefined as Map<string, unknown> | undefined,
    ...overrides,
  };

  globalRunRegistry.set(RUN_ID, entry as any);
  return { messageList, saveQueueManager, bgManager, entry };
}

function executeStep(pubsub: any, initData: any, input?: any, resumeData?: unknown, suspendData?: unknown) {
  const step = createDurableToolCallStep();
  return (step as any).execute({
    inputData: input ?? baseInput(),
    mastra: { getLogger: () => undefined },
    suspend: vi.fn(),
    resumeData,
    suspendData,
    requestContext: new Map(),
    getInitData: () => initData,
    [PUBSUB_SYMBOL]: pubsub,
  });
}

afterEach(() => {
  if (globalRunRegistry.has(RUN_ID)) {
    globalRunRegistry.delete(RUN_ID);
  }
  vi.clearAllMocks();
});

describe('durable tool-call background task dispatch', () => {
  it('fails closed when a built-in durable run loses its runtime registry entry', async () => {
    const pubsub = mockPubsub();

    const initData = makeInitData({ runtimeResolution: 'registry-required' });
    await expect(executeStep(pubsub, initData)).rejects.toMatchObject({
      id: 'DURABLE_AGENT_RUNTIME_REGISTRY_MISSING',
    });
    expect(globalRunRegistry.has(RUN_ID)).toBe(false);

    // A retry must fail identically rather than inheriting a placeholder seeded
    // by abort-listener bootstrap and degrading to ToolNotFoundError.
    await expect(executeStep(pubsub, initData)).rejects.toMatchObject({
      id: 'DURABLE_AGENT_RUNTIME_REGISTRY_MISSING',
    });
    expect(globalRunRegistry.has(RUN_ID)).toBe(false);

    globalRunRegistry.set(RUN_ID, {
      isPlaceholder: true,
      runtimeBindingId: RUNTIME_BINDING_ID,
      tools: {},
      model: undefined as any,
    } as any);
    await expect(executeStep(pubsub, initData)).rejects.toMatchObject({
      id: 'DURABLE_AGENT_RUNTIME_REGISTRY_MISSING',
    });
    expect(_resolveTool).not.toHaveBeenCalled();
  });

  it('does not fall back to a current Mastra tool for a registry-required run', async () => {
    const pubsub = mockPubsub();
    setupRegistry({ tools: {} });

    const result = await executeStep(pubsub, makeInitData({ runtimeResolution: 'registry-required' }));
    expect(result.error).toMatchObject({ name: 'ToolNotFoundError' });
    expect(_resolveTool).not.toHaveBeenCalled();
  });

  it('fails closed when a replacement run loses its registry before tool dispatch', async () => {
    await expect(
      executeStep(
        mockPubsub(),
        makeInitData({ options: { requireToolApproval: false, toolSurfaceFence: [TOOL_NAME] } }),
      ),
    ).rejects.toThrow(/Cannot reconstruct replacement tool implementations/);
  });

  it('fails closed when a replacement registry loses an allowed implementation before dispatch', async () => {
    setupRegistry({ tools: {} });
    const backingExecute = vi.fn().mockResolvedValue('backing');
    vi.mocked(_resolveTool).mockReturnValue({ execute: backingExecute } as any);

    await expect(
      executeStep(
        mockPubsub(),
        makeInitData({ options: { requireToolApproval: false, toolSurfaceFence: [TOOL_NAME] } }),
      ),
    ).rejects.toThrow(/has no own concrete implementation/);
    expect(backingExecute).not.toHaveBeenCalled();
  });

  it('does not dispatch an out-of-fence registry tool or a same-name backing tool', async () => {
    const allowedExecute = vi.fn().mockResolvedValue('allowed');
    const hiddenExecute = vi.fn().mockResolvedValue('hidden');
    const backingExecute = vi.fn().mockResolvedValue('backing');
    setupRegistry({
      tools: {
        allowedTool: { execute: allowedExecute },
        hiddenTool: { execute: hiddenExecute },
      },
    });
    vi.mocked(_resolveTool).mockReturnValue({ execute: backingExecute } as any);

    const result = await executeStep(
      mockPubsub(),
      makeInitData({ options: { requireToolApproval: false, toolSurfaceFence: ['allowedTool'] } }),
      { ...baseInput(), toolName: 'hiddenTool' },
    );

    expect(result.error).toEqual(expect.objectContaining({ name: 'ToolNotFoundError' }));
    expect(result.error.message).toContain('Available tools: allowedTool.');
    expect(result.error.message).not.toContain('Available tools: allowedTool, hiddenTool');
    expect(allowedExecute).not.toHaveBeenCalled();
    expect(hiddenExecute).not.toHaveBeenCalled();
    expect(backingExecute).not.toHaveBeenCalled();
  });

  it('does not advertise fenced-out tools retained in activeTools', async () => {
    setupRegistry({
      tools: {
        allowedTool: { execute: vi.fn().mockResolvedValue('allowed') },
        hiddenTool: { execute: vi.fn().mockResolvedValue('hidden') },
      },
    });

    const result = await executeStep(
      mockPubsub(),
      makeInitData({
        options: {
          requireToolApproval: false,
          toolSurfaceFence: ['allowedTool'],
          activeTools: ['allowedTool', 'hiddenTool'],
        },
      }),
      { ...baseInput(), toolName: 'hiddenTool' },
    );

    expect(result.error).toEqual(
      expect.objectContaining({
        name: 'ToolNotFoundError',
        message: expect.stringContaining('Available tools: allowedTool.'),
      }),
    );
    expect(result.error.message).not.toContain('Available tools: allowedTool, hiddenTool');
  });

  it.each([false, 0, '', null, undefined])(
    'resumes a suspended background task with falsy or absent payload %#',
    async resumeData => {
      const pubsub = mockPubsub();
      setupRegistry();
      const initData = makeInitData();
      const input = baseInput();
      const resume = vi.fn().mockResolvedValue({ id: 'task-resumed' });
      const dispatch = vi.fn();

      vi.mocked(resolveBackgroundConfig).mockReturnValue({ runInBackground: true } as any);
      vi.mocked(createBackgroundTask).mockReturnValue({
        checkIfSuspended: vi.fn().mockResolvedValue(true),
        resume,
        dispatch,
        // The shared dispatch ladder (upstream structure) binds the handle's
        // waitForCompletion eagerly when it returns a dispatched outcome; the
        // resumed path never calls it for a deferred disposition.
        waitForCompletion: vi.fn(),
      } as any);

      const result = await executeStep(pubsub, initData, input, resumeData, {
        version: 1,
        type: 'suspension',
        runId: RUN_ID,
        iterationCount: 0,
        stepId: 'durable-tool-call',
        toolCallId: TOOL_CALL_ID,
        toolName: TOOL_NAME,
        identityDigest: createToolCallIdentityDigest({
          toolCallId: TOOL_CALL_ID,
          toolName: TOOL_NAME,
          args: input.args,
        }),
      });

      expect(resume).toHaveBeenCalledWith(resumeData);
      expect(dispatch).not.toHaveBeenCalled();
      expect(result.result).toContain('Background task resumed');
    },
  );

  it('resumes an approved in-tool background suspension and preserves the grant', async () => {
    const pubsub = mockPubsub();
    setupRegistry();
    const initData = makeInitData();
    const input = baseInput();
    const resume = vi.fn().mockResolvedValue({ id: 'task-resumed' });
    const dispatch = vi.fn();
    const resumeData = { approved: true, reason: 'Reviewed by admin' };

    vi.mocked(resolveBackgroundConfig).mockReturnValue({ runInBackground: true } as any);
    vi.mocked(createBackgroundTask).mockReturnValue({
      checkIfSuspended: vi.fn().mockResolvedValue(true),
      resume,
      dispatch,
      // See the falsy-payload resume test: the ladder binds this eagerly.
      waitForCompletion: vi.fn(),
    } as any);

    const result = await executeStep(pubsub, initData, input, resumeData, {
      version: 1,
      type: 'approval',
      approvalSource: 'tool-execution',
      runId: RUN_ID,
      iterationCount: 0,
      stepId: 'durable-tool-call',
      toolCallId: TOOL_CALL_ID,
      toolName: TOOL_NAME,
      identityDigest: createToolCallIdentityDigest({
        toolCallId: TOOL_CALL_ID,
        toolName: TOOL_NAME,
        args: input.args,
      }),
    });

    expect(resume).toHaveBeenCalledWith(resumeData);
    expect(dispatch).not.toHaveBeenCalled();
    expect(result).toMatchObject({
      approval: { id: TOOL_CALL_ID, approved: true, reason: 'Reviewed by admin' },
    });
  });

  it('preserves the grant on an approved in-tool suspension that completes as an awaited background task', async () => {
    const pubsub = mockPubsub();
    setupRegistry();
    const initData = makeInitData();
    const input = baseInput();
    const authoritativeResult = { summary: 'awaited approval result' };
    const resumeData = { approved: true, reason: 'Reviewed by admin' };

    vi.mocked(resolveBackgroundConfig).mockReturnValue({
      runInBackground: true,
      disposition: 'awaited',
      timeoutMs: 30_000,
      maxRetries: 0,
    } as any);

    let capturedOnResult: any;
    const resume = vi.fn().mockResolvedValue({ id: 'task-resumed-await' });
    const dispatch = vi.fn();
    const waitForCompletion = vi.fn(async () => {
      await capturedOnResult({
        runId: RUN_ID,
        taskId: 'task-resumed-await',
        toolCallId: TOOL_CALL_ID,
        toolName: TOOL_NAME,
        agentId: AGENT_ID,
        result: authoritativeResult,
        status: 'completed',
        startedAt: new Date(),
        completedAt: new Date(),
      });
      return { id: 'task-resumed-await', status: 'completed', result: authoritativeResult };
    });
    vi.mocked(createBackgroundTask).mockImplementation((_mgr: any, opts: any) => {
      capturedOnResult = opts.context.onResult;
      return {
        dispatch,
        resume,
        checkIfExisting: vi.fn().mockResolvedValue(undefined),
        checkIfSuspended: vi.fn().mockResolvedValue(true),
        checkIfRunning: vi.fn().mockResolvedValue(false),
        restart: vi.fn(),
        task: { id: 'task-resumed-await' },
        cancel: vi.fn(),
        waitForCompletion,
      } as any;
    });

    // An approved in-tool suspension resumed as an AWAITED background task:
    // the resumed leg (not `started`) must still carry the grant — llm-mapping
    // commits approval from `toolResult.approval`, so dropping it here loses
    // the approval provenance on recall.
    const result = await executeStep(pubsub, initData, input, resumeData, {
      version: 1,
      type: 'approval',
      approvalSource: 'tool-execution',
      runId: RUN_ID,
      iterationCount: 0,
      stepId: 'durable-tool-call',
      toolCallId: TOOL_CALL_ID,
      toolName: TOOL_NAME,
      identityDigest: createToolCallIdentityDigest({
        toolCallId: TOOL_CALL_ID,
        toolName: TOOL_NAME,
        args: input.args,
      }),
    });

    expect(resume).toHaveBeenCalledWith(resumeData);
    expect(dispatch).not.toHaveBeenCalled();
    expect(result.result).toEqual(authoritativeResult);
    expect(result).toMatchObject({
      approval: { id: TOOL_CALL_ID, approved: true, reason: 'Reviewed by admin' },
    });
  });

  it('dispatches a background task and returns a placeholder result', async () => {
    const pubsub = mockPubsub();
    setupRegistry();
    const initData = makeInitData();

    vi.mocked(resolveBackgroundConfig).mockReturnValue({
      runInBackground: true,
      disposition: 'deferred',
      timeoutMs: 30_000,
      maxRetries: 2,
    } as any);

    const mockTask = { id: 'task-abc' };
    const waitForCompletion = vi.fn();
    vi.mocked(createBackgroundTask).mockReturnValue({
      dispatch: vi.fn().mockResolvedValue({ task: mockTask, fallbackToSync: false }),
      checkIfRunning: vi.fn().mockResolvedValue(false),
      restart: vi.fn(),
      task: mockTask,
      cancel: vi.fn(),
      waitForCompletion,
    } as any);

    const result = await executeStep(pubsub, initData);

    expect(result.result).toContain('Background task started');
    expect(result.result).toContain('task-abc');
    expect(result.result).toContain(TOOL_NAME);
    expect(result.providerMetadata).toMatchObject({
      mastra: { backgroundTask: { taskId: 'task-abc', status: 'running' } },
    });
    expect(waitForCompletion).not.toHaveBeenCalled();
  });

  it('waits for an awaited task and returns its authoritative result', async () => {
    const pubsub = mockPubsub();
    const { messageList } = setupRegistry();
    const initData = makeInitData();
    const authoritativeResult = { summary: 'authoritative result' };

    vi.mocked(resolveBackgroundConfig).mockReturnValue({
      runInBackground: true,
      disposition: 'awaited',
      timeoutMs: 30_000,
      maxRetries: 0,
    } as any);

    let capturedOnResult: any;
    const waitForCompletion = vi.fn(async () => {
      await capturedOnResult({
        runId: RUN_ID,
        taskId: 'task-awaited',
        toolCallId: TOOL_CALL_ID,
        toolName: TOOL_NAME,
        agentId: AGENT_ID,
        result: authoritativeResult,
        status: 'completed',
        startedAt: new Date(),
        completedAt: new Date(),
      });
      return { id: 'task-awaited', status: 'completed', result: authoritativeResult };
    });
    vi.mocked(createBackgroundTask).mockImplementation((_mgr: any, opts: any) => {
      capturedOnResult = opts.context.onResult;
      return {
        dispatch: vi.fn().mockResolvedValue({ task: { id: 'task-awaited' }, fallbackToSync: false }),
        checkIfExisting: vi.fn().mockResolvedValue(undefined),
        checkIfRunning: vi.fn().mockResolvedValue(false),
        restart: vi.fn(),
        task: { id: 'task-awaited' },
        cancel: vi.fn(),
        waitForCompletion,
      } as any;
    });

    const result = await executeStep(pubsub, initData);

    // Fork's abort transport installs an AbortController on the run-registry
    // entry (workers must hear remote aborts), so the awaited disposition
    // receives the entry's live signal — not `undefined` as upstream asserted
    // before the transport existed.
    // Fork (PF): the awaited disposition also observes a suspended task (the
    // main loop's includeSuspended contract) so it can suspend the parent.
    expect(waitForCompletion).toHaveBeenCalledWith({
      abortSignal: (globalRunRegistry.get(RUN_ID) as any).abortSignal,
      includeSuspended: true,
    });
    expect(result.result).toEqual(authoritativeResult);
    expect(result.providerMetadata).toMatchObject({
      mastra: { backgroundTask: { taskId: 'task-awaited', status: 'completed' } },
    });
    expect(messageList.updateToolInvocation).toHaveBeenCalledTimes(1);
  });

  it('stops awaiting background work when the durable run is aborted', async () => {
    const pubsub = mockPubsub();
    const abortController = new AbortController();
    // Pass the controller (not just its signal) so fork's abort transport
    // reuses it instead of installing a fresh one — the test's abort() must
    // reach the awaited waitForCompletion.
    setupRegistry({ abortController, abortSignal: abortController.signal });
    const initData = makeInitData();

    vi.mocked(resolveBackgroundConfig).mockReturnValue({
      runInBackground: true,
      disposition: 'awaited',
      timeoutMs: 30_000,
      maxRetries: 0,
    } as any);

    const waitForCompletion = vi.fn(
      ({ abortSignal }: { abortSignal?: AbortSignal }) =>
        new Promise((_resolve, reject) => {
          abortSignal?.addEventListener('abort', () => reject(abortSignal.reason), { once: true });
        }),
    );
    vi.mocked(createBackgroundTask).mockReturnValue({
      dispatch: vi.fn().mockResolvedValue({ task: { id: 'task-aborted' }, fallbackToSync: false }),
      checkIfExisting: vi.fn().mockResolvedValue(undefined),
      checkIfRunning: vi.fn().mockResolvedValue(false),
      restart: vi.fn(),
      task: { id: 'task-aborted' },
      cancel: vi.fn(),
      waitForCompletion,
    } as any);

    const execution = executeStep(pubsub, initData);
    await vi.waitFor(() =>
      expect(waitForCompletion).toHaveBeenCalledWith({ abortSignal: abortController.signal, includeSuspended: true }),
    );
    abortController.abort(new Error('durable run aborted'));

    await expect(execution).rejects.toThrow('durable run aborted');
  });

  it('reconciles an awaited task that completed before workflow replay without dispatching a duplicate', async () => {
    const pubsub = mockPubsub();
    const { messageList } = setupRegistry();
    const initData = makeInitData();
    const authoritativeResult = { summary: 'persisted result' };
    const terminalTask = {
      id: 'task-terminal',
      status: 'completed',
      toolName: TOOL_NAME,
      toolCallId: TOOL_CALL_ID,
      args: { topic: 'quantum' },
      agentId: AGENT_ID,
      runId: RUN_ID,
      result: authoritativeResult,
      createdAt: new Date('2026-09-21T12:00:00.000Z'),
      startedAt: new Date('2026-09-21T12:00:01.000Z'),
      completedAt: new Date('2026-09-21T12:00:02.000Z'),
      retryCount: 0,
      maxRetries: 0,
      timeoutMs: 30_000,
    } as any;

    vi.mocked(resolveBackgroundConfig).mockReturnValue({
      runInBackground: true,
      disposition: 'awaited',
      timeoutMs: 30_000,
      maxRetries: 0,
    } as any);

    const dispatch = vi.fn();
    const checkIfRunning = vi.fn();
    const waitForCompletion = vi.fn().mockResolvedValue(terminalTask);
    vi.mocked(createBackgroundTask).mockImplementation(
      () =>
        ({
          dispatch,
          checkIfExisting: vi.fn().mockResolvedValue(terminalTask),
          checkIfRunning,
          restart: vi.fn(),
          task: terminalTask,
          cancel: vi.fn(),
          waitForCompletion,
        }) as any,
    );

    const result = await executeStep(pubsub, initData);

    expect(dispatch).not.toHaveBeenCalled();
    expect(checkIfRunning).not.toHaveBeenCalled();
    // See the awaited-result test: fork's abort transport installs the signal.
    // Fork (PF): the awaited disposition also observes a suspended task (the
    // main loop's includeSuspended contract) so it can suspend the parent.
    expect(waitForCompletion).toHaveBeenCalledWith({
      abortSignal: (globalRunRegistry.get(RUN_ID) as any).abortSignal,
      includeSuspended: true,
    });
    expect(result.result).toEqual(authoritativeResult);
    expect(result.providerMetadata).toMatchObject({
      mastra: { backgroundTask: { taskId: terminalTask.id, status: 'completed' } },
    });
    expect(messageList.updateToolInvocation).toHaveBeenCalledTimes(1);
  });

  it('adopts an awaited task that was still pending when the workflow replayed', async () => {
    const pubsub = mockPubsub();
    setupRegistry();
    const initData = makeInitData();
    const authoritativeResult = { summary: 'persisted pending result' };

    vi.mocked(resolveBackgroundConfig).mockReturnValue({
      runInBackground: true,
      disposition: 'awaited',
      timeoutMs: 30_000,
      maxRetries: 1,
    } as any);

    const pendingTask = {
      id: 'task-pending',
      status: 'pending',
      toolName: TOOL_NAME,
      toolCallId: TOOL_CALL_ID,
      args: { topic: 'quantum' },
      agentId: AGENT_ID,
      runId: RUN_ID,
      createdAt: new Date('2026-09-21T12:00:00.000Z'),
      retryCount: 0,
      maxRetries: 1,
      timeoutMs: 30_000,
    } as any;
    const dispatch = vi.fn().mockResolvedValue({ task: { id: 'task-duplicate' }, fallbackToSync: false });
    const restart = vi.fn().mockResolvedValue(pendingTask);
    vi.mocked(createBackgroundTask).mockImplementation(
      (_manager: any, options: any) =>
        ({
          dispatch,
          checkIfExisting: vi.fn().mockResolvedValue(pendingTask),
          checkIfRunning: vi.fn().mockResolvedValue(false),
          restart,
          task: pendingTask,
          cancel: vi.fn(),
          waitForCompletion: vi.fn(async () => {
            await options.context.onResult({
              runId: RUN_ID,
              taskId: 'task-pending',
              toolCallId: TOOL_CALL_ID,
              toolName: TOOL_NAME,
              agentId: AGENT_ID,
              result: authoritativeResult,
              status: 'completed',
              startedAt: new Date(),
              completedAt: new Date(),
            });
            return {
              id: 'task-pending',
              status: 'completed',
              result: authoritativeResult,
            };
          }),
        }) as any,
    );

    const result = await executeStep(pubsub, initData);

    expect(dispatch).not.toHaveBeenCalled();
    expect(restart).toHaveBeenCalledTimes(1);
    expect(result.result).toEqual(authoritativeResult);
    expect(result.providerMetadata).toMatchObject({
      mastra: { backgroundTask: { taskId: 'task-pending', status: 'completed' } },
    });
  });

  it('fails closed when an adopted awaited task cannot be restarted', async () => {
    const pubsub = mockPubsub();
    setupRegistry();
    const initData = makeInitData();
    const runningTask = {
      id: 'task-running',
      status: 'running',
      toolName: TOOL_NAME,
      toolCallId: TOOL_CALL_ID,
      args: { topic: 'quantum' },
      agentId: AGENT_ID,
      runId: RUN_ID,
      createdAt: new Date('2026-09-21T12:00:00.000Z'),
      startedAt: new Date('2026-09-21T12:00:01.000Z'),
      retryCount: 0,
      maxRetries: 1,
      timeoutMs: 30_000,
    } as any;

    vi.mocked(resolveBackgroundConfig).mockReturnValue({
      runInBackground: true,
      disposition: 'awaited',
      timeoutMs: 30_000,
      maxRetries: 1,
    } as any);

    const dispatch = vi.fn();
    vi.mocked(createBackgroundTask).mockReturnValue({
      dispatch,
      checkIfExisting: vi.fn().mockResolvedValue(runningTask),
      restart: vi.fn().mockRejectedValue(new Error('restart failed')),
      task: runningTask,
      cancel: vi.fn(),
      waitForCompletion: vi.fn(),
    } as any);

    await expect(executeStep(pubsub, initData)).rejects.toThrow('restart failed');
    expect(dispatch).not.toHaveBeenCalled();
  });

  it('reconciles an adopted task that becomes terminal before restart', async () => {
    const pubsub = mockPubsub();
    setupRegistry();
    const initData = makeInitData();
    const authoritativeResult = { summary: 'raced terminal result' };
    const runningTask = {
      id: 'task-raced',
      status: 'running',
      toolName: TOOL_NAME,
      toolCallId: TOOL_CALL_ID,
      args: { topic: 'quantum' },
      agentId: AGENT_ID,
      runId: RUN_ID,
      createdAt: new Date('2026-09-21T12:00:00.000Z'),
      startedAt: new Date('2026-09-21T12:00:01.000Z'),
      retryCount: 0,
      maxRetries: 1,
      timeoutMs: 30_000,
    } as any;
    const completedTask = {
      ...runningTask,
      status: 'completed',
      result: authoritativeResult,
      completedAt: new Date('2026-09-21T12:00:02.000Z'),
    } as any;

    vi.mocked(resolveBackgroundConfig).mockReturnValue({
      runInBackground: true,
      disposition: 'awaited',
      timeoutMs: 30_000,
      maxRetries: 1,
    } as any);

    const dispatch = vi.fn();
    vi.mocked(createBackgroundTask).mockReturnValue({
      dispatch,
      checkIfExisting: vi.fn().mockResolvedValueOnce(runningTask).mockResolvedValueOnce(completedTask),
      restart: vi.fn().mockRejectedValue(new Error("Cannot restart task in status 'completed'")),
      task: completedTask,
      cancel: vi.fn(),
      waitForCompletion: vi.fn().mockResolvedValue(completedTask),
    } as any);

    const result = await executeStep(pubsub, initData);

    expect(dispatch).not.toHaveBeenCalled();
    expect(result.result).toEqual(authoritativeResult);
    expect(result.providerMetadata).toMatchObject({
      mastra: { backgroundTask: { taskId: 'task-raced', status: 'completed' } },
    });
  });

  it.each([
    { status: 'failed', error: { message: 'persisted failure' }, expectedError: 'persisted failure' },
    { status: 'cancelled', error: undefined, expectedError: 'Background task cancelled: task-cancelled' },
    { status: 'timed_out', error: undefined, expectedError: 'Background task timed out: task-timed_out' },
  ])('propagates a persisted awaited $status task without dispatching a duplicate', async testCase => {
    const pubsub = mockPubsub();
    const { messageList } = setupRegistry();
    const initData = makeInitData();
    const terminalTask = {
      id: `task-${testCase.status}`,
      status: testCase.status,
      toolName: TOOL_NAME,
      toolCallId: TOOL_CALL_ID,
      args: { topic: 'quantum' },
      agentId: AGENT_ID,
      runId: RUN_ID,
      error: testCase.error,
      createdAt: new Date('2026-09-21T12:00:00.000Z'),
      startedAt: new Date('2026-09-21T12:00:01.000Z'),
      completedAt: new Date('2026-09-21T12:00:02.000Z'),
      retryCount: 0,
      maxRetries: 0,
      timeoutMs: 30_000,
    } as any;

    vi.mocked(resolveBackgroundConfig).mockReturnValue({
      runInBackground: true,
      disposition: 'awaited',
      timeoutMs: 30_000,
      maxRetries: 0,
    } as any);

    const dispatch = vi.fn();
    vi.mocked(createBackgroundTask).mockReturnValue({
      dispatch,
      checkIfExisting: vi.fn().mockResolvedValue(terminalTask),
      checkIfRunning: vi.fn(),
      restart: vi.fn(),
      task: terminalTask,
      cancel: vi.fn(),
      waitForCompletion: vi.fn().mockResolvedValue(terminalTask),
    } as any);

    await expect(executeStep(pubsub, initData)).rejects.toThrow(testCase.expectedError);
    expect(dispatch).not.toHaveBeenCalled();
    expect(messageList.updateToolInvocation).toHaveBeenCalledTimes(1);
    expect(messageList.updateToolInvocation).toHaveBeenCalledWith(
      expect.objectContaining({
        toolInvocation: expect.objectContaining({
          state: 'output-error',
          errorText: `Background task failed: ${testCase.expectedError}`,
        }),
      }),
      expect.anything(),
    );
  });

  it('does not reconcile an awaited terminal task twice after the transcript was already persisted', async () => {
    const pubsub = mockPubsub();
    const { messageList, saveQueueManager } = setupRegistry();
    const initData = makeInitData();
    const authoritativeResult = { summary: 'persisted result' };
    const terminalTask = {
      id: 'task-terminal',
      status: 'completed',
      toolName: TOOL_NAME,
      toolCallId: TOOL_CALL_ID,
      args: { topic: 'quantum' },
      agentId: AGENT_ID,
      runId: RUN_ID,
      result: authoritativeResult,
      createdAt: new Date('2026-09-21T12:00:00.000Z'),
      startedAt: new Date('2026-09-21T12:00:01.000Z'),
      completedAt: new Date('2026-09-21T12:00:02.000Z'),
      retryCount: 0,
      maxRetries: 0,
      timeoutMs: 30_000,
    } as any;
    messageList.get.all.db.mockReturnValue([
      {
        role: 'assistant',
        content: {
          parts: [
            {
              type: 'tool-invocation',
              toolInvocation: {
                state: 'result',
                toolCallId: TOOL_CALL_ID,
                toolName: TOOL_NAME,
                args: { topic: 'quantum' },
                result: authoritativeResult,
              },
              providerMetadata: {
                mastra: { backgroundTask: { taskId: terminalTask.id, status: 'completed' } },
              },
            },
          ],
        },
      },
    ]);

    vi.mocked(resolveBackgroundConfig).mockReturnValue({
      runInBackground: true,
      disposition: 'awaited',
      timeoutMs: 30_000,
      maxRetries: 0,
    } as any);

    const dispatch = vi.fn();
    vi.mocked(createBackgroundTask).mockReturnValue({
      dispatch,
      checkIfExisting: vi.fn().mockResolvedValue(terminalTask),
      checkIfRunning: vi.fn(),
      restart: vi.fn(),
      task: terminalTask,
      cancel: vi.fn(),
      waitForCompletion: vi.fn().mockResolvedValue(terminalTask),
    } as any);

    const result = await executeStep(pubsub, initData);

    expect(result.result).toEqual(authoritativeResult);
    expect(dispatch).not.toHaveBeenCalled();
    expect(messageList.updateToolInvocation).not.toHaveBeenCalled();
    expect(messageList.add).not.toHaveBeenCalled();
    expect(saveQueueManager.flushMessages).not.toHaveBeenCalled();
  });

  it('skips a transcript part from a different background task and applies the reconciled result', async () => {
    // Providers reuse tool-call ids ("call_0", "call_1", ...) on every turn,
    // so a memory-backed transcript legitimately contains parts with the same
    // toolCallId but a different taskId — earlier dispatches, not conflicts.
    // The identity scan must key on (toolCallId, taskId) and skip foreign
    // parts instead of failing closed.
    const pubsub = mockPubsub();
    const { messageList } = setupRegistry();
    const initData = makeInitData();
    const authoritativeResult = { summary: 'persisted result' };
    const terminalTask = {
      id: 'task-terminal',
      status: 'completed',
      toolName: TOOL_NAME,
      toolCallId: TOOL_CALL_ID,
      args: { topic: 'quantum' },
      agentId: AGENT_ID,
      runId: RUN_ID,
      result: authoritativeResult,
      createdAt: new Date('2026-09-21T12:00:00.000Z'),
      startedAt: new Date('2026-09-21T12:00:01.000Z'),
      completedAt: new Date('2026-09-21T12:00:02.000Z'),
      retryCount: 0,
      maxRetries: 0,
      timeoutMs: 30_000,
    } as any;
    messageList.get.all.db.mockReturnValue([
      {
        role: 'assistant',
        content: {
          parts: [
            {
              type: 'tool-invocation',
              toolInvocation: {
                state: 'result',
                toolCallId: TOOL_CALL_ID,
                toolName: TOOL_NAME,
                args: { topic: 'quantum' },
                result: { summary: 'different result' },
              },
              providerMetadata: {
                mastra: { backgroundTask: { taskId: 'task-other', status: 'completed' } },
              },
            },
          ],
        },
      },
    ]);

    vi.mocked(resolveBackgroundConfig).mockReturnValue({
      runInBackground: true,
      disposition: 'awaited',
      timeoutMs: 30_000,
      maxRetries: 0,
    } as any);

    const dispatch = vi.fn();
    vi.mocked(createBackgroundTask).mockReturnValue({
      dispatch,
      checkIfExisting: vi.fn().mockResolvedValue(terminalTask),
      checkIfRunning: vi.fn(),
      restart: vi.fn(),
      task: terminalTask,
      cancel: vi.fn(),
      waitForCompletion: vi.fn().mockResolvedValue(terminalTask),
    } as any);

    const result = await executeStep(pubsub, initData);

    // The reconciled result is returned and written as this dispatch's own
    // record; the foreign part is left untouched and nothing re-dispatches.
    expect(result.result).toEqual(authoritativeResult);
    expect(dispatch).not.toHaveBeenCalled();
    expect(messageList.updateToolInvocation).toHaveBeenCalledTimes(1);
    expect(messageList.updateToolInvocation).toHaveBeenCalledWith(
      expect.objectContaining({
        toolInvocation: expect.objectContaining({ toolCallId: TOOL_CALL_ID, result: authoritativeResult }),
      }),
      expect.objectContaining({
        backgroundTasks: { [TOOL_CALL_ID]: expect.objectContaining({ taskId: 'task-terminal' }) },
      }),
    );
  });

  it('fails closed when the persisted transcript contains a conflicting terminal status', async () => {
    const pubsub = mockPubsub();
    const { messageList } = setupRegistry();
    const initData = makeInitData();
    const terminalTask = {
      id: 'task-terminal',
      status: 'completed',
      toolName: TOOL_NAME,
      toolCallId: TOOL_CALL_ID,
      args: { topic: 'quantum' },
      agentId: AGENT_ID,
      runId: RUN_ID,
      result: { summary: 'persisted result' },
      createdAt: new Date('2026-09-21T12:00:00.000Z'),
      startedAt: new Date('2026-09-21T12:00:01.000Z'),
      completedAt: new Date('2026-09-21T12:00:02.000Z'),
      retryCount: 0,
      maxRetries: 0,
      timeoutMs: 30_000,
    } as any;
    messageList.get.all.db.mockReturnValue([
      {
        role: 'assistant',
        content: {
          parts: [
            {
              type: 'tool-invocation',
              toolInvocation: {
                state: 'result',
                toolCallId: TOOL_CALL_ID,
                toolName: TOOL_NAME,
                args: { topic: 'quantum' },
                result: { summary: 'different result' },
              },
              providerMetadata: {
                mastra: { backgroundTask: { taskId: 'task-terminal', status: 'failed' } },
              },
            },
          ],
        },
      },
    ]);

    vi.mocked(resolveBackgroundConfig).mockReturnValue({
      runInBackground: true,
      disposition: 'awaited',
      timeoutMs: 30_000,
      maxRetries: 0,
    } as any);

    const dispatch = vi.fn();
    vi.mocked(createBackgroundTask).mockReturnValue({
      dispatch,
      checkIfExisting: vi.fn().mockResolvedValue(terminalTask),
      checkIfRunning: vi.fn(),
      restart: vi.fn(),
      task: terminalTask,
      cancel: vi.fn(),
      waitForCompletion: vi.fn().mockResolvedValue(terminalTask),
    } as any);

    await expect(executeStep(pubsub, initData)).rejects.toThrow(
      'Background task status conflict for task "task-terminal": expected "completed", found "failed"',
    );
    expect(dispatch).not.toHaveBeenCalled();
    expect(messageList.updateToolInvocation).not.toHaveBeenCalled();
  });

  it.each([
    { outcome: 'resumed', resumeData: false, checkIfSuspended: true, checkIfRunning: false },
    { outcome: 'restarted', resumeData: undefined, checkIfSuspended: false, checkIfRunning: true },
  ])('waits for an awaited $outcome task and returns its authoritative result', async testCase => {
    const pubsub = mockPubsub();
    const { messageList } = setupRegistry();
    const initData = makeInitData();
    const taskId = `task-${testCase.outcome}`;
    const authoritativeResult = { summary: `${testCase.outcome} result` };

    vi.mocked(resolveBackgroundConfig).mockReturnValue({
      runInBackground: true,
      disposition: 'awaited',
      timeoutMs: 30_000,
      maxRetries: 0,
    } as any);

    let capturedOnResult: any;
    const resume = vi.fn().mockResolvedValue({ id: taskId });
    const restart = vi.fn().mockResolvedValue({ id: taskId });
    const dispatch = vi.fn();
    const waitForCompletion = vi.fn(async () => {
      await capturedOnResult({
        runId: RUN_ID,
        taskId,
        toolCallId: TOOL_CALL_ID,
        toolName: TOOL_NAME,
        agentId: AGENT_ID,
        result: authoritativeResult,
        status: 'completed',
        startedAt: new Date(),
        completedAt: new Date(),
      });
      return { id: taskId, status: 'completed', result: authoritativeResult };
    });
    vi.mocked(createBackgroundTask).mockImplementation((_mgr: any, opts: any) => {
      capturedOnResult = opts.context.onResult;
      return {
        dispatch,
        resume,
        checkIfExisting: vi.fn().mockResolvedValue(undefined),
        checkIfSuspended: vi.fn().mockResolvedValue(testCase.checkIfSuspended),
        checkIfRunning: vi.fn().mockResolvedValue(testCase.checkIfRunning),
        restart,
        task: { id: taskId },
        cancel: vi.fn(),
        waitForCompletion,
      } as any;
    });

    // Fork's resume-evidence envelope authenticates the workflow-owned
    // resume leg (the resumed case carries a falsy payload; the restarted
    // case has no resume payload and simply passes the fence's identity
    // check before its restart path).
    const result = await executeStep(pubsub, initData, undefined, testCase.resumeData, {
      version: 1,
      type: 'suspension',
      runId: RUN_ID,
      iterationCount: 0,
      stepId: 'durable-tool-call',
      toolCallId: TOOL_CALL_ID,
      toolName: TOOL_NAME,
      identityDigest: createToolCallIdentityDigest({
        toolCallId: TOOL_CALL_ID,
        toolName: TOOL_NAME,
        args: baseInput().args,
      }),
    });

    // See the awaited-result test: fork's abort transport installs the signal.
    // Fork (PF): the awaited disposition also observes a suspended task (the
    // main loop's includeSuspended contract) so it can suspend the parent.
    expect(waitForCompletion).toHaveBeenCalledWith({
      abortSignal: (globalRunRegistry.get(RUN_ID) as any).abortSignal,
      includeSuspended: true,
    });
    expect(result.result).toEqual(authoritativeResult);
    expect(result.providerMetadata).toMatchObject({
      mastra: { backgroundTask: { taskId, status: 'completed' } },
    });
    expect(messageList.updateToolInvocation).toHaveBeenCalledTimes(1);
    expect(dispatch).not.toHaveBeenCalled();
    if (testCase.outcome === 'resumed') {
      expect(resume).toHaveBeenCalledWith(testCase.resumeData);
      expect(restart).not.toHaveBeenCalled();
    } else {
      expect(restart).toHaveBeenCalledTimes(1);
      expect(resume).not.toHaveBeenCalled();
    }
  });

  it('passes background work identity and disposition to the durable task executor', async () => {
    const pubsub = mockPubsub();
    const { entry } = setupRegistry();
    const initData = makeInitData();

    vi.mocked(resolveBackgroundConfig).mockReturnValue({
      runInBackground: true,
      disposition: 'awaited',
      timeoutMs: 30_000,
      maxRetries: 0,
    } as any);

    let capturedContext: any;
    vi.mocked(createBackgroundTask).mockImplementation((_mgr: any, opts: any) => {
      capturedContext = opts.context;
      return {
        dispatch: vi.fn().mockResolvedValue({ task: { id: 'task-context' }, fallbackToSync: false }),
        checkIfExisting: vi.fn().mockResolvedValue(undefined),
        checkIfRunning: vi.fn().mockResolvedValue(false),
        restart: vi.fn(),
        task: { id: 'task-context' },
        cancel: vi.fn(),
        waitForCompletion: vi.fn(() => new Promise(() => {})),
      } as any;
    });

    const execution = executeStep(pubsub, initData);
    await vi.waitFor(() => expect(capturedContext).toBeDefined());
    await capturedContext.executor.execute({ topic: 'quantum' }, {});

    expect(entry.tools[TOOL_NAME].execute).toHaveBeenCalledWith(
      { topic: 'quantum' },
      expect.objectContaining({
        isBackgroundTask: true,
        [BACKGROUND_WORK_CONTEXT]: {
          originRunId: RUN_ID,
          originToolCallId: TOOL_CALL_ID,
          taskId: 'task-context',
          invocationKind: 'tool',
          disposition: 'awaited',
        },
      }),
    );

    // The awaited step is intentionally still blocked because this test only
    // exercises the executor context, not terminal task reconciliation.
    void execution;
  });

  it('returns an awaited cancellation without waiting for result reconciliation', async () => {
    const pubsub = mockPubsub();
    const execute = vi.fn().mockResolvedValue({ summary: 'sync fallback' });
    setupRegistry({
      tools: {
        [TOOL_NAME]: {
          execute,
          backgroundConfig: { enabled: true },
        },
      },
    });
    const initData = makeInitData();

    vi.mocked(resolveBackgroundConfig).mockReturnValue({
      runInBackground: true,
      disposition: 'awaited',
      timeoutMs: 30_000,
      maxRetries: 0,
    } as any);

    const waitForCompletion = vi.fn().mockResolvedValue({ id: 'task-cancelled', status: 'cancelled' });
    vi.mocked(createBackgroundTask).mockReturnValue({
      dispatch: vi.fn().mockResolvedValue({ task: { id: 'task-cancelled' }, fallbackToSync: false }),
      checkIfExisting: vi.fn().mockResolvedValue(null),
      checkIfRunning: vi.fn().mockResolvedValue(false),
      restart: vi.fn(),
      task: { id: 'task-cancelled' },
      cancel: vi.fn(),
      waitForCompletion,
    } as any);

    await expect(executeStep(pubsub, initData)).rejects.toThrow('Background task cancelled: task-cancelled');
    expect(execute).not.toHaveBeenCalled();
  });

  it('exposes the adoption bridge to durable background tools and waits for completion', async () => {
    const pubsub = mockPubsub();
    let resolveCompletion!: (result: { summary: string }) => void;
    const completion = new Promise<{ summary: string }>(resolve => {
      resolveCompletion = resolve;
    });
    let toolOptions: any;
    const execute = vi.fn(async (_args: unknown, options: any) => {
      toolOptions = options;
      options.background.adopt({ completion });
      return { summary: 'acknowledged' };
    });
    setupRegistry({
      tools: {
        [TOOL_NAME]: {
          execute,
          backgroundConfig: { enabled: true },
        },
      },
    });
    const initData = makeInitData();

    vi.mocked(resolveBackgroundConfig).mockReturnValue({
      runInBackground: true,
      disposition: 'deferred',
      timeoutMs: 30_000,
      maxRetries: 0,
    } as any);

    let capturedExecutor: any;
    const mockTask = { id: 'task-adopted' };
    vi.mocked(createBackgroundTask).mockImplementation((_manager: any, options: any) => {
      capturedExecutor = options.context.executor;
      return {
        dispatch: vi.fn().mockResolvedValue({ task: mockTask, fallbackToSync: false }),
        checkIfRunning: vi.fn().mockResolvedValue(false),
        restart: vi.fn(),
        task: mockTask,
        cancel: vi.fn(),
        waitForCompletion: vi.fn(),
      } as any;
    });

    await executeStep(pubsub, initData);
    let settled = false;
    const executorResult = capturedExecutor.execute(baseInput().args).finally(() => {
      settled = true;
    });
    await vi.waitFor(() => expect(execute).toHaveBeenCalledOnce());

    expect(settled).toBe(false);
    expect(toolOptions.isBackgroundTask).toBe(true);
    expect(toolOptions.background).toMatchObject({
      taskId: 'task-adopted',
      disposition: 'deferred',
    });

    resolveCompletion({ summary: 'finished' });
    await expect(executorResult).resolves.toEqual({ summary: 'finished' });
  });

  it('validates adopted durable background results against the tool output schema', async () => {
    const pubsub = mockPubsub();
    setupRegistry({
      tools: {
        [TOOL_NAME]: {
          backgroundConfig: { enabled: true },
          outputSchema: z.object({ summary: z.number() }),
          execute: vi.fn(async (_args: unknown, options: any) => {
            options.background.adopt({ completion: Promise.resolve({ summary: 'invalid' }) });
            return { summary: 42 };
          }),
        },
      },
    });
    const initData = makeInitData();

    vi.mocked(resolveBackgroundConfig).mockReturnValue({
      runInBackground: true,
      disposition: 'deferred',
      timeoutMs: 30_000,
      maxRetries: 0,
    } as any);

    let capturedExecutor: any;
    const mockTask = { id: 'task-invalid-adopted' };
    vi.mocked(createBackgroundTask).mockImplementation((_manager: any, options: any) => {
      capturedExecutor = options.context.executor;
      return {
        dispatch: vi.fn().mockResolvedValue({ task: mockTask, fallbackToSync: false }),
        checkIfRunning: vi.fn().mockResolvedValue(false),
        restart: vi.fn(),
        task: mockTask,
        cancel: vi.fn(),
        waitForCompletion: vi.fn(),
      } as any;
    });

    await executeStep(pubsub, initData);
    await expect(capturedExecutor.execute(baseInput().args)).resolves.toMatchObject({
      error: true,
      message: expect.stringContaining(`Tool output validation failed for ${TOOL_NAME}`),
    });
  });

  it('falls back to sync execution when fallbackToSync is true', async () => {
    const pubsub = mockPubsub();
    const { entry: _entry } = setupRegistry();
    const initData = makeInitData();

    vi.mocked(resolveBackgroundConfig).mockReturnValue({
      runInBackground: true,
      timeoutMs: 30_000,
      maxRetries: 0,
    } as any);

    vi.mocked(createBackgroundTask).mockReturnValue({
      dispatch: vi.fn().mockResolvedValue({ task: { id: 't1' }, fallbackToSync: true }),
      checkIfRunning: vi.fn().mockResolvedValue(false),
      restart: vi.fn(),
      task: { id: 't1' },
      cancel: vi.fn(),
      waitForCompletion: vi.fn(),
    } as any);

    const result = await executeStep(pubsub, initData);

    // Should have fallen through to synchronous execution
    expect(result.result).toEqual({ summary: 'done' });
  });

  it('falls back to sync execution when dispatch throws', async () => {
    const pubsub = mockPubsub();
    setupRegistry();
    const initData = makeInitData();

    vi.mocked(resolveBackgroundConfig).mockReturnValue({
      runInBackground: true,
      timeoutMs: 30_000,
      maxRetries: 0,
    } as any);

    vi.mocked(createBackgroundTask).mockReturnValue({
      dispatch: vi.fn().mockRejectedValue(new Error('dispatch boom')),
      checkIfRunning: vi.fn().mockResolvedValue(false),
      restart: vi.fn(),
      task: { id: 't1' } as any,
      cancel: vi.fn(),
      waitForCompletion: vi.fn(),
    } as any);

    const result = await executeStep(pubsub, initData);

    // Fell through to sync, tool executed normally
    expect(result.result).toEqual({ summary: 'done' });
  });

  it('emits background-task-started chunk via PubSub after successful dispatch', async () => {
    const pubsub = mockPubsub();
    setupRegistry();
    const initData = makeInitData();

    vi.mocked(resolveBackgroundConfig).mockReturnValue({
      runInBackground: true,
      timeoutMs: 30_000,
      maxRetries: 0,
    } as any);

    vi.mocked(createBackgroundTask).mockReturnValue({
      dispatch: vi.fn().mockResolvedValue({ task: { id: 'task-x' }, fallbackToSync: false }),
      checkIfRunning: vi.fn().mockResolvedValue(false),
      restart: vi.fn(),
      task: { id: 'task-x' },
      cancel: vi.fn(),
      waitForCompletion: vi.fn(),
    } as any);

    await executeStep(pubsub, initData);

    expect(vi.mocked(emitChunkEvent)).toHaveBeenCalledWith(
      pubsub,
      RUN_ID,
      expect.objectContaining({
        type: 'background-task-started',
        payload: expect.objectContaining({
          taskId: 'task-x',
          toolName: TOOL_NAME,
          toolCallId: TOOL_CALL_ID,
        }),
      }),
    );
  });

  it('keeps the task dispatched when status chunk emission fails', async () => {
    const pubsub = mockPubsub();
    const execute = vi.fn().mockResolvedValue({ summary: 'sync fallback' });
    setupRegistry({
      tools: {
        [TOOL_NAME]: {
          execute,
          backgroundConfig: { enabled: true },
        },
      },
    });
    const initData = makeInitData();

    vi.mocked(resolveBackgroundConfig).mockReturnValue({
      runInBackground: true,
      timeoutMs: 30_000,
      maxRetries: 0,
    } as any);
    vi.mocked(emitChunkEvent).mockRejectedValueOnce(new Error('status chunk failed'));
    vi.mocked(createBackgroundTask).mockReturnValue({
      dispatch: vi.fn().mockResolvedValue({ task: { id: 'task-x' }, fallbackToSync: false }),
      checkIfRunning: vi.fn().mockResolvedValue(false),
      restart: vi.fn(),
      task: { id: 'task-x' },
      cancel: vi.fn(),
      waitForCompletion: vi.fn(),
    } as any);

    await expect(executeStep(pubsub, initData)).resolves.toEqual(
      expect.objectContaining({
        result: expect.stringContaining('Background task started'),
      }),
    );
    expect(execute).not.toHaveBeenCalled();
  });

  it.each([
    { status: 'completed', existingInvocation: true },
    { status: 'failed', existingInvocation: true },
    { status: 'completed', existingInvocation: false },
    { status: 'failed', existingInvocation: false },
  ])(
    'onResult persists $status metadata (existing invocation: $existingInvocation)',
    async ({ status, existingInvocation }) => {
      const pubsub = mockPubsub();
      const { messageList, saveQueueManager } = setupRegistry();
      const initData = makeInitData();
      messageList.updateToolInvocation.mockReturnValue(existingInvocation);
      const providerMetadata = { vendor: { trace: 'keep' }, mastra: { custom: 'keep' } };

      let capturedOnResult: any;
      vi.mocked(resolveBackgroundConfig).mockReturnValue({
        runInBackground: true,
        timeoutMs: 30_000,
        maxRetries: 0,
      } as any);

      vi.mocked(createBackgroundTask).mockImplementation((_mgr: any, opts: any) => {
        capturedOnResult = opts.context.onResult;
        return {
          dispatch: vi.fn().mockResolvedValue({ task: { id: 't-r' }, fallbackToSync: false }),
          checkIfRunning: vi.fn().mockResolvedValue(false),
          restart: vi.fn(),
          task: { id: 't-r' },
          cancel: vi.fn(),
          waitForCompletion: vi.fn(),
        } as any;
      });

      await executeStep(pubsub, initData, { ...baseInput(), providerMetadata });

      // Simulate bg task completion
      await capturedOnResult({
        runId: RUN_ID,
        taskId: 't-r',
        toolCallId: TOOL_CALL_ID,
        toolName: TOOL_NAME,
        agentId: AGENT_ID,
        result: { summary: 'real result' },
        status,
        error: status === 'failed' ? { message: 'boom' } : undefined,
        startedAt: new Date(),
        completedAt: new Date(),
      });

      expect(messageList.updateToolInvocation).toHaveBeenCalledWith(
        expect.objectContaining({
          type: 'tool-invocation',
          providerMetadata: {
            vendor: { trace: 'keep' },
            mastra: { custom: 'keep', modelOutput: null, backgroundTask: { taskId: 't-r', status } },
          },
          toolInvocation: expect.objectContaining({
            toolCallId: TOOL_CALL_ID,
            ...(status === 'failed'
              ? { state: 'output-error', errorText: 'Background task failed: boom' }
              : { state: 'result', result: { summary: 'real result' } }),
          }),
        }),
        expect.objectContaining({
          backgroundTasks: expect.objectContaining({
            [TOOL_CALL_ID]: expect.objectContaining({ taskId: 't-r' }),
          }),
        }),
      );

      if (!existingInvocation) {
        expect(messageList.add).toHaveBeenCalledWith(
          [
            expect.objectContaining({
              role: 'tool',
              content: [
                expect.objectContaining({
                  type: 'tool-result',
                  providerOptions: {
                    vendor: { trace: 'keep' },
                    mastra: { custom: 'keep', modelOutput: null, backgroundTask: { taskId: 't-r', status } },
                  },
                }),
              ],
            }),
          ],
          'response',
        );
      }
      expect(saveQueueManager.flushMessages).toHaveBeenCalledWith(messageList, 'thread-1', undefined);
    },
  );

  // Fork-main test restored after the PF-4402 upstream merge dropped it.
  it('onResult hook injects real result into MessageList and flushes to memory', async () => {
    const pubsub = mockPubsub();
    const { messageList, saveQueueManager } = setupRegistry();
    const initData = makeInitData();

    let capturedOnResult: any;
    vi.mocked(resolveBackgroundConfig).mockReturnValue({
      runInBackground: true,
      timeoutMs: 30_000,
      maxRetries: 0,
    } as any);

    vi.mocked(createBackgroundTask).mockImplementation((_mgr: any, opts: any) => {
      capturedOnResult = opts.context.onResult;
      return {
        dispatch: vi.fn().mockResolvedValue({ task: { id: 't-r' }, fallbackToSync: false }),
        checkIfRunning: vi.fn().mockResolvedValue(false),
        restart: vi.fn(),
        task: { id: 't-r' },
        cancel: vi.fn(),
        waitForCompletion: vi.fn(),
      } as any;
    });

    await executeStep(pubsub, initData);

    // Simulate bg task completion
    await capturedOnResult({
      runId: RUN_ID,
      taskId: 't-r',
      toolCallId: TOOL_CALL_ID,
      toolName: TOOL_NAME,
      agentId: AGENT_ID,
      result: { summary: 'real result' },
      status: 'completed',
      startedAt: new Date(),
      completedAt: new Date(),
    });

    expect(messageList.updateToolInvocation).toHaveBeenCalledWith(
      expect.objectContaining({
        type: 'tool-invocation',
        toolInvocation: expect.objectContaining({
          state: 'result',
          toolCallId: TOOL_CALL_ID,
          result: { summary: 'real result' },
        }),
      }),
      expect.objectContaining({
        backgroundTasks: expect.objectContaining({
          [TOOL_CALL_ID]: expect.objectContaining({ taskId: 't-r' }),
        }),
      }),
    );

    expect(saveQueueManager.flushMessages).toHaveBeenCalledWith(messageList, 'thread-1', undefined);
  });

  it('onExecution hook updates message metadata with startedAt/taskId', async () => {
    const pubsub = mockPubsub();
    const { messageList } = setupRegistry();
    const initData = makeInitData();

    let capturedOnExecution: any;
    vi.mocked(resolveBackgroundConfig).mockReturnValue({
      runInBackground: true,
      timeoutMs: 30_000,
      maxRetries: 0,
    } as any);

    vi.mocked(createBackgroundTask).mockImplementation((_mgr: any, opts: any) => {
      capturedOnExecution = opts.context.onExecution;
      return {
        dispatch: vi.fn().mockResolvedValue({ task: { id: 't-e' }, fallbackToSync: false }),
        checkIfRunning: vi.fn().mockResolvedValue(false),
        restart: vi.fn(),
        task: { id: 't-e' },
        cancel: vi.fn(),
        waitForCompletion: vi.fn(),
      } as any;
    });

    await executeStep(pubsub, initData);

    const startedAt = new Date();
    await capturedOnExecution({
      runId: RUN_ID,
      taskId: 't-e',
      toolCallId: TOOL_CALL_ID,
      toolName: TOOL_NAME,
      agentId: AGENT_ID,
      startedAt,
    });

    expect(messageList.updateMessageMetadataByToolCallId).toHaveBeenCalledWith(
      TOOL_CALL_ID,
      expect.objectContaining({
        backgroundTasks: expect.objectContaining({
          [TOOL_CALL_ID]: expect.objectContaining({
            startedAt,
            taskId: 't-e',
          }),
        }),
      }),
    );
    expect(messageList.updateToolInvocation).not.toHaveBeenCalled();
  });

  it('onChunk emits tool-call + tool-result chunks via PubSub on completion', async () => {
    const pubsub = mockPubsub();
    setupRegistry();
    const initData = makeInitData();

    let capturedOnChunk: any;
    vi.mocked(resolveBackgroundConfig).mockReturnValue({
      runInBackground: true,
      timeoutMs: 30_000,
      maxRetries: 0,
    } as any);

    vi.mocked(createBackgroundTask).mockImplementation((_mgr: any, opts: any) => {
      capturedOnChunk = opts.context.onChunk;
      return {
        dispatch: vi.fn().mockResolvedValue({ task: { id: 't-c' }, fallbackToSync: false }),
        checkIfRunning: vi.fn().mockResolvedValue(false),
        restart: vi.fn(),
        task: { id: 't-c' },
        cancel: vi.fn(),
        waitForCompletion: vi.fn(),
      } as any;
    });

    await executeStep(pubsub, initData);
    vi.mocked(emitChunkEvent).mockClear();

    // Simulate bg-task-completed chunk from a different runId (continuation scenario)
    capturedOnChunk({
      type: 'background-task-completed',
      payload: {
        runId: 'run-bg-2',
        taskId: 't-c',
        toolCallId: TOOL_CALL_ID,
        toolName: TOOL_NAME,
        result: { summary: 'done' },
      },
    });

    const calls = vi.mocked(emitChunkEvent).mock.calls;
    const types = calls.map(c => c[2].type);
    expect(types).toContain('tool-call');
    expect(types).toContain('tool-result');
    expect(calls.find(c => c[2].type === 'tool-result')?.[2]).toMatchObject({
      payload: { providerMetadata: { mastra: { backgroundTask: { taskId: 't-c', status: 'completed' } } } },
    });
  });

  it('onChunk emits tool-call + tool-error chunks via PubSub on failure', async () => {
    const pubsub = mockPubsub();
    setupRegistry();
    const initData = makeInitData();

    let capturedOnChunk: any;
    vi.mocked(resolveBackgroundConfig).mockReturnValue({
      runInBackground: true,
      timeoutMs: 30_000,
      maxRetries: 0,
    } as any);

    vi.mocked(createBackgroundTask).mockImplementation((_mgr: any, opts: any) => {
      capturedOnChunk = opts.context.onChunk;
      return {
        dispatch: vi.fn().mockResolvedValue({ task: { id: 't-f' }, fallbackToSync: false }),
        checkIfRunning: vi.fn().mockResolvedValue(false),
        restart: vi.fn(),
        task: { id: 't-f' },
        cancel: vi.fn(),
        waitForCompletion: vi.fn(),
      } as any;
    });

    await executeStep(pubsub, initData);
    vi.mocked(emitChunkEvent).mockClear();

    capturedOnChunk({
      type: 'background-task-failed',
      payload: {
        runId: 'run-bg-3',
        taskId: 't-f',
        toolCallId: TOOL_CALL_ID,
        toolName: TOOL_NAME,
        error: { message: 'boom' },
      },
    });

    const calls = vi.mocked(emitChunkEvent).mock.calls;
    const types = calls.map(c => c[2].type);
    expect(types).toContain('tool-call');
    expect(types).toContain('tool-error');
    expect(calls.find(c => c[2].type === 'tool-error')?.[2]).toMatchObject({
      payload: { providerMetadata: { mastra: { backgroundTask: { taskId: 't-f', status: 'failed' } } } },
    });
  });

  it('passes threadId and resourceId in the task payload', async () => {
    const pubsub = mockPubsub();
    setupRegistry();
    const initData = makeInitData();

    vi.mocked(resolveBackgroundConfig).mockReturnValue({
      runInBackground: true,
      timeoutMs: 30_000,
      maxRetries: 0,
    } as any);

    vi.mocked(createBackgroundTask).mockReturnValue({
      dispatch: vi.fn().mockResolvedValue({ task: { id: 't-p' }, fallbackToSync: false }),
      checkIfRunning: vi.fn().mockResolvedValue(false),
      restart: vi.fn(),
      task: { id: 't-p' },
      cancel: vi.fn(),
      waitForCompletion: vi.fn(),
    } as any);

    await executeStep(pubsub, initData);

    const callArgs = vi.mocked(createBackgroundTask).mock.calls[0]![1]!;
    expect(callArgs.threadId).toBe('thread-1');
    expect(callArgs.resourceId).toBe('user-1');
  });
});

describe('durable tool-call activeTools enforcement', () => {
  it('rejects Mastra-resolved tools outside activeTools when the run registry is unavailable', async () => {
    const pubsub = mockPubsub();
    const hiddenExecute = vi.fn().mockResolvedValue('hidden');
    vi.mocked(_resolveTool).mockReturnValue({
      execute: hiddenExecute,
    } as any);

    const result = await executeStep(
      pubsub,
      makeInitData({
        options: {
          requireToolApproval: false,
          activeTools: ['allowedTool'],
        },
      }),
      {
        ...baseInput(),
        toolName: 'hiddenTool',
      },
    );

    expect(result.error).toEqual(
      expect.objectContaining({
        name: 'ToolNotFoundError',
        message: expect.stringContaining('Available tools: allowedTool'),
      }),
    );
    expect(hiddenExecute).not.toHaveBeenCalled();
  });
});

describe('durable tool-call background revalidation', () => {
  it('revalidates the hook on each attempt; a mid-flight revocation denies the retry', async () => {
    const pubsub = mockPubsub();
    const { entry } = setupRegistry();
    const toolExecute = entry.tools[TOOL_NAME].execute;

    let granted = true;
    const hook = vi.fn(async () => (granted ? ('allow' as const) : ('deny' as const)));
    // Registry-held live context: the transported request context arrives
    // with functions stripped, exactly like a real worker boundary.
    entry.requestContext = new Map([[ON_BEFORE_TOOL_EXECUTION_KEY, hook]]);

    toolExecute.mockImplementation(async () => {
      // The grant is revoked while attempt 1 is in flight; attempt 1 fails.
      granted = false;
      throw new Error('transient failure');
    });

    vi.mocked(resolveBackgroundConfig).mockReturnValue({
      runInBackground: true,
      timeoutMs: 30_000,
      maxRetries: 1,
    } as any);

    let executor: any;
    let taskSpec: any;
    vi.mocked(createBackgroundTask).mockImplementation((_manager: any, spec: any) => {
      taskSpec = spec;
      executor = spec.context.executor;
      return {
        dispatch: vi.fn().mockResolvedValue({ task: { id: 'task-bg-gate' }, fallbackToSync: false }),
        checkIfRunning: vi.fn().mockResolvedValue(false),
        restart: vi.fn(),
        task: { id: 'task-bg-gate' },
        cancel: vi.fn(),
        waitForCompletion: vi.fn(),
      } as any;
    });

    const result = await executeStep(pubsub, makeInitData());
    expect(result.result).toContain('Background task started');
    expect(executor).toBeDefined();
    // The hook closure cannot survive recovery — the requirement persists on
    // the task so a statically-resolved executor fails closed instead.
    expect(taskSpec?.requiresToolPermissionHook).toBe(true);

    // Attempt 1 — revalidated (grant still held), executes, fails transiently.
    await expect(executor.execute({ topic: 'quantum' }, {})).rejects.toThrow('transient failure');
    expect(toolExecute).toHaveBeenCalledTimes(1);

    // Attempt 2 — grant revoked in between: revalidation denies before any
    // side effect, with the name the bg-task workflow classifies non-retryable.
    await expect(executor.execute({ topic: 'quantum' }, {})).rejects.toMatchObject({
      name: TOOL_PERMISSION_DENIED_ERROR_NAME,
    });
    expect(toolExecute).toHaveBeenCalledTimes(1);
    // Dispatch gate + attempt 1 + attempt 2.
    expect(hook).toHaveBeenCalledTimes(3);
  });
});

describe('durable tool-call background resume with falsy payload (#22363 parity)', () => {
  const setupSuspendedTask = () => {
    const pubsub = mockPubsub();
    setupRegistry();
    const initData = makeInitData();

    vi.mocked(resolveBackgroundConfig).mockReturnValue({
      runInBackground: true,
      timeoutMs: 30_000,
      maxRetries: 0,
    } as any);

    const resume = vi.fn().mockResolvedValue({ id: 'resumed-task' });
    const dispatch = vi.fn().mockResolvedValue({ task: { id: 'brand-new-task' }, fallbackToSync: false });
    vi.mocked(createBackgroundTask).mockReturnValue({
      dispatch,
      resume,
      checkIfSuspended: vi.fn().mockResolvedValue(true),
      checkIfRunning: vi.fn().mockResolvedValue(false),
      restart: vi.fn(),
      task: { id: 'suspended-task' },
      cancel: vi.fn(),
      waitForCompletion: vi.fn(),
    } as any);

    return { pubsub, initData, resume, dispatch };
  };

  // A tool with a primitive resumeSchema can be resumed with `false` / `0` / `''`.
  // Treating those as "no resume data" would dispatch a second task and strand
  // the suspended one (same bug as #22363 in the regular loop). `undefined` is
  // the authenticated no-payload resume: the durable step's suspension envelope
  // proves resume intent, so the ladder must still wake the suspended task.
  it.each([false, 0, '', undefined])(
    'resumes the suspended task instead of dispatching when resumeData is %j',
    async payload => {
      const { pubsub, initData, resume, dispatch } = setupSuspendedTask();

      // Fork's resume-evidence envelope: the durable step authenticates a
      // workflow-owned resume against the persisted suspension record before it
      // may reattach to the suspended background task (hasMatchingWorkflowIdentity).
      const result = await executeStep(pubsub, initData, undefined, payload, {
        version: 1,
        type: 'suspension',
        runId: RUN_ID,
        iterationCount: 0,
        stepId: 'durable-tool-call',
        toolCallId: TOOL_CALL_ID,
        toolName: TOOL_NAME,
        identityDigest: createToolCallIdentityDigest({
          toolCallId: TOOL_CALL_ID,
          toolName: TOOL_NAME,
          args: baseInput().args,
        }),
      });

      expect(resume).toHaveBeenCalledWith(payload);
      expect(dispatch).not.toHaveBeenCalled();
      expect(result.result).toContain('Background task resumed');
      expect(result.providerMetadata).toMatchObject({
        mastra: { backgroundTask: { taskId: 'resumed-task', status: 'running' } },
      });
    },
  );

  it('wakes a suspended awaited task on an authenticated resume with no payload', async () => {
    const pubsub = mockPubsub();
    setupRegistry();
    const initData = makeInitData();
    const authoritativeResult = { summary: 'resumed without payload' };

    vi.mocked(resolveBackgroundConfig).mockReturnValue({
      runInBackground: true,
      disposition: 'awaited',
      timeoutMs: 30_000,
      maxRetries: 0,
    } as any);

    let capturedOnResult: any;
    const resume = vi.fn().mockResolvedValue({ id: 'task-resumed-await' });
    const dispatch = vi.fn();
    const waitForCompletion = vi.fn(async () => {
      await capturedOnResult({
        runId: RUN_ID,
        taskId: 'task-resumed-await',
        toolCallId: TOOL_CALL_ID,
        toolName: TOOL_NAME,
        agentId: AGENT_ID,
        result: authoritativeResult,
        status: 'completed',
        startedAt: new Date(),
        completedAt: new Date(),
      });
      return { id: 'task-resumed-await', status: 'completed', result: authoritativeResult };
    });
    vi.mocked(createBackgroundTask).mockImplementation((_mgr: any, opts: any) => {
      capturedOnResult = opts.context.onResult;
      return {
        dispatch,
        resume,
        checkIfExisting: vi.fn().mockResolvedValue({ id: 'task-suspended', status: 'suspended' }),
        checkIfSuspended: vi.fn().mockResolvedValue(true),
        checkIfRunning: vi.fn().mockResolvedValue(false),
        restart: vi.fn(),
        task: { id: 'task-suspended' },
        cancel: vi.fn(),
        waitForCompletion,
      } as any;
    });

    // Authenticated resume envelope with NO resume payload: the ladder must
    // resume (wake) the suspended awaited task, not reattach it silently —
    // payload presence is not the resume discriminator.
    const result = await executeStep(pubsub, initData, undefined, undefined, {
      version: 1,
      type: 'suspension',
      runId: RUN_ID,
      iterationCount: 0,
      stepId: 'durable-tool-call',
      toolCallId: TOOL_CALL_ID,
      toolName: TOOL_NAME,
      identityDigest: createToolCallIdentityDigest({
        toolCallId: TOOL_CALL_ID,
        toolName: TOOL_NAME,
        args: baseInput().args,
      }),
    });

    expect(resume).toHaveBeenCalledTimes(1);
    expect(resume).toHaveBeenCalledWith(undefined);
    expect(dispatch).not.toHaveBeenCalled();
    expect(result.result).toEqual(authoritativeResult);
    expect(result.providerMetadata).toMatchObject({
      mastra: { backgroundTask: { taskId: 'task-resumed-await', status: 'completed' } },
    });
  });

  it('dispatches a fresh task when resumeData is absent', async () => {
    const { pubsub, initData, resume, dispatch } = setupSuspendedTask();

    const result = await executeStep(pubsub, initData);

    expect(resume).not.toHaveBeenCalled();
    expect(dispatch).toHaveBeenCalled();
    expect(result.result).toContain('Background task started');
  });
});

describe('durable tool-call per-invocation processor data collection (#19375)', () => {
  it('collects each invocation\u2019s processor data parts into its own step output', async () => {
    const pubsub = mockPubsub();
    // Synchronous execution path: the processToolResult writer collects the
    // data parts while the step itself returns the tool result.
    vi.mocked(resolveBackgroundConfig).mockReturnValue({ runInBackground: false } as any);
    // One shared registry entry across two successive invocations: the runner
    // is cached per run, so a writer cached alongside it would retain the
    // first invocation\u2019s collector.
    setupRegistry({
      outputProcessors: [
        {
          id: 'per-invocation-emitter',
          name: 'Per-Invocation Emitter',
          processToolResult: async ({ writer }: any) => {
            await writer?.custom({ type: 'data-invocation-note', data: { note: 'note' } });
          },
        },
      ],
      processorStates: new Map(),
    });
    const initData = makeInitData();

    const first = await executeStep(pubsub, initData);
    expect(first.result).toEqual({ summary: 'done' });
    expect(first.processorDataParts).toEqual([
      { type: 'data-invocation-note', data: { note: 'note' }, messageId: undefined },
    ]);

    // Second tool-call iteration on the same registry entry/pubsub: its
    // data parts must be collected by THIS invocation\u2019s collector, not
    // silently diverted into the first invocation\u2019s (already consumed)
    // array — otherwise the mapping step persists nothing for it.
    const second = await executeStep(pubsub, initData);
    expect(second.processorDataParts).toEqual([
      { type: 'data-invocation-note', data: { note: 'note' }, messageId: undefined },
    ]);
  });
});
