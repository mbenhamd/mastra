/**
 * Reproduction for issue #17218 on the DURABLE agent engine.
 *
 * The durable loop keeps its own copy of the tool-call + mapping steps. Before this fix
 * they had the same write-path gap as the non-durable loop: a declined approval was
 * persisted as a plain successful `result` string (no `output-denied`, no `approval`),
 * and an approval dropped the `approval` field — so neither round-tripped on recall.
 *
 * These tests pin the two changed steps directly (deterministic, no workflow engine):
 *   - createDurableToolCallStep: a decline returns `approval { approved: false }` and NO
 *     `result`; an approve returns the tool result tagged with `approval { approved: true }`.
 *   - createDurableLLMMappingStep: a declined tool result persists as `output-denied` with
 *     `approval`; an approved one persists as `result` carrying `approval` — and both
 *     round-trip to the expected AI SDK v6 UI parts.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { PUBSUB_SYMBOL } from '../../../../workflows/constants';
import type { MastraDBMessage } from '../../../message-list';
import { MessageList } from '../../../message-list';
import { createToolCallIdentityDigest } from '../../../tool-call-identity';
import { globalRunRegistry } from '../../run-registry';
import { toolApprovalRequirement } from '../../utils/resolve-runtime';
import { createDurableLLMMappingStep } from './llm-mapping';
import { createDurableToolCallStep } from './tool-call';

vi.mock('../../../../workflows', () => ({
  createStep: (config: unknown) => config,
}));

vi.mock('../../utils/resolve-runtime', () => ({
  resolveTool: vi.fn(),
  toolApprovalRequirement: vi.fn().mockResolvedValue({ required: true, reasons: ['tool-config'] }),
}));

vi.mock('../../stream-adapter', () => ({
  emitChunkEvent: vi.fn().mockResolvedValue(undefined),
  emitSuspendedEvent: vi.fn().mockResolvedValue(undefined),
}));

const RUN_ID = 'run-approval-1';
const RUNTIME_BINDING_ID = 'binding-approval-1';
const AGENT_ID = 'agent-1';
const TOOL_NAME = 'findUserTool';
const TOOL_CALL_ID = 'call-1';
const THREAD_ID = 'thread-1';
const RESOURCE_ID = 'user-1';
const DECLINE_REASON = 'Tool call was not approved by the user';
const TOOL_ARGS = { name: 'Dero Israel' };
const TOOL_RESULT = { name: 'Dero Israel', email: 'dero@mail.com' };

function mockPubsub() {
  return { publish: vi.fn(), subscribe: vi.fn(), unsubscribe: vi.fn(), flush: vi.fn() };
}

function makeInitData() {
  return {
    runId: RUN_ID,
    runtimeBindingId: RUNTIME_BINDING_ID,
    agentId: AGENT_ID,
    options: { requireToolApproval: true },
    state: { threadId: THREAD_ID, resourceId: RESOURCE_ID, memoryConfig: undefined, threadExists: true },
  };
}

function setupRegistry(execute: (...args: any[]) => any) {
  globalRunRegistry.set(RUN_ID, {
    runtimeBindingId: RUNTIME_BINDING_ID,
    tools: { [TOOL_NAME]: { execute } },
    requireToolApproval: true,
    model: {} as any,
  } as any);
}

function makeSuspendEnvelope(overrides: Record<string, unknown> = {}) {
  return {
    version: 1,
    type: 'approval',
    approvalSource: 'tool-gate',
    runId: RUN_ID,
    iterationCount: 0,
    stepId: 'durable-tool-call',
    toolCallId: TOOL_CALL_ID,
    toolName: TOOL_NAME,
    args: TOOL_ARGS,
    identityDigest: createToolCallIdentityDigest({
      toolCallId: TOOL_CALL_ID,
      toolName: TOOL_NAME,
      args: TOOL_ARGS,
    }),
    ...overrides,
  };
}

function runToolCallStep(
  resumeData: unknown,
  suspendData: unknown = makeSuspendEnvelope(),
  suspend: ReturnType<typeof vi.fn> = vi.fn(),
) {
  const step = createDurableToolCallStep();
  return (step as any).execute({
    inputData: { toolCallId: TOOL_CALL_ID, toolName: TOOL_NAME, args: TOOL_ARGS },
    mastra: { getLogger: () => undefined },
    suspend,
    resumeData,
    suspendData,
    requestContext: new Map(),
    getInitData: () => makeInitData(),
    [PUBSUB_SYMBOL]: mockPubsub(),
  });
}

/**
 * Seed a message list with a pending tool-call (state 'call') exactly like the durable
 * LLM execution step does, so the mapping step's updateToolInvocation can resolve it.
 */
function seedMessageListState() {
  const messageList = new MessageList({ threadId: THREAD_ID, resourceId: RESOURCE_ID });
  const assistantMessage: MastraDBMessage = {
    id: 'msg-1',
    role: 'assistant',
    content: {
      format: 2,
      parts: [
        {
          type: 'tool-invocation',
          toolInvocation: { state: 'call', toolCallId: TOOL_CALL_ID, toolName: TOOL_NAME, args: TOOL_ARGS },
        },
      ],
    },
    createdAt: new Date(),
  };
  messageList.add(assistantMessage, 'response');
  return messageList.serialize();
}

async function runMappingStep(toolResults: unknown[]) {
  const step = createDurableLLMMappingStep();
  const output = await (step as any).execute({
    inputData: {
      llmOutput: {
        messageListState: seedMessageListState(),
        stepResult: {
          isContinued: true,
          reason: 'tool-calls',
          totalUsage: { inputTokens: 0, outputTokens: 0, totalTokens: 0 },
        },
        text: '',
        toolCalls: [],
      },
      toolResults,
      runId: RUN_ID,
      agentId: AGENT_ID,
      messageId: 'msg-1',
      state: { threadId: THREAD_ID, resourceId: RESOURCE_ID, threadExists: true },
    },
    mastra: { getLogger: () => undefined },
    requestContext: new Map(),
  });

  const recalled = new MessageList({ threadId: THREAD_ID, resourceId: RESOURCE_ID });
  recalled.deserialize(output.messageListState);

  const stored = recalled.get.all
    .db()
    .flatMap((m: MastraDBMessage) => m.content.parts ?? [])
    .find((p: any) => p.type === 'tool-invocation' && p.toolInvocation?.toolCallId === TOOL_CALL_ID)?.toolInvocation as
    | Record<string, any>
    | undefined;

  const v6 = recalled.get.all.aiV6
    .ui()
    .flatMap(m => m.parts)
    .find((p: any) => 'toolCallId' in p && p.toolCallId === TOOL_CALL_ID) as Record<string, any> | undefined;

  return { stored, v6 };
}

afterEach(() => {
  if (globalRunRegistry.has(RUN_ID)) {
    globalRunRegistry.delete(RUN_ID);
  }
  vi.clearAllMocks();
});

describe('issue #17218 (durable engine): tool-call step records the approval decision', () => {
  it('persists a versioned exact identity envelope for an approval suspension', async () => {
    const suspend = vi.fn();
    setupRegistry(vi.fn().mockResolvedValue(TOOL_RESULT));

    await runToolCallStep(undefined, null, suspend);

    expect(suspend).toHaveBeenCalledWith(
      expect.objectContaining({
        version: 1,
        type: 'approval',
        runId: RUN_ID,
        iterationCount: 0,
        stepId: 'durable-tool-call',
        toolCallId: TOOL_CALL_ID,
        toolName: TOOL_NAME,
        args: TOOL_ARGS,
        identityDigest: createToolCallIdentityDigest({
          toolCallId: TOOL_CALL_ID,
          toolName: TOOL_NAME,
          args: TOOL_ARGS,
        }),
      }),
      { resumeLabel: TOOL_CALL_ID },
    );
  });

  it.each([
    { kind: 'general suspension', options: undefined, type: 'suspension', approvalSource: undefined },
    {
      kind: 'in-tool approval',
      options: { requireToolApproval: true },
      type: 'approval',
      approvalSource: 'tool-execution',
    },
  ])('persists a versioned exact identity envelope for $kind', async ({ options, type, approvalSource }) => {
    const suspend = vi.fn();
    vi.mocked(toolApprovalRequirement).mockResolvedValueOnce({ required: false, reasons: [] });
    setupRegistry(
      vi.fn(async (_args, context) => {
        await context.suspend({ reason: 'wait' }, options);
        return TOOL_RESULT;
      }),
    );

    await runToolCallStep(undefined, null, suspend);

    expect(suspend).toHaveBeenCalledWith(
      expect.objectContaining({
        version: 1,
        type,
        ...(approvalSource ? { approvalSource } : {}),
        runId: RUN_ID,
        iterationCount: 0,
        stepId: 'durable-tool-call',
        toolCallId: TOOL_CALL_ID,
        toolName: TOOL_NAME,
        args: TOOL_ARGS,
        identityDigest: createToolCallIdentityDigest({
          toolCallId: TOOL_CALL_ID,
          toolName: TOOL_NAME,
          args: TOOL_ARGS,
        }),
      }),
      { resumeLabel: TOOL_CALL_ID },
    );
  });

  it('delivers an approved in-tool decision back to the suspended durable tool', async () => {
    const execute = vi.fn(async (_args: unknown, context: any) => ({ resumed: context.resumeData?.approved }));
    vi.mocked(toolApprovalRequirement).mockResolvedValueOnce({ required: false, reasons: [] });
    setupRegistry(execute);

    const result = await runToolCallStep({ approved: true }, makeSuspendEnvelope({ approvalSource: 'tool-execution' }));

    expect(execute).toHaveBeenCalledWith(TOOL_ARGS, expect.objectContaining({ resumeData: { approved: true } }));
    expect(result).toEqual({
      toolCallId: TOOL_CALL_ID,
      toolName: TOOL_NAME,
      args: TOOL_ARGS,
      result: { resumed: true },
      approval: { id: TOOL_CALL_ID, approved: true },
    });
  });

  it('decline returns approval { approved: false } with the reason and NO result', async () => {
    const execute = vi.fn().mockResolvedValue(TOOL_RESULT);
    setupRegistry(execute);

    const result = await runToolCallStep({ approved: false });

    // The declined tool must NOT run.
    expect(execute).not.toHaveBeenCalled();
    // It must return the approval decision (not a `result` string) so the mapping step can
    // persist it as `output-denied`.
    expect(result.result).toBeUndefined();
    expect(result.approval).toEqual({ id: TOOL_CALL_ID, approved: false, reason: DECLINE_REASON });
  });

  it('preserves a user-provided decline reason', async () => {
    const execute = vi.fn().mockResolvedValue(TOOL_RESULT);
    setupRegistry(execute);

    const result = await runToolCallStep({ approved: false, reason: 'Not safe' });

    expect(execute).not.toHaveBeenCalled();
    expect(result.approval).toEqual({ id: TOOL_CALL_ID, approved: false, reason: 'Not safe' });
  });

  it.each([{ approved: 'yes' }, {}, { approved: true, injected: 'payload' }])(
    'fails closed for malformed approval resume %#',
    async resumeData => {
      const execute = vi.fn().mockResolvedValue(TOOL_RESULT);
      setupRegistry(execute);

      const result = await runToolCallStep(resumeData);

      expect(execute).not.toHaveBeenCalled();
      expect(result.result).toBeUndefined();
      expect(result.approval).toBeUndefined();
      expect(result.error).toEqual({
        name: 'DurableResumeValidationError',
        message: 'Durable tool resume evidence did not match the suspended tool call',
      });
    },
  );

  it('approve returns the tool result tagged with approval { approved: true }', async () => {
    const execute = vi.fn().mockResolvedValue(TOOL_RESULT);
    setupRegistry(execute);

    const result = await runToolCallStep({ approved: true, reason: 'Reviewed by admin' });

    expect(execute).toHaveBeenCalledTimes(1);
    expect(result.result).toEqual(TOOL_RESULT);
    expect(result.approval).toEqual({ id: TOOL_CALL_ID, approved: true, reason: 'Reviewed by admin' });
  });

  it('carries and restores approval provenance across an ordinary suspension', async () => {
    const suspend = vi.fn();
    setupRegistry(
      vi.fn(async (_args, context) => {
        await context.suspend({ reason: 'need-more-input' });
        return TOOL_RESULT;
      }),
    );

    await runToolCallStep({ approved: true }, makeSuspendEnvelope(), suspend);

    const suspensionEnvelope = suspend.mock.calls.find(call => call[0]?.type === 'suspension')?.[0];
    expect(suspensionEnvelope).toMatchObject({
      type: 'suspension',
      toolCallId: TOOL_CALL_ID,
      approval: { id: TOOL_CALL_ID, approved: true },
    });

    setupRegistry(vi.fn().mockResolvedValue(TOOL_RESULT));
    const resumed = await runToolCallStep({ answer: 'continue' }, suspensionEnvelope);

    expect(resumed.result).toEqual(TOOL_RESULT);
    expect(resumed.approval).toEqual({ id: TOOL_CALL_ID, approved: true });
  });

  it('preserves approval provenance when the dynamic requirement changes before resume', async () => {
    const execute = vi.fn().mockResolvedValue(TOOL_RESULT);
    setupRegistry(execute);
    vi.mocked(toolApprovalRequirement).mockResolvedValueOnce({ required: false, reasons: [] });

    const result = await runToolCallStep({ approved: true });

    expect(execute).toHaveBeenCalledTimes(1);
    expect(toolApprovalRequirement).not.toHaveBeenCalled();
    expect(result.approval).toEqual({ id: TOOL_CALL_ID, approved: true });
  });

  it('passes approved-shaped general suspension data through to the tool', async () => {
    const execute = vi.fn().mockResolvedValue(TOOL_RESULT);
    setupRegistry(execute);
    const resumeData = { approved: false };

    const result = await runToolCallStep(resumeData, makeSuspendEnvelope({ type: 'suspension' }));

    expect(execute).toHaveBeenCalledWith(
      TOOL_ARGS,
      expect.objectContaining({
        resumeData,
      }),
    );
    expect(result.result).toEqual(TOOL_RESULT);
    expect(result.approval).toBeUndefined();
  });

  it.each([
    makeSuspendEnvelope({ toolCallId: 'call-other' }),
    makeSuspendEnvelope({ toolName: 'other-tool' }),
    makeSuspendEnvelope({ type: 'corrupted-approval-marker' }),
    makeSuspendEnvelope({ runId: 'run-other' }),
    makeSuspendEnvelope({ iterationCount: 1 }),
    makeSuspendEnvelope({ stepId: 'other-step' }),
    makeSuspendEnvelope({ identityDigest: 'tampered-digest' }),
    makeSuspendEnvelope({ version: 2 }),
  ])('fails closed for mismatched or unknown durable envelope %#', async suspendData => {
    const execute = vi.fn().mockResolvedValue(TOOL_RESULT);
    setupRegistry(execute);

    const result = await runToolCallStep({ approved: true }, suspendData);

    expect(execute).not.toHaveBeenCalled();
    expect(toolApprovalRequirement).not.toHaveBeenCalled();
    expect(result.approval).toBeUndefined();
    expect(result.error).toMatchObject({ name: 'DurableResumeValidationError' });
  });
});

describe('issue #17218 (durable engine): mapping step round-trips approvals on recall', () => {
  it('a declined tool result persists as output-denied + approval and recalls as a v6 output-denied part', async () => {
    const { stored, v6 } = await runMappingStep([
      {
        toolCallId: TOOL_CALL_ID,
        toolName: TOOL_NAME,
        args: TOOL_ARGS,
        approval: { id: TOOL_CALL_ID, approved: false, reason: DECLINE_REASON },
      },
    ]);

    // Stored MastraToolInvocation is a denial, not a plain successful result.
    expect(stored).toBeDefined();
    expect(stored?.state).toBe('output-denied');
    expect(stored?.result).toBeUndefined();
    expect(stored?.approval).toMatchObject({ approved: false, reason: DECLINE_REASON });

    // Recalled v6 UI part reflects the denial.
    expect(v6).toBeDefined();
    expect(v6?.state).toBe('output-denied');
    expect(v6?.approval).toMatchObject({ approved: false, reason: DECLINE_REASON });
  });

  it('an approved tool result persists the approval and recalls it on the v6 output-available part', async () => {
    const { stored, v6 } = await runMappingStep([
      {
        toolCallId: TOOL_CALL_ID,
        toolName: TOOL_NAME,
        args: TOOL_ARGS,
        result: TOOL_RESULT,
        approval: { id: TOOL_CALL_ID, approved: true },
      },
    ]);

    expect(stored).toBeDefined();
    expect(stored?.state).toBe('result');
    expect(stored?.result).toBe(JSON.stringify(TOOL_RESULT));
    expect(stored?.approval).toMatchObject({ approved: true });

    expect(v6).toBeDefined();
    expect(v6?.state).toBe('output-available');
    expect(v6?.approval).toMatchObject({ approved: true });
  });
});
