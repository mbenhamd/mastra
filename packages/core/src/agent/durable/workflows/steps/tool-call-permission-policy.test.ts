import { afterEach, describe, expect, it, vi } from 'vitest';

import { RequestContext } from '../../../../request-context';
import { PUBSUB_SYMBOL } from '../../../../workflows/constants';
import { MessageList } from '../../../message-list';
import { createToolCallIdentityDigest } from '../../../tool-call-identity';
import { TOOL_PERMISSION_POLICY_KEY, TOOL_PERMISSION_POLICY_REQUIRED_KEY } from '../../../tool-permission-prefilter';
import { DurableStepIds } from '../../constants';
import { globalRunRegistry } from '../../run-registry';
import { createDurableToolCallStep } from './tool-call';

vi.mock('../../../../workflows', () => ({
  createStep: (config: unknown) => config,
}));

vi.mock('../../utils/resolve-runtime', () => ({
  resolveTool: vi.fn(),
  rebuildRunToolsFromMastra: vi.fn().mockResolvedValue(undefined),
  toolApprovalRequirement: vi.fn().mockResolvedValue({ required: false, reasons: [] }),
}));

vi.mock('../../stream-adapter', () => ({
  emitChunkEvent: vi.fn().mockResolvedValue(undefined),
  emitSuspendedEvent: vi.fn().mockResolvedValue(undefined),
}));

const RUN_ID = 'permission-policy-run';
const RUNTIME_BINDING_ID = 'permission-policy-binding';
const TOOL_NAME = 'write_file';
const TOOL_CALL_ID = 'permission-policy-call';
const TOOL_ARGS = { path: 'paper.tex' };

function initData(permissionPolicyRequired?: boolean) {
  return {
    runId: RUN_ID,
    runtimeBindingId: RUNTIME_BINDING_ID,
    agentId: 'permission-agent',
    options: { permissionPolicyRequired },
    toolsMetadata: [],
    messageListState: new MessageList().serialize(),
    state: {},
  };
}

function suspendEnvelope() {
  return {
    version: 1,
    type: 'approval',
    approvalSource: 'tool-gate',
    runId: RUN_ID,
    iterationCount: 0,
    stepId: DurableStepIds.TOOL_CALL,
    toolCallId: TOOL_CALL_ID,
    toolName: TOOL_NAME,
    args: TOOL_ARGS,
    identityDigest: createToolCallIdentityDigest({
      toolCallId: TOOL_CALL_ID,
      toolName: TOOL_NAME,
      args: TOOL_ARGS,
    }),
  };
}

function toolSuspensionEnvelope(approval?: { id: string; approved: boolean; reason?: string }) {
  return {
    version: 1,
    type: 'suspension',
    runId: RUN_ID,
    iterationCount: 0,
    stepId: DurableStepIds.TOOL_CALL,
    toolCallId: TOOL_CALL_ID,
    toolName: TOOL_NAME,
    args: TOOL_ARGS,
    identityDigest: createToolCallIdentityDigest({
      toolCallId: TOOL_CALL_ID,
      toolName: TOOL_NAME,
      args: TOOL_ARGS,
    }),
    suspendPayload: { question: 'Continue?' },
    ...(approval ? { approval } : {}),
  };
}

function executeStep(options: {
  requestContext?: RequestContext;
  permissionPolicyRequired?: boolean;
  resumeData?: unknown;
  suspendData?: unknown;
  suspend?: ReturnType<typeof vi.fn>;
  resolveToolPermission?: Parameters<typeof createDurableToolCallStep>[0]['resolveToolPermission'];
}) {
  const step = createDurableToolCallStep({ resolveToolPermission: options.resolveToolPermission });
  return (step as any).execute({
    inputData: { iterationCount: 0, toolCallId: TOOL_CALL_ID, toolName: TOOL_NAME, args: TOOL_ARGS },
    mastra: { getLogger: () => undefined },
    suspend: options.suspend ?? vi.fn(),
    resumeData: options.resumeData,
    suspendData: options.suspendData,
    requestContext: options.requestContext ?? new RequestContext(),
    getInitData: () => initData(options.permissionPolicyRequired),
    [PUBSUB_SYMBOL]: { publish: vi.fn() },
  });
}

function installTool(execute = vi.fn().mockResolvedValue({ ok: true })) {
  globalRunRegistry.set(RUN_ID, {
    runtimeBindingId: RUNTIME_BINDING_ID,
    tools: { [TOOL_NAME]: { id: TOOL_NAME, execute } },
    model: {} as any,
  } as any);
  return execute;
}

afterEach(() => {
  globalRunRegistry.delete(RUN_ID);
  vi.clearAllMocks();
});

describe('durable tool-call per-tool permission policy', () => {
  it('denies at action time without executing the tool', async () => {
    const toolExecute = installTool();
    const requestContext = new RequestContext();
    requestContext.set(TOOL_PERMISSION_POLICY_KEY, () => 'deny');

    const result = await executeStep({ requestContext, permissionPolicyRequired: true });

    expect(result).toMatchObject({
      disposition: 'denied',
      result: `Tool "${TOOL_NAME}" was denied by the session permission policy.`,
    });
    expect(toolExecute).not.toHaveBeenCalled();
  });

  it('turns a live ask decision into a durable approval suspension', async () => {
    const toolExecute = installTool();
    const requestContext = new RequestContext();
    requestContext.set(TOOL_PERMISSION_POLICY_KEY, () => 'ask');
    const suspend = vi.fn().mockReturnValue({ status: 'suspended' });

    const result = await executeStep({ requestContext, permissionPolicyRequired: true, suspend });

    expect(result).toEqual({ status: 'suspended' });
    expect(suspend).toHaveBeenCalledWith(
      expect.objectContaining({ approvalReasons: ['policy'], toolCallId: TOOL_CALL_ID }),
      { resumeLabel: TOOL_CALL_ID },
    );
    expect(toolExecute).not.toHaveBeenCalled();
  });

  it('re-evaluates a prior ask as deny on approval resume', async () => {
    const toolExecute = installTool();
    const currentRequestContext = new RequestContext();
    currentRequestContext.set(TOOL_PERMISSION_POLICY_KEY, () => 'deny');

    const result = await executeStep({
      requestContext: currentRequestContext,
      permissionPolicyRequired: true,
      resumeData: { approved: true },
      suspendData: suspendEnvelope(),
    });

    expect(result).toMatchObject({ disposition: 'denied' });
    expect(toolExecute).not.toHaveBeenCalled();
  });

  it('fails closed on a cold worker when the required evaluator is missing', async () => {
    const toolExecute = installTool();

    const result = await executeStep({ permissionPolicyRequired: true });

    expect(result).toMatchObject({ disposition: 'denied' });
    expect(toolExecute).not.toHaveBeenCalled();
  });

  it('fails closed when a resume-only durable marker requires a missing evaluator', async () => {
    const toolExecute = installTool();
    const requestContext = new RequestContext();
    requestContext.set(TOOL_PERMISSION_POLICY_REQUIRED_KEY, true);

    const result = await executeStep({ requestContext });

    expect(result).toMatchObject({ disposition: 'denied' });
    expect(toolExecute).not.toHaveBeenCalled();
  });

  it('uses a trusted worker resolver without serializing a policy closure', async () => {
    const toolExecute = installTool();
    const requestContext = new RequestContext();
    requestContext.set('sessionId', 'session-1');
    const resolveToolPermission = vi.fn().mockResolvedValue('allow');

    const result = await executeStep({ requestContext, resolveToolPermission });

    expect(result.result).toEqual({ ok: true });
    expect(toolExecute).toHaveBeenCalledOnce();
    expect(resolveToolPermission).toHaveBeenCalledWith({
      runId: RUN_ID,
      agentId: 'permission-agent',
      toolCallId: TOOL_CALL_ID,
      toolName: TOOL_NAME,
      args: TOOL_ARGS,
      requestContext,
      isResume: false,
    });
  });

  it('fails closed when the trusted worker resolver throws', async () => {
    const toolExecute = installTool();
    const resolveToolPermission = vi.fn().mockRejectedValue(new Error('policy store unavailable'));

    const result = await executeStep({ resolveToolPermission });

    expect(result).toMatchObject({ disposition: 'denied' });
    expect(toolExecute).not.toHaveBeenCalled();
  });

  it('combines live and worker policy with deny taking precedence', async () => {
    const toolExecute = installTool();
    const requestContext = new RequestContext();
    const livePolicy = vi.fn().mockReturnValue('deny');
    const resolveToolPermission = vi.fn().mockResolvedValue('allow');
    requestContext.set(TOOL_PERMISSION_POLICY_KEY, livePolicy);

    const result = await executeStep({ requestContext, resolveToolPermission });

    expect(result).toMatchObject({ disposition: 'denied' });
    expect(livePolicy).toHaveBeenCalledWith(TOOL_NAME);
    expect(resolveToolPermission).toHaveBeenCalledOnce();
    expect(toolExecute).not.toHaveBeenCalled();
  });

  it('combines live and worker policy with ask taking precedence over allow', async () => {
    const toolExecute = installTool();
    const requestContext = new RequestContext();
    requestContext.set(TOOL_PERMISSION_POLICY_KEY, () => 'allow');
    const resolveToolPermission = vi.fn().mockResolvedValue('ask');
    const suspend = vi.fn().mockReturnValue({ status: 'suspended' });

    const result = await executeStep({ requestContext, resolveToolPermission, suspend });

    expect(result).toEqual({ status: 'suspended' });
    expect(suspend).toHaveBeenCalledWith(
      expect.objectContaining({ approvalReasons: ['policy'], toolCallId: TOOL_CALL_ID }),
      { resumeLabel: TOOL_CALL_ID },
    );
    expect(toolExecute).not.toHaveBeenCalled();
  });

  it('fails closed when the live policy throws even if the worker resolver allows', async () => {
    const toolExecute = installTool();
    const requestContext = new RequestContext();
    requestContext.set(TOOL_PERMISSION_POLICY_KEY, () => {
      throw new Error('live policy unavailable');
    });
    const resolveToolPermission = vi.fn().mockResolvedValue('allow');

    const result = await executeStep({ requestContext, resolveToolPermission });

    expect(result).toMatchObject({ disposition: 'denied' });
    expect(resolveToolPermission).toHaveBeenCalledOnce();
    expect(toolExecute).not.toHaveBeenCalled();
  });

  it('classifies resume(undefined) from authenticated suspension provenance as a resume', async () => {
    const toolExecute = installTool();
    const staleRegistryContext = new RequestContext();
    staleRegistryContext.set(TOOL_PERMISSION_POLICY_KEY, () => 'deny');
    globalRunRegistry.get(RUN_ID)!.requestContext = staleRegistryContext;
    const resolveToolPermission = vi.fn().mockResolvedValue('allow');

    const result = await executeStep({
      resumeData: undefined,
      suspendData: toolSuspensionEnvelope(),
      resolveToolPermission,
    });

    expect(result.result).toEqual({ ok: true });
    expect(toolExecute).toHaveBeenCalledOnce();
    expect(resolveToolPermission).toHaveBeenCalledWith(expect.objectContaining({ isResume: true }));
  });

  it('rejects untrusted suspension provenance before classifying resume(undefined)', async () => {
    const toolExecute = installTool();
    const resolveToolPermission = vi.fn().mockResolvedValue('allow');

    const result = await executeStep({
      resumeData: undefined,
      suspendData: { ...toolSuspensionEnvelope(), toolCallId: 'different-call' },
      resolveToolPermission,
    });

    expect(result).toMatchObject({
      error: {
        name: 'DurableResumeValidationError',
      },
    });
    expect(resolveToolPermission).not.toHaveBeenCalled();
    expect(toolExecute).not.toHaveBeenCalled();
  });

  it('fails closed when an in-tool suspension resume newly requires approval', async () => {
    const toolExecute = installTool();
    const requestContext = new RequestContext();
    requestContext.set(TOOL_PERMISSION_POLICY_KEY, () => 'ask');

    const result = await executeStep({
      requestContext,
      permissionPolicyRequired: true,
      resumeData: { answer: 'continue' },
      suspendData: toolSuspensionEnvelope(),
    });

    expect(result).toMatchObject({
      disposition: 'denied',
      result: `Tool "${TOOL_NAME}" was not resumed because the session permission policy requires a new approval.`,
    });
    expect(toolExecute).not.toHaveBeenCalled();
  });

  it('honors a prior approval grant when that tool later resumes an internal suspension under ask', async () => {
    const toolExecute = installTool();
    const requestContext = new RequestContext();
    requestContext.set(TOOL_PERMISSION_POLICY_KEY, () => 'ask');

    const result = await executeStep({
      requestContext,
      permissionPolicyRequired: true,
      resumeData: { answer: 'continue' },
      suspendData: toolSuspensionEnvelope({ id: TOOL_CALL_ID, approved: true }),
    });

    expect(result.result).toEqual({ ok: true });
    expect(toolExecute).toHaveBeenCalledOnce();
  });

  it('keeps ordinary durable calls unchanged when no policy was configured', async () => {
    const toolExecute = installTool();

    const result = await executeStep({});

    expect(result.result).toEqual({ ok: true });
    expect(toolExecute).toHaveBeenCalledOnce();
  });
});
