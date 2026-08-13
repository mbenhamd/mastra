import { MockLanguageModelV2 } from '@internal/ai-sdk-v5/test';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { RequestContext } from '../../../../request-context';
import { AGENT_RESPONSE_RECOVERY_STEP } from '../../../merge-execution-options';
import { MessageList } from '../../../message-list';
import { globalRunRegistry, registerGlobalRunRegistryEntry } from '../../run-registry';
import * as resolveRuntime from '../../utils/resolve-runtime';
import { createDurableLLMExecutionStep } from './llm-execution';

vi.mock('../../utils/resolve-runtime', () => ({
  resolveRuntimeDependencies: vi.fn(),
  resolveModelFromListEntry: vi.fn(),
}));

afterEach(() => {
  globalRunRegistry.delete('warm-recovery-run');
  globalRunRegistry.delete('cold-recovery-run');
  vi.clearAllMocks();
});

describe('durable response recovery admission', () => {
  it('uses neither provider retries nor fallback models after live admission', async () => {
    const primaryDoStream = vi.fn(async () => {
      throw new Error('recovery provider failed');
    });
    const backupDoStream = vi.fn(async () => {
      throw new Error('fallback provider must not be called');
    });
    const primary = new MockLanguageModelV2({ doStream: primaryDoStream });
    const backup = new MockLanguageModelV2({ doStream: backupDoStream });
    const messageList = new MessageList();

    vi.mocked(resolveRuntime.resolveRuntimeDependencies).mockResolvedValue({
      messageList,
      tools: {},
      model: primary,
      modelList: [
        { id: 'primary', model: primary },
        { id: 'backup', model: backup },
      ],
      inputProcessors: [],
      llmRequestInputProcessors: [],
      outputProcessors: [],
    } as any);
    registerGlobalRunRegistryEntry('warm-recovery-run', {
      runtimeBindingId: 'warm-binding',
      prepareStep: async () => ({
        activeTools: [],
        toolChoice: 'none',
        [AGENT_RESPONSE_RECOVERY_STEP]: true,
      }),
      inputProcessors: [],
      outputProcessors: [],
      errorProcessors: [],
      messageList,
    } as any);

    const result = await (createDurableLLMExecutionStep() as any).execute({
      inputData: {
        __workflowKind: 'durable-agent',
        runId: 'warm-recovery-run',
        runtimeBindingId: 'warm-binding',
        agentId: 'recovery-agent',
        messageListState: messageList.serialize(),
        toolsMetadata: [],
        modelConfig: {
          provider: primary.provider,
          modelId: primary.modelId,
          specificationVersion: primary.specificationVersion,
        },
        modelList: [
          { id: 'primary', config: {}, maxRetries: 2, enabled: true },
          { id: 'backup', config: {}, maxRetries: 2, enabled: true },
        ],
        options: {},
        responseRecovery: { phase: 'reserved', reservedAtIteration: 1 },
        state: {},
        messageId: 'message-1',
        stepIndex: 1,
      },
      mastra: undefined,
      tracingContext: undefined,
      requestContext: new RequestContext(),
      abortSignal: undefined,
    });

    expect(primaryDoStream).toHaveBeenCalledTimes(1);
    expect(backupDoStream).not.toHaveBeenCalled();
    expect(result).toMatchObject({
      text: '',
      toolCalls: [],
      stepResult: { reason: 'error', isContinued: false },
      responseRecoveryConsumed: true,
    });
  });

  it('does not treat a serialized reservation as cold-worker provider authorization', async () => {
    const doStream = vi.fn(async () => {
      throw new Error('provider must not be called');
    });
    const model = new MockLanguageModelV2({ doStream });
    const messageList = new MessageList();

    vi.mocked(resolveRuntime.resolveRuntimeDependencies).mockResolvedValue({
      messageList,
      tools: {},
      model,
      modelList: undefined,
      inputProcessors: [],
      llmRequestInputProcessors: [],
      outputProcessors: [],
    } as any);

    const result = await (createDurableLLMExecutionStep() as any).execute({
      inputData: {
        __workflowKind: 'durable-agent',
        runId: 'cold-recovery-run',
        agentId: 'recovery-agent',
        messageListState: messageList.serialize(),
        toolsMetadata: [],
        modelConfig: {
          provider: model.provider,
          modelId: model.modelId,
          specificationVersion: model.specificationVersion,
        },
        options: {},
        responseRecovery: { phase: 'reserved', reservedAtIteration: 1 },
        state: {},
        messageId: 'message-1',
        stepIndex: 1,
      },
      mastra: undefined,
      tracingContext: undefined,
      requestContext: new RequestContext(),
      abortSignal: undefined,
    });

    expect(doStream).not.toHaveBeenCalled();
    expect(result).toMatchObject({
      text: '',
      toolCalls: [],
      stepResult: { reason: 'error', isContinued: false },
    });
  });
});
