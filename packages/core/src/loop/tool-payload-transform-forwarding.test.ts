import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { ToolPayloadTransformPolicy } from '../tools/types';
import { loop } from './loop';
import { createMessageListWithUserMessage, createTestModels, defaultSettings } from './test-utils/utils';

const { workflowLoopStreamMock } = vi.hoisted(() => ({
  workflowLoopStreamMock: vi.fn(),
}));

vi.mock('./workflows/stream', () => ({
  workflowLoopStream: workflowLoopStreamMock,
}));

describe('loop forwards _internal.toolPayloadTransform', () => {
  beforeEach(() => {
    workflowLoopStreamMock.mockReset();
    workflowLoopStreamMock.mockReturnValue(new ReadableStream());
  });

  it('preserves the policy in the internal state used to hydrate the run scope', () => {
    const toolPayloadTransform: ToolPayloadTransformPolicy = {
      transformToolPayload: async context => context.input,
    };
    const settings = defaultSettings();

    loop({
      ...settings,
      _internal: { ...settings._internal, toolPayloadTransform },
      methodType: 'stream',
      runId: 'test-run-id',
      messageList: createMessageListWithUserMessage(),
      models: createTestModels(),
    } as any);

    expect(workflowLoopStreamMock).toHaveBeenCalledOnce();
    expect(workflowLoopStreamMock).toHaveBeenCalledWith(
      expect.objectContaining({
        _internal: expect.objectContaining({ toolPayloadTransform }),
      }),
    );
  });
});
