import { beforeEach, describe, expect, it, vi } from 'vitest';
import type { MastraModelOutput } from '../../stream/base/output';
import { MastraLLMVNext } from './model.loop';

const { loopMock } = vi.hoisted(() => ({
  loopMock: vi.fn(() => ({ kind: 'loop-output' })),
}));

vi.mock('../../loop', () => ({ loop: loopMock }));

const messageList = {
  get: {
    all: {
      aiV5: {
        model: () => [],
      },
    },
  },
  getAllSystemMessages: () => [],
};

function createModel() {
  return new MastraLLMVNext({
    models: [
      {
        id: 'test-model',
        maxRetries: 0,
        model: {
          provider: 'test-provider',
          modelId: 'test-model',
        } as never,
      },
    ],
  });
}

describe('MastraLLMVNext stop conditions', () => {
  beforeEach(() => {
    loopMock.mockClear();
  });

  it('composes maxSteps with a caller-provided stop condition', async () => {
    const customStop = vi.fn(() => false);

    createModel().stream({
      messageList,
      requestContext: {} as never,
      tracingContext: {},
      methodType: 'stream',
      maxSteps: 10,
      stopWhen: customStop,
    } as never) as MastraModelOutput;

    const stopWhen = loopMock.mock.calls[0]?.[0].stopWhen;
    expect(stopWhen).toBeInstanceOf(Array);
    expect(stopWhen).toHaveLength(2);
    expect(stopWhen[1]).toBe(customStop);
    expect(await stopWhen[0]({ steps: Array.from({ length: 9 }, () => ({})) } as never)).toBe(false);
    expect(await stopWhen[0]({ steps: Array.from({ length: 10 }, () => ({})) } as never)).toBe(true);
  });

  it('preserves every caller-provided stop condition when maxSteps is set', () => {
    const firstStop = vi.fn(() => false);
    const secondStop = vi.fn(() => false);

    createModel().stream({
      messageList,
      requestContext: {} as never,
      tracingContext: {},
      methodType: 'stream',
      maxSteps: 3,
      stopWhen: [firstStop, secondStop],
    } as never) as MastraModelOutput;

    const stopWhen = loopMock.mock.calls[0]?.[0].stopWhen;
    expect(stopWhen).toHaveLength(3);
    expect(stopWhen.slice(1)).toEqual([firstStop, secondStop]);
  });

  it.each([0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, Number.MAX_SAFE_INTEGER + 1])(
    'rejects invalid maxSteps %s',
    maxSteps => {
      expect(() =>
        createModel().stream({
          messageList,
          requestContext: {} as never,
          tracingContext: {},
          methodType: 'stream',
          maxSteps,
        } as never),
      ).toThrow('maxSteps must be a positive safe integer');
      expect(loopMock).not.toHaveBeenCalled();
    },
  );

  it.each([-1, 1.5, Number.NaN, Number.POSITIVE_INFINITY, Number.MAX_SAFE_INTEGER + 1])(
    'rejects invalid recoveryMaxSteps %s',
    recoveryMaxSteps => {
      expect(() =>
        createModel().stream({
          messageList,
          requestContext: {} as never,
          tracingContext: {},
          methodType: 'stream',
          maxSteps: 1,
          recoveryMaxSteps,
        } as never),
      ).toThrow('recoveryMaxSteps must be a non-negative safe integer');
      expect(loopMock).not.toHaveBeenCalled();
    },
  );

  it('allows zero recoveryMaxSteps', () => {
    expect(() =>
      createModel().stream({
        messageList,
        requestContext: {} as never,
        tracingContext: {},
        methodType: 'stream',
        maxSteps: 1,
        recoveryMaxSteps: 0,
      } as never),
    ).not.toThrow();
  });

  it('uses a monotonic cap for restored step counts already above maxSteps', async () => {
    createModel().stream({
      messageList,
      requestContext: {} as never,
      tracingContext: {},
      methodType: 'stream',
      maxSteps: 2,
    } as never) as MastraModelOutput;

    const stopWhen = loopMock.mock.calls[0]?.[0].stopWhen;
    expect(await stopWhen[0]({ steps: [{}, {}, {}] } as never)).toBe(true);
  });
});
