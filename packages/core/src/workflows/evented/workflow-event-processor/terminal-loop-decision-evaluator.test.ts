import { afterEach, describe, expect, it, vi } from 'vitest';
import { PUBSUB_SYMBOL } from '../../constants';
import type { LoopConditionFunction } from '../../step';
import {
  MAX_WORKFLOW_TERMINAL_LOOP_DECISION_TIMEOUT_MS,
  getWorkflowTerminalLoopConditionSourceHash,
  nextWorkflowTerminalLoopDecisionReplanAttempt,
} from '../../terminal-continuation';
import type { WorkflowTerminalLoopConditionFrameV1 } from '../../terminal-continuation';
import {
  EventedWorkflowTerminalLoopDecisionEvaluator,
  evaluateEventedWorkflowTerminalLoopDecision,
} from './terminal-loop-decision-evaluator';

function frame(condition: Function, decisionKey = `sha256:${'a'.repeat(64)}`): WorkflowTerminalLoopConditionFrameV1 {
  return {
    version: 1,
    kind: 'loop-condition-frame',
    request: {
      version: 1,
      kind: 'loop-condition',
      decisionKey: decisionKey as `sha256:${string}`,
      loopType: 'dowhile',
      previousIterationCount: 2,
    },
    parentWorkflowName: 'parent',
    parentRunId: 'parent-run',
    parentRevision: 'revision-1',
    conditionId: 'nested-condition',
    conditionSourceHash: getWorkflowTerminalLoopConditionSourceHash(Function.prototype.toString.call(condition)),
    inputData: { answer: 42 },
    state: { keepGoing: true, nested: { value: 1 } },
    requestContext: { tenant: 'one', nested: { value: 1 } },
    stepResults: {
      input: { initial: true },
      nested: {
        status: 'success',
        output: { answer: 42 },
        metadata: { iterationCount: 3, nestedRunId: 'child-run' },
      },
    },
    resumeData: { approval: 'yes' },
    retryCount: 0,
    iterationCount: 3,
  };
}

function evaluation(condition: any, decisionKey?: string) {
  return {
    frame: frame(condition, decisionKey),
    conditionId: 'nested-condition',
    condition,
  };
}

afterEach(() => {
  vi.useRealTimers();
});

describe('evented workflow terminal loop decision evaluator', () => {
  it('adapts the isolated restartable frame and returns only a bound boolean decision', async () => {
    const condition = vi.fn(async context => {
      expect(context).toMatchObject({
        workflowId: 'parent',
        runId: 'parent-run',
        inputData: { answer: 42 },
        state: { keepGoing: true, nested: { value: 1 } },
        resumeData: { approval: 'yes' },
        retryCount: 0,
        iterationCount: 3,
      });
      expect(context.requestContext.get('tenant')).toBe('one');
      expect(context.getInitData()).toEqual({ initial: true });
      expect(context.getStepResult('nested')).toEqual({ answer: 42 });
      return true;
    });

    const result = await evaluateEventedWorkflowTerminalLoopDecision({
      ...evaluation(condition),
    });

    expect(result).toMatchObject({
      status: 'evaluated',
      evaluatedDecision: { conditionResult: true, decisionKey: frame(condition).request.decisionKey },
    });
    expect(condition).toHaveBeenCalledTimes(1);
  });

  it('preserves live callback own-property shape when optional values are absent', async () => {
    const condition = async (context: Record<string, unknown>) => {
      expect(Object.hasOwn(context, 'inputData')).toBe(true);
      expect(Object.hasOwn(context, 'resumeData')).toBe(true);
      expect(context.inputData).toBeUndefined();
      expect(context.resumeData).toBeUndefined();
      return true;
    };
    const withoutOptionalValues = frame(condition);
    delete withoutOptionalValues.inputData;
    delete withoutOptionalValues.resumeData;
    await expect(
      evaluateEventedWorkflowTerminalLoopDecision({
        frame: withoutOptionalValues,
        conditionId: 'nested-condition',
        condition: condition as never,
      }),
    ).resolves.toMatchObject({ status: 'evaluated' });
  });

  it.each([
    [
      'state',
      async ({ state }: any) => {
        state.nested.value = 2;
        return true;
      },
    ],
    [
      'request context',
      async ({ requestContext }: any) => {
        requestContext.set('tenant', 'two');
        return true;
      },
    ],
    [
      'input data',
      async ({ inputData }: any) => {
        inputData.answer = 7;
        return true;
      },
    ],
    [
      'resume data',
      async ({ resumeData }: any) => {
        resumeData.approval = 'no';
        return true;
      },
    ],
    [
      'initial data',
      async ({ getInitData }: any) => {
        getInitData().initial = false;
        return true;
      },
    ],
    [
      'step result',
      async ({ getStepResult }: any) => {
        getStepResult('nested').answer = 7;
        return true;
      },
    ],
  ] as const)('rejects %s mutation without leaking it into the durable frame', async (_label, condition) => {
    const input = frame(condition);
    const result = await evaluateEventedWorkflowTerminalLoopDecision({
      frame: input,
      conditionId: 'nested-condition',
      condition: condition as LoopConditionFunction<any, any, any, any, any, any>,
    });
    expect(result.status).toBe('unsupported_mutation');
    expect(input).toEqual(frame(condition));
  });

  it('keeps throw and invalid return values as explicit non-decisions', async () => {
    const thrown = { secret: 'must not escape' };
    const throwing = (async () => {
      throw thrown;
    }) as LoopConditionFunction<any, any, any, any, any, any>;
    await expect(evaluateEventedWorkflowTerminalLoopDecision(evaluation(throwing))).resolves.toEqual({
      status: 'failed',
      request: frame(throwing).request,
    });
    const invalid = (async () => 'yes') as never;
    await expect(evaluateEventedWorkflowTerminalLoopDecision(evaluation(invalid))).resolves.toMatchObject({
      status: 'invalid_result',
    });
  });

  it('classifies a hostile thrown value without invoking its proxy traps', async () => {
    const getPrototypeOf = vi.fn(() => {
      throw new Error('must not execute');
    });
    const thrown = new Proxy({}, { getPrototypeOf });
    const condition = async () => {
      throw thrown;
    };

    await expect(evaluateEventedWorkflowTerminalLoopDecision(evaluation(condition))).resolves.toMatchObject({
      status: 'failed',
    });
    expect(getPrototypeOf).not.toHaveBeenCalled();
  });

  it('classifies a thrown value without traversing a proxied prototype chain', async () => {
    const getPrototypeOf = vi.fn(() => {
      throw new Error('must not execute');
    });
    const thrown = Object.create(new Proxy({}, { getPrototypeOf }));
    const condition = async () => {
      throw thrown;
    };

    await expect(evaluateEventedWorkflowTerminalLoopDecision(evaluation(condition))).resolves.toMatchObject({
      status: 'failed',
    });
    expect(getPrototypeOf).not.toHaveBeenCalled();
  });

  it('fails closed when the registered callback identity differs from the locked graph', async () => {
    const locked = async () => true;
    const changed = vi.fn(async () => false);
    await expect(
      evaluateEventedWorkflowTerminalLoopDecision({
        ...evaluation(changed),
        frame: frame(locked),
      }),
    ).resolves.toMatchObject({ status: 'callback_mismatch' });
    await expect(
      evaluateEventedWorkflowTerminalLoopDecision({
        ...evaluation(locked),
        conditionId: 'different-step',
      }),
    ).resolves.toMatchObject({ status: 'callback_mismatch' });
    const proxiedTarget = vi.fn(async () => true);
    await expect(
      evaluateEventedWorkflowTerminalLoopDecision({
        ...evaluation(locked),
        condition: new Proxy(proxiedTarget, {}),
      }),
    ).resolves.toMatchObject({ status: 'callback_mismatch' });
    expect(changed).not.toHaveBeenCalled();
    expect(proxiedTarget).not.toHaveBeenCalled();
  });

  it.each([
    ['Mastra', async ({ mastra }: any) => Boolean(mastra.getAgent('one'))],
    ['writer', async ({ writer }: any) => (await writer.write('side effect')) === undefined],
    ['PubSub', async (context: any) => Boolean(context[PUBSUB_SYMBOL].publish)],
    ['engine', async ({ engine }: any) => Boolean(engine.execute)],
    ['bail', async ({ bail }: any) => Boolean(bail(true))],
  ] as const)('rejects the %s capability instead of executing a framework side effect', async (_label, condition) => {
    await expect(evaluateEventedWorkflowTerminalLoopDecision(evaluation(condition))).resolves.toMatchObject({
      status: 'unsupported_effect',
    });
  });

  it.each([
    [
      'Mastra',
      async ({ mastra }: any) => {
        try {
          mastra.getAgent('one');
        } catch {}
        return true;
      },
    ],
    [
      'bail',
      async ({ bail }: any) => {
        try {
          bail(true);
        } catch {}
        return true;
      },
    ],
  ] as const)('retains a caught forbidden %s attempt as a typed non-decision', async (_label, condition) => {
    await expect(evaluateEventedWorkflowTerminalLoopDecision(evaluation(condition))).resolves.toMatchObject({
      status: 'unsupported_effect',
    });
  });

  it('does not invoke an already-aborted attempt and lets callback abort win over a boolean', async () => {
    const before = new AbortController();
    before.abort();
    const neverCalled = vi.fn(async () => true);
    await expect(
      evaluateEventedWorkflowTerminalLoopDecision({
        ...evaluation(neverCalled),
        abortSignal: before.signal,
      }),
    ).resolves.toMatchObject({ status: 'aborted' });
    expect(neverCalled).not.toHaveBeenCalled();

    const aborting = async ({ abort }: any) => {
      abort();
      return true;
    };
    await expect(evaluateEventedWorkflowTerminalLoopDecision(evaluation(aborting))).resolves.toMatchObject({
      status: 'aborted',
    });
  });

  it('classifies external abort and cooperative timeout without producing a decision', async () => {
    const external = new AbortController();
    const cooperative = vi.fn(
      ({ abortSignal }: any) =>
        new Promise<boolean>(resolve => abortSignal.addEventListener('abort', () => resolve(true), { once: true })),
    );
    const waiting = evaluateEventedWorkflowTerminalLoopDecision({
      ...evaluation(cooperative),
      abortSignal: external.signal,
    });
    external.abort();
    await expect(waiting).resolves.toMatchObject({ status: 'aborted' });
    expect(cooperative).not.toHaveBeenCalled();

    vi.useFakeTimers();
    const timed = evaluateEventedWorkflowTerminalLoopDecision({
      ...evaluation(cooperative),
      timeoutMs: 20,
    });
    await vi.advanceTimersByTimeAsync(20);
    await expect(timed).resolves.toMatchObject({ status: 'timed_out' });
  });

  it('rejects deadlines outside the explicit cooperative timeout bound', async () => {
    const condition = async () => true;
    await expect(
      evaluateEventedWorkflowTerminalLoopDecision({ ...evaluation(condition), timeoutMs: 0 }),
    ).rejects.toThrow(/timeoutMs/);
    await expect(
      evaluateEventedWorkflowTerminalLoopDecision({
        ...evaluation(condition),
        timeoutMs: MAX_WORKFLOW_TERMINAL_LOOP_DECISION_TIMEOUT_MS + 1,
      }),
    ).rejects.toThrow(/timeoutMs/);
  });

  it('uses native AbortSignal intrinsics without invoking hostile own shadows', async () => {
    const controller = new AbortController();
    const hostile = vi.fn(() => {
      throw new Error('must not execute');
    });
    Object.defineProperties(controller.signal, {
      aborted: { configurable: true, value: true },
      addEventListener: { configurable: true, value: hostile },
      removeEventListener: { configurable: true, value: hostile },
    });
    const condition = vi.fn(async () => true);
    const evaluator = new EventedWorkflowTerminalLoopDecisionEvaluator();
    await expect(
      evaluator.evaluate({ ...evaluation(condition), abortSignal: controller.signal }),
    ).resolves.toMatchObject({ status: 'evaluated' });
    await expect(evaluator.evaluate(evaluation(condition))).resolves.toMatchObject({ status: 'evaluated' });
    expect(condition).toHaveBeenCalledTimes(1);
    expect(hostile).not.toHaveBeenCalled();
  });

  it('rejects a proxied AbortSignal without invoking its prototype trap', async () => {
    const getPrototypeOf = vi.fn(() => {
      throw new Error('must not execute');
    });
    const signal = new Proxy(new AbortController().signal, { getPrototypeOf });
    const condition = vi.fn(async () => true);

    await expect(
      evaluateEventedWorkflowTerminalLoopDecision({ ...evaluation(condition), abortSignal: signal }),
    ).rejects.toThrow(/native AbortSignal/);
    expect(condition).not.toHaveBeenCalled();
    expect(getPrototypeOf).not.toHaveBeenCalled();
  });

  it('rejects an AbortSignal impostor without traversing a proxied prototype chain', async () => {
    const getPrototypeOf = vi.fn(() => {
      throw new Error('must not execute');
    });
    const signal = Object.create(new Proxy({}, { getPrototypeOf }));
    const condition = vi.fn(async () => true);

    await expect(
      evaluateEventedWorkflowTerminalLoopDecision({ ...evaluation(condition), abortSignal: signal }),
    ).rejects.toThrow(/native AbortSignal/);
    expect(condition).not.toHaveBeenCalled();
    expect(getPrototypeOf).not.toHaveBeenCalled();
  });

  it('coalesces one decision key in-process but reevaluates after a crash or new revision key', async () => {
    const condition = vi.fn(async () => true);
    const evaluator = new EventedWorkflowTerminalLoopDecisionEvaluator();
    const first = evaluator.evaluate(evaluation(condition));
    const concurrent = evaluator.evaluate(evaluation(condition));
    expect(await first).toMatchObject({ status: 'evaluated' });
    expect(await concurrent).toMatchObject({ status: 'evaluated' });
    await evaluator.evaluate(evaluation(condition));
    expect(condition).toHaveBeenCalledTimes(1);

    const restarted = new EventedWorkflowTerminalLoopDecisionEvaluator();
    await restarted.evaluate(evaluation(condition));
    expect(condition).toHaveBeenCalledTimes(2);
    await restarted.evaluate(evaluation(condition, `sha256:${'b'.repeat(64)}`));
    expect(condition).toHaveBeenCalledTimes(3);
  });

  it('returns distinct closed result copies without exposing the cached decision to mutation', async () => {
    const condition = vi.fn(async () => true);
    const evaluator = new EventedWorkflowTerminalLoopDecisionEvaluator();
    const [first, concurrent] = await Promise.all([
      evaluator.evaluate(evaluation(condition)),
      evaluator.evaluate(evaluation(condition)),
    ]);
    expect(first).not.toBe(concurrent);
    expect(first.request).not.toBe(concurrent.request);
    expect(first.status).toBe('evaluated');
    expect(concurrent.status).toBe('evaluated');
    if (first.status !== 'evaluated' || concurrent.status !== 'evaluated') throw new Error('expected decisions');
    expect(first.evaluatedDecision).not.toBe(concurrent.evaluatedDecision);

    first.request.decisionKey = `sha256:${'b'.repeat(64)}`;
    first.evaluatedDecision.decisionKey = `sha256:${'b'.repeat(64)}`;
    first.evaluatedDecision.conditionResult = false;

    await expect(evaluator.evaluate(evaluation(condition))).resolves.toMatchObject({
      status: 'evaluated',
      request: { decisionKey: `sha256:${'a'.repeat(64)}` },
      evaluatedDecision: { decisionKey: `sha256:${'a'.repeat(64)}`, conditionResult: true },
    });
    expect(condition).toHaveBeenCalledTimes(1);
  });

  it('limits repeated fresh-key CAS replans to three callback attempts', async () => {
    const condition = vi.fn(async () => true);
    const evaluator = new EventedWorkflowTerminalLoopDecisionEvaluator();
    let attempt: number | undefined = 0;
    while (attempt !== undefined) {
      await expect(
        evaluator.evaluate(evaluation(condition, `sha256:${attempt.toString(16).padStart(64, '0')}`)),
      ).resolves.toMatchObject({ status: 'evaluated' });
      attempt = nextWorkflowTerminalLoopDecisionReplanAttempt(attempt);
    }
    expect(condition).toHaveBeenCalledTimes(3);
  });

  it('does not let an aborted result poison a later same-key attempt', async () => {
    const controller = new AbortController();
    controller.abort();
    const condition = vi.fn(async () => true);
    const evaluator = new EventedWorkflowTerminalLoopDecisionEvaluator();
    await expect(
      evaluator.evaluate({ ...evaluation(condition), abortSignal: controller.signal }),
    ).resolves.toMatchObject({ status: 'aborted' });
    await Promise.resolve();
    await expect(evaluator.evaluate(evaluation(condition))).resolves.toMatchObject({ status: 'evaluated' });
    expect(condition).toHaveBeenCalledTimes(1);
  });

  it('retains the key after a post-start external abort until the callback settles', async () => {
    let started!: () => void;
    const didStart = new Promise<void>(resolve => (started = resolve));
    const condition = vi.fn(() => {
      started();
      return new Promise<boolean>(() => {});
    });
    const controller = new AbortController();
    const evaluator = new EventedWorkflowTerminalLoopDecisionEvaluator();
    const first = evaluator.evaluate({ ...evaluation(condition), abortSignal: controller.signal });
    await didStart;
    controller.abort();
    await expect(first).resolves.toMatchObject({ status: 'aborted' });
    await expect(evaluator.evaluate(evaluation(condition))).resolves.toMatchObject({ status: 'aborted' });
    expect(condition).toHaveBeenCalledTimes(1);
    expect(evaluator.getStats()).toMatchObject({ retainedInFlight: 1, capacityExceeded: 0 });
  });

  it('retains the key when callback abort is followed by a non-settling promise', async () => {
    const condition = vi.fn(({ abort }: any) => {
      abort();
      return new Promise<boolean>(() => {});
    });
    const evaluator = new EventedWorkflowTerminalLoopDecisionEvaluator();
    await expect(evaluator.evaluate(evaluation(condition))).resolves.toMatchObject({ status: 'aborted' });
    await expect(evaluator.evaluate(evaluation(condition))).resolves.toMatchObject({ status: 'aborted' });
    expect(condition).toHaveBeenCalledTimes(1);
    expect(evaluator.getStats().retainedInFlight).toBe(1);
  });

  it.each(['resolve', 'reject'] as const)(
    'evicts a post-abort key after the underlying callback settles late by %s',
    async settlement => {
      let started!: () => void;
      const didStart = new Promise<void>(resolve => (started = resolve));
      let resolveLate!: (value: boolean) => void;
      let rejectLate!: (error: Error) => void;
      const condition = vi.fn(() => {
        if (condition.mock.calls.length > 1) return true;
        started();
        return new Promise<boolean>((resolve, reject) => {
          resolveLate = resolve;
          rejectLate = reject;
        });
      });
      const controller = new AbortController();
      const evaluator = new EventedWorkflowTerminalLoopDecisionEvaluator();
      const first = evaluator.evaluate({ ...evaluation(condition), abortSignal: controller.signal });
      await didStart;
      controller.abort();
      await expect(first).resolves.toMatchObject({ status: 'aborted' });
      if (settlement === 'resolve') resolveLate(false);
      else rejectLate(new Error('late failure'));
      await vi.waitFor(() => expect(evaluator.getStats().retainedInFlight).toBe(0));
      await expect(evaluator.evaluate(evaluation(condition))).resolves.toMatchObject({ status: 'evaluated' });
      expect(condition).toHaveBeenCalledTimes(2);
    },
  );

  it('does not let failure, invalid output, or callback mismatch poison a later same-key attempt', async () => {
    const evaluator = new EventedWorkflowTerminalLoopDecisionEvaluator();
    const failed = vi.fn(async () => {
      throw new Error('failed');
    });
    await expect(evaluator.evaluate(evaluation(failed))).resolves.toMatchObject({ status: 'failed' });
    await Promise.resolve();

    const invalid = vi.fn(async () => 'invalid');
    await expect(evaluator.evaluate(evaluation(invalid as never))).resolves.toMatchObject({ status: 'invalid_result' });
    await Promise.resolve();

    let validCalls = 0;
    let changedCalls = 0;
    const valid = async () => {
      validCalls++;
      return true;
    };
    const changed = async () => {
      changedCalls++;
      return false;
    };
    await expect(evaluator.evaluate({ ...evaluation(changed), frame: frame(valid) })).resolves.toMatchObject({
      status: 'callback_mismatch',
    });
    await Promise.resolve();
    await expect(evaluator.evaluate(evaluation(valid))).resolves.toMatchObject({ status: 'evaluated' });
    expect(failed).toHaveBeenCalledTimes(1);
    expect(invalid).toHaveBeenCalledTimes(1);
    expect(changedCalls).toBe(0);
    expect(validCalls).toBe(1);
  });

  it('rejects hostile evaluation wrappers without invoking accessors', async () => {
    const condition = async () => true;
    const getter = vi.fn(() => frame(condition));
    const hostile = { conditionId: 'nested-condition', condition } as Record<string, unknown>;
    Object.defineProperty(hostile, 'frame', { enumerable: true, get: getter });
    await expect(evaluateEventedWorkflowTerminalLoopDecision(hostile as never)).rejects.toThrow(/data fields/);
    expect(getter).not.toHaveBeenCalled();
    await expect(
      evaluateEventedWorkflowTerminalLoopDecision(new Proxy(evaluation(condition), {}) as never),
    ).rejects.toThrow(/plain data object/);
  });

  it('bounds retained timed-out decisions and fails closed at capacity', async () => {
    vi.useFakeTimers();
    const condition = vi.fn(() => new Promise<boolean>(() => {}));
    const evaluator = new EventedWorkflowTerminalLoopDecisionEvaluator();
    const attempts = Array.from({ length: 256 }, (_, index) =>
      evaluator.evaluate({
        ...evaluation(condition, `sha256:${index.toString(16).padStart(64, '0')}`),
        timeoutMs: 1,
      }),
    );
    await vi.advanceTimersByTimeAsync(1);
    await expect(Promise.all(attempts)).resolves.toHaveLength(256);
    await expect(evaluator.evaluate(evaluation(condition, `sha256:${'f'.repeat(64)}`))).resolves.toMatchObject({
      status: 'capacity_exceeded',
    });
    expect(condition).toHaveBeenCalledTimes(256);
    expect(evaluator.getStats()).toEqual({ size: 256, retainedInFlight: 256, capacityExceeded: 1 });
  });

  it('recovers capacity after all timed-out callbacks eventually settle', async () => {
    vi.useFakeTimers();
    const resolvers: Array<(value: boolean) => void> = [];
    const condition = vi.fn(
      () =>
        new Promise<boolean>(resolve => {
          resolvers.push(resolve);
        }),
    );
    const evaluator = new EventedWorkflowTerminalLoopDecisionEvaluator();
    const attempts = Array.from({ length: 256 }, (_, index) =>
      evaluator.evaluate({
        ...evaluation(condition, `sha256:${index.toString(16).padStart(64, '0')}`),
        timeoutMs: 1,
      }),
    );
    await vi.advanceTimersByTimeAsync(1);
    await expect(Promise.all(attempts)).resolves.toHaveLength(256);
    expect(evaluator.getStats().retainedInFlight).toBe(256);
    for (const resolve of resolvers) resolve(false);
    await vi.waitFor(() => expect(evaluator.getStats().size).toBe(0));

    const fresh = evaluator.evaluate({
      ...evaluation(condition, `sha256:${'d'.repeat(64)}`),
      timeoutMs: 1,
    });
    await vi.advanceTimersByTimeAsync(1);
    await expect(fresh).resolves.toMatchObject({ status: 'timed_out' });
    expect(condition).toHaveBeenCalledTimes(257);
  });

  it('evicts completed successful decisions instead of rejecting fresh work at capacity', async () => {
    const condition = vi.fn(async () => true);
    const evaluator = new EventedWorkflowTerminalLoopDecisionEvaluator();
    await Promise.all(
      Array.from({ length: 256 }, (_, index) =>
        evaluator.evaluate(evaluation(condition, `sha256:${index.toString(16).padStart(64, '0')}`)),
      ),
    );
    await expect(evaluator.evaluate(evaluation(condition, `sha256:${'e'.repeat(64)}`))).resolves.toMatchObject({
      status: 'evaluated',
    });
    expect(condition).toHaveBeenCalledTimes(257);
  });

  it('retains a non-cooperative timeout by decision key so redelivery cannot overlap it', async () => {
    vi.useFakeTimers();
    const condition = vi.fn(() => new Promise<boolean>(() => {}));
    const evaluator = new EventedWorkflowTerminalLoopDecisionEvaluator();
    const first = evaluator.evaluate({
      ...evaluation(condition),
      timeoutMs: 10,
    });
    await vi.advanceTimersByTimeAsync(10);
    await expect(first).resolves.toMatchObject({ status: 'timed_out' });
    await expect(evaluator.evaluate({ ...evaluation(condition), timeoutMs: 10 })).resolves.toMatchObject({
      status: 'timed_out',
    });
    expect(condition).toHaveBeenCalledTimes(1);
  });
});
