import { isProxy } from 'node:util/types';
import { RequestContext } from '../../../di';
import { PUBSUB_SYMBOL, STREAM_FORMAT_SYMBOL } from '../../constants';
import { getStepResult } from '../../step';
import type { LoopConditionFunction } from '../../step';
import {
  DEFAULT_WORKFLOW_TERMINAL_LOOP_DECISION_TIMEOUT_MS,
  MAX_WORKFLOW_TERMINAL_LOOP_DECISION_TIMEOUT_MS,
  completeWorkflowTerminalLoopDecision,
  copyWorkflowTerminalLoopConditionFrame,
  getWorkflowTerminalLoopConditionSourceHash,
  validateWorkflowTerminalStructuralString,
} from '../../terminal-continuation';
import type {
  WorkflowTerminalEvaluatedLoopDecisionV1,
  WorkflowTerminalLoopConditionFrameV1,
  WorkflowTerminalLoopDecisionRequestV1,
} from '../../terminal-continuation';
import { getWorkflowTerminalCanonicalJson, materializeWorkflowTerminalCanonicalJson } from '../../terminal-recovery';

const MAX_PROCESS_LOCAL_LOOP_DECISIONS = 256;
const abortSignalAbortedGetter = Object.getOwnPropertyDescriptor(AbortSignal.prototype, 'aborted')!.get!;
const addEventListener = EventTarget.prototype.addEventListener;
const removeEventListener = EventTarget.prototype.removeEventListener;

function isSignalAborted(signal: AbortSignal): boolean {
  return Boolean(abortSignalAbortedGetter.call(signal));
}

function addAbortListener(signal: AbortSignal, listener: () => void): void {
  addEventListener.call(signal, 'abort', listener, { once: true });
}

function removeAbortListener(signal: AbortSignal, listener: () => void): void {
  removeEventListener.call(signal, 'abort', listener);
}

const unsupportedDurableLoopConditionEffects = new WeakSet<object>();

class UnsupportedDurableLoopConditionEffect extends Error {
  constructor(capability: string) {
    super(`Durable loop conditions cannot use ${capability}`);
    this.name = 'UnsupportedDurableLoopConditionEffect';
    unsupportedDurableLoopConditionEffects.add(this);
  }
}

function isUnsupportedDurableLoopConditionEffect(value: unknown): boolean {
  return (
    ((typeof value === 'object' && value !== null) || typeof value === 'function') &&
    unsupportedDurableLoopConditionEffects.has(value)
  );
}

export type EventedWorkflowTerminalLoopDecisionEvaluationResult =
  | {
      status: 'evaluated';
      request: WorkflowTerminalLoopDecisionRequestV1;
      evaluatedDecision: WorkflowTerminalEvaluatedLoopDecisionV1;
    }
  | {
      status:
        | 'failed'
        | 'aborted'
        | 'timed_out'
        | 'callback_mismatch'
        | 'invalid_result'
        | 'unsupported_effect'
        | 'unsupported_mutation'
        | 'capacity_exceeded';
      request: WorkflowTerminalLoopDecisionRequestV1;
    };

function copyEvaluationResult(
  result: EventedWorkflowTerminalLoopDecisionEvaluationResult,
): EventedWorkflowTerminalLoopDecisionEvaluationResult {
  const request: WorkflowTerminalLoopDecisionRequestV1 = {
    version: 1,
    kind: 'loop-condition',
    decisionKey: result.request.decisionKey,
    loopType: result.request.loopType,
    previousIterationCount: result.request.previousIterationCount,
  };
  if (result.status !== 'evaluated') return { status: result.status, request };
  return {
    status: 'evaluated',
    request,
    evaluatedDecision: {
      version: 1,
      kind: 'loop-condition',
      decisionKey: result.evaluatedDecision.decisionKey,
      conditionResult: result.evaluatedDecision.conditionResult,
    },
  };
}

export interface EvaluateEventedWorkflowTerminalLoopDecisionInput {
  frame: WorkflowTerminalLoopConditionFrameV1;
  conditionId: string;
  condition: LoopConditionFunction<any, any, any, any, any, any, any>;
  abortSignal?: AbortSignal;
  timeoutMs?: number;
}

function copyEvaluationInput(input: unknown): EvaluateEventedWorkflowTerminalLoopDecisionInput {
  if (input === null || typeof input !== 'object' || Array.isArray(input) || isProxy(input)) {
    throw new TypeError('loop decision evaluation input must be a plain data object');
  }
  const prototype = Object.getPrototypeOf(input);
  if (prototype !== Object.prototype && prototype !== null) {
    throw new TypeError('loop decision evaluation input must be a plain data object');
  }
  const descriptors = Object.getOwnPropertyDescriptors(input);
  const allowed = new Set(['frame', 'conditionId', 'condition', 'abortSignal', 'timeoutMs']);
  for (const key of Reflect.ownKeys(descriptors)) {
    const descriptor = descriptors[key as string];
    if (
      typeof key !== 'string' ||
      !allowed.has(key) ||
      !descriptor ||
      !descriptor.enumerable ||
      !('value' in descriptor)
    ) {
      throw new TypeError('loop decision evaluation input must contain only known enumerable data fields');
    }
  }
  for (const required of ['frame', 'conditionId', 'condition']) {
    if (!Object.hasOwn(descriptors, required)) {
      throw new TypeError(`loop decision evaluation input is missing ${required}`);
    }
  }
  const condition = descriptors.condition!.value;
  if (typeof condition !== 'function') {
    throw new TypeError('loop decision condition must be a function');
  }
  const abortSignal = descriptors.abortSignal?.value;
  if (abortSignal !== undefined) {
    if (abortSignal === null || typeof abortSignal !== 'object' || isProxy(abortSignal)) {
      throw new TypeError('loop decision abortSignal must be a native AbortSignal');
    }
    try {
      isSignalAborted(abortSignal as AbortSignal);
    } catch {
      throw new TypeError('loop decision abortSignal must be a native AbortSignal');
    }
  }
  const timeoutMs = descriptors.timeoutMs?.value;
  return {
    frame: copyWorkflowTerminalLoopConditionFrame(descriptors.frame!.value),
    conditionId: validateWorkflowTerminalStructuralString(descriptors.conditionId!.value, 'loop decision conditionId'),
    condition,
    ...(abortSignal === undefined ? {} : { abortSignal }),
    ...(timeoutMs === undefined ? {} : { timeoutMs: timeout(timeoutMs) }),
  };
}

function timeout(value: unknown): number {
  const result = value ?? DEFAULT_WORKFLOW_TERMINAL_LOOP_DECISION_TIMEOUT_MS;
  if (
    !Number.isSafeInteger(result) ||
    (result as number) < 1 ||
    (result as number) > MAX_WORKFLOW_TERMINAL_LOOP_DECISION_TIMEOUT_MS
  ) {
    throw new TypeError(
      `loop decision timeoutMs must be between 1 and ${MAX_WORKFLOW_TERMINAL_LOOP_DECISION_TIMEOUT_MS}`,
    );
  }
  return result as number;
}

function evaluationObservation(input: {
  inputData?: unknown;
  state: unknown;
  requestContext: RequestContext;
  stepResults: unknown;
  resumeData?: unknown;
}): string {
  return getWorkflowTerminalCanonicalJson(
    materializeWorkflowTerminalCanonicalJson(
      {
        ...(input.inputData === undefined ? {} : { inputData: input.inputData }),
        state: input.state,
        requestContext: Object.fromEntries(input.requestContext.entries()),
        stepResults: input.stepResults,
        ...(input.resumeData === undefined ? {} : { resumeData: input.resumeData }),
      },
      'loop condition evaluation inputs',
    ),
  );
}

interface UnsupportedEffectObservation {
  attempted: boolean;
}

function rejectUnsupportedEffect(observation: UnsupportedEffectObservation, name: string): never {
  observation.attempted = true;
  throw new UnsupportedDurableLoopConditionEffect(name);
}

function deniedCapability(name: string, observation: UnsupportedEffectObservation): object {
  const reject = () => rejectUnsupportedEffect(observation, name);
  return new Proxy(Object.create(null) as object, {
    defineProperty: reject,
    deleteProperty: reject,
    get: reject,
    getOwnPropertyDescriptor: reject,
    getPrototypeOf: reject,
    has: reject,
    isExtensible: reject,
    ownKeys: reject,
    preventExtensions: reject,
    set: reject,
    setPrototypeOf: reject,
  });
}

function callbackSourceMatches(
  input: EvaluateEventedWorkflowTerminalLoopDecisionInput,
  frame: WorkflowTerminalLoopConditionFrameV1,
) {
  if (isProxy(input.condition)) return false;
  try {
    return (
      input.conditionId === frame.conditionId &&
      getWorkflowTerminalLoopConditionSourceHash(Function.prototype.toString.call(input.condition)) ===
        frame.conditionSourceHash
    );
  } catch {
    return false;
  }
}

/**
 * Executes one capability-reduced callback attempt. It never reads storage,
 * publishes an event, or converts a non-decision outcome into a boolean.
 */
interface StartedLoopDecisionEvaluation {
  result: Promise<EventedWorkflowTerminalLoopDecisionEvaluationResult>;
  callbackSettled: Promise<void>;
  wasCallbackInvoked(): boolean;
  isCallbackSettled(): boolean;
}

function startEventedWorkflowTerminalLoopDecision(
  input: EvaluateEventedWorkflowTerminalLoopDecisionInput,
): StartedLoopDecisionEvaluation {
  const safeInput = copyEvaluationInput(input);
  return startValidatedEventedWorkflowTerminalLoopDecision(safeInput);
}

function startValidatedEventedWorkflowTerminalLoopDecision(
  safeInput: EvaluateEventedWorkflowTerminalLoopDecisionInput,
): StartedLoopDecisionEvaluation {
  const frame = safeInput.frame;
  const request = frame.request;
  const timeoutMs = timeout(safeInput.timeoutMs);
  if (!callbackSourceMatches(safeInput, frame)) {
    return {
      result: Promise.resolve({ status: 'callback_mismatch', request }),
      callbackSettled: Promise.resolve(),
      wasCallbackInvoked: () => false,
      isCallbackSettled: () => true,
    };
  }
  if (safeInput.abortSignal && isSignalAborted(safeInput.abortSignal)) {
    return {
      result: Promise.resolve({ status: 'aborted', request }),
      callbackSettled: Promise.resolve(),
      wasCallbackInvoked: () => false,
      isCallbackSettled: () => true,
    };
  }

  const sandbox = copyWorkflowTerminalLoopConditionFrame(frame);
  const requestContext = new RequestContext(Object.entries(sandbox.requestContext) as any);
  const baseline = evaluationObservation({
    ...(sandbox.inputData === undefined ? {} : { inputData: sandbox.inputData }),
    state: sandbox.state,
    requestContext,
    stepResults: sandbox.stepResults,
    ...(sandbox.resumeData === undefined ? {} : { resumeData: sandbox.resumeData }),
  });
  const controller = new AbortController();
  let abortStatus: 'aborted' | 'timed_out' = 'aborted';
  const onExternalAbort = () => controller.abort();
  if (safeInput.abortSignal) addAbortListener(safeInput.abortSignal, onExternalAbort);
  const timer = setTimeout(() => {
    abortStatus = 'timed_out';
    controller.abort();
  }, timeoutMs);
  const unsupportedEffect: UnsupportedEffectObservation = { attempted: false };
  const mastra = deniedCapability('Mastra', unsupportedEffect);
  const writer = deniedCapability('writer', unsupportedEffect);
  const pubsub = deniedCapability('PubSub', unsupportedEffect);
  const engine = deniedCapability('workflow engine', unsupportedEffect);

  let callbackInvoked = false;
  let callbackHasSettled = false;
  const callback = Promise.resolve().then(() => {
    if (controller.signal.aborted) return undefined;
    callbackInvoked = true;
    return safeInput.condition({
      workflowId: sandbox.parentWorkflowName,
      runId: sandbox.parentRunId,
      mastra: mastra as never,
      requestContext,
      inputData: sandbox.inputData,
      state: sandbox.state,
      retryCount: 0,
      resumeData: sandbox.resumeData,
      getInitData: () => sandbox.stepResults.input,
      getStepResult: getStepResult.bind(null, sandbox.stepResults as any),
      bail: () => rejectUnsupportedEffect(unsupportedEffect, 'bail'),
      abort: () => controller.abort(),
      [PUBSUB_SYMBOL]: pubsub as never,
      [STREAM_FORMAT_SYMBOL]: undefined,
      engine: engine as never,
      abortSignal: controller.signal,
      writer: writer as never,
      iterationCount: sandbox.iterationCount,
    } as never);
  });
  const settled = callback.then(
    value => ({ type: 'resolved' as const, value }),
    error => ({ type: 'failed' as const, error }),
  );
  const callbackSettled = settled.then(() => {
    callbackHasSettled = true;
  });
  const aborted = new Promise<{ type: 'aborted' }>(resolve => {
    addAbortListener(controller.signal, () => resolve({ type: 'aborted' }));
  });

  const result = (async (): Promise<EventedWorkflowTerminalLoopDecisionEvaluationResult> => {
    try {
      const outcome = await Promise.race([settled, aborted]);
      if (outcome.type === 'aborted' || controller.signal.aborted) {
        return { status: abortStatus, request };
      }
      if (
        unsupportedEffect.attempted ||
        (outcome.type === 'failed' && isUnsupportedDurableLoopConditionEffect(outcome.error))
      ) {
        return { status: 'unsupported_effect', request };
      }
      let observation: string;
      try {
        observation = evaluationObservation({
          ...(sandbox.inputData === undefined ? {} : { inputData: sandbox.inputData }),
          state: sandbox.state,
          requestContext,
          stepResults: sandbox.stepResults,
          ...(sandbox.resumeData === undefined ? {} : { resumeData: sandbox.resumeData }),
        });
      } catch {
        return { status: 'unsupported_mutation', request };
      }
      if (observation !== baseline) return { status: 'unsupported_mutation', request };
      if (outcome.type === 'failed') {
        return { status: 'failed', request };
      }
      if (typeof outcome.value !== 'boolean') return { status: 'invalid_result', request };
      return {
        status: 'evaluated',
        request,
        evaluatedDecision: completeWorkflowTerminalLoopDecision(request, outcome.value),
      };
    } finally {
      clearTimeout(timer);
      if (safeInput.abortSignal) removeAbortListener(safeInput.abortSignal, onExternalAbort);
    }
  })();
  return {
    result,
    callbackSettled,
    wasCallbackInvoked: () => callbackInvoked,
    isCallbackSettled: () => callbackHasSettled,
  };
}

export async function evaluateEventedWorkflowTerminalLoopDecision(
  input: EvaluateEventedWorkflowTerminalLoopDecisionInput,
): Promise<EventedWorkflowTerminalLoopDecisionEvaluationResult> {
  return startEventedWorkflowTerminalLoopDecision(input).result;
}

interface EvaluationEntry {
  promise: Promise<EventedWorkflowTerminalLoopDecisionEvaluationResult>;
  retained: false | 'evaluated' | 'in_flight';
  isCallbackSettled(): boolean;
}

/**
 * Bounded process-local coalescing. Successful decisions remain reusable, while
 * aborts/timeouts retain their decision key only until the invoked callback
 * actually settles. JavaScript cannot terminate a non-cooperative callback, so
 * a permanently pending callback remains retained without a TTL. Capacity
 * exhaustion fails closed and is exposed through getStats().
 */
export class EventedWorkflowTerminalLoopDecisionEvaluator {
  readonly #evaluations = new Map<string, EvaluationEntry>();
  #capacityExceeded = 0;

  getStats(): { size: number; retainedInFlight: number; capacityExceeded: number } {
    let retainedInFlight = 0;
    for (const entry of this.#evaluations.values()) {
      if (entry.retained === 'in_flight') retainedInFlight++;
    }
    return { size: this.#evaluations.size, retainedInFlight, capacityExceeded: this.#capacityExceeded };
  }

  evaluate(
    input: EvaluateEventedWorkflowTerminalLoopDecisionInput,
  ): Promise<EventedWorkflowTerminalLoopDecisionEvaluationResult> {
    const safeInput = copyEvaluationInput(input);
    const frame = safeInput.frame;
    const key = frame.request.decisionKey;
    const existing = this.#evaluations.get(key);
    if (existing) {
      if (existing.retained === 'in_flight' && existing.isCallbackSettled()) {
        this.#evaluations.delete(key);
      } else {
        return existing.promise.then(copyEvaluationResult);
      }
    }
    this.#evictCompleted();
    if (this.#evaluations.size >= MAX_PROCESS_LOCAL_LOOP_DECISIONS) {
      this.#capacityExceeded++;
      return Promise.resolve({ status: 'capacity_exceeded', request: frame.request });
    }
    const started = startValidatedEventedWorkflowTerminalLoopDecision(safeInput);
    const entry: EvaluationEntry = {
      promise: started.result,
      retained: false,
      isCallbackSettled: started.isCallbackSettled,
    };
    this.#evaluations.set(key, entry);
    void entry.promise.then(
      result => {
        const current = this.#evaluations.get(key);
        if (current !== entry) return;
        if (result.status === 'evaluated') {
          entry.retained = 'evaluated';
          this.#evictCompleted();
        } else if (
          (result.status === 'aborted' || result.status === 'timed_out') &&
          started.wasCallbackInvoked() &&
          !started.isCallbackSettled()
        ) {
          entry.retained = 'in_flight';
          void started.callbackSettled.then(() => {
            if (this.#evaluations.get(key) === entry) this.#evaluations.delete(key);
          });
        } else {
          this.#evaluations.delete(key);
        }
      },
      () => {
        if (this.#evaluations.get(key) === entry) {
          this.#evaluations.delete(key);
        }
      },
    );
    return entry.promise.then(copyEvaluationResult);
  }

  #evictCompleted(): void {
    if (this.#evaluations.size < MAX_PROCESS_LOCAL_LOOP_DECISIONS) return;
    for (const [key, entry] of this.#evaluations) {
      if (entry.retained === 'evaluated') {
        this.#evaluations.delete(key);
        if (this.#evaluations.size < MAX_PROCESS_LOCAL_LOOP_DECISIONS) return;
      }
    }
  }
}
