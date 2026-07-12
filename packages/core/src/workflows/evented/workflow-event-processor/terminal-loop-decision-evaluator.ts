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

class UnsupportedDurableLoopConditionEffect extends Error {
  constructor(capability: string) {
    super(`Durable loop conditions cannot use ${capability}`);
    this.name = 'UnsupportedDurableLoopConditionEffect';
  }
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
  if (abortSignal !== undefined && (!(abortSignal instanceof AbortSignal) || isProxy(abortSignal))) {
    throw new TypeError('loop decision abortSignal must be a native AbortSignal');
  }
  if (abortSignal !== undefined) {
    try {
      isSignalAborted(abortSignal);
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

function deniedCapability(name: string): object {
  return new Proxy(Object.create(null) as object, {
    defineProperty() {
      throw new UnsupportedDurableLoopConditionEffect(name);
    },
    deleteProperty() {
      throw new UnsupportedDurableLoopConditionEffect(name);
    },
    get() {
      throw new UnsupportedDurableLoopConditionEffect(name);
    },
    getOwnPropertyDescriptor() {
      throw new UnsupportedDurableLoopConditionEffect(name);
    },
    getPrototypeOf() {
      throw new UnsupportedDurableLoopConditionEffect(name);
    },
    has() {
      throw new UnsupportedDurableLoopConditionEffect(name);
    },
    ownKeys() {
      throw new UnsupportedDurableLoopConditionEffect(name);
    },
    set() {
      throw new UnsupportedDurableLoopConditionEffect(name);
    },
    setPrototypeOf() {
      throw new UnsupportedDurableLoopConditionEffect(name);
    },
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
export async function evaluateEventedWorkflowTerminalLoopDecision(
  input: EvaluateEventedWorkflowTerminalLoopDecisionInput,
): Promise<EventedWorkflowTerminalLoopDecisionEvaluationResult> {
  const safeInput = copyEvaluationInput(input);
  const frame = safeInput.frame;
  const request = frame.request;
  const timeoutMs = timeout(safeInput.timeoutMs);
  if (!callbackSourceMatches(safeInput, frame)) return { status: 'callback_mismatch', request };
  if (safeInput.abortSignal && isSignalAborted(safeInput.abortSignal)) return { status: 'aborted', request };

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
  const mastra = deniedCapability('Mastra');
  const writer = deniedCapability('writer');
  const pubsub = deniedCapability('PubSub');
  const engine = deniedCapability('workflow engine');

  const callback = Promise.resolve().then(() => {
    if (controller.signal.aborted) return undefined;
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
      bail: () => {
        throw new UnsupportedDurableLoopConditionEffect('bail');
      },
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
  const aborted = new Promise<{ type: 'aborted' }>(resolve => {
    addAbortListener(controller.signal, () => resolve({ type: 'aborted' }));
  });

  try {
    const outcome = await Promise.race([settled, aborted]);
    if (outcome.type === 'aborted' || controller.signal.aborted) {
      return { status: abortStatus, request };
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
      return {
        status: outcome.error instanceof UnsupportedDurableLoopConditionEffect ? 'unsupported_effect' : 'failed',
        request,
      };
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
}

interface EvaluationEntry {
  promise: Promise<EventedWorkflowTerminalLoopDecisionEvaluationResult>;
  retained: false | 'evaluated' | 'timed_out';
}

/**
 * Bounded process-local coalescing. Only successful decisions and timed-out
 * non-decisions are retained; caller aborts, failures, and invalid inputs cannot
 * poison a later same-key attempt. Timed-out entries deliberately have no TTL:
 * JavaScript cannot terminate a non-cooperative callback, so expiring one could
 * overlap the still-running same-key attempt. Capacity exhaustion fails closed.
 */
export class EventedWorkflowTerminalLoopDecisionEvaluator {
  readonly #evaluations = new Map<string, EvaluationEntry>();

  evaluate(
    input: EvaluateEventedWorkflowTerminalLoopDecisionInput,
  ): Promise<EventedWorkflowTerminalLoopDecisionEvaluationResult> {
    const safeInput = copyEvaluationInput(input);
    const frame = safeInput.frame;
    const key = frame.request.decisionKey;
    const existing = this.#evaluations.get(key);
    if (existing) return existing.promise;
    this.#evictCompleted();
    if (this.#evaluations.size >= MAX_PROCESS_LOCAL_LOOP_DECISIONS) {
      return Promise.resolve({ status: 'capacity_exceeded', request: frame.request });
    }
    const entry: EvaluationEntry = {
      promise: evaluateEventedWorkflowTerminalLoopDecision(safeInput),
      retained: false,
    };
    this.#evaluations.set(key, entry);
    void entry.promise.then(
      result => {
        const current = this.#evaluations.get(key);
        if (current !== entry) return;
        if (result.status === 'evaluated' || result.status === 'timed_out') {
          entry.retained = result.status;
          this.#evictCompleted();
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
    return entry.promise;
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
