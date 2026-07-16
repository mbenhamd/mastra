import { createHash, randomUUID } from 'node:crypto';

import type { PubSub } from '../events/pubsub';

/**
 * Opaque identity for one execution lineage of a workflow run.
 *
 * A generation is retained while a suspended run is resumed. Restart and
 * time-travel executions create a new generation, even when they reuse the
 * same workflow and run identifiers. This is intentionally unrelated to the
 * numeric generation used by the internal workflow registry.
 */
export type WorkflowExecutionGeneration = string;

/** Public identity required to subscribe to exactly one workflow execution. */
export type WorkflowLifecycleExecutionIdentity = {
  workflowId: string;
  runId: string;
  executionGeneration: WorkflowExecutionGeneration;
  topic: string;
};

export type WorkflowStepLifecycleIdentity = {
  stepId: string;
  stepCallId: string;
  /**
   * One-based semantic execution attempt for this step call. Broker
   * redelivery retains the same ordinal and lifecycle identity.
   */
  stepAttempt: number;
};

export type WorkflowLifecycleTransitionIdentity = {
  /** Zero for initial execution, incremented once for each durable resume. */
  resumeAttempt: number;
};

type WorkflowLifecycleStarted = WorkflowLifecycleTransitionIdentity & {
  type: 'workflow.started';
  input?: unknown;
};

type WorkflowLifecycleResumed = WorkflowLifecycleTransitionIdentity & {
  type: 'workflow.resumed';
  resumeData?: unknown;
};

type WorkflowLifecycleSuspended = WorkflowLifecycleTransitionIdentity & {
  type: 'workflow.suspended';
  suspendedStepIds: string[];
};

type WorkflowLifecycleFailed = WorkflowLifecycleTransitionIdentity & {
  type: 'workflow.failed';
  error: unknown;
};

type WorkflowLifecycleCanceled = WorkflowLifecycleTransitionIdentity & {
  type: 'workflow.canceled';
  reason?: unknown;
};

type WorkflowLifecycleFinished = WorkflowLifecycleTransitionIdentity & {
  type: 'workflow.finished';
  status: 'success' | 'failed' | 'canceled' | 'tripwire';
  result?: unknown;
  error?: unknown;
};

type WorkflowStepLifecycleStarted = WorkflowStepLifecycleIdentity & {
  type: 'step.started';
  input?: unknown;
};

type WorkflowStepLifecycleRetrying = WorkflowStepLifecycleIdentity & {
  type: 'step.retrying';
  error?: unknown;
};

type WorkflowStepLifecycleResumed = WorkflowStepLifecycleIdentity & {
  type: 'step.resumed';
  resumeData?: unknown;
};

type WorkflowStepLifecycleSuspended = WorkflowStepLifecycleIdentity & {
  type: 'step.suspended';
  suspendPayload?: unknown;
};

type WorkflowStepLifecycleCompleted = WorkflowStepLifecycleIdentity & {
  type: 'step.completed';
  output?: unknown;
};

type WorkflowStepLifecycleFailed = WorkflowStepLifecycleIdentity & {
  type: 'step.failed';
  error: unknown;
};

type WorkflowStepLifecycleCanceled = WorkflowStepLifecycleIdentity & {
  type: 'step.canceled';
  reason?: unknown;
};

type WorkflowStepLifecycleFinished = WorkflowStepLifecycleIdentity & {
  type: 'step.finished';
  status: 'success' | 'failed' | 'suspended' | 'canceled' | 'tripwire';
};

/**
 * Exhaustive, application-facing workflow lifecycle transitions.
 *
 * This union is deliberately independent from `WorkflowStreamEvent`: stream
 * events also contain arbitrary model, tool, and UI chunks, while lifecycle
 * consumers need a closed set of state transitions.
 */
export type WorkflowLifecycleEvent =
  | WorkflowLifecycleStarted
  | WorkflowLifecycleResumed
  | WorkflowLifecycleSuspended
  | WorkflowLifecycleFailed
  | WorkflowLifecycleCanceled
  | WorkflowLifecycleFinished
  | WorkflowStepLifecycleStarted
  | WorkflowStepLifecycleRetrying
  | WorkflowStepLifecycleResumed
  | WorkflowStepLifecycleSuspended
  | WorkflowStepLifecycleCompleted
  | WorkflowStepLifecycleFailed
  | WorkflowStepLifecycleCanceled
  | WorkflowStepLifecycleFinished;

/** Data written once to the canonical lifecycle topic. */
export type WorkflowLifecycleRecord<TEvent extends WorkflowLifecycleEvent = WorkflowLifecycleEvent> = {
  schemaVersion: 1;
  workflowId: string;
  runId: string;
  executionGeneration: WorkflowExecutionGeneration;
  event: TEvent;
};

/**
 * Retained lifecycle delivery exposed by replay-capable transports.
 *
 * `logGeneration` belongs to the retained log and must not be confused with
 * `executionGeneration`, which belongs to workflow execution semantics.
 */
export type WorkflowLifecycleEnvelope<TEvent extends WorkflowLifecycleEvent = WorkflowLifecycleEvent> =
  WorkflowLifecycleRecord<TEvent> & {
    eventId: string;
    cursor: number;
    logGeneration: string;
    createdAt: Date;
    deliveryAttempt: number;
  };

export type WorkflowStepLifecycleState = {
  stepCallId: string;
  stepAttempt: number;
};

export type WorkflowStepLifecycleStateMap = Record<string, WorkflowStepLifecycleState>;

const lifecycleCleanupTimers = new WeakMap<PubSub, Map<string, ReturnType<typeof setTimeout>>>();
const MIN_WORKFLOW_LIFECYCLE_CLEANUP_DELAY_MS = 30_000;

function scheduleWorkflowLifecycleTopicCleanup(pubsub: PubSub, topic: string): void {
  // Exact-replay transports advertise how long reconnects remain valid. Plain
  // persistent transports do not, but still need a bounded cleanup backstop.
  // Keep every terminal topic available for at least a short reconnect window.
  const retentionMs = Math.max(
    MIN_WORKFLOW_LIFECYCLE_CLEANUP_DELAY_MS,
    pubsub.indexedReplay?.retentionMs ?? MIN_WORKFLOW_LIFECYCLE_CLEANUP_DELAY_MS,
  );

  let timers = lifecycleCleanupTimers.get(pubsub);
  if (!timers) {
    timers = new Map();
    lifecycleCleanupTimers.set(pubsub, timers);
  }
  const existing = timers.get(topic);
  if (existing) clearTimeout(existing);

  const deadline = Date.now() + retentionMs;
  const arm = () => {
    const remaining = deadline - Date.now();
    if (remaining > 0) {
      const timer = setTimeout(arm, Math.min(remaining, 2_147_483_647));
      timer.unref?.();
      timers!.set(topic, timer);
      return;
    }
    timers!.delete(topic);
    if (timers!.size === 0) lifecycleCleanupTimers.delete(pubsub);
    void pubsub.clearTopic(topic);
  };
  arm();
}

/**
 * Reconcile broker-carried step state with the authoritative persisted state.
 *
 * Broker payloads are cloned so dispatch cannot mutate an event object that a
 * transport may redeliver by reference. For the same execution generation,
 * persisted attempts are monotonic and conflicting deterministic call ids are
 * rejected instead of silently replacing projection identity.
 */
export function mergeWorkflowStepLifecycleStates(
  incoming: WorkflowStepLifecycleStateMap | undefined,
  persisted: WorkflowStepLifecycleStateMap | undefined,
): WorkflowStepLifecycleStateMap {
  const merged: WorkflowStepLifecycleStateMap = {};

  for (const [key, state] of Object.entries(persisted ?? {})) {
    merged[key] = { ...state };
  }
  for (const [key, state] of Object.entries(incoming ?? {})) {
    const retained = merged[key];
    if (!retained) {
      merged[key] = { ...state };
      continue;
    }
    if (retained.stepCallId !== state.stepCallId) {
      throw new Error(`Workflow lifecycle state ${key} has conflicting step-call identity`);
    }
    retained.stepAttempt = Math.max(retained.stepAttempt, state.stepAttempt);
  }

  return merged;
}

export function createWorkflowExecutionGeneration(): WorkflowExecutionGeneration {
  return randomUUID();
}

/** Fail closed when an in-flight execution loses its reserved lineage. */
export function requireWorkflowExecutionGeneration(value: unknown, owner: string): WorkflowExecutionGeneration {
  if (typeof value !== 'string' || value.length === 0) {
    throw new Error(`${owner} requires a reserved workflow execution generation`);
  }
  return value;
}

function encodeTopicSegment(value: string): string {
  // `encodeURIComponent` leaves dots intact. Encode them too because dots are
  // the canonical segment delimiter for lifecycle topics.
  return encodeURIComponent(value).replaceAll('.', '%2E');
}

/**
 * Canonical topic for one exact workflow execution lineage.
 *
 * Including all three identities prevents two workflows that intentionally
 * reuse a run id, or a restarted execution that reuses both ids, from sharing
 * retained lifecycle history.
 */
export function getWorkflowLifecycleTopic(params: {
  workflowId: string;
  runId: string;
  executionGeneration: WorkflowExecutionGeneration;
}): string {
  return [
    'workflow.lifecycle.v1',
    encodeTopicSegment(params.workflowId),
    encodeTopicSegment(params.runId),
    encodeTopicSegment(params.executionGeneration),
  ].join('.');
}

function lifecycleCoordinateKey(params: {
  stepId: string;
  executionPath: readonly number[];
  foreachIndex?: number;
  iterationCount?: number;
}): string {
  return JSON.stringify([
    params.stepId,
    params.executionPath,
    params.foreachIndex ?? null,
    params.iterationCount ?? null,
  ]);
}

/**
 * Resolve stable step-call identity for one execution coordinate.
 *
 * The digest is deterministic so broker redelivery and process restart do not
 * mint a different call id. The mutable state map retains the one-based
 * semantic attempt count across retry and suspend/resume transitions; broker
 * redelivery of that attempt does not mint a second lifecycle identity.
 */
export function getOrCreateWorkflowStepLifecycleState(params: {
  workflowId: string;
  runId: string;
  executionGeneration: WorkflowExecutionGeneration;
  stepId: string;
  executionPath: readonly number[];
  foreachIndex?: number;
  iterationCount?: number;
  states: WorkflowStepLifecycleStateMap;
}): { key: string; state: WorkflowStepLifecycleState } {
  const key = lifecycleCoordinateKey(params);
  const retained = params.states[key];
  if (retained) return { key, state: retained };

  const stepCallId = createHash('sha256')
    .update('mastra.workflow.step-call.v1\0', 'utf8')
    .update(params.workflowId, 'utf8')
    .update('\0', 'utf8')
    .update(params.runId, 'utf8')
    .update('\0', 'utf8')
    .update(params.executionGeneration, 'utf8')
    .update('\0', 'utf8')
    .update(key, 'utf8')
    .digest('hex');
  const state = { stepCallId: `wfsc:v1:${stepCallId}`, stepAttempt: 0 };
  params.states[key] = state;
  return { key, state };
}

export async function publishWorkflowLifecycleEvent<TEvent extends WorkflowLifecycleEvent>(params: {
  pubsub: PubSub;
  workflowId: string;
  runId: string;
  executionGeneration: WorkflowExecutionGeneration;
  event: TEvent;
}): Promise<void> {
  const record: WorkflowLifecycleRecord<TEvent> = {
    schemaVersion: 1,
    workflowId: params.workflowId,
    runId: params.runId,
    executionGeneration: params.executionGeneration,
    event: params.event,
  };
  const id = getWorkflowLifecycleEventId(record);
  const topic = getWorkflowLifecycleTopic(params);
  await params.pubsub.publish(topic, {
    type: 'workflow.lifecycle',
    id,
    runId: params.runId,
    data: record,
  });
  if (params.event.type === 'workflow.finished') scheduleWorkflowLifecycleTopicCleanup(params.pubsub, topic);
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0;
}

function isStepAttempt(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 1;
}

function isResumeAttempt(value: unknown): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= 0;
}

function hasStepIdentity(value: Record<string, unknown>): boolean {
  return isNonEmptyString(value.stepId) && isNonEmptyString(value.stepCallId) && isStepAttempt(value.stepAttempt);
}

function isStringArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every(isNonEmptyString);
}

const workflowFinishedStatuses = new Set(['success', 'failed', 'canceled', 'tripwire']);
const stepFinishedStatuses = new Set(['success', 'failed', 'suspended', 'canceled', 'tripwire']);

/**
 * Validate a lifecycle event from unknown transport data.
 *
 * Rich outputs/errors remain `unknown`, but every discriminant and identity
 * field is checked. Arbitrary stream or tool chunks therefore cannot be
 * mistaken for lifecycle transitions.
 */
export function isWorkflowLifecycleEvent(value: unknown): value is WorkflowLifecycleEvent {
  if (!isRecord(value) || !isNonEmptyString(value.type)) return false;

  switch (value.type) {
    case 'workflow.started':
    case 'workflow.resumed':
    case 'workflow.canceled':
      return isResumeAttempt(value.resumeAttempt);
    case 'workflow.suspended':
      return isResumeAttempt(value.resumeAttempt) && isStringArray(value.suspendedStepIds);
    case 'workflow.failed':
      return isResumeAttempt(value.resumeAttempt) && Object.hasOwn(value, 'error');
    case 'workflow.finished':
      return (
        isResumeAttempt(value.resumeAttempt) &&
        typeof value.status === 'string' &&
        workflowFinishedStatuses.has(value.status)
      );
    case 'step.started':
    case 'step.retrying':
    case 'step.resumed':
    case 'step.suspended':
    case 'step.completed':
    case 'step.canceled':
      return hasStepIdentity(value);
    case 'step.failed':
      return hasStepIdentity(value) && Object.hasOwn(value, 'error');
    case 'step.finished':
      return hasStepIdentity(value) && typeof value.status === 'string' && stepFinishedStatuses.has(value.status);
    default:
      return false;
  }
}

/** Stable projector key across transport redelivery and transition republish. */
export function getWorkflowLifecycleSemanticKey(record: WorkflowLifecycleRecord): string {
  const base = JSON.stringify([record.workflowId, record.runId, record.executionGeneration]);
  const outcome = 'status' in record.event ? `:${record.event.status}` : '';
  return 'stepCallId' in record.event
    ? `${base}:${record.event.type}:${record.event.stepCallId}:${record.event.stepAttempt}${outcome}`
    : `${base}:${record.event.type}:${record.event.resumeAttempt}${outcome}`;
}

/** Stable transport identity for one semantic lifecycle transition. */
export function getWorkflowLifecycleEventId(record: WorkflowLifecycleRecord): string {
  const digest = createHash('sha256').update('mastra.workflow.lifecycle-event.v1\0', 'utf8');
  digest.update(getWorkflowLifecycleSemanticKey(record), 'utf8');
  return `wfle:v1:${digest.digest('hex')}`;
}

export class WorkflowLifecycleRecordError extends Error {
  readonly reason: 'malformed' | 'identity-mismatch';

  constructor(reason: 'malformed' | 'identity-mismatch', message: string) {
    super(message);
    this.name = 'WorkflowLifecycleRecordError';
    this.reason = reason;
  }
}

/**
 * Validate canonical topic data without asserting a raw stream-event shape.
 *
 * This function is intended for delivery from a canonical lifecycle topic, so
 * malformed data and identity mismatches throw. Consumers must nack/fail the
 * delivery rather than acknowledging and advancing past a poison record.
 */
export function parseWorkflowLifecycleRecord(
  value: unknown,
  expected?: {
    workflowId: string;
    runId: string;
    executionGeneration: WorkflowExecutionGeneration;
  },
): WorkflowLifecycleRecord {
  if (!isRecord(value) || value.schemaVersion !== 1) {
    throw new WorkflowLifecycleRecordError('malformed', 'Workflow lifecycle record has an invalid schema version');
  }
  if (!isNonEmptyString(value.workflowId) || !isNonEmptyString(value.runId)) {
    throw new WorkflowLifecycleRecordError('malformed', 'Workflow lifecycle record has invalid workflow identity');
  }
  if (!isNonEmptyString(value.executionGeneration) || !isWorkflowLifecycleEvent(value.event)) {
    throw new WorkflowLifecycleRecordError('malformed', 'Workflow lifecycle record has invalid execution data');
  }
  if (
    expected &&
    (value.workflowId !== expected.workflowId ||
      value.runId !== expected.runId ||
      value.executionGeneration !== expected.executionGeneration)
  ) {
    throw new WorkflowLifecycleRecordError(
      'identity-mismatch',
      'Workflow lifecycle record does not match the subscribed execution identity',
    );
  }

  return {
    schemaVersion: 1,
    workflowId: value.workflowId,
    runId: value.runId,
    executionGeneration: value.executionGeneration,
    event: value.event,
  };
}
