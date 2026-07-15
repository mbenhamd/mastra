import { ErrorCategory, ErrorDomain, MastraError } from '../error';
import { IndexedReplayCursorError, IndexedReplayIntegrityError } from '../events/pubsub';
import type { PubSub } from '../events/pubsub';
import type { Event, EventCallback } from '../events/types';
import {
  getWorkflowLifecycleEventId,
  getWorkflowLifecycleTopic,
  parseWorkflowLifecycleRecord,
  WorkflowLifecycleRecordError,
} from './lifecycle-events';
import type {
  WorkflowExecutionGeneration,
  WorkflowLifecycleEnvelope,
  WorkflowLifecycleRecord,
} from './lifecycle-events';
import type { WorkflowLifecycleEventCallback, WorkflowLifecycleWatchOptions } from './types';

function replayOffset(afterCursor: number | undefined): number {
  if (afterCursor === undefined) {
    return 0;
  }

  if (!Number.isSafeInteger(afterCursor) || afterCursor < 0 || afterCursor >= Number.MAX_SAFE_INTEGER) {
    throw new MastraError({
      id: 'WORKFLOW_LIFECYCLE_INVALID_CURSOR',
      domain: ErrorDomain.MASTRA_WORKFLOW,
      category: ErrorCategory.USER,
      text: 'Workflow lifecycle afterCursor must be a non-negative safe integer with a representable successor',
      details: { afterCursor },
    });
  }

  return afterCursor + 1;
}

function lifecycleEnvelope(params: {
  workflowId: string;
  runId: string;
  executionGeneration: WorkflowExecutionGeneration;
  event: Event;
}): WorkflowLifecycleEnvelope {
  const { workflowId, runId, executionGeneration, event } = params;
  if (!Number.isSafeInteger(event.index) || event.index! < 0) {
    throw new IndexedReplayIntegrityError(
      'malformed-retained-event',
      'Workflow lifecycle event is missing a valid indexed replay cursor',
    );
  }
  if (typeof event.logGeneration !== 'string' || event.logGeneration.length === 0) {
    throw new IndexedReplayIntegrityError(
      'malformed-retained-event',
      'Workflow lifecycle event is missing its retained-log generation',
    );
  }

  if (
    event.type !== 'workflow.lifecycle' ||
    event.runId !== runId ||
    typeof event.id !== 'string' ||
    event.id.length === 0 ||
    !(event.createdAt instanceof Date) ||
    Number.isNaN(event.createdAt.getTime()) ||
    (event.deliveryAttempt !== undefined && (!Number.isSafeInteger(event.deliveryAttempt) || event.deliveryAttempt < 1))
  ) {
    throw new IndexedReplayIntegrityError(
      'malformed-retained-event',
      'Workflow lifecycle delivery has an invalid transport envelope',
    );
  }

  let record: WorkflowLifecycleRecord;
  try {
    record = parseWorkflowLifecycleRecord(event.data, { workflowId, runId, executionGeneration });
  } catch (error) {
    if (error instanceof WorkflowLifecycleRecordError) {
      throw new IndexedReplayIntegrityError(
        error.reason === 'identity-mismatch' ? 'identity-mismatch' : 'malformed-retained-event',
        error.message,
      );
    }
    throw error;
  }
  const expectedEventId = getWorkflowLifecycleEventId(record);
  if (event.id !== expectedEventId) {
    throw new IndexedReplayIntegrityError(
      'identity-mismatch',
      `Workflow lifecycle event identity mismatch: expected ${expectedEventId}, received ${event.id}`,
    );
  }

  return {
    ...record,
    eventId: event.id,
    cursor: event.index!,
    createdAt: event.createdAt,
    deliveryAttempt: event.deliveryAttempt ?? 1,
    logGeneration: event.logGeneration,
  };
}

function lifecycleReplayError(
  error: unknown,
  context: { workflowId: string; runId: string; executionGeneration: WorkflowExecutionGeneration },
): Error {
  const { workflowId, runId, executionGeneration } = context;
  if (error instanceof IndexedReplayCursorError) {
    return new MastraError({
      id:
        error.reason === 'generation-mismatch'
          ? 'WORKFLOW_LIFECYCLE_LOG_GENERATION_MISMATCH'
          : error.reason === 'cursor-too-old'
            ? 'WORKFLOW_LIFECYCLE_CURSOR_TOO_OLD'
            : 'WORKFLOW_LIFECYCLE_CURSOR_AHEAD',
      domain: ErrorDomain.MASTRA_WORKFLOW,
      category: ErrorCategory.USER,
      text: error.message,
      details: {
        workflowId,
        runId,
        executionGeneration,
        requestedCursor: error.requestedCursor,
        ...(error.requestedLogGeneration === undefined ? {} : { requestedLogGeneration: error.requestedLogGeneration }),
        logGeneration: error.range.logGeneration,
        firstCursor: error.range.firstCursor,
        nextCursor: error.range.nextCursor,
        replayScope: error.range.scope,
        retentionMs: error.range.retentionMs,
        maxEvents: error.range.maxEvents,
      },
    });
  }
  if (error instanceof IndexedReplayIntegrityError) {
    return new MastraError({
      id: 'WORKFLOW_LIFECYCLE_REPLAY_INTEGRITY_FAILURE',
      domain: ErrorDomain.MASTRA_WORKFLOW,
      category: ErrorCategory.SYSTEM,
      text: error.message,
      details: { workflowId, runId, executionGeneration, reason: error.reason },
    });
  }
  if (error instanceof Error) return error;
  return new MastraError({
    id: 'WORKFLOW_LIFECYCLE_REPLAY_FAILURE',
    domain: ErrorDomain.MASTRA_WORKFLOW,
    category: ErrorCategory.SYSTEM,
    text: 'Workflow lifecycle replay failed with a non-Error value',
    details: { workflowId, runId, executionGeneration },
  });
}

/**
 * Subscribe to a run's retained workflow events in cursor order.
 *
 * Delivery is serialized. A callback is acknowledged only after its returned
 * promise resolves; a rejection is negatively acknowledged when the transport
 * supplies a nack handle. A cache-only replay has no broker acknowledgement
 * handle: successful callback resolution is the commit boundary for that
 * subscription cursor, so durable consumers must persist their projection
 * before resolving. A replay callback rejection fails subscription setup and
 * leaves the cursor available for a later retry.
 *
 * Terminal failures after setup are reported through `options.onError` and
 * stop the underlying replay subscription before an ambiguous event can be
 * acknowledged.
 */
export async function watchWorkflowLifecycleEvents(params: {
  pubsub: PubSub;
  workflowId: string;
  runId: string;
  executionGeneration: WorkflowExecutionGeneration;
  callback: WorkflowLifecycleEventCallback;
  options?: WorkflowLifecycleWatchOptions;
}): Promise<() => Promise<void>> {
  const { pubsub, workflowId, runId, executionGeneration, callback, options } = params;
  const offset = replayOffset(options?.afterCursor);

  if (options?.afterCursor !== undefined && !options.afterLogGeneration) {
    throw new MastraError({
      id: 'WORKFLOW_LIFECYCLE_LOG_GENERATION_REQUIRED',
      domain: ErrorDomain.MASTRA_WORKFLOW,
      category: ErrorCategory.USER,
      text: 'Workflow lifecycle resume requires the log generation committed with afterCursor',
      details: { workflowId, runId, afterCursor: options.afterCursor },
    });
  }

  if (!pubsub.supportsIndexedReplay) {
    throw new MastraError({
      id: 'WORKFLOW_LIFECYCLE_REPLAY_UNAVAILABLE',
      domain: ErrorDomain.MASTRA_WORKFLOW,
      category: ErrorCategory.USER,
      text: 'Workflow lifecycle watching requires a PubSub implementation with indexed replay support, such as CachingPubSub',
      details: { workflowId, runId },
    });
  }

  if (pubsub.indexedReplay?.scope !== 'durable' && !options?.allowProcessLocalReplay) {
    throw new MastraError({
      id: 'WORKFLOW_LIFECYCLE_DURABLE_REPLAY_REQUIRED',
      domain: ErrorDomain.MASTRA_WORKFLOW,
      category: ErrorCategory.USER,
      text: 'Workflow lifecycle watching requires durable indexed replay; process-local replay must be opted into explicitly',
      details: {
        workflowId,
        runId,
        replayScope: pubsub.indexedReplay?.scope ?? 'unavailable',
      },
    });
  }

  const topic = getWorkflowLifecycleTopic({ workflowId, runId, executionGeneration });
  const replayRange = await pubsub.getIndexedReplayRange(topic);
  if (!replayRange) {
    throw new MastraError({
      id: 'WORKFLOW_LIFECYCLE_REPLAY_RANGE_UNAVAILABLE',
      domain: ErrorDomain.MASTRA_WORKFLOW,
      category: ErrorCategory.SYSTEM,
      text: 'Workflow lifecycle indexed replay did not expose its retained cursor range',
      details: { workflowId, runId },
    });
  }
  let deliveryChain = Promise.resolve();

  const deliver = async (event: Event, ack?: () => Promise<void>, nack?: () => Promise<void>): Promise<void> => {
    try {
      await callback(lifecycleEnvelope({ workflowId, runId, executionGeneration, event }));
      await ack?.();
    } catch (error) {
      if (nack) {
        await nack();
      }
      throw error;
    }
  };

  const watchCallback: EventCallback = (event, ack, nack) => {
    const delivery = deliveryChain.then(() => deliver(event, ack, nack));
    // Keep the serial queue usable after a live delivery is nacked or a replay
    // setup fails. The individual delivery promise still reports the failure
    // to a replay-capable PubSub that awaits its callback.
    deliveryChain = delivery.catch(() => {});
    return delivery;
  };

  try {
    await pubsub.subscribeFromOffset(topic, offset, watchCallback, {
      logGeneration: options?.afterLogGeneration ?? replayRange.logGeneration,
      onError: async error => {
        await options?.onError?.(lifecycleReplayError(error, { workflowId, runId, executionGeneration }));
      },
    });
  } catch (error) {
    throw lifecycleReplayError(error, { workflowId, runId, executionGeneration });
  }

  return async () => {
    await pubsub.unsubscribe(topic, watchCallback);
    await deliveryChain;
  };
}
