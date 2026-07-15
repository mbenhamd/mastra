import { ErrorCategory, ErrorDomain, MastraError } from '../error';
import { IndexedReplayCursorError, IndexedReplayIntegrityError } from '../events/pubsub';
import type { PubSub } from '../events/pubsub';
import type { Event, EventCallback } from '../events/types';
import type {
  WorkflowLifecycleEvent,
  WorkflowLifecycleEventCallback,
  WorkflowLifecycleWatchOptions,
  WorkflowStreamEvent,
} from './types';

const workflowEventTopic = (runId: string) => `workflow.events.v2.${runId}`;

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

function lifecycleEvent(params: { workflowId: string; runId: string; event: Event }): WorkflowLifecycleEvent {
  const { workflowId, runId, event } = params;
  if (!Number.isSafeInteger(event.index) || event.index! < 0) {
    throw new MastraError({
      id: 'WORKFLOW_LIFECYCLE_EVENT_MISSING_CURSOR',
      domain: ErrorDomain.MASTRA_WORKFLOW,
      category: ErrorCategory.SYSTEM,
      text: 'Workflow lifecycle event is missing a valid indexed replay cursor',
      details: { workflowId, runId, eventId: event.id },
    });
  }
  if (typeof event.logGeneration !== 'string' || event.logGeneration.length === 0) {
    throw new MastraError({
      id: 'WORKFLOW_LIFECYCLE_EVENT_MISSING_LOG_GENERATION',
      domain: ErrorDomain.MASTRA_WORKFLOW,
      category: ErrorCategory.SYSTEM,
      text: 'Workflow lifecycle event is missing its retained-log generation',
      details: { workflowId, runId, eventId: event.id, cursor: event.index! },
    });
  }

  return {
    eventId: event.id,
    cursor: event.index!,
    createdAt: event.createdAt,
    deliveryAttempt: event.deliveryAttempt ?? 1,
    logGeneration: event.logGeneration,
    workflowId,
    runId,
    event: event.data as WorkflowStreamEvent,
  };
}

function lifecycleReplayError(error: unknown, context: { workflowId: string; runId: string }): Error {
  const { workflowId, runId } = context;
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
      details: { workflowId, runId, reason: error.reason },
    });
  }
  if (error instanceof Error) return error;
  return new MastraError({
    id: 'WORKFLOW_LIFECYCLE_REPLAY_FAILURE',
    domain: ErrorDomain.MASTRA_WORKFLOW,
    category: ErrorCategory.SYSTEM,
    text: 'Workflow lifecycle replay failed with a non-Error value',
    details: { workflowId, runId },
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
  callback: WorkflowLifecycleEventCallback;
  options?: WorkflowLifecycleWatchOptions;
}): Promise<() => Promise<void>> {
  const { pubsub, workflowId, runId, callback, options } = params;
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

  const topic = workflowEventTopic(runId);
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
    // The topic is run-scoped. A mismatched event is malformed for this
    // subscription, but acknowledging it prevents one poison message from
    // blocking every valid event that follows.
    if (event.runId !== runId) {
      await ack?.();
      return;
    }

    try {
      await callback(lifecycleEvent({ workflowId, runId, event }));
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
        await options?.onError?.(lifecycleReplayError(error, { workflowId, runId }));
      },
    });
  } catch (error) {
    throw lifecycleReplayError(error, { workflowId, runId });
  }

  return async () => {
    await pubsub.unsubscribe(topic, watchCallback);
    await deliveryChain;
  };
}
