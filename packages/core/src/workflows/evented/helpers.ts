/**
 * Helper functions for evented workflow execution.
 */

import { TripWire } from '../../agent/trip-wire';
import { MASTRA_AUTH_ORGANIZATION_KEY } from '../../request-context';
import type { StepResult } from '../types';
import type { Workflow } from '../workflow';
import type { ParentWorkflow } from './workflow-event-processor';

/**
 * Whether this run's durable snapshot row is expected to exist — the axis
 * that makes an `{}` write result survivable. Top-level evented runs always
 * persist their initial record in `EventedRun.start`, so a missing row there
 * means deletion. A nested run's row exists only when the CHILD's own
 * `shouldPersistSnapshot` opted in: `parentWorkflow.shouldPersistSnapshot`
 * describes the parent's snapshot, not this run's, so consulting it would
 * abandon transient children of durable parents (their first completed step
 * would halt without advancing, hanging the parent) and would let deleted
 * durable children of transient parents fall through as if opted out.
 */
export function runExpectsPersistedRow(
  workflow: Workflow,
  parentWorkflow: ParentWorkflow | undefined,
  stepResults: Record<string, StepResult<any, any, any, any>> | undefined,
): boolean {
  if (parentWorkflow === undefined) return true;
  return (
    workflow.options?.shouldPersistSnapshot?.({
      stepResults: stepResults ?? {},
      workflowStatus: 'running',
    }) ?? true
  );
}

/** Keep authenticated selection live in events, but never recover it from a stored context. */
export function getPersistedRequestContext(requestContext: Record<string, any>): Record<string, any> {
  if (!requestContext || !Object.hasOwn(requestContext, MASTRA_AUTH_ORGANIZATION_KEY)) return requestContext;
  const persisted = { ...requestContext };
  delete persisted[MASTRA_AUTH_ORGANIZATION_KEY];
  return persisted;
}

/**
 * Interface for tripwire chunks in the stream.
 * These chunks are emitted when a processor triggers a tripwire.
 */
export interface TripwireChunk {
  type: 'tripwire';
  payload: {
    reason: string;
    retry?: boolean;
    metadata?: unknown;
    processorId?: string;
  };
}

/**
 * Type guard to check if a chunk is a tripwire chunk.
 * @param chunk - The chunk to check
 * @returns True if the chunk is a TripwireChunk
 */
export function isTripwireChunk(chunk: unknown): chunk is TripwireChunk {
  return (
    chunk !== null && typeof chunk === 'object' && 'type' in chunk && chunk.type === 'tripwire' && 'payload' in chunk
  );
}

/**
 * Creates a TripWire error from a tripwire chunk.
 * @param chunk - The tripwire chunk from the stream
 * @returns A TripWire error instance
 */
export function createTripWireFromChunk(chunk: TripwireChunk): TripWire {
  const { payload } = chunk;
  return new TripWire(
    payload.reason || 'Agent tripwire triggered',
    {
      retry: payload.retry,
      metadata: payload.metadata,
    },
    payload.processorId,
  );
}

/**
 * Extracts text delta from a stream chunk, handling V1 vs V2 differences.
 *
 * V1 (AI SDK v4): Uses `chunk.textDelta` for raw text
 * V2 (AI SDK v5): Uses `chunk.payload.text` for normalized text
 *
 * @param chunk - The stream chunk
 * @param isV2Model - Whether this is a V2 model (uses normalized payload)
 * @returns The text delta string, or undefined if not a text-delta chunk
 */
export function getTextDeltaFromChunk(
  chunk: { type: string; textDelta?: string; payload?: { text?: string } },
  isV2Model: boolean,
): string | undefined {
  if (chunk.type !== 'text-delta') {
    return undefined;
  }
  return isV2Model ? chunk.payload?.text : chunk.textDelta;
}

/**
 * Parameters for resolving the current workflow state.
 */
export interface ResolveStateParams {
  /** State from a step result (highest priority). Uses `any` to accommodate various StepResult types. */
  stepResult?: unknown;
  /** State from all step results */
  stepResults?: { __state?: Record<string, unknown> };
  /** State passed directly */
  state?: Record<string, unknown>;
}

/**
 * Resolves the current workflow state from multiple potential sources.
 * Priority order: stepResult.__state > stepResults.__state > state > empty object
 *
 * @param params - The state sources to check
 * @returns The resolved state object
 */
export function resolveCurrentState(params: ResolveStateParams): Record<string, unknown> {
  const { stepResult, stepResults, state } = params;
  return (stepResult as any)?.__state ?? stepResults?.__state ?? state ?? {};
}
