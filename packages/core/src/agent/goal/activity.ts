import type { MastraUnion } from '../../action';
import type { RequestContext } from '../../request-context';
import type { GoalObjectiveRecord } from '../../storage/domains/thread-state/base';
import { cacheGoalObjective, clearCachedGoalObjective } from './activity-cache';
import { mutateObjective, readObjective, resolveGoalStore } from './objective';
import type { ResolvedGoalStore } from './objective';

interface ActiveGoalSegment {
  mastra: MastraUnion | undefined;
  agentId: string;
  resourceId: string;
  threadId: string;
  objectiveId: string;
  startedAt: number;
  store: ResolvedGoalStore;
}

interface GoalActivityTarget {
  mastra: MastraUnion | undefined;
  agentId: string;
  resourceId: string | undefined;
  threadId: string | undefined;
  runId: string;
  requestContext?: RequestContext;
  now?: () => number;
}

const activeSegments = new Map<string, ActiveGoalSegment>();
const checkpointedDurations = new Map<string, { objectiveId: string; durationMs: number }>();
const writeQueues = new WeakMap<ResolvedGoalStore, Map<string, Promise<void>>>();

function objectiveScopeKey(agentId: string, resourceId: string, threadId: string): string {
  return `${agentId}:${resourceId}:${threadId}`;
}

function segmentKey(agentId: string, runId: string): string {
  return `${agentId}:${runId}`;
}

function normalizeDuration(value: number | undefined): number {
  return value !== undefined && Number.isFinite(value) && value >= 0 ? value : 0;
}

function debugFailure(mastra: MastraUnion | undefined, message: string, context: Record<string, unknown>): void {
  try {
    mastra?.getLogger()?.debug(message, context);
  } catch {
    // Logging must not turn best-effort timing persistence into an agent failure.
  }
}

function enqueueThreadWrite(store: ResolvedGoalStore, threadId: string, write: () => Promise<void>): Promise<void> {
  let storeQueues = writeQueues.get(store);
  if (!storeQueues) {
    storeQueues = new Map();
    writeQueues.set(store, storeQueues);
  }

  const previous = storeQueues.get(threadId) ?? Promise.resolve();
  const next = previous.catch(() => {}).then(write);
  storeQueues.set(threadId, next);
  return next.finally(() => {
    if (storeQueues.get(threadId) === next) {
      storeQueues.delete(threadId);
    }
  });
}

/** Begin an in-process active-pursuit segment for an active thread objective. */
export async function beginGoalActivity({
  mastra,
  agentId,
  resourceId,
  threadId,
  runId,
  requestContext,
  now = Date.now,
}: GoalActivityTarget): Promise<void> {
  if (!resourceId || !threadId) return;

  const key = segmentKey(agentId, runId);
  if (activeSegments.has(key)) return;

  clearCachedGoalObjective(requestContext);
  let store: ResolvedGoalStore | undefined;
  let objective: GoalObjectiveRecord | undefined;
  try {
    store = await resolveGoalStore(mastra);
    objective = await readObjective(store, resourceId, threadId);
    cacheGoalObjective(requestContext, threadId, objective);
  } catch (error) {
    debugFailure(mastra, 'Failed to begin goal activity tracking', { error, agentId, threadId, runId });
    return;
  }
  if (!store || objective?.status !== 'active') return;

  const objectiveId = objective.id ?? objective.objective;
  checkpointedDurations.set(objectiveScopeKey(agentId, resourceId, threadId), {
    objectiveId,
    durationMs: normalizeDuration(objective.activeDurationMs),
  });
  activeSegments.set(key, { mastra, agentId, resourceId, threadId, objectiveId, startedAt: now(), store });
}

/**
 * Stop and durably checkpoint an active-pursuit segment. Calling this for an
 * already-stopped run is a no-op.
 */
export async function stopGoalActivity({
  agentId,
  runId,
  now = Date.now,
}: Pick<GoalActivityTarget, 'agentId' | 'runId' | 'now'>): Promise<void> {
  const key = segmentKey(agentId, runId);
  const segment = activeSegments.get(key);
  if (!segment) return;

  activeSegments.delete(key);
  const stoppedAt = now();
  const elapsedMs = Math.max(0, stoppedAt - segment.startedAt);

  try {
    await enqueueThreadWrite(segment.store, segment.threadId, async () => {
      // The id check and the duration increment must be computed against the
      // record as it stands inside the store's mutation lane. A separate read
      // followed by a wholesale write would resurrect (or overwrite) an
      // objective that a concurrent clear/replace/verdict committed between
      // the two.
      const outcome = await mutateObjective<{
        record: GoalObjectiveRecord | undefined;
        activeDurationMs: number | undefined;
      }>(segment.store, segment.resourceId, segment.threadId, current => {
        if (!current || (current.id ?? current.objective) !== segment.objectiveId) {
          return { operation: 'keep', result: { record: current, activeDurationMs: undefined } };
        }
        const activeDurationMs = normalizeDuration(current.activeDurationMs) + elapsedMs;
        const value: GoalObjectiveRecord = {
          ...current,
          activeDurationMs,
          updatedAt: Math.max(current.updatedAt, stoppedAt),
        };
        return { operation: 'set', value, result: { record: value, activeDurationMs } };
      });
      // Only advance the in-memory checkpoint when the durable write applied;
      // a skipped write must not make the display lane report time the store
      // never accepted.
      if (outcome?.activeDurationMs !== undefined) {
        checkpointedDurations.set(objectiveScopeKey(segment.agentId, segment.resourceId, segment.threadId), {
          objectiveId: segment.objectiveId,
          durationMs: outcome.activeDurationMs,
        });
      }
    });
  } catch (error) {
    debugFailure(segment.mastra, 'Failed to persist goal activity duration', {
      error,
      agentId: segment.agentId,
      threadId: segment.threadId,
      runId,
    });
  }
}

/**
 * Read the wall-clock start of the earliest live core-owned segment for the
 * objective, for display surfaces that tick elapsed pursuit time between
 * duration checkpoints. Undefined when no matching segment is live.
 */
export function getGoalActivitySegmentStartMs({
  agentId,
  resourceId,
  threadId,
  objectiveId,
}: {
  agentId: string;
  resourceId: string | undefined;
  threadId: string | undefined;
  objectiveId: string | undefined;
}): number | undefined {
  if (!resourceId || !threadId || !objectiveId) return undefined;
  let earliest: number | undefined;
  for (const segment of activeSegments.values()) {
    if (
      segment.agentId === agentId &&
      segment.resourceId === resourceId &&
      segment.threadId === threadId &&
      segment.objectiveId === objectiveId &&
      (earliest === undefined || segment.startedAt < earliest)
    ) {
      earliest = segment.startedAt;
    }
  }
  return earliest;
}

/** Read the persisted duration plus all live core-owned segments for display. */
export function getGoalActivityDurationMs({
  agentId,
  resourceId,
  threadId,
  objectiveId,
  activeDurationMs,
  now = Date.now,
}: {
  agentId: string;
  resourceId: string | undefined;
  threadId: string | undefined;
  objectiveId: string | undefined;
  activeDurationMs: number | undefined;
  now?: () => number;
}): number {
  let durationMs = normalizeDuration(activeDurationMs);
  if (!resourceId || !threadId || !objectiveId) return durationMs;
  const checkpoint = checkpointedDurations.get(objectiveScopeKey(agentId, resourceId, threadId));
  if (checkpoint?.objectiveId === objectiveId) {
    durationMs = Math.max(durationMs, checkpoint.durationMs);
  }

  for (const segment of activeSegments.values()) {
    if (
      segment.agentId === agentId &&
      segment.resourceId === resourceId &&
      segment.threadId === threadId &&
      segment.objectiveId === objectiveId
    ) {
      durationMs += Math.max(0, now() - segment.startedAt);
    }
  }
  return durationMs;
}
