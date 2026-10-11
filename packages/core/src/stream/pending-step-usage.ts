import type { LanguageModelUsage } from './types';

const PENDING_STEP_USAGE_FIELDS = [
  'inputTokens',
  'outputTokens',
  'totalTokens',
  'reasoningTokens',
  'cachedInputTokens',
  'cacheCreationInputTokens',
  'cacheCreationInputTokens5m',
  'cacheCreationInputTokens1h',
] as const;

/**
 * Token counts of a model step whose tool call suspended the run, before that
 * step's `step-finish` is emitted (PF-5349). Numeric counts only: it is
 * serialized into the suspension snapshot, so provider-specific `raw` data is
 * never carried.
 */
export type PendingStepUsage = Partial<Record<(typeof PENDING_STEP_USAGE_FIELDS)[number], number>>;

/** JSON-safe projection of a step's usage; `undefined` when it has no counts. */
export function toPendingStepUsage(usage: Partial<LanguageModelUsage> | undefined): PendingStepUsage | undefined {
  if (!usage) return undefined;
  const projected: PendingStepUsage = {};
  for (const field of PENDING_STEP_USAGE_FIELDS) {
    const value = usage[field];
    if (typeof value === 'number' && Number.isFinite(value) && value >= 0) projected[field] = value;
  }
  return Object.keys(projected).length > 0 ? projected : undefined;
}

/** Add pending step counts to completed-step usage; completed `raw` is kept. */
export function addPendingStepUsage(
  usage: LanguageModelUsage,
  pending: PendingStepUsage | undefined,
): LanguageModelUsage {
  if (pending === undefined) return usage;
  const sum: LanguageModelUsage = { ...usage };
  for (const field of PENDING_STEP_USAGE_FIELDS) {
    const left = usage[field];
    const right = pending[field];
    if (left === undefined && right === undefined) continue;
    sum[field] = (left ?? 0) + (right ?? 0);
  }
  return sum;
}
