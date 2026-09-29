import {
  executeRetentionPrune,
  resolveRetentionTargets,
  retentionCutoffMs,
  runRetentionBatches,
} from '@mastra/core/storage';
import type {
  TABLE_NAMES,
  PruneOptions,
  PruneResult,
  RetentionPruneTarget,
  TableRetentionPolicy,
} from '@mastra/core/storage';
import type { PgDB } from './db';

export type PruneTarget = RetentionPruneTarget<TABLE_NAMES>;

type PruneCutoff = Date | number;

/**
 * Convert a policy's `maxAge` into a cutoff bound matching the anchor's storage
 * type: a `Date` for `timestamptz` columns (pg compares timezone-aware), or a
 * raw millisecond number for `bigint` epoch-ms columns.
 */
export function cutoffFor(policy: TableRetentionPolicy, anchorType: 'timestamp' | 'epoch-ms', now = Date.now()) {
  const cutoffMs = retentionCutoffMs(policy, now);
  return anchorType === 'epoch-ms' ? cutoffMs : new Date(cutoffMs);
}

export const runBatchedDelete = runRetentionBatches;

export function runPrune({
  db,
  domain,
  targets,
  options,
  deleteBatch,
}: {
  db: PgDB;
  domain: string;
  targets: PruneTarget[];
  options?: PruneOptions;
  deleteBatch?: (target: PruneTarget, cutoff: PruneCutoff, limit: number) => Promise<number>;
}): Promise<PruneResult[]> {
  return executeRetentionPrune({
    domain,
    targets,
    options,
    cutoffFor: (target, now) => cutoffFor(target.policy, target.anchorType ?? 'timestamp', now),
    // Fork extension: domains can substitute a retraction-aware delete batch
    // (e.g. observational-memory message retraction); default to pruned rows.
    deleteBatch: (target, cutoff, limit) =>
      deleteBatch
        ? deleteBatch(target, cutoff, limit)
        : db.pruneBatch({ tableName: target.table, column: target.column, cutoff, limit }),
  });
}

export function resolveTargets({
  policies,
  descriptor,
  order,
}: {
  policies: Record<string, TableRetentionPolicy>;
  descriptor: Record<
    string,
    { table: string; column: string; indexed: boolean; anchorType?: 'timestamp' | 'epoch-ms' }
  >;
  order: string[];
}): PruneTarget[] {
  return resolveRetentionTargets<TABLE_NAMES>({ policies, descriptor, order });
}
