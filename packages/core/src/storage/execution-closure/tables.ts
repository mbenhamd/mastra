import {
  TABLE_HARNESS_ATTACHMENTS,
  TABLE_HARNESS_ATTACHMENT_OPERATIONS,
  TABLE_HARNESS_ATTACHMENT_REFERENCES,
  TABLE_HARNESS_CHANNEL_ACTION_RECEIPTS,
  TABLE_HARNESS_CHANNEL_ACTION_TOKENS,
  TABLE_HARNESS_CHANNEL_BINDINGS,
  TABLE_HARNESS_CHANNEL_INBOX,
  TABLE_HARNESS_CHANNEL_OUTBOX,
  TABLE_HARNESS_MESSAGE_RESULTS,
  TABLE_HARNESS_OPERATION_TOMBSTONES,
  TABLE_HARNESS_PLAN_TASKS,
  TABLE_HARNESS_PROVIDER_CALLBACK_BINDINGS,
  TABLE_HARNESS_RUN_SUMMARIES,
  TABLE_HARNESS_SESSIONS,
  TABLE_HARNESS_SESSION_EVENTS,
  TABLE_HARNESS_SESSION_PROJECTION_FENCES,
  TABLE_HARNESS_SESSION_PROJECTION_INTENTS,
  TABLE_HARNESS_SESSION_PROJECTION_PRESSURE,
  TABLE_HARNESS_TERMINAL_ADMISSIONS,
  TABLE_HARNESS_TERMINAL_INTENTS,
  TABLE_HARNESS_TERMINAL_PRESSURE,
  TABLE_HARNESS_TERMINAL_TOMBSTONES,
  TABLE_HARNESS_THREAD_DELETE_FENCES,
  TABLE_HARNESS_WAKEUPS,
  TABLE_HARNESS_WORKSPACE_ACTIONS,
  TABLE_MESSAGES,
  TABLE_OBSERVATIONAL_MEMORY,
  TABLE_RESOURCES,
  TABLE_THREADS,
  TABLE_THREAD_STATE,
  TABLE_WORKFLOW_SNAPSHOT,
  TABLE_WORKFLOW_SNAPSHOT_HANDOFF,
} from '../constants';
import type { ExecutionClosureTableName, ExecutionClosureTableRole } from './types';

/**
 * Dimension a row binds to the closure through. `session`/`thread`/`run`/
 * `resource` name the id set collected at export; `harness` is the namespace
 * itself (global per-harness rows).
 */
export type ExecutionClosureScopeDimension = 'session' | 'thread' | 'run' | 'resource' | 'harness';

export interface ExecutionClosureTableSpec {
  role: ExecutionClosureTableRole;
  /**
   * OR-combined scope filters: a row belongs to the closure when ANY listed
   * (column -> dimension) value is a member. An empty list with
   * `harnessScoped` means the row is bound by `harness_name` alone.
   */
  scope: { column: string; dimension: Exclude<ExecutionClosureScopeDimension, 'harness'> }[];
  /** Bound only by `harness_name` (global per-harness rows like pressure). */
  harnessScoped?: boolean;
  /**
   * Columns nulled on import. Live claim/lease/owner fields travel in the
   * export as evidence but must not resurrect execution authority — the
   * importer clears them so the runtime allocates fresh claims.
   */
  clearOnImport?: readonly string[];
}

const sessionScope = { column: 'session_id', dimension: 'session' as const };
const threadScope = (column = 'thread_id') => ({ column, dimension: 'thread' as const });

const state = (
  scope: ExecutionClosureTableSpec['scope'],
  harnessScoped = false,
  clearOnImport?: readonly string[],
) => ({
  role: 'state' as const,
  scope,
  harnessScoped,
  clearOnImport,
});
const fence = (scope: ExecutionClosureTableSpec['scope'], harnessScoped = false) => ({
  role: 'fence' as const,
  scope,
  harnessScoped,
});
const authority = (scope: ExecutionClosureTableSpec['scope'], harnessScoped = false) => ({
  role: 'authority' as const,
  scope,
  harnessScoped,
});

/**
 * The closed world of tables that make up a continuation closure. Every
 * closure-member table has an explicit role; a table not listed here is not
 * exported. New closure-owned tables must pick a role deliberately — the
 * registry cannot infer one.
 */
export const EXECUTION_CLOSURE_TABLES: Partial<Record<ExecutionClosureTableName, ExecutionClosureTableSpec>> = {
  // --- Harness session subtree ---
  // Live lease/owner fields clear on import: the restored session arrives
  // dormant with a fresh session_incarnation allocated by the importer, so an
  // unexpired old lease can never carry authority across the boundary.
  [TABLE_HARNESS_SESSIONS]: state([{ column: 'id', dimension: 'session' }], false, ['owner_id', 'lease_expires_at']),
  [TABLE_HARNESS_SESSION_EVENTS]: state([sessionScope]),
  [TABLE_HARNESS_MESSAGE_RESULTS]: state([sessionScope]),
  [TABLE_HARNESS_PLAN_TASKS]: state([sessionScope]),
  [TABLE_HARNESS_RUN_SUMMARIES]: state([sessionScope]),
  [TABLE_HARNESS_WORKSPACE_ACTIONS]: state([sessionScope]),
  [TABLE_HARNESS_ATTACHMENTS]: state([sessionScope]),
  [TABLE_HARNESS_ATTACHMENT_REFERENCES]: state([sessionScope]),
  [TABLE_HARNESS_ATTACHMENT_OPERATIONS]: authority([sessionScope]),

  // --- Terminal handoff + operation fences: exported for audit, never revived ---
  [TABLE_HARNESS_TERMINAL_ADMISSIONS]: fence([sessionScope]),
  [TABLE_HARNESS_TERMINAL_INTENTS]: fence([sessionScope]),
  [TABLE_HARNESS_TERMINAL_TOMBSTONES]: fence([sessionScope]),
  // Pressure rows are live per-harness coordination counters, not session
  // history — exported as evidence under `authority` so an import never
  // overwrites current counters.
  [TABLE_HARNESS_TERMINAL_PRESSURE]: authority([], true),
  [TABLE_HARNESS_OPERATION_TOMBSTONES]: fence([sessionScope]),
  [TABLE_HARNESS_THREAD_DELETE_FENCES]: fence([threadScope('thread_id')]),
  // Projection intents are pending application work — execution authority,
  // not evidence. The post-import runtime rebuilds them from restored state.
  [TABLE_HARNESS_SESSION_PROJECTION_INTENTS]: authority([sessionScope]),
  [TABLE_HARNESS_SESSION_PROJECTION_FENCES]: fence([sessionScope]),
  [TABLE_HARNESS_SESSION_PROJECTION_PRESSURE]: authority([], true),

  // --- Channel queues/signals: receipts stay readable, delivery authority resets ---
  [TABLE_HARNESS_CHANNEL_INBOX]: authority([sessionScope, threadScope()]),
  [TABLE_HARNESS_CHANNEL_BINDINGS]: authority([sessionScope, threadScope()]),
  [TABLE_HARNESS_CHANNEL_ACTION_TOKENS]: authority([{ column: 'owning_session_id', dimension: 'session' }]),
  [TABLE_HARNESS_CHANNEL_ACTION_RECEIPTS]: authority([{ column: 'owning_session_id', dimension: 'session' }]),
  [TABLE_HARNESS_CHANNEL_OUTBOX]: authority([sessionScope, { column: 'owning_session_id', dimension: 'session' }]),
  [TABLE_HARNESS_WAKEUPS]: authority([sessionScope]),
  // Provider callback bindings are provider-scoped shared routing state —
  // exported as evidence; a fresh incarnation re-establishes its own and an
  // import never revives stale routing.
  [TABLE_HARNESS_PROVIDER_CALLBACK_BINDINGS]: authority([], true),

  // --- Memory/OM closure ---
  [TABLE_THREADS]: state([{ column: 'id', dimension: 'thread' }]),
  [TABLE_MESSAGES]: state([threadScope()]),
  [TABLE_THREAD_STATE]: state([threadScope('threadId')]),
  [TABLE_OBSERVATIONAL_MEMORY]: state([threadScope('threadId'), { column: 'resourceId', dimension: 'resource' }]),
  [TABLE_RESOURCES]: { role: 'shared-resource', scope: [{ column: 'id', dimension: 'resource' }] },

  // --- Workflow snapshots + terminal lineage ---
  // Definitions are deploy-time catalog rows, not execution state — the
  // snapshot carries `serialized_step_graph` for replay, so the closure binds
  // the run-scoped tables only.
  [TABLE_WORKFLOW_SNAPSHOT]: state([{ column: 'run_id', dimension: 'run' }]),
  [TABLE_WORKFLOW_SNAPSHOT_HANDOFF]: state([{ column: 'run_id', dimension: 'run' }]),
  // The terminalization row holds the run's terminal record AND live claim
  // fields; claim columns clear on import so the new incarnation re-claims.
  mastra_workflow_terminalizations: state([{ column: 'run_id', dimension: 'run' }], false, [
    'owner_id',
    'claim_token',
    'claim_generation',
    'lease_expires_at',
  ]),
  mastra_workflow_terminal_effects_v2: state([{ column: 'run_id', dimension: 'run' }]),
  // Destination receipts are idempotency evidence: restoring them prevents an
  // effect from being re-delivered; dropping them would invite a paid-attempt
  // replay. Continuation plans FK-reference receipts and effects, so receipts
  // must register (and therefore import) before plans.
  mastra_workflow_terminal_destination_receipts_v2: fence([{ column: 'run_id', dimension: 'run' }]),
  mastra_workflow_terminal_continuation_plans_v2: state([{ column: 'run_id', dimension: 'run' }]),
  mastra_workflow_terminal_snapshots_v2: state([{ column: 'run_id', dimension: 'run' }]),
  mastra_workflow_terminal_recovery_ancestries: state([{ column: 'run_id', dimension: 'run' }]),
  mastra_workflow_parent_revisions: state([{ column: 'run_id', dimension: 'run' }]),
  // `mastra_workflow_schema_migrations` /
  // `mastra_workflow_parent_revision_migration_epoch` are global migration
  // infrastructure, not per-run state — deliberately outside the closure.
};
