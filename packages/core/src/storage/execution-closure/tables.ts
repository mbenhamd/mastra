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

export interface ExecutionClosureScopeFilter {
  column: string;
  dimension: Exclude<ExecutionClosureScopeDimension, 'harness'>;
  /**
   * Optional row-level guard: the filter only applies when
   * `row[when.column] === when.equals`. A row can carry foreign ids in a
   * scoped column (a thread-scoped observational-memory row also stores its
   * owning resource id), so the scope discriminator must choose which
   * predicate applies.
   */
  when?: { column: string; equals: string };
}

export interface ExecutionClosureTableSpec {
  role: ExecutionClosureTableRole;
  /**
   * OR-combined scope filters: a row belongs to the closure when ANY listed
   * (column -> dimension) value is a member. An empty list with
   * `harnessScoped` means the row is bound by `harness_name` alone.
   */
  scope: ExecutionClosureScopeFilter[];
  /**
   * Composite (workflow_name, run_id) binding. Run ids are only unique within
   * a workflow, so run-scoped workflow rows match exported (name, run) pairs —
   * never the run id alone.
   */
  runPairScope?: { workflowNameColumn: string; runIdColumn: string };
  /** Bound only by `harness_name` (global per-harness rows like pressure). */
  harnessScoped?: boolean;
  /**
   * Columns nulled on import. Live claim/lease/owner fields travel in the
   * export as evidence but must not resurrect execution authority — the
   * importer clears them so the runtime allocates fresh claims.
   */
  clearOnImport?: readonly string[];
  /**
   * Live delivery rows whose stale source claim must not gate re-delivery:
   * rows with `statusColumn` in `from` are imported as `to`. Combined with
   * `clearOnImport` on the claim columns, a migrated intent becomes claimable
   * under a fresh destination claim instead of waiting out a source lease
   * that no destination worker can renew.
   */
  requeueOnImport?: { statusColumn: string; from: readonly string[]; to: string };
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
const fence = (
  scope: ExecutionClosureTableSpec['scope'],
  harnessScoped = false,
  clearOnImport?: readonly string[],
  requeueOnImport?: ExecutionClosureTableSpec['requeueOnImport'],
) => ({
  role: 'fence' as const,
  scope,
  harnessScoped,
  clearOnImport,
  requeueOnImport,
});
const authority = (scope: ExecutionClosureTableSpec['scope'], harnessScoped = false) => ({
  role: 'authority' as const,
  scope,
  harnessScoped,
});
const runPair = { workflowNameColumn: 'workflow_name', runIdColumn: 'run_id' } as const;

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
  // Terminal intents carry live delivery state: a `claimed` row's source
  // claim/lease is meaningless on the destination, so the claim fields clear
  // and the row is re-queued as `pending`. Settled rows (acked/dead/fenced)
  // stay durable evidence. The claim scan does not consult the session
  // incarnation, so without the requeue a stale claim could stall delivery
  // for the full source lease.
  [TABLE_HARNESS_TERMINAL_INTENTS]: fence([sessionScope], false, ['claim_id', 'claim_expires_at'], {
    statusColumn: 'status',
    from: ['claimed'],
    to: 'pending',
  }),
  [TABLE_HARNESS_TERMINAL_TOMBSTONES]: fence([sessionScope]),
  // Pressure rows are live per-harness coordination counters, not session
  // history — exported as evidence under `authority` so an import never
  // overwrites current counters. The importer rebuilds the destination
  // counter from the live intents it actually restores.
  [TABLE_HARNESS_TERMINAL_PRESSURE]: authority([], true),
  [TABLE_HARNESS_OPERATION_TOMBSTONES]: fence([sessionScope]),
  // Thread-delete fence rows are a live lease (owner_id + lease_id +
  // renewable expires_at), not durable deletion evidence: restoring one
  // would recreate a source worker's lease that no destination worker owns
  // or renews, blocking session admission on that thread until expiry.
  [TABLE_HARNESS_THREAD_DELETE_FENCES]: authority([threadScope('thread_id')]),
  // Projection intents are pending application work — execution authority,
  // not evidence. The post-import runtime rebuilds them from restored state.
  [TABLE_HARNESS_SESSION_PROJECTION_INTENTS]: authority([sessionScope]),
  [TABLE_HARNESS_SESSION_PROJECTION_FENCES]: fence([sessionScope]),
  [TABLE_HARNESS_SESSION_PROJECTION_PRESSURE]: authority([], true),

  // --- Channel queues/signals: receipts stay readable, delivery authority resets ---
  [TABLE_HARNESS_CHANNEL_INBOX]: authority([sessionScope, threadScope()]),
  [TABLE_HARNESS_CHANNEL_BINDINGS]: authority([sessionScope, threadScope()]),
  [TABLE_HARNESS_CHANNEL_ACTION_TOKENS]: authority([{ column: 'owning_session_id', dimension: 'session' }]),
  // Action receipts are the durable idempotency ledger for action tokens —
  // `applied`/`conflict`/`dead` outcomes must survive migration or a provider
  // redelivery re-applies the action. They still carry a live claim while a
  // delivery is in flight, so claim/retry fields clear on import; cleared
  // claim fields already make a live receipt claimable again.
  [TABLE_HARNESS_CHANNEL_ACTION_RECEIPTS]: fence([{ column: 'owning_session_id', dimension: 'session' }], false, [
    'claim_id',
    'claim_expires_at',
    'next_attempt_at',
  ]),
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
  // OM rows carry both a `scope` discriminator and both id columns — a
  // thread-scoped row still stores its owning resourceId. Unguarded OR scope
  // would pull every thread-scoped row sharing the resource, so each
  // predicate applies only to rows of its declared scope.
  [TABLE_OBSERVATIONAL_MEMORY]: state([
    { column: 'threadId', dimension: 'thread', when: { column: 'scope', equals: 'thread' } },
    { column: 'resourceId', dimension: 'resource', when: { column: 'scope', equals: 'resource' } },
  ]),
  [TABLE_RESOURCES]: { role: 'shared-resource', scope: [{ column: 'id', dimension: 'resource' }] },

  // --- Workflow snapshots + terminal lineage ---
  // Definitions are deploy-time catalog rows, not execution state — the
  // snapshot carries `serialized_step_graph` for replay, so the closure binds
  // the run-scoped tables only. Workflow identity is the composite
  // (workflow_name, run_id): a bare run_id can collide across workflows, so
  // every workflow table binds through `runPairScope` and the exporter pins
  // run ids it cannot attribute to a single workflow.
  [TABLE_WORKFLOW_SNAPSHOT]: { role: 'state', scope: [], runPairScope: runPair },
  // The snapshot handoff is the run's live mutation fence — canonical
  // snapshot writes reject while ANY handoff row exists, so restoring one
  // would make the imported run permanently unwritable. The exporter keeps it
  // as evidence; the importer materializes its snapshot as canonical state.
  [TABLE_WORKFLOW_SNAPSHOT_HANDOFF]: { role: 'authority', scope: [], runPairScope: runPair },
  // The terminalization row holds the run's terminal record AND live claim
  // fields; the owner/token/lease clear on import so the new incarnation
  // re-claims. `claim_generation` is NOT cleared: it is the monotonic fencing
  // counter a claimant increments, and the workflow decoder rejects a
  // generation of 0 — preserving the positive source generation keeps the
  // imported record readable and safely reclaimable.
  mastra_workflow_terminalizations: {
    role: 'state',
    scope: [],
    runPairScope: runPair,
    clearOnImport: ['owner_id', 'claim_token', 'lease_expires_at'],
  },
  mastra_workflow_terminal_effects_v2: { role: 'state', scope: [], runPairScope: runPair },
  // Destination receipts are idempotency evidence: restoring them prevents an
  // effect from being re-delivered; dropping them would invite a paid-attempt
  // replay. Continuation plans FK-reference receipts and effects, so receipts
  // must register (and therefore import) before plans.
  mastra_workflow_terminal_destination_receipts_v2: { role: 'fence', scope: [], runPairScope: runPair },
  mastra_workflow_terminal_continuation_plans_v2: { role: 'state', scope: [], runPairScope: runPair },
  mastra_workflow_terminal_snapshots_v2: { role: 'state', scope: [], runPairScope: runPair },
  mastra_workflow_terminal_recovery_ancestries: { role: 'state', scope: [], runPairScope: runPair },
  mastra_workflow_parent_revisions: { role: 'state', scope: [], runPairScope: runPair },
  // `mastra_workflow_schema_migrations` /
  // `mastra_workflow_parent_revision_migration_epoch` are global migration
  // infrastructure, not per-run state — deliberately outside the closure.
};
