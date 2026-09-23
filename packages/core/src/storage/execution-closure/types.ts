import type { TABLE_NAMES, TABLE_OBSERVATIONAL_MEMORY } from '../constants';

/**
 * Workflow-terminal tables owned by the workflows storage domain. These names
 * are module-local constants in the PG store (not `TABLE_NAMES` members) but
 * are part of the continuation closure: terminal effects, snapshots,
 * destination receipts, continuation plans, recovery ancestries, and parent
 * revisions — everything needed to resume or fence a terminal handoff.
 */
export const WORKFLOW_TERMINAL_CLOSURE_TABLES = [
  'mastra_workflow_terminalizations',
  'mastra_workflow_terminal_effects_v2',
  'mastra_workflow_terminal_snapshots_v2',
  'mastra_workflow_terminal_recovery_ancestries',
  'mastra_workflow_terminal_destination_receipts_v2',
  'mastra_workflow_terminal_continuation_plans_v2',
  'mastra_workflow_parent_revisions',
] as const;

export type WorkflowTerminalClosureTable = (typeof WORKFLOW_TERMINAL_CLOSURE_TABLES)[number];

/**
 * Table names that may appear in an execution closure: the core `TABLE_NAMES`
 * union, the optional Observational Memory feature table (which lives in
 * `OBSERVATIONAL_MEMORY_TABLE_SCHEMA` outside the core union), and the
 * workflow-terminal tables above.
 */
export type ExecutionClosureTableName = TABLE_NAMES | typeof TABLE_OBSERVATIONAL_MEMORY | WorkflowTerminalClosureTable;

/**
 * Identifies the exported unit: one harness session subtree (a root session and
 * every descendant reached through `parent_session_id`). All historical IDs are
 * preserved verbatim; only execution authority is re-allocated on import.
 */
export interface ExecutionClosureKey {
  /** Harness namespace the root session belongs to. */
  harnessName: string;
  /** Root session id. */
  sessionId: string;
}

/**
 * How a table's rows participate in the closure:
 * - `state` — durable rows restored verbatim on import.
 * - `fence` — fence/evidence rows re-installed verbatim; they never revive
 *   live authority (tombstones, terminal intents/admissions, delete fences,
 *   projection fences, destination receipts).
 * - `authority` — rows exported as evidence but never restored. Execution
 *   authority (claims, leases, wakeup/delivery schedules, channel bindings/
 *   tokens/outbox, pending attachment/projection work) is re-allocated by the
 *   runtime after import, so stale callbacks and paid attempts cannot revive.
 * - `shared-resource` — shared rows (resource working memory) restored only
 *   when absent so an old archive cannot overwrite current owner controls.
 */
export type ExecutionClosureTableRole = 'state' | 'fence' | 'authority' | 'shared-resource';

/** Why a closure unit was pinned instead of exported complete. */
export interface ExecutionClosurePin {
  reason: string;
  detail: Record<string, unknown>;
}

export interface ExecutionClosureTableDigest {
  table: ExecutionClosureTableName;
  role: ExecutionClosureTableRole;
  rowCount: number;
  /** `sha256:` digest over the canonical serialization of every exported row. */
  sha256: `sha256:${string}`;
}

/**
 * Versioned manifest over a complete continuation closure. The manifest is the
 * verification contract: an import verifies every listed table's digest before
 * the unit may transition to ready, and unknown ancestry/ownership pins the
 * unit instead of partially activating it.
 */
export interface ExecutionClosureManifest {
  format: 'mastra-execution-closure';
  /** Manifest format version. Unsupported versions pin the import. */
  version: 1;
  key: ExecutionClosureKey;
  /**
   * Provenance of the exporting store — recorded by the exporter, never
   * synthesized by the verifier. `schemaName` is the physical schema the rows
   * were read from; `packageVersion` records the producing package version
   * when the caller knows it.
   */
  source?: {
    store?: string;
    schemaName?: string;
    packageVersion?: string;
  };
  /** Root + descendant session ids, sorted. */
  sessionIds: string[];
  /** session id -> `session_incarnation` captured at export. */
  incarnations: Record<string, string>;
  threadIds: string[];
  runIds: string[];
  resourceIds: string[];
  /** Every exported table, sorted by table name. */
  tables: ExecutionClosureTableDigest[];
  /** `pinned` when unknown ancestry/ownership pinned the unit during export. */
  completeness: 'complete' | 'pinned';
  pins: ExecutionClosurePin[];
}

export interface ExecutionClosurePayload {
  manifest: ExecutionClosureManifest;
  /** table name -> exported rows (column name -> raw column value). */
  rows: Partial<Record<ExecutionClosureTableName, Record<string, unknown>[]>>;
}

/**
 * Outcome of a staged import. `imported` means every `state`/`fence` row was
 * inserted (or already present — re-imports converge) and sessions carry fresh
 * incarnation/cleared lease authority. `pinned` means the payload still
 * imported best-effort but the manifest recorded unknown ancestry/ownership —
 * the caller must surface the pins rather than treating the unit as whole.
 */
export interface ExecutionClosureImportResult {
  status: 'imported' | 'pinned';
  /** session id -> fresh `session_incarnation` allocated by the import. */
  incarnations: Record<string, string>;
  /** table -> rows inserted by this import (0 when already present). */
  inserted: Record<string, number>;
  /** table -> rows skipped: already present, or `authority` rows never restored. */
  skipped: Record<string, number>;
  pins: ExecutionClosurePin[];
}
