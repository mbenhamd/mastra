/**
 * The run spans of a `Session` (PF-8738): one in-memory accumulator per
 * in-flight run for the canonical `run_completed` event and the durable run
 * summary, and the memory of which runs were already finalized.
 *
 * **The rules this owner enforces.**
 * - A run has at most one open span. `open` keeps a span already open for the
 *   run: a resume reuses the run id, so its span covers the whole run,
 *   including any suspension.
 * - Tool calls are counted only in an open span. `toolEnded` folds every
 *   ended call into the rollup (its duration only when known) and forgets the
 *   call's start.
 * - A run is finalized at most once. `finalize` succeeds the first time and
 *   closes the run's span; later calls for the same run return `undefined`.
 * - The memory of finalized runs is bounded: once it holds
 *   {@link MAX_FINALIZED_RUN_IDS} runs, the next `finalize` forgets all of
 *   them before recording its own run. This wholesale clear is the session's
 *   behaviour before this owner, kept on purpose here. It is a known hazard:
 *   a late duplicate terminal for a forgotten run would complete that run a
 *   second time. PF-8766 replaces it with an oldest-first bound.
 * - `dispose` forgets every open span and every finalized run.
 *
 * The span identity is opaque here: the session captures it, and captures it
 * only when a span is actually opened.
 */

import type { RunToolRollup } from './events';

/** Bound on the finalized-run memory (cleared wholesale when full). */
export const MAX_FINALIZED_RUN_IDS = 4096;

/** A run span as the run's completion reports it. */
export interface RunSpanSnapshot<TIdentity> {
  startedAt: number;
  identity: TIdentity;
  toolRollup: RunToolRollup;
}

interface OpenRunSpan<TIdentity> {
  startedAt: number;
  identity: TIdentity;
  /** Per-tool-call start times, used to compute `tool_end.durationMs` even across an in-process suspend. */
  activeToolStarts: Map<string, { toolName: string; startedAt: number }>;
  rollup: {
    count: number;
    errors: number;
    totalDurationMs: number;
    maxDurationMs: number;
    perTool: Map<string, { count: number; errors: number; totalDurationMs: number }>;
  };
}

export class RunSpanTracker<TIdentity> {
  private readonly _open = new Map<string, OpenRunSpan<TIdentity>>();
  private readonly _finalized = new Set<string>();

  /** Open the run's span unless one is already open; `identity` is called only when a span opens. */
  open(runId: string, startedAt: number, identity: () => TIdentity): void {
    if (this._open.has(runId)) return;
    this._open.set(runId, {
      startedAt,
      identity: identity(),
      activeToolStarts: new Map(),
      rollup: { count: 0, errors: 0, totalDurationMs: 0, maxDurationMs: 0, perTool: new Map() },
    });
  }

  /** Record a tool call's start in the run's open span, if there is one. */
  toolStarted(runId: string | undefined, toolCallId: string | undefined, toolName: string | undefined): void {
    const span = runId !== undefined ? this._open.get(runId) : undefined;
    if (span && toolCallId !== undefined) {
      span.activeToolStarts.set(toolCallId, { toolName: toolName ?? 'unknown', startedAt: Date.now() });
    }
  }

  /**
   * Fold an ended tool call into the run's open span. Returns the call's
   * duration (the given one, else measured from its recorded start) when the
   * run has an open span and the duration is known; otherwise `undefined`.
   */
  toolEnded(
    runId: string | undefined,
    call: { toolCallId?: string; toolName?: string; isError?: boolean; durationMs?: number },
  ): number | undefined {
    const span = runId !== undefined ? this._open.get(runId) : undefined;
    if (!span) return undefined;
    const toolName = call.toolName ?? 'unknown';
    const start = call.toolCallId !== undefined ? span.activeToolStarts.get(call.toolCallId) : undefined;
    let durationMs: number | undefined = call.durationMs;
    if (durationMs === undefined && start) durationMs = Math.max(0, Date.now() - start.startedAt);
    if (call.toolCallId !== undefined) span.activeToolStarts.delete(call.toolCallId);
    // Fold into the rollup (count every emitted terminal; duration only when known).
    const r = span.rollup;
    r.count += 1;
    const isError = call.isError === true;
    if (isError) r.errors += 1;
    if (durationMs !== undefined) {
      r.totalDurationMs += durationMs;
      if (durationMs > r.maxDurationMs) r.maxDurationMs = durationMs;
    }
    const per = r.perTool.get(toolName) ?? { count: 0, errors: 0, totalDurationMs: 0 };
    per.count += 1;
    if (isError) per.errors += 1;
    if (durationMs !== undefined) per.totalDurationMs += durationMs;
    r.perTool.set(toolName, per);
    return durationMs;
  }

  /** The run's open span as its completion would report it, or `undefined` when none is open. */
  snapshot(runId: string): RunSpanSnapshot<TIdentity> | undefined {
    const span = this._open.get(runId);
    if (span === undefined) return undefined;
    return { startedAt: span.startedAt, identity: span.identity, toolRollup: materializeToolRollup(span.rollup) };
  }

  /** Whether the run was already finalized (within the bounded memory). */
  isFinalized(runId: string): boolean {
    return this._finalized.has(runId);
  }

  /**
   * Finalize the run: the first time, record it as finalized, close its span
   * and return that span (`span` is `undefined` when none was open); for a run
   * already finalized, change nothing and return `undefined`.
   */
  finalize(runId: string): { span: RunSpanSnapshot<TIdentity> | undefined } | undefined {
    if (this._finalized.has(runId)) return undefined;
    if (this._finalized.size >= MAX_FINALIZED_RUN_IDS) this._finalized.clear();
    this._finalized.add(runId);
    const span = this.snapshot(runId);
    this._open.delete(runId);
    return { span };
  }

  /** Forget every open span and every finalized run. */
  dispose(): void {
    this._open.clear();
    this._finalized.clear();
  }
}

/** Convert the in-memory rollup (Maps) into the JSON-safe {@link RunToolRollup}. */
function materializeToolRollup(rollup: OpenRunSpan<unknown>['rollup']): RunToolRollup {
  const perTool: RunToolRollup['perTool'] = {};
  for (const [name, v] of rollup.perTool) {
    perTool[name] = { count: v.count, errors: v.errors, totalDurationMs: v.totalDurationMs };
  }
  return {
    count: rollup.count,
    errors: rollup.errors,
    totalDurationMs: rollup.totalDurationMs,
    maxDurationMs: rollup.maxDurationMs,
    perTool,
  };
}
