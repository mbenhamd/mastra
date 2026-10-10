/**
 * Tracing-context forwarding from a parent Harness turn into its subagent runs.
 *
 * A subagent runs on its own child session, so without forwarding its agent
 * run starts a fresh trace: none of the parent run's span metadata reaches the
 * child's spans. Forwarding keeps subagent spans in the parent's trace, under
 * the parent's sampling decision and trace state, and lets them inherit the
 * parent's span metadata exactly as ordinary child spans do (the child's own
 * run identity still wins).
 *
 * - Inline `spawn_subagent`: the parent tool call awaits the child, so the
 *   child run attaches to the tool call's live span, like an agent-as-tool
 *   sub-agent run.
 * - Durable `task_delegate`: the child outlives the delegating tool call and
 *   often the parent turn. Attached as an ordinary descendant, it would be
 *   force-closed by the parent's `endTree()` (on suspension, abort or error)
 *   while still running. The delegation therefore gets its own child span
 *   with a detached lifetime: it is part of the parent's trace and inherits
 *   everything a child span inherits, but only the delegation drive ends it.
 *
 * Only span state is forwarded. Request contexts are untouched, so the
 * inherited-request-context projection rules still apply to the child.
 */

import type { AnySpan, TracingContext } from '../../observability';
import { SpanType } from '../../observability';

/** The span a subagent run should attach to, or `undefined` when untraced. */
export function liveSubagentTracingContext(parent: TracingContext | undefined): TracingContext | undefined {
  const span = parent?.currentSpan;
  return span !== undefined ? { currentSpan: span } : undefined;
}

/**
 * Start the span that parents one durable delegation's child runs. Returns
 * `undefined` when the delegating call is untraced; an unsampled parent yields
 * an unsampled delegation. The caller ends it when the delegation drive
 * settles.
 */
export function startDelegatedSubagentSpan(parent: TracingContext | undefined, agentType: string): AnySpan | undefined {
  return parent?.currentSpan?.createChildSpan({
    type: SpanType.GENERIC,
    name: `subagent delegation: '${agentType}'`,
    detachedLifetime: true,
  });
}

/**
 * How a live delegation drive settled its delegation:
 * - `settled`: the child closed and the durable plan task was reconciled.
 * - `close_failed`: the child could not be closed; reconciliation is retried.
 * - `not_reconciled`: the fenced plan-task reconciliation did not commit.
 * - `error`: the drive threw before settling.
 */
export type DelegationDriveSettlement = 'settled' | 'close_failed' | 'not_reconciled' | 'error';

/**
 * End a delegation span once its drive has settled. The span records the
 * child's outcome and the settlement; an unsettled delegation ends as an
 * error, so a delegation the drive could not settle is visible in the trace.
 */
export function endDelegatedSubagentSpan(
  span: AnySpan | undefined,
  outcome: string,
  settlement: DelegationDriveSettlement,
): void {
  if (span === undefined) return;
  const output = { outcome, settlement };
  if (settlement === 'settled') {
    span.end({ output });
    return;
  }
  span.update({ output });
  span.error({ error: new Error(`delegation was not settled: ${settlement}`), endSpan: true });
}
