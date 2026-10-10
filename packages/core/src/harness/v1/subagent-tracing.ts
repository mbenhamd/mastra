/**
 * Tracing-context forwarding from a parent Harness turn into its subagent runs.
 *
 * A subagent runs on its own child session, so without forwarding its agent
 * run starts a fresh trace: none of the parent run's span metadata reaches the
 * child's spans. Forwarding keeps subagent spans in the parent's trace and lets
 * them inherit the parent's span metadata exactly as ordinary child spans do
 * (the child's own run identity still wins).
 *
 * - Inline `spawn_subagent`: the parent tool call awaits the child, so the
 *   child run attaches to the tool call's live span, like an agent-as-tool
 *   sub-agent run.
 * - Durable `task_delegate`: the child outlives the delegating tool call and
 *   often the parent turn. A live attachment would let the parent's
 *   `endTree()` (on suspension, abort or error) force-close the still-running
 *   child's spans, so the delegation gets its own span instead. It joins the
 *   parent trace by id, under the delegating tool call, carries a copy of that
 *   span's metadata, and lasts as long as the live delegation drive.
 *
 * Only span state is forwarded. Request contexts are untouched, so the
 * inherited-request-context projection rules still apply to the child.
 */

import type { AnySpan, TracingContext } from '../../observability';
import { SpanType } from '../../observability';

/** The live span a subagent run should attach to, or `undefined` when untraced. */
export function liveSubagentTracingContext(parent: TracingContext | undefined): TracingContext | undefined {
  const span = parent?.currentSpan;
  return span?.isValid ? { currentSpan: span } : undefined;
}

/**
 * Start the span that parents one durable delegation's child runs. Returns
 * `undefined` when the delegating call is untraced. The caller ends it when
 * the live delegation drive settles.
 */
export function startDelegatedSubagentSpan(parent: TracingContext | undefined, agentType: string): AnySpan | undefined {
  const span = parent?.currentSpan;
  if (!span?.isValid) return undefined;
  return span.observabilityInstance.startSpan({
    type: SpanType.GENERIC,
    name: `subagent delegation: '${agentType}'`,
    traceId: span.traceId,
    parentSpanId: span.getExportedSpanId?.() ?? span.id,
    metadata: { ...span.metadata },
  });
}
