import { MASTRA_RESOURCE_ID_KEY, MASTRA_THREAD_ID_KEY, RequestContext } from '../../request-context';

export interface ResolvedScope {
  threadId: string | undefined;
  resourceId: string | undefined;
  scopeKey: string | null;
}

export const TERMINAL_BG_CHUNKS = new Set([
  'background-task-completed',
  'background-task-failed',
  'background-task-cancelled',
]);

/**
 * Resolve memory / thread / resource for this call, matching `#execute`
 * semantics (RequestContext-scoped keys override caller-supplied memory
 * args). Returns `null` when no memory backend is configured — caller
 * falls through to a plain stream in that case.
 */
export async function resolveStreamUntilIdleScope(
  agent: { getMemory: (options?: any) => Promise<any> },
  mergedOptions: Record<string, any>,
): Promise<ResolvedScope | null> {
  const requestContext = (mergedOptions?.requestContext as RequestContext | undefined) ?? new RequestContext();
  const memory = await agent.getMemory({ requestContext });
  if (!memory) return null;

  const threadIdFromContext = requestContext.get(MASTRA_THREAD_ID_KEY) as string | undefined;
  const resourceIdFromContext = requestContext.get(MASTRA_RESOURCE_ID_KEY) as string | undefined;
  const threadIdFromArgs =
    typeof mergedOptions?.memory?.thread === 'string'
      ? mergedOptions.memory.thread
      : (mergedOptions?.memory?.thread as { id?: string } | undefined)?.id;

  const threadId = threadIdFromContext ?? threadIdFromArgs;
  const resourceId = resourceIdFromContext ?? (mergedOptions?.memory?.resource as string | undefined);

  // Scope key = `threadId|resourceId`. Calls without either get null (no
  // active-stream coordination — no way to meaningfully identify "the same
  // conversation").
  const scopeKey = threadId || resourceId ? `${threadId ?? ''}|${resourceId ?? ''}` : null;

  return { threadId, resourceId, scopeKey };
}

/**
 * Build the ephemeral user-prompt text that tells the LLM which tool-call
 * IDs just completed. The directive stops the LLM from (a) re-processing
 * results already handled on a prior continuation and (b) mimicking the
 * prior assistant ack text ("I'm running it in the background") and
 * re-dispatching the same tool.
 */
export function buildContinuationDirective(batch: Array<Record<string, unknown>>): string {
  const entries = batch
    .map(chunk => {
      const payload = (chunk as { payload?: Record<string, unknown> }).payload ?? {};
      return {
        toolCallId: payload.toolCallId as string | undefined,
        toolName: payload.toolName as string | undefined,
      };
    })
    .filter(e => !!e.toolCallId);

  const idList = entries.map(e => (e.toolName ? `${e.toolCallId} (${e.toolName})` : e.toolCallId)).join(', ');

  return (
    `Background task(s) you previously dispatched have completed. ` +
    `Process ONLY these tool-call IDs (their results are now in the conversation): ${idList}. ` +
    `IMPORTANT: Do NOT process any tool-call IDs that were not in the list, ` +
    `and do NOT call the same tool again — the result is already available. ` +
    `Use these result(s) to answer the user's original question.`
  );
}

export function buildContinuationOpts(
  baseContinuationOpts: Record<string, any>,
  callerContext: any[] | undefined,
  batch: Array<Record<string, unknown>>,
): Record<string, any> {
  const directive = buildContinuationDirective(batch);
  return {
    ...baseContinuationOpts,
    context: [...(callerContext ?? []), { role: 'user' as const, content: directive }],
  };
}

export function acquireStreamSlot(activeStreams: Map<string, () => void>, scopeKey: string | null, closer: () => void): void {
  if (!scopeKey) return;
  const priorClose = activeStreams.get(scopeKey);
  priorClose?.();
  activeStreams.set(scopeKey, closer);
}

export function releaseStreamSlot(activeStreams: Map<string, () => void>, scopeKey: string | null, closer: () => void): void {
  if (!scopeKey) return;
  // Only delete if it's still our stream
  if (activeStreams.get(scopeKey) === closer) {
    activeStreams.delete(scopeKey);
  }
}
