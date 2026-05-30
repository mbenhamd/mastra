import type { RequestContext } from '../request-context';

/**
 * Harness v1 §14.7 model-visible channel tool fence.
 *
 * On a turn admitted under an active harness `ChannelBinding`, the legacy
 * `AgentChannels` direct-provider tools (e.g. `add_reaction`) post straight to
 * the platform, bypassing the harness durable outbox / permission / event-ordering
 * guarantees. `Agent.convertTools()` omits them from the assembled surface AND
 * reserves their names; this module makes that reservation re-enforceable at the
 * later, dynamic tool-mutation points (input processors that return tools after
 * the surface is assembled — `Agent.__runProcessInputStep` and the agentic-execution
 * loop's `runProcessInputStep`), so a processor cannot re-introduce a reserved
 * channel tool name on a channel-bound turn.
 *
 * The reserved-name set is stamped onto the per-call `RequestContext` (the same
 * instance threads uncloned from `convertTools` through the loop), so downstream
 * sites enforce it without widening shared loop/workflow schemas.
 */

/** Internal request-context key carrying the reserved channel tool names. */
export const HARNESS_CHANNEL_RESERVED_TOOLS_KEY = '__harnessChannelReservedTools';

/**
 * §14.7 gate predicate. A turn is "channel-bound" when it carries BOTH the harness
 * slot and a resolved channel request-context — the latter is set only for turns
 * admitted under an active `ChannelBinding`. Requiring the harness slot too keeps
 * the fence inert for a non-harness agent that happens to use a `channel` key.
 */
export function isHarnessChannelBoundTurn(requestContext: RequestContext): boolean {
  return requestContext.get('harness') !== undefined && requestContext.get('channel') !== undefined;
}

/** Stamp the reserved channel tool names so downstream tool-mutation sites can re-fence. */
export function stampChannelToolFence(requestContext: RequestContext, reservedNames: Set<string>): void {
  requestContext.set(HARNESS_CHANNEL_RESERVED_TOOLS_KEY, reservedNames);
}

/** Read a previously-stamped reserved channel tool name set, if any. */
export function readChannelToolFence(requestContext: RequestContext | undefined): Set<string> | undefined {
  const value = requestContext?.get(HARNESS_CHANNEL_RESERVED_TOOLS_KEY);
  return value instanceof Set ? (value as Set<string>) : undefined;
}

/**
 * Drop every reserved channel tool name from the (mutable) tool surface — the
 * §14.7 "rejected at the gate regardless of permission policy" outcome: the name
 * never reaches the model. Stripping (not throwing) avoids letting a channel user
 * abort turns by naming a tool. No-op when nothing collides.
 */
export function enforceChannelToolFence(
  tools: Record<string, unknown>,
  reservedNames: Set<string>,
  logger?: { warn: (message: string) => void },
): void {
  for (const reserved of reservedNames) {
    if (tools[reserved] !== undefined) {
      logger?.warn(
        `[harness §14.7] Stripped tool "${reserved}": it collides with a reserved channel tool name and is withheld from the model-visible tool surface on a channel-bound turn (regardless of permission policy).`,
      );
      delete tools[reserved];
    }
  }
}
