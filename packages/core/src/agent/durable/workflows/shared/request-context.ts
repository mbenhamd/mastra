import { RequestContext } from '../../../../request-context';
import { restoreRunAgentScopes } from '../../../scopes';

export interface DurableRequestContextResolution {
  requestContext: RequestContext;
  customContext: Record<string, unknown> | undefined;
}

/**
 * Resolve the RequestContext visible to an in-process durable step.
 *
 * The registry owns the complete live context. The workflow input contains
 * only explicitly allowlisted JSON values so cold recovery never turns a
 * durable snapshot into a credential cache. Prefer the live context when it is
 * available and fall back to the persisted allowlist after process loss.
 */
export function resolveDurableRequestContext(
  liveRequestContext: RequestContext | undefined,
  persistedEntries: Record<string, unknown> | undefined,
): DurableRequestContextResolution {
  let requestContext = liveRequestContext;
  if (!requestContext) {
    requestContext = new RequestContext(Object.entries(persistedEntries ?? {}));
    // Persisted entries lose the run-scope registration; re-register agent
    // scopes so cold-recovered steps see the same scope set as the live run.
    restoreRunAgentScopes(requestContext);
  }
  const customContext = Object.fromEntries(requestContext.entries());

  return {
    requestContext,
    customContext: Object.keys(customContext).length > 0 ? customContext : undefined,
  };
}
