import { ErrorCategory, ErrorDomain, MastraError } from '../../../error';
import type { RunRegistryEntry, SerializableToolHookPolicy } from '../types';

const TOOL_HOOK_POLICY_UNAVAILABLE_MESSAGE =
  'Durable per-execution tool hooks are unavailable on this worker. Per-execution hook closures require the original run registry entry; configure durable policies on the Agent or start a new run.';

/** Stop before execution when the original policy-bound tool surface is unavailable. */
export function throwDurableToolHookPolicyUnavailable(): never {
  throw new MastraError({
    id: 'DURABLE_AGENT_TOOL_HOOK_POLICY_UNAVAILABLE',
    domain: ErrorDomain.AGENT,
    category: ErrorCategory.USER,
    text: TOOL_HOOK_POLICY_UNAVAILABLE_MESSAGE,
  });
}

/**
 * Reject a durable execution when its per-run tool-hook closures are no longer
 * bound to the exact process-local policy recorded in workflow input.
 */
export function assertDurableToolHookPolicyAvailable(options: {
  serialized?: SerializableToolHookPolicy;
  registryEntry?: RunRegistryEntry;
}): void {
  const { serialized, registryEntry } = options;
  const registered = registryEntry?.toolHookPolicy;
  if (!serialized && !registered) return;

  const matches =
    !!serialized &&
    !!registered &&
    registryEntry.isPlaceholder !== true &&
    serialized.kind === 'run-registry' &&
    serialized.id === registered.id &&
    serialized.beforeToolCall === (typeof registered.hooks.beforeToolCall === 'function') &&
    serialized.afterToolCall === (typeof registered.hooks.afterToolCall === 'function');
  if (matches) return;

  throwDurableToolHookPolicyUnavailable();
}
