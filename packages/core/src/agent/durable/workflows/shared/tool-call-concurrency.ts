import { normalizeToolCallConcurrency } from '../../../../loop/workflows/agentic-execution/tool-call-concurrency';
import { DurableAgentDefaults } from '../../constants';
import type { DurableToolCallInput, SerializableDurableOptions, SerializableToolMetadata } from '../../types';

type DurablePermissionPolicyPreflight = (
  toolCall: Pick<DurableToolCallInput, 'toolCallId' | 'toolName' | 'args'>,
) => unknown;

/**
 * Resolves the effective tool-call foreach concurrency for a durable agentic
 * workflow from the serialized workflow input (iteration state) and the
 * step's tool calls.
 *
 * Mirrors @mastra/core's non-durable loop semantics
 * (loop/workflows/agentic-execution/tool-call-concurrency.ts):
 * - Global `requireToolApproval` forces sequential execution.
 * - A required per-tool permission policy remains sequential unless the live
 *   evaluator is explicitly identified as an immutable per-turn snapshot and
 *   returns `allow` for every emitted call. Missing, throwing, invalid,
 *   promise-valued, `ask`, and `deny` policies all fail conservatively to one.
 * - Any tool in the step's *effective active tool set* with `requireApproval`
 *   or `hasSuspendSchema` forces sequential execution so approval/suspension
 *   flows never race with concurrent tool calls. The check is against the
 *   active tool set, NOT the tools the model actually called: a registered
 *   suspending/approval tool the model skipped this step must still force
 *   sequential execution, since a concurrently-running sibling tool would
 *   race the suspension.
 * - Otherwise the configured `toolCallConcurrency` applies
 *   (default {@link DurableAgentDefaults.TOOL_CALL_CONCURRENCY}).
 *
 * The active tool set is the `activeTools` allowlist the LLM step stamps on
 * each tool call (processors may narrow or clear it; all calls in one step
 * share the value, `null` = restriction cleared → unrestricted). When the
 * calls carry no stamp, the run-level `activeTools` option applies.
 *
 * Designed to be called from a foreach concurrency resolver at execution
 * time. Serialized run state supplies the durable requirement while an
 * optional live policy snapshot can only narrow or unlock the batch's
 * concurrency; no policy decision is persisted as authority.
 */
export function resolveDurableToolCallConcurrency({
  options,
  toolsMetadata,
  toolCalls,
  permissionPolicy,
  permissionPolicyStable = false,
  permissionPolicyRequired = false,
}: {
  options?: Pick<
    SerializableDurableOptions,
    'requireToolApproval' | 'permissionPolicyRequired' | 'toolCallConcurrency' | 'activeTools'
  >;
  toolsMetadata?: SerializableToolMetadata[];
  toolCalls?: Pick<DurableToolCallInput, 'toolCallId' | 'toolName' | 'args' | 'activeTools'>[];
  /** Live policy preflight used only to select a concurrency limit. */
  permissionPolicy?: DurablePermissionPolicyPreflight;
  /** True only for a trusted immutable per-turn policy snapshot. */
  permissionPolicyStable?: boolean;
  /** Runtime marker may require a policy even when serialized options do not. */
  permissionPolicyRequired?: boolean;
}): number {
  if (options?.requireToolApproval) {
    return 1;
  }

  const { limit, strategy } = normalizeToolCallConcurrency(options?.toolCallConcurrency);

  let consideredTools: SerializableToolMetadata[];
  if (strategy === 'called') {
    // Opt-in: consider only the tools the model actually called this step. A
    // batch that never called a suspend/approval tool cannot suspend this step,
    // so it may run at the configured concurrency.
    const calledNames = new Set((toolCalls ?? []).map(tc => tc.toolName).filter((name): name is string => !!name));
    consideredTools = (toolsMetadata ?? []).filter(tool => calledNames.has(tool.name));
  } else {
    // Default: consider the step's effective active tool set (a registered
    // suspend/approval tool forces sequential even if the model skipped it).
    const stamped = toolCalls?.find(tc => tc.activeTools !== undefined);
    const activeTools = stamped ? stamped.activeTools : options?.activeTools;
    consideredTools =
      activeTools === undefined || activeTools === null
        ? (toolsMetadata ?? [])
        : (toolsMetadata ?? []).filter(tool => activeTools.includes(tool.name));
  }

  if (consideredTools.some(tool => tool.hasSuspendSchema || tool.requireApproval)) {
    return 1;
  }

  const requiresPermissionPolicy = options?.permissionPolicyRequired === true || permissionPolicyRequired;
  if (requiresPermissionPolicy || permissionPolicy) {
    if (!permissionPolicy || !permissionPolicyStable) return 1;

    try {
      for (const toolCall of toolCalls ?? []) {
        const decision = permissionPolicy(toolCall);
        // Async policy state cannot be resolved by the synchronous workflow
        // concurrency hook. It remains sequential and is awaited later by the
        // authoritative tool-call step.
        if (
          typeof decision === 'object' &&
          decision !== null &&
          'then' in decision &&
          typeof (decision as { then?: unknown }).then === 'function'
        ) {
          void Promise.resolve(decision).catch(() => undefined);
          return 1;
        }
        if (decision !== 'allow') return 1;
      }
    } catch {
      return 1;
    }
  }

  return limit > 0 ? limit : DurableAgentDefaults.TOOL_CALL_CONCURRENCY;
}
