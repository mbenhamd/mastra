import { isProviderDefinedTool } from '../../tools/toolchecks';

/**
 * Tool choice for an admitted response-only recovery step (PF-5349).
 *
 * The recovery step keeps the tool schemas of the step before it, because a
 * changed tool list invalidates the provider's cached prompt prefix. `'none'`
 * cannot be forced for that: provider preparation strips every tool when the
 * choice is `'none'` (`stream/aisdk/v5/compat/prepare-tools.ts`). The loop instead
 * refuses to execute any client tool call the recovery step emits.
 *
 * - A forced choice (`'required'` or a named tool) would make a reply impossible,
 *   so it relaxes to `'auto'`.
 * - Provider-executed tools run on the provider side, where the loop cannot
 *   refuse them, so a surface that exposes one falls back to `'none'` (no tools,
 *   a cache miss on this step only).
 * - Any other choice, including a step that already chose `'none'`, is kept.
 */
export function responseRecoveryToolChoice<T>(
  toolChoice: T,
  tools: Record<string, unknown> | undefined,
  activeTools: readonly string[] | undefined,
): T | 'auto' | 'none' {
  const exposed = Object.entries(tools ?? {}).filter(([name]) => activeTools == null || activeTools.includes(name));
  if (exposed.some(([, tool]) => isProviderDefinedTool(tool as never))) return 'none';
  if (toolChoice === 'required') return 'auto';
  if (toolChoice !== null && typeof toolChoice === 'object') return 'auto';
  return toolChoice;
}
