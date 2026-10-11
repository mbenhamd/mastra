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

/**
 * Names of the tools a step selects for its provider request: every tool, or
 * the active subset, and none when the choice is `'none'` (provider preparation
 * strips them).
 */
function selectedToolNames(
  tools: Record<string, unknown> | undefined,
  activeTools: readonly string[] | undefined,
  toolChoice: unknown,
): string[] {
  if (toolChoice === 'none') return [];
  const names = Object.keys(tools ?? {});
  return activeTools == null ? names : names.filter(name => activeTools.includes(name));
}

/**
 * Tool selection for an admitted response-only recovery step (PF-5349).
 *
 * The step advertises the tools it would offer itself, narrowed to `ceiling`:
 * the names the latest ordinary request of the run advertised. Processors on
 * the recovery step therefore cannot offer a tool that request did not, however
 * they change `tools`, `activeTools` or `toolChoice`. Without a recorded
 * ceiling, or when nothing survives the bound, the step is tool-free.
 */
export function boundResponseRecoveryToolSelection<T>(
  ceiling: readonly string[] | undefined,
  tools: Record<string, unknown> | undefined,
  activeTools: readonly string[] | undefined,
  toolChoice: T,
): { activeTools: string[]; toolChoice: T | 'auto' | 'none' } {
  const allowed =
    ceiling === undefined
      ? []
      : selectedToolNames(tools, activeTools, toolChoice).filter(name => ceiling.includes(name));
  if (allowed.length === 0) return { activeTools: [], toolChoice: 'none' };
  return { activeTools: allowed, toolChoice: responseRecoveryToolChoice(toolChoice, tools, allowed) };
}
