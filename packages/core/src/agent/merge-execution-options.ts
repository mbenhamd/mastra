import { deepMerge } from '../utils';

/**
 * Internal execution-option composers run after caller options have been
 * merged over agent defaults. They let framework-owned lanes add behavior
 * without replacing an application's configured callback.
 *
 * @internal
 */
export const AGENT_EXECUTION_OPTION_COMPOSERS = Symbol('agentExecutionOptionComposers');

export type AgentExecutionOptionComposers = Partial<
  Record<'onIterationComplete' | 'prepareStep', (existing: unknown) => unknown>
> & {
  /** Internal recovery calls that may run only after the ordinary maxSteps boundary. */
  recoveryMaxSteps?: number;
};
export type AgentExecutionOptionComposerFactory = () => AgentExecutionOptionComposers;

/**
 * Merge agent defaults beneath per-execution options while preserving the
 * replacement tool-surface ceiling. Plain deep merge is unsafe here because
 * it would retain default toolsets when a caller explicitly requested a
 * replacement surface.
 */
export function mergeAgentExecutionOptions(
  defaultOptions: Record<string, any>,
  callerOptions: Record<string, any>,
): Record<string, any> {
  const merged = deepMerge(
    defaultOptions as Record<string, unknown>,
    callerOptions as Record<string, unknown>,
  ) as Record<string, any>;

  if (merged.toolsetsMode === 'replace') {
    if (callerOptions.toolsetsMode === 'replace') {
      merged.toolsets = callerOptions.toolsets ?? {};
    } else if (callerOptions.toolsets !== undefined) {
      merged.toolsets = callerOptions.toolsets;
    }
  }

  const createComposers = (callerOptions as Record<PropertyKey, any>)[AGENT_EXECUTION_OPTION_COMPOSERS] as
    AgentExecutionOptionComposerFactory | undefined;
  const composers = createComposers?.();
  if (composers?.recoveryMaxSteps !== undefined) {
    merged.recoveryMaxSteps = composers.recoveryMaxSteps;
  }
  for (const key of ['onIterationComplete', 'prepareStep'] as const) {
    const compose = composers?.[key];
    if (compose) merged[key] = compose(merged[key]);
  }

  return merged;
}
