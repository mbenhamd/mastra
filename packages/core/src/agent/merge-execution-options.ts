import { deepMerge } from '../utils';

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

  return merged;
}
