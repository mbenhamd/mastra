/**
 * Per-type subagent step and token budgets (`SubagentDefinition.maxSteps` /
 * `maxTotalTokens`).
 *
 * A child session whose type declares a budget gets one meter for its
 * lifetime. The Harness iteration hook feeds it every completed provider step
 * and stops the run at the step boundary where a budget is reached; a final
 * iteration is never counted as exhaustion because the run is ending anyway.
 * The spawn tool and the delegation driver then report
 * `harness.subagent_budget_exhausted` with the bounded partial summary
 * instead of a generic incomplete result.
 */

import type { IterationCompleteContext } from '../../agent/agent.types';
import type { SubagentDefinition } from './types';

export const HARNESS_SUBAGENT_BUDGET_EXHAUSTED_CODE = 'harness.subagent_budget_exhausted' as const;

/** Which budget stopped the child, its configured limit, and what the child had used. */
export interface HarnessSubagentBudgetExhaustion {
  budget: 'steps' | 'tokens';
  limit: number;
  used: number;
}

export interface HarnessSubagentBudgetMeter {
  /**
   * Record one completed iteration. Returns true when the run must stop here
   * because a budget is reached and the model would otherwise continue.
   */
  observe(context: Pick<IterationCompleteContext, 'isFinal' | 'usage' | 'runId' | 'iteration'>): boolean;
  readonly exhausted: HarnessSubagentBudgetExhaustion | undefined;
}

function stepTokens(usage: IterationCompleteContext['usage']): number {
  if (usage === undefined) return 0;
  const count = (value: unknown) => (typeof value === 'number' && Number.isFinite(value) && value > 0 ? value : 0);
  const total = count(usage.totalTokens);
  // Providers that report only input/output tokens still spend the budget.
  return Math.max(total, count(usage.inputTokens) + count(usage.outputTokens));
}

/** Returns undefined when the type declares no budget (today's behaviour). */
export function createHarnessSubagentBudgetMeter(
  definition: Pick<SubagentDefinition, 'maxSteps' | 'maxTotalTokens'> | undefined,
): HarnessSubagentBudgetMeter | undefined {
  const maxSteps = definition?.maxSteps;
  const maxTotalTokens = definition?.maxTotalTokens;
  if (maxSteps === undefined && maxTotalTokens === undefined) return undefined;
  let steps = 0;
  let tokens = 0;
  let exhausted: HarnessSubagentBudgetExhaustion | undefined;
  // Durable loop predicates may be replayed for the same iteration before the
  // transition commits; a replay repeats the earlier decision without counting.
  let last: { runId: string; iteration: number; stop: boolean } | undefined;
  return {
    observe(context) {
      if (last !== undefined && last.runId === context.runId && last.iteration === context.iteration) return last.stop;
      const stop = observeOnce(context);
      last = { runId: context.runId, iteration: context.iteration, stop };
      return stop;
    },
    get exhausted() {
      return exhausted;
    },
  };

  function observeOnce(context: Pick<IterationCompleteContext, 'isFinal' | 'usage'>): boolean {
    if (exhausted !== undefined) return true;
    steps += 1;
    tokens += stepTokens(context.usage);
    if (context.isFinal) return false;
    if (maxSteps !== undefined && steps >= maxSteps) {
      exhausted = { budget: 'steps', limit: maxSteps, used: steps };
    } else if (maxTotalTokens !== undefined && tokens >= maxTotalTokens) {
      exhausted = { budget: 'tokens', limit: maxTotalTokens, used: tokens };
    }
    return exhausted !== undefined;
  }
}

/** Parent-visible error for a child stopped by its budget before it reported an outcome. */
export function harnessSubagentBudgetExhaustedError(
  agentType: string,
  exhaustion: HarnessSubagentBudgetExhaustion,
): { code: typeof HARNESS_SUBAGENT_BUDGET_EXHAUSTED_CODE; message: string } {
  const unit = exhaustion.budget === 'steps' ? 'step' : 'token';
  return {
    code: HARNESS_SUBAGENT_BUDGET_EXHAUSTED_CODE,
    message:
      `Subagent "${agentType}" reached its ${unit} budget (${exhaustion.used} of ${exhaustion.limit} ${unit}s) ` +
      'before it reported an outcome. Its text and tool trace are partial; the task is not complete.',
  };
}
