/**
 * Per-type subagent step and token budgets (`SubagentDefinition.maxSteps` /
 * `maxTotalTokens`).
 *
 * A child session whose type declares a budget gets one meter for its
 * lifetime; its spend is persisted with a suspended run so a restart keeps it.
 * The Harness iteration hook feeds it every completed provider step and stops
 * the run at the step boundary where a budget is reached, unless that
 * iteration really ends the child (a final step nothing reopens).
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

/** Steps and tokens a child has spent; persisted across a suspension so a restart keeps them. */
export interface HarnessSubagentBudgetSpent {
  steps: number;
  tokens: number;
}

export interface HarnessSubagentBudgetMeter {
  /**
   * Record one completed iteration of `segment` (one run segment's options).
   * An iteration at or below one this segment already recorded is a replay
   * and is not counted again. Returns true when a budget is reached.
   */
  observe(context: Pick<IterationCompleteContext, 'iteration' | 'usage'>, segment: object): boolean;
  /** Mark the child as stopped by its budget, at its current spend. */
  exhaust(): void;
  /** Seed spend persisted before a restart; never lowers the current spend. */
  restore(spent: HarnessSubagentBudgetSpent | undefined): void;
  spent(): HarnessSubagentBudgetSpent;
  readonly exhausted: HarnessSubagentBudgetExhaustion | undefined;
}

function stepTokens(usage: IterationCompleteContext['usage']): number {
  if (usage === undefined) return 0;
  const count = (value: unknown) => (typeof value === 'number' && Number.isFinite(value) && value > 0 ? value : 0);
  const total = count(usage.totalTokens);
  // Providers that report only input/output tokens still spend the budget.
  return Math.max(total, count(usage.inputTokens) + count(usage.outputTokens));
}

function isSpendCount(value: unknown): value is number {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0;
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
  const lastIterationBySegment = new WeakMap<object, number>();
  const stepsReached = () => maxSteps !== undefined && steps >= maxSteps;
  const tokensReached = () => maxTotalTokens !== undefined && tokens >= maxTotalTokens;
  return {
    observe(context, segment) {
      const last = lastIterationBySegment.get(segment);
      if (last === undefined || context.iteration > last) {
        lastIterationBySegment.set(segment, context.iteration);
        steps += 1;
        tokens += stepTokens(context.usage);
      }
      return stepsReached() || tokensReached();
    },
    exhaust() {
      if (exhausted !== undefined) return;
      if (maxSteps !== undefined && (stepsReached() || maxTotalTokens === undefined)) {
        exhausted = { budget: 'steps', limit: maxSteps, used: steps };
      } else if (maxTotalTokens !== undefined) {
        exhausted = { budget: 'tokens', limit: maxTotalTokens, used: tokens };
      }
    },
    restore(spent) {
      if (spent === undefined) return;
      if (isSpendCount(spent.steps)) steps = Math.max(steps, spent.steps);
      if (isSpendCount(spent.tokens)) tokens = Math.max(tokens, spent.tokens);
    },
    spent() {
      return { steps, tokens };
    },
    get exhausted() {
      return exhausted;
    },
  };
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
