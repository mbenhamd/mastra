import type { MastraLanguageModel } from '../../llm/model/shared.types';
import type { RunProcessInputStepResult } from '../../processors';
import { recordStructuredOutputSourceDecision } from '../../stream/aisdk/v5/execute';

/**
 * Shape of the per-step LLM input fields that an input-processor (or
 * `prepareStep`) is allowed to override.
 *
 * Note: this interface lists the canonical fields, but the helper preserves
 * any additional properties on the current step (e.g. `workspace` from
 * `PrepareStepResult`) by spreading `current` first.
 */
export interface StepInputFields {
  messageId: string;
  model: MastraLanguageModel;
  tools?: any;
  toolChoice?: any;
  activeTools?: string[] | undefined;
  providerOptions?: any;
  modelSettings?: any;
  structuredOutput?: any;
  // Index signature so extra fields like `workspace` survive the merge
  // without having to enumerate every loop-level option here.
  [key: string]: any;
}

/**
 * Merge an input-step processor result back into the current step. Mirrors
 * the regular agentic-execution step's `Object.assign(currentStep, ...)`
 * semantics so the durable and non-durable paths apply `prepareStep` and
 * other input processors identically.
 *
 * Fields that the runner did not touch are preserved verbatim; fields the
 * runner did touch fully replace the current value. `modelSettings` is
 * intentionally replaced rather than shallow-merged — that matches the
 * non-durable path and matches the contract documented on processor
 * results, where the returned `modelSettings` is the final shape for this
 * step.
 */
export function composeStepInput(
  current: StepInputFields,
  processInputStepResult: RunProcessInputStepResult | undefined,
): StepInputFields {
  if (!processInputStepResult) {
    return current;
  }
  // Spread `current` first so any extra fields (e.g. `workspace` from
  // `PrepareStepResult`) survive the merge. Then overlay the processor
  // result on top — this matches the original `Object.assign(currentStep,
  // processInputStepResult)` semantics, where all keys present on the
  // processor result fully replace the corresponding keys on the current
  // step.
  return {
    ...current,
    ...processInputStepResult,
    // Restore type-narrowed defaults for the canonical fields if the
    // processor returned undefined for any of them.
    messageId: processInputStepResult.messageId ?? current.messageId,
    model: processInputStepResult.model ?? current.model,
    ...('structuredOutput' in processInputStepResult
      ? { structuredOutput: withSourceDecision(current.structuredOutput, processInputStepResult.structuredOutput) }
      : {}),
  };
}

/**
 * PF-7309: the structured-output source decision follows the schema the step actually uses. A
 * processor that keeps the schema keeps its recorded decision; one that supplies a different schema
 * gets a decision recorded from that schema (a carried-over decision for the old schema is replaced).
 */
function withSourceDecision(current: any, next: any): any {
  if (!next?.schema) return next;
  if (next.schema === current?.schema) {
    return next.sourceDecision || !current?.sourceDecision ? next : { ...next, sourceDecision: current.sourceDecision };
  }
  return { ...next, sourceDecision: recordStructuredOutputSourceDecision(next.schema) };
}
