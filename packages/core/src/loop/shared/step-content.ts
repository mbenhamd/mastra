import type { StepResult } from '@internal/ai-sdk-v5';
import type { MessageList } from '../../agent/message-list';
import type { StepContentLocation } from '../../agent/message-list/message-list';

type StepWithTripwire = { readonly tripwire?: { readonly retry?: boolean } };

/** Where the most recent step of a run wrote its response, and which entry of `steps` it is. */
export type LocatedStepContent = { readonly stepIndex: number; readonly location: StepContentLocation };

/**
 * Whether a processor retry rolled this step's parts back off the MessageList.
 *
 * A granted retry splices the rejected attempt out of the response message,
 * including the `step-start` marker that opened it, but the rejected StepResult
 * stays in `steps` with `tripwire.retry` set. A retry that was requested but not
 * granted (budget exhausted, response-only recovery) keeps its parts, yet it also
 * ends the run, so no later iteration counts it.
 */
export function stepWasRolledBack(step: StepWithTripwire | undefined): boolean {
  return step?.tripwire?.retry === true;
}

/**
 * One-based `modelContent(stepNumber)` position of `steps[index]`.
 *
 * The extractor counts `step-start` markers, so only steps whose parts are
 * still in the message occupy a position: a rolled-back attempt does not.
 */
export function messageStepNumber(steps: ReadonlyArray<StepWithTripwire>, index: number): number {
  let stepNumber = 1;
  for (let i = 0; i < index; i++) {
    if (!stepWasRolledBack(steps[i])) stepNumber++;
  }
  return stepNumber;
}

/**
 * The content of `steps[index]` as the MessageList holds it now.
 *
 * Read from where the step's iteration wrote its response when that location
 * is known for this step (PF-5364). Otherwise — a durable run that rebuilt its
 * MessageList, or a boundary a processor replaced beyond recognition — fall
 * back to counting `step-start` markers.
 */
export function readStepContent(
  messageList: MessageList,
  steps: ReadonlyArray<StepWithTripwire>,
  index: number,
  located: LocatedStepContent | undefined,
): StepResult<any>['content'] {
  const content = located?.stepIndex === index ? messageList.stepContent(located.location) : undefined;
  return content ?? messageList.get.response.aiV5.modelContent(messageStepNumber(steps, index));
}
