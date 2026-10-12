import type { StepResult } from '@internal/ai-sdk-v5';
import type { MessageList } from '../../agent/message-list';
import type { StepContentLocation } from '../../agent/message-list/message-list';

type StepWithTripwire = { readonly tripwire?: { readonly rolledBack?: boolean } };

/**
 * Whether a granted processor retry removed this step's response from the MessageList.
 *
 * The rejected attempt's parts, including the `step-start` marker that opened
 * them, are rolled back, but its StepResult stays in `steps` for history. A retry
 * that was requested and not granted keeps its parts, so `tripwire.retry` alone
 * does not say this.
 */
export function stepWasRolledBack(step: StepWithTripwire | undefined): boolean {
  return step?.tripwire?.rolledBack === true;
}

/**
 * One-based `modelContent(stepNumber)` position of `steps[index]`.
 *
 * `StepContentExtractor` finds a step by counting `step-start` markers, and a
 * rolled-back attempt no longer has one. Counting it would point every later
 * step at the marker of the step after it: a repaired answer would read as
 * empty, and Harness would treat the turn as silent (PF-5364).
 */
export function messageStepNumber(steps: ReadonlyArray<StepWithTripwire>, index: number): number {
  let stepNumber = 1;
  for (let i = 0; i < index; i++) {
    if (!stepWasRolledBack(steps[i])) stepNumber++;
  }
  return stepNumber;
}

/** Where the most recent step of a run writes its response, and which entry of `steps` it is. */
export type LocatedStepContent = { readonly stepIndex: number; readonly location: StepContentLocation };

/**
 * The content of `steps[index]` as the MessageList holds it now.
 *
 * Read from where the step's iteration wrote its response when that was
 * recorded for this step. Otherwise (a durable run that rebuilt its
 * MessageList) fall back to counting `step-start` markers.
 *
 * @returns the content, or `undefined` when only the fallback was available and
 *   it found nothing: the list no longer holds the step, which is not the same
 *   as the step having no content, so callers keep what they captured.
 */
export function readStepContent(
  messageList: MessageList,
  steps: ReadonlyArray<StepWithTripwire>,
  index: number,
  located: LocatedStepContent | undefined,
): StepResult<any>['content'] | undefined {
  if (located?.stepIndex === index) return messageList.stepContent(located.location);
  const counted = messageList.get.response.aiV5.modelContent(messageStepNumber(steps, index));
  return counted.length > 0 ? counted : undefined;
}
