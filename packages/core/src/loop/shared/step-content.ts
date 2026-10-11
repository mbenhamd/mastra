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
