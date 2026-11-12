import { describe, expect, it } from 'vitest';

import { WakeAdmissions } from './session-wake-admissions';

/**
 * PF-8389: `WakeAdmissions` owns the session's settlement wake admissions.
 * These pin what the wake admission, release and dispatch recovery paths rely
 * on.
 */
describe('WakeAdmissions', () => {
  it('admits one attempt per admission until that attempt leaves', () => {
    const wakes = new WakeAdmissions();

    expect(wakes.enter('wake-1')).toBe(true);
    expect(wakes.enter('wake-1')).toBe(false);
    expect(wakes.enter('wake-2')).toBe(true);
    expect(wakes.isInFlight('wake-1')).toBe(true);
    expect(wakes.inFlightCount).toBe(2);

    wakes.leave('wake-1');
    expect(wakes.isInFlight('wake-1')).toBe(false);
    expect(wakes.inFlightCount).toBe(1);
    expect(wakes.enter('wake-1')).toBe(true);
  });

  it('release hands an admission from in flight to released, and forgetReleased drops it', () => {
    const wakes = new WakeAdmissions();
    wakes.enter('wake-1');

    wakes.release('wake-1');
    expect(wakes.isInFlight('wake-1')).toBe(false);
    expect(wakes.isReleased('wake-1')).toBe(true);
    expect(wakes.releasedSnapshot()).toEqual(['wake-1']);

    wakes.forgetReleased('wake-1');
    expect(wakes.isReleased('wake-1')).toBe(false);
    expect(wakes.releasedSnapshot()).toEqual([]);
  });

  it('releasedSnapshot is a copy taken when it is called', () => {
    const wakes = new WakeAdmissions();
    wakes.release('wake-1');
    const atScanStart = wakes.releasedSnapshot();

    wakes.release('wake-2');
    wakes.forgetReleased('wake-1');

    expect(atScanStart).toEqual(['wake-1']);
    expect(wakes.releasedSnapshot()).toEqual(['wake-2']);
  });
});
