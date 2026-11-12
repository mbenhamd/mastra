import { describe, expect, it } from 'vitest';

import { MessageTokenLedger } from './session-message-token-ledger';

function reservation() {
  return { promise: Promise.resolve() };
}

/**
 * PF-8451: `MessageTokenLedger` owns which runs' message-token usage this
 * process counted and which runs a suspended capture is counting.
 */
describe('MessageTokenLedger', () => {
  it('a committed reservation marks the run counted; a rolled-back one leaves it uncounted', () => {
    const ledger = new MessageTokenLedger();
    const committed = reservation();
    const rolledBack = reservation();
    ledger.reserve('run-a', committed);
    ledger.reserve('run-b', rolledBack);
    expect(ledger.isReserved('run-a')).toBe(true);
    expect(ledger.reservationFor('run-b')).toBe(rolledBack);

    ledger.commit('run-a', committed);
    ledger.rollback('run-b', rolledBack);

    expect(ledger.isAccounted('run-a')).toBe(true);
    expect(ledger.isAccounted('run-b')).toBe(false);
    for (const runId of ['run-a', 'run-b']) {
      expect(ledger.isReserved(runId)).toBe(false);
      expect(ledger.reservationFor(runId)).toBeUndefined();
    }
  });

  it('evict forgets the run in all three collections', () => {
    const ledger = new MessageTokenLedger();
    ledger.markAccounted('run-a');
    ledger.reserve('run-a', reservation());

    ledger.evict('run-a');

    expect(ledger.isAccounted('run-a')).toBe(false);
    expect(ledger.isReserved('run-a')).toBe(false);
    expect(ledger.reservationFor('run-a')).toBeUndefined();
  });

  // Today's behaviour, pinned so that changing it is a decision: when a run is
  // reserved twice, the first holder's commit or rollback clears the run's
  // reservation mark but leaves the second holder's deferred in place.
  it.each(['commit', 'rollback'] as const)(
    'question: should the first holder of a twice-reserved run clear the reservation mark on %s?',
    operation => {
      const ledger = new MessageTokenLedger();
      const first = reservation();
      const second = reservation();
      ledger.reserve('run-a', first);
      ledger.reserve('run-a', second);

      ledger[operation]('run-a', first);

      expect(ledger.isReserved('run-a')).toBe(false);
      expect(ledger.reservationFor('run-a')).toBe(second);
      expect(ledger.isAccounted('run-a')).toBe(operation === 'commit');
    },
  );
});
