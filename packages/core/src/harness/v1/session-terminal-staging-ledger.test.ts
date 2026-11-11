import { describe, expect, it } from 'vitest';

import type { HarnessTerminalAdmissionRecord } from '../../storage/domains/harness';
import { TerminalStagingLedger } from './session-terminal-staging-ledger';

/** The ledger reads only an admission's status; the rest of the record is opaque to it. */
function admission(status: HarnessTerminalAdmissionRecord['status']): HarnessTerminalAdmissionRecord {
  return { id: `admission-${status}`, status } as HarnessTerminalAdmissionRecord;
}

/**
 * PF-8389: `TerminalStagingLedger` owns the session's per-run terminal staging
 * state. These pin what the staging, commit and session-save paths rely on.
 */
describe('TerminalStagingLedger', () => {
  it('forgetStaging keeps the native mark and forgetRun drops it', () => {
    const ledger = new TerminalStagingLedger();
    for (const runId of ['run-a', 'run-b']) {
      ledger.admitNative(runId, admission('pending'));
      ledger.markStagedAndAccounted(runId);
    }

    ledger.forgetStaging('run-a');
    ledger.forgetRun('run-b');

    for (const runId of ['run-a', 'run-b']) {
      expect(ledger.isAccounted(runId)).toBe(false);
      expect(ledger.isStaged(runId)).toBe(false);
      expect(ledger.admissionFor(runId)).toBeUndefined();
    }
    expect(ledger.isNative('run-a')).toBe(true);
    expect(ledger.isNative('run-b')).toBe(false);
    expect(ledger.nativeRunCount).toBe(1);
    expect(ledger.admissionCount).toBe(0);
  });

  it('cachePending caches a loaded admission only while it is pending', () => {
    const ledger = new TerminalStagingLedger();
    const pending = admission('pending');

    ledger.cachePending('run-a', admission('committed'));
    ledger.cachePending('run-b', null);
    ledger.cachePending('run-c', pending);

    expect(ledger.admissionFor('run-a')).toBeUndefined();
    expect(ledger.admissionFor('run-b')).toBeUndefined();
    expect(ledger.admissionFor('run-c')).toBe(pending);
    expect(ledger.admissionCount).toBe(1);
    expect(ledger.isNative('run-c')).toBe(false);
  });

  it('accountedSnapshot is a copy of the fences at call time, and undefined when there are none', () => {
    const ledger = new TerminalStagingLedger();
    expect(ledger.accountedSnapshot()).toBeUndefined();

    ledger.markAccounted('run-a');
    ledger.markStagedAndAccounted('run-b');
    const saved = ledger.accountedSnapshot();
    ledger.sweepFence('run-a');

    expect(saved).toEqual(['run-a', 'run-b']);
    expect(ledger.accountedSnapshot()).toEqual(['run-b']);
    expect(ledger.isStaged('run-a')).toBe(false);
    expect(ledger.isStaged('run-b')).toBe(true);
  });

  it('accountedRunIds is the live set, so a sweep sees fences retired while it iterates', () => {
    const ledger = new TerminalStagingLedger();
    ledger.markAccounted('run-a');
    ledger.markAccounted('run-b');
    ledger.markAccounted('run-c');

    const visited: string[] = [];
    for (const runId of ledger.accountedRunIds) {
      visited.push(runId);
      if (runId === 'run-a') ledger.sweepFence('run-b');
    }

    expect(visited).toEqual(['run-a', 'run-c']);
    expect(ledger.accountedSnapshot()).toEqual(['run-a', 'run-c']);
  });
});
