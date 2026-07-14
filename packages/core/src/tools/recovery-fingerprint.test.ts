import { describe, expect, it } from 'vitest';
import { createToolRecoveryFingerprint } from './recovery-fingerprint';

describe('createToolRecoveryFingerprint', () => {
  it('distinguishes regular-expression source and flags', () => {
    expect(createToolRecoveryFingerprint(/reports/iu)).not.toBe(createToolRecoveryFingerprint(/reports/gu));
    expect(createToolRecoveryFingerprint(/reports/iu)).not.toBe(createToolRecoveryFingerprint(/records/iu));
  });

  it('distinguishes enumerable regular-expression configuration', () => {
    const first = /reports/gu as RegExp & { policy?: string };
    const second = /reports/gu as RegExp & { policy?: string };
    first.policy = 'internal';
    second.policy = 'external';

    expect(createToolRecoveryFingerprint(first)).not.toBe(createToolRecoveryFingerprint(second));
  });

  it('distinguishes regular-expression execution position', () => {
    const first = /reports/gu;
    const second = /reports/gu;
    first.lastIndex = 2;
    second.lastIndex = 4;

    expect(createToolRecoveryFingerprint(first)).not.toBe(createToolRecoveryFingerprint(second));
  });

  it('normalizes non-JSON regular-expression execution positions', () => {
    const bigintPosition = /reports/gu;
    bigintPosition.lastIndex = 1n as unknown as number;
    expect(() => createToolRecoveryFingerprint(bigintPosition)).not.toThrow();

    const notANumberPosition = /reports/gu;
    const infinitePosition = /reports/gu;
    notANumberPosition.lastIndex = Number.NaN;
    infinitePosition.lastIndex = Number.POSITIVE_INFINITY;
    expect(createToolRecoveryFingerprint(notANumberPosition)).not.toBe(createToolRecoveryFingerprint(infinitePosition));
  });

  it('keeps special-value sentinels distinct from ordinary objects', () => {
    expect(createToolRecoveryFingerprint(/reports/iu)).not.toBe(
      createToolRecoveryFingerprint({ $regexp: 'reports', $flags: 'iu' }),
    );
    expect(createToolRecoveryFingerprint(new Date('2026-01-01T00:00:00.000Z'))).not.toBe(
      createToolRecoveryFingerprint({ $date: '2026-01-01T00:00:00.000Z' }),
    );
  });
});
