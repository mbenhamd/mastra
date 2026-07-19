import { describe, expect, it } from 'vitest';
import { z } from 'zod';
import {
  createToolRecoveryFingerprint,
  defineLazyToolRecoveryFingerprint,
  normalizeToolRecoverySchemaIdentity,
} from './recovery-fingerprint';

describe('defineLazyToolRecoveryFingerprint', () => {
  it.each([Object.freeze, Object.seal])('stays memoized when the tool is made non-configurable', lock => {
    let computations = 0;
    const tool = {} as { recoveryFingerprint: string };
    defineLazyToolRecoveryFingerprint(tool, () => {
      computations += 1;
      return 'fingerprint';
    });
    lock(tool);

    expect(tool.recoveryFingerprint).toBe('fingerprint');
    expect(tool.recoveryFingerprint).toBe('fingerprint');
    expect(computations).toBe(1);
  });
});

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

  it('rejects regular-expression subclasses whose prototype behavior cannot be fingerprinted', () => {
    class InternalPolicy extends RegExp {
      override test(value: string): boolean {
        return value.startsWith('internal:') && super.test(value);
      }
    }

    expect(() => createToolRecoveryFingerprint({ policy: new InternalPolicy('reports', 'u') })).toThrow(
      'Cannot create a durable recovery fingerprint for RegExp subclass at "$tool.policy"',
    );
  });

  it('keeps special-value sentinels distinct from ordinary objects', () => {
    expect(createToolRecoveryFingerprint(/reports/iu)).not.toBe(
      createToolRecoveryFingerprint({ $regexp: 'reports', $flags: 'iu' }),
    );
    expect(createToolRecoveryFingerprint(new Date('2026-01-01T00:00:00.000Z'))).not.toBe(
      createToolRecoveryFingerprint({ $date: '2026-01-01T00:00:00.000Z' }),
    );
  });

  it('includes runtime-only schema refinements and transforms', () => {
    const minimumTwo = z.string().refine(value => value.length > 2);
    const minimumThree = z.string().refine(value => value.length > 3);
    const upper = z.string().transform(value => value.toUpperCase());
    const lower = z.string().transform(value => value.toLowerCase());

    expect(createToolRecoveryFingerprint(normalizeToolRecoverySchemaIdentity(minimumTwo))).not.toBe(
      createToolRecoveryFingerprint(normalizeToolRecoverySchemaIdentity(minimumThree)),
    );
    expect(createToolRecoveryFingerprint(normalizeToolRecoverySchemaIdentity(upper))).not.toBe(
      createToolRecoveryFingerprint(normalizeToolRecoverySchemaIdentity(lower)),
    );
  });

  it('keeps equivalent object schemas stable across property order', () => {
    const first = z.object({ value: z.string(), count: z.number() });
    const second = z.object({ count: z.number(), value: z.string() });

    expect(createToolRecoveryFingerprint(normalizeToolRecoverySchemaIdentity(first))).toBe(
      createToolRecoveryFingerprint(normalizeToolRecoverySchemaIdentity(second)),
    );
  });
});
