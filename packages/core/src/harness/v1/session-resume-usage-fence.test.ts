import { describe, expect, it } from 'vitest';

import { ResumeUsageFence } from './session-resume-usage-fence';

const usage = (totalTokens: number) => ({ promptTokens: totalTokens, completionTokens: 0, totalTokens });

describe('ResumeUsageFence', () => {
  it('applies the baseline only to the generation it was armed for', () => {
    const fence = new ResumeUsageFence();
    expect(fence.baselineFor(() => 'gen-a')).toBeUndefined();
    const a = usage(5);
    fence.arm('gen-a', a);
    expect(fence.isArmedFor('gen-a')).toBe(true);
    expect(fence.isArmedFor('gen-b')).toBe(false);
    expect(fence.baselineFor(() => 'gen-a')).toBe(a);
    expect(fence.baselineFor(() => 'gen-b')).toBeUndefined();
  });

  it('keeps one baseline: a later arm replaces the earlier one, for the same generation or another', () => {
    const fence = new ResumeUsageFence();
    const later = usage(9);
    fence.arm('gen-a', usage(5));
    fence.arm('gen-a', later);
    expect(fence.baselineFor(() => 'gen-a')).toBe(later);
    const other = usage(3);
    fence.arm('gen-b', other);
    expect(fence.isArmedFor('gen-a')).toBe(false);
    expect(fence.baselineFor(() => 'gen-b')).toBe(other);
  });

  it('stays armed while its generation is still parked, and disarms once none or another is', () => {
    const fence = new ResumeUsageFence();
    const a = usage(5);
    fence.arm('gen-a', a);
    fence.disarmUnlessParked(() => 'gen-a');
    expect(fence.baselineFor(() => 'gen-a')).toBe(a);
    fence.disarmUnlessParked(() => 'gen-b');
    expect(fence.isArmedFor('gen-a')).toBe(false);

    fence.arm('gen-c', usage(1));
    fence.disarmUnlessParked(undefined);
    expect(fence.isArmedFor('gen-c')).toBe(false);
  });

  it('computes no generation key while unarmed', () => {
    const fence = new ResumeUsageFence();
    let computed = 0;
    const key = () => {
      computed++;
      return 'gen-a';
    };
    expect(fence.baselineFor(key)).toBeUndefined();
    fence.disarmUnlessParked(key);
    expect(computed).toBe(0);
  });
});
