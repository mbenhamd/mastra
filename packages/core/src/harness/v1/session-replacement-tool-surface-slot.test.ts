import { describe, expect, it } from 'vitest';

import type { ToolSurfaceFence } from '../../agent/tool-surface-fence';

import { ReplacementToolSurfaceSlot } from './session-replacement-tool-surface-slot';

const fence = (name: string) => ({ name }) as unknown as ToolSurfaceFence;

describe('ReplacementToolSurfaceSlot', () => {
  it('returns the retained surface only for the run it was retained for', () => {
    const slot = new ReplacementToolSurfaceSlot();
    const a = fence('a');
    expect(slot.fenceFor('run-a')).toBeUndefined();
    slot.retain('run-a', a);
    expect(slot.fenceFor('run-a')).toBe(a);
    expect(slot.fenceFor('run-b')).toBeUndefined();
  });

  it('keeps at most one surface: a later retain replaces the earlier one, for the same run or another', () => {
    const slot = new ReplacementToolSurfaceSlot();
    const first = fence('first');
    const second = fence('second');
    const other = fence('other');
    slot.retain('run-a', first);
    slot.retain('run-a', second);
    expect(slot.fenceFor('run-a')).toBe(second);
    slot.retain('run-b', other);
    expect(slot.fenceFor('run-a')).toBeUndefined();
    expect(slot.fenceFor('run-b')).toBe(other);
  });

  it('releases a surface only through a drop for its own run or dispose', () => {
    const slot = new ReplacementToolSurfaceSlot();
    const a = fence('a');
    slot.retain('run-a', a);
    slot.drop('run-b');
    expect(slot.fenceFor('run-a')).toBe(a);
    slot.drop('run-a');
    expect(slot.fenceFor('run-a')).toBeUndefined();

    slot.retain('run-c', fence('c'));
    slot.dispose();
    expect(slot.fenceFor('run-c')).toBeUndefined();
  });
});
