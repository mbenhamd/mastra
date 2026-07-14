import { describe, expect, it, vi } from 'vitest';
import {
  MAX_EVENTED_RESUME_LABEL_BYTES,
  MAX_EVENTED_RESUME_LABEL_COUNT,
  MAX_EVENTED_RESUME_LABEL_METADATA_BYTES,
  createEventedResumeLabels,
  createEventedResumeLabelsForTarget,
  mergeEventedResumeLabels,
  normalizeEventedResumeLabels,
} from './resume-label';

describe('evented resume-label contract', () => {
  it('accepts the exact UTF-8 name boundary and rejects the next byte', () => {
    const exact = 'a'.repeat(MAX_EVENTED_RESUME_LABEL_BYTES);
    expect(createEventedResumeLabelsForTarget(exact, { stepId: 'step' })[exact]).toEqual({ stepId: 'step' });

    expect(() => createEventedResumeLabelsForTarget(`${exact}a`, { stepId: 'step' })).toThrowError(
      'Invalid workflow resume label metadata',
    );
  });

  it('measures multibyte labels in UTF-8 bytes', () => {
    const exact = 'é'.repeat(MAX_EVENTED_RESUME_LABEL_BYTES / 2);
    expect(createEventedResumeLabelsForTarget(exact, { stepId: 'step' })[exact]).toEqual({ stepId: 'step' });

    expect(() => createEventedResumeLabelsForTarget(`${exact}é`, { stepId: 'step' })).toThrowError(
      'Invalid workflow resume label metadata',
    );
  });

  it('rejects clearly oversized strings before UTF-8 encoding them', () => {
    const encode = vi.spyOn(TextEncoder.prototype, 'encode');
    try {
      expect(() =>
        createEventedResumeLabelsForTarget('a'.repeat(MAX_EVENTED_RESUME_LABEL_BYTES + 1), { stepId: 'step' }),
      ).toThrowError('Invalid workflow resume label metadata');
      expect(encode).not.toHaveBeenCalled();

      expect(() =>
        createEventedResumeLabelsForTarget('x', {
          stepId: 's'.repeat(MAX_EVENTED_RESUME_LABEL_METADATA_BYTES + 1),
        }),
      ).toThrowError('Invalid workflow resume label metadata');
      expect(encode).toHaveBeenCalledOnce();
      expect(encode).toHaveBeenCalledWith('x');
    } finally {
      encode.mockRestore();
    }
  });

  it('enforces exact entry and aggregate metadata boundaries', () => {
    let labels = createEventedResumeLabels();
    for (let index = 0; index < MAX_EVENTED_RESUME_LABEL_COUNT; index++) {
      labels = mergeEventedResumeLabels(labels, {
        [`label-${index}`]: { stepId: 'step' },
      });
    }
    expect(Object.keys(labels)).toHaveLength(MAX_EVENTED_RESUME_LABEL_COUNT);
    expect(() => mergeEventedResumeLabels(labels, { overflow: { stepId: 'step' } })).toThrowError(
      'Invalid workflow resume label metadata',
    );

    const serializedOverhead = new TextEncoder().encode(JSON.stringify({ x: { stepId: '' } })).byteLength;
    const exactStepId = 's'.repeat(MAX_EVENTED_RESUME_LABEL_METADATA_BYTES - serializedOverhead);
    expect(createEventedResumeLabelsForTarget('x', { stepId: exactStepId }).x).toEqual({ stepId: exactStepId });
    expect(() => createEventedResumeLabelsForTarget('x', { stepId: `${exactStepId}s` })).toThrowError(
      'Invalid workflow resume label metadata',
    );

    const escapedStepId = '\0'.repeat(
      Math.ceil((MAX_EVENTED_RESUME_LABEL_METADATA_BYTES - serializedOverhead + 1) / 6),
    );
    expect(new TextEncoder().encode(escapedStepId).byteLength).toBeLessThan(MAX_EVENTED_RESUME_LABEL_METADATA_BYTES);
    expect(new TextEncoder().encode(JSON.stringify({ x: { stepId: escapedStepId } })).byteLength).toBeGreaterThan(
      MAX_EVENTED_RESUME_LABEL_METADATA_BYTES,
    );
    expect(() => createEventedResumeLabelsForTarget('x', { stepId: escapedStepId })).toThrowError(
      'Invalid workflow resume label metadata',
    );
  });

  it('rejects collisions and preserves magic-key labels as own data properties', () => {
    const labels = createEventedResumeLabelsForTarget(['__proto__', 'constructor'], { stepId: 'step' });
    expect(Object.prototype.hasOwnProperty.call(labels, '__proto__')).toBe(true);
    expect(Object.prototype.hasOwnProperty.call(labels, 'constructor')).toBe(true);
    expect(labels.__proto__).toEqual({ stepId: 'step' });

    expect(() => mergeEventedResumeLabels(labels, { constructor: { stepId: 'other-step' } })).toThrowError(
      'Invalid workflow resume label metadata',
    );
  });

  it('rejects accessors without invoking them', () => {
    const getter = vi.fn(() => ({ stepId: 'step' }));
    const labels = {};
    Object.defineProperty(labels, 'approve', { enumerable: true, get: getter });

    expect(() => normalizeEventedResumeLabels(labels)).toThrowError('Invalid workflow resume label metadata');
    expect(getter).not.toHaveBeenCalled();
  });

  it('rejects proxies before invoking reflection traps', () => {
    const trap = vi.fn(() => {
      throw new Error('proxy trap must not run');
    });
    const handler = {
      get: trap,
      getOwnPropertyDescriptor: trap,
      getPrototypeOf: trap,
      ownKeys: trap,
    } as ProxyHandler<Record<string, unknown>>;

    expect(() => normalizeEventedResumeLabels(new Proxy({}, handler))).toThrowError(
      'Invalid workflow resume label metadata',
    );
    expect(() => normalizeEventedResumeLabels({ approve: new Proxy({ stepId: 'step' }, handler) })).toThrowError(
      'Invalid workflow resume label metadata',
    );

    const labelArrayProxy = new Proxy(['approve'], handler as ProxyHandler<string[]>);
    expect(() => createEventedResumeLabelsForTarget(labelArrayProxy, { stepId: 'step' })).toThrowError(
      'Invalid workflow resume label metadata',
    );

    const revoked = Proxy.revocable({}, {});
    revoked.revoke();
    expect(() => normalizeEventedResumeLabels(revoked.proxy)).toThrowError('Invalid workflow resume label metadata');
    expect(trap).not.toHaveBeenCalled();
  });

  it('accepts only plain data records and safe foreach indices', () => {
    const customLabelMap = Object.create({ inherited: true });
    customLabelMap.approve = { stepId: 'step' };
    expect(() => normalizeEventedResumeLabels(customLabelMap)).toThrowError('Invalid workflow resume label metadata');

    const customTarget = Object.create({ inherited: true });
    customTarget.stepId = 'step';
    expect(() => normalizeEventedResumeLabels({ approve: customTarget })).toThrowError(
      'Invalid workflow resume label metadata',
    );

    expect(
      normalizeEventedResumeLabels({ approve: { stepId: 'step', foreachIndex: Number.MAX_SAFE_INTEGER } }).approve,
    ).toEqual({ stepId: 'step', foreachIndex: Number.MAX_SAFE_INTEGER });
    expect(() =>
      normalizeEventedResumeLabels({ approve: { stepId: 'step', foreachIndex: Number.MAX_SAFE_INTEGER + 1 } }),
    ).toThrowError('Invalid workflow resume label metadata');
  });

  it('descriptor-copies only dense data-only label arrays', () => {
    const getter = vi.fn(() => 'approve');
    const accessorLabels: string[] = [];
    Object.defineProperty(accessorLabels, '0', { enumerable: true, get: getter });
    accessorLabels.length = 1;
    expect(() => createEventedResumeLabelsForTarget(accessorLabels, { stepId: 'step' })).toThrowError(
      'Invalid workflow resume label metadata',
    );
    expect(getter).not.toHaveBeenCalled();

    expect(() => createEventedResumeLabelsForTarget(new Array(1), { stepId: 'step' })).toThrowError(
      'Invalid workflow resume label metadata',
    );

    const labelsWithExtraProperty = ['approve'];
    Object.defineProperty(labelsWithExtraProperty, 'extra', { value: 'revise', enumerable: true });
    expect(() => createEventedResumeLabelsForTarget(labelsWithExtraProperty, { stepId: 'step' })).toThrowError(
      'Invalid workflow resume label metadata',
    );

    const customPrototypeLabels = ['approve'];
    Object.setPrototypeOf(customPrototypeLabels, Object.create(Array.prototype));
    expect(() => createEventedResumeLabelsForTarget(customPrototypeLabels, { stepId: 'step' })).toThrowError(
      'Invalid workflow resume label metadata',
    );
  });
});
