import { describe, expect, it } from 'vitest';
import {
  MAX_WORKFLOW_RESUME_LABEL_BYTES,
  MAX_WORKFLOW_RESUME_LABEL_METADATA_BYTES,
  MAX_WORKFLOW_RESUME_LABELS,
  addWorkflowResumeLabel,
  createWorkflowResumeLabels,
  mergeWorkflowResumeLabels,
  readWorkflowResumeLabels,
} from './resume-labels';

describe('evented workflow resume-label contract', () => {
  it('accepts the exact label boundary and rejects one byte over it', () => {
    const labels = createWorkflowResumeLabels();
    addWorkflowResumeLabel(labels, 'a'.repeat(MAX_WORKFLOW_RESUME_LABEL_BYTES), { stepId: 'step' });
    expect(Object.keys(labels)).toHaveLength(1);
    expect(() =>
      addWorkflowResumeLabel(labels, 'a'.repeat(MAX_WORKFLOW_RESUME_LABEL_BYTES + 1), { stepId: 'step' }),
    ).toThrow('Workflow resume label exceeds the size limit');
  });

  it('deduplicates one coordinate and rejects a conflicting coordinate', () => {
    const labels = createWorkflowResumeLabels();
    addWorkflowResumeLabel(labels, 'approve', { stepId: 'branch-a', foreachIndex: 1 });
    addWorkflowResumeLabel(labels, 'approve', { stepId: 'branch-a', foreachIndex: 1 });
    expect(Object.keys(labels)).toEqual(['approve']);
    expect(() => addWorkflowResumeLabel(labels, 'approve', { stepId: 'branch-b', foreachIndex: 1 })).toThrow(
      'Workflow resume label collision',
    );
    expect(() => addWorkflowResumeLabel(labels, 'approve', { stepId: 'branch-a', foreachIndex: 2 })).toThrow(
      'Workflow resume label collision',
    );
  });

  it('quarantines a combined map when independent branches reuse a label', () => {
    const labels = createWorkflowResumeLabels();
    mergeWorkflowResumeLabels(labels, { approve: { stepId: 'branch-a' } });
    mergeWorkflowResumeLabels(labels, { approve: { stepId: 'branch-b' } });
    mergeWorkflowResumeLabels(labels, { revise: { stepId: 'branch-c' } });
    expect(labels).toEqual({});
  });

  it('enforces the map-entry and aggregate metadata budgets', () => {
    const labels = createWorkflowResumeLabels();
    for (let index = 0; index < MAX_WORKFLOW_RESUME_LABELS; index++) {
      addWorkflowResumeLabel(labels, `label-${index}`, { stepId: 'step' });
    }
    expect(() => addWorkflowResumeLabel(labels, 'one-too-many', { stepId: 'step' })).toThrow(
      'Workflow resume label count exceeds the limit',
    );

    const oversized = createWorkflowResumeLabels();
    expect(() =>
      addWorkflowResumeLabel(oversized, 'large', { stepId: 's'.repeat(MAX_WORKFLOW_RESUME_LABEL_METADATA_BYTES) }),
    ).toThrow('Workflow resume label metadata exceeds the size limit');
  });

  it('reads only bounded own data properties into a null-prototype map', () => {
    const labels = readWorkflowResumeLabels(
      JSON.parse('{"__proto__":{"stepId":"safe-step"},"approve":{"stepId":"approval-step"}}'),
    );
    expect(Object.getPrototypeOf(labels)).toBeNull();
    expect(labels.__proto__).toEqual({ stepId: 'safe-step', foreachIndex: undefined });
    expect(labels.approve).toEqual({ stepId: 'approval-step', foreachIndex: undefined });

    const accessor = {};
    Object.defineProperty(accessor, 'approve', { enumerable: true, get: () => ({ stepId: 'step' }) });
    expect(() => readWorkflowResumeLabels(accessor)).toThrow('Workflow resume label metadata is invalid');
  });
});
