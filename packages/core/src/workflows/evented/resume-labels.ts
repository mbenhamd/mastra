export const MAX_WORKFLOW_RESUME_LABEL_BYTES = 256;
export const MAX_WORKFLOW_RESUME_LABELS = 128;
export const MAX_WORKFLOW_RESUME_LABEL_METADATA_BYTES = 64 * 1024;

export type WorkflowResumeLabelTarget = { stepId: string; foreachIndex?: number };
export type WorkflowResumeLabels = Record<string, WorkflowResumeLabelTarget>;

const utf8Length = (value: string) => new TextEncoder().encode(value).byteLength;
const quarantinedResumeLabelMaps = new WeakSet<WorkflowResumeLabels>();

function invalidResumeLabels(): never {
  throw new Error('Workflow resume label metadata is invalid');
}

export function assertWorkflowResumeLabel(label: unknown): asserts label is string {
  if (typeof label !== 'string' || label.length === 0) {
    throw new Error('Resume label must be a non-empty string');
  }
  if (utf8Length(label) > MAX_WORKFLOW_RESUME_LABEL_BYTES) {
    throw new Error('Workflow resume label exceeds the size limit');
  }
}

function assertWorkflowResumeLabelTarget(target: unknown): asserts target is WorkflowResumeLabelTarget {
  if (target === null || typeof target !== 'object' || Array.isArray(target)) invalidResumeLabels();
  const descriptors = Object.getOwnPropertyDescriptors(target);
  const keys = Object.keys(descriptors);
  if (Reflect.ownKeys(target).length !== keys.length) invalidResumeLabels();
  if (keys.some(key => key !== 'stepId' && key !== 'foreachIndex')) invalidResumeLabels();
  const stepId = descriptors.stepId;
  const foreachIndex = descriptors.foreachIndex;
  if (!stepId || !('value' in stepId) || typeof stepId.value !== 'string' || stepId.value.length === 0) {
    invalidResumeLabels();
  }
  if (
    foreachIndex &&
    (!('value' in foreachIndex) ||
      (foreachIndex.value !== undefined &&
        (!Number.isInteger(foreachIndex.value) || (foreachIndex.value as number) < 0)))
  ) {
    invalidResumeLabels();
  }
}

export function createWorkflowResumeLabels(): WorkflowResumeLabels {
  return Object.create(null) as WorkflowResumeLabels;
}

function resumeLabelMetadataBytes(labels: WorkflowResumeLabels): number {
  let bytes = 0;
  for (const [label, target] of Object.entries(labels)) {
    bytes += utf8Length(label) + utf8Length(target.stepId) + (target.foreachIndex === undefined ? 0 : 8);
  }
  return bytes;
}

export function addWorkflowResumeLabel(labels: WorkflowResumeLabels, label: unknown, target: unknown): void {
  assertWorkflowResumeLabel(label);
  assertWorkflowResumeLabelTarget(target);
  const existing = Object.prototype.hasOwnProperty.call(labels, label) ? labels[label] : undefined;
  if (existing) {
    if (existing.stepId === target.stepId && existing.foreachIndex === target.foreachIndex) return;
    throw new Error('Workflow resume label collision');
  }
  if (Object.keys(labels).length >= MAX_WORKFLOW_RESUME_LABELS) {
    throw new Error('Workflow resume label count exceeds the limit');
  }
  labels[label] = { stepId: target.stepId, foreachIndex: target.foreachIndex };
  if (resumeLabelMetadataBytes(labels) > MAX_WORKFLOW_RESUME_LABEL_METADATA_BYTES) {
    delete labels[label];
    throw new Error('Workflow resume label metadata exceeds the size limit');
  }
}

export function readWorkflowResumeLabels(value: unknown): WorkflowResumeLabels {
  if (value === undefined) return createWorkflowResumeLabels();
  if (value === null || typeof value !== 'object' || Array.isArray(value)) invalidResumeLabels();
  const labels = createWorkflowResumeLabels();
  const descriptors = Object.getOwnPropertyDescriptors(value);
  if (Reflect.ownKeys(value).length !== Object.keys(descriptors).length) invalidResumeLabels();
  for (const [label, descriptor] of Object.entries(descriptors)) {
    if (!descriptor.enumerable || !('value' in descriptor)) invalidResumeLabels();
    addWorkflowResumeLabel(labels, label, descriptor.value);
  }
  return labels;
}

export function mergeWorkflowResumeLabels(
  labels: WorkflowResumeLabels,
  source: unknown,
  mapTarget: (target: WorkflowResumeLabelTarget) => WorkflowResumeLabelTarget = target => target,
): void {
  if (quarantinedResumeLabelMaps.has(labels)) return;
  try {
    for (const [label, target] of Object.entries(readWorkflowResumeLabels(source))) {
      addWorkflowResumeLabel(labels, label, mapTarget(target));
    }
  } catch {
    for (const label of Object.keys(labels)) delete labels[label];
    quarantinedResumeLabelMaps.add(labels);
  }
}
