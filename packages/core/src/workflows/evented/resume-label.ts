import { isProxy } from 'node:util/types';

export type EventedResumeLabelTarget = {
  stepId: string;
  foreachIndex?: number;
};

export type EventedResumeLabels = Record<string, EventedResumeLabelTarget>;

export const MAX_EVENTED_RESUME_LABEL_BYTES = 256;
export const MAX_EVENTED_RESUME_LABEL_COUNT = 64;
export const MAX_EVENTED_RESUME_LABEL_METADATA_BYTES = 16_384;

const textEncoder = new TextEncoder();

function invalidResumeLabelMetadata(): never {
  throw new Error('Invalid workflow resume label metadata');
}

function utf8Bytes(value: string) {
  return textEncoder.encode(value).byteLength;
}

function isDataRecord(value: unknown): value is Record<string, unknown> {
  if (value === null || typeof value !== 'object' || isProxy(value) || Array.isArray(value)) {
    return false;
  }

  const prototype = Object.getPrototypeOf(value);
  return prototype === Object.prototype || prototype === null;
}

function assertResumeLabelName(label: unknown): asserts label is string {
  if (
    typeof label !== 'string' ||
    label.length === 0 ||
    label.length > MAX_EVENTED_RESUME_LABEL_BYTES ||
    utf8Bytes(label) > MAX_EVENTED_RESUME_LABEL_BYTES
  ) {
    invalidResumeLabelMetadata();
  }
}

export function assertEventedResumeLabelName(label: unknown): asserts label is string {
  assertResumeLabelName(label);
}

function normalizeResumeLabelTarget(value: unknown): EventedResumeLabelTarget {
  if (!isDataRecord(value)) {
    return invalidResumeLabelMetadata();
  }

  if (Object.getOwnPropertySymbols(value).length > 0) {
    return invalidResumeLabelMetadata();
  }

  const ownNames = Object.getOwnPropertyNames(value);
  if (ownNames.some(name => name !== 'stepId' && name !== 'foreachIndex')) {
    return invalidResumeLabelMetadata();
  }

  const stepIdDescriptor = Object.getOwnPropertyDescriptor(value, 'stepId');
  const foreachIndexDescriptor = Object.getOwnPropertyDescriptor(value, 'foreachIndex');
  if (
    !stepIdDescriptor ||
    !stepIdDescriptor.enumerable ||
    !('value' in stepIdDescriptor) ||
    typeof stepIdDescriptor.value !== 'string' ||
    stepIdDescriptor.value.length === 0 ||
    stepIdDescriptor.value.length > MAX_EVENTED_RESUME_LABEL_METADATA_BYTES ||
    (foreachIndexDescriptor !== undefined &&
      (!foreachIndexDescriptor.enumerable ||
        !('value' in foreachIndexDescriptor) ||
        (foreachIndexDescriptor.value !== undefined &&
          (!Number.isSafeInteger(foreachIndexDescriptor.value) || foreachIndexDescriptor.value < 0))))
  ) {
    return invalidResumeLabelMetadata();
  }

  return {
    stepId: stepIdDescriptor.value,
    ...(foreachIndexDescriptor?.value !== undefined ? { foreachIndex: foreachIndexDescriptor.value } : {}),
  };
}

function defineResumeLabel(labels: EventedResumeLabels, label: string, target: EventedResumeLabelTarget): void {
  Object.defineProperty(labels, label, {
    value: target,
    enumerable: true,
    configurable: true,
    writable: true,
  });
}

function resumeLabelMetadataBytes(labels: EventedResumeLabels) {
  return utf8Bytes(JSON.stringify(labels));
}

export function createEventedResumeLabels(): EventedResumeLabels {
  return Object.create(null) as EventedResumeLabels;
}

function addEventedResumeLabel(labels: EventedResumeLabels, label: unknown, targetValue: unknown): void {
  assertResumeLabelName(label);
  if (Object.prototype.hasOwnProperty.call(labels, label)) {
    invalidResumeLabelMetadata();
  }

  const target = normalizeResumeLabelTarget(targetValue);
  if (Object.keys(labels).length >= MAX_EVENTED_RESUME_LABEL_COUNT) {
    invalidResumeLabelMetadata();
  }

  const candidate = createEventedResumeLabels();
  for (const [existingLabel, existingTarget] of Object.entries(labels)) {
    defineResumeLabel(candidate, existingLabel, existingTarget);
  }
  defineResumeLabel(candidate, label, target);
  if (resumeLabelMetadataBytes(candidate) > MAX_EVENTED_RESUME_LABEL_METADATA_BYTES) {
    invalidResumeLabelMetadata();
  }

  defineResumeLabel(labels, label, target);
}

export function normalizeEventedResumeLabels(value: unknown): EventedResumeLabels {
  const labels = createEventedResumeLabels();
  if (value === undefined) return labels;
  if (!isDataRecord(value)) {
    return invalidResumeLabelMetadata();
  }
  if (Object.getOwnPropertySymbols(value).length > 0) {
    return invalidResumeLabelMetadata();
  }

  for (const label of Object.getOwnPropertyNames(value)) {
    const descriptor = Object.getOwnPropertyDescriptor(value, label);
    if (!descriptor?.enumerable || !('value' in descriptor)) {
      return invalidResumeLabelMetadata();
    }
    addEventedResumeLabel(labels, label, descriptor.value);
  }
  return labels;
}

export function mergeEventedResumeLabels(
  current: unknown,
  additional: unknown,
  mapTarget: (target: EventedResumeLabelTarget) => EventedResumeLabelTarget = target => target,
): EventedResumeLabels {
  const merged = normalizeEventedResumeLabels(current);
  const normalizedAdditional = normalizeEventedResumeLabels(additional);
  for (const [label, target] of Object.entries(normalizedAdditional)) {
    addEventedResumeLabel(merged, label, mapTarget(target));
  }
  return merged;
}

export function createEventedResumeLabelsForTarget(
  values: string | string[] | undefined,
  target: EventedResumeLabelTarget,
): EventedResumeLabels {
  const labels = createEventedResumeLabels();
  if (values === undefined) return labels;

  let normalizedValues: unknown[];
  if (typeof values === 'string') {
    normalizedValues = [values];
  } else {
    if (isProxy(values) || !Array.isArray(values) || Object.getPrototypeOf(values) !== Array.prototype) {
      return invalidResumeLabelMetadata();
    }

    const lengthDescriptor = Object.getOwnPropertyDescriptor(values, 'length');
    if (
      !lengthDescriptor ||
      !('value' in lengthDescriptor) ||
      !Number.isSafeInteger(lengthDescriptor.value) ||
      lengthDescriptor.value > MAX_EVENTED_RESUME_LABEL_COUNT ||
      Object.getOwnPropertySymbols(values).length > 0 ||
      Object.getOwnPropertyNames(values).length !== lengthDescriptor.value + 1
    ) {
      return invalidResumeLabelMetadata();
    }

    normalizedValues = [];
    for (let index = 0; index < lengthDescriptor.value; index++) {
      const descriptor = Object.getOwnPropertyDescriptor(values, String(index));
      if (!descriptor?.enumerable || !('value' in descriptor)) {
        return invalidResumeLabelMetadata();
      }
      normalizedValues.push(descriptor.value);
    }
  }

  for (const label of normalizedValues) {
    addEventedResumeLabel(labels, label, target);
  }
  return labels;
}
