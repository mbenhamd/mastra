import { Buffer } from 'node:buffer';
import { createHash } from 'node:crypto';
import type { WorkflowTerminalEffectRecord } from '../types';
import { getDenseDataArray, getPlainDataDescriptors } from './data-shape';
import { MAX_TERMINAL_PATH_LENGTH, validateWorkflowTerminalStructuralString } from './graph-fingerprint';

type IntegrityInput =
  | Omit<
      Extract<WorkflowTerminalEffectRecord, { kind: 'parent-workflow-step-end' }>,
      'effectKey' | 'payloadHash' | 'createdAt'
    >
  | Omit<Extract<WorkflowTerminalEffectRecord, { kind: 'workflow-finish' }>, 'effectKey' | 'payloadHash' | 'createdAt'>;

function dataDescriptors(value: unknown): Record<string, PropertyDescriptor> {
  return getPlainDataDescriptors(value, {
    allowNullPrototype: true,
    typeError: 'workflow terminal effect must be a plain data object',
    fieldsError: 'workflow terminal effect contains symbol or accessor fields',
    maxKeys: 13,
    maxKeysError: 'workflow terminal effect contains unknown, missing, symbol, or accessor fields',
  });
}

function canonicalPath(value: unknown): number[] {
  const entries = getDenseDataArray(value, {
    typeError: 'parentExecutionPath must be a dense data-only path',
    lengthError: 'parentExecutionPath has an invalid length',
    dataError: 'parentExecutionPath must be dense and data-only',
    minLength: 1,
    maxLength: MAX_TERMINAL_PATH_LENGTH,
  });
  return entries.map(entry => {
    if (!Number.isSafeInteger(entry) || (entry as number) < 0 || Object.is(entry, -0)) {
      throw new TypeError('parentExecutionPath contains an invalid index');
    }
    return entry as number;
  });
}

function materializeIntegrityInput(value: unknown): IntegrityInput {
  const descriptors = dataDescriptors(value);
  const kind = descriptors.kind?.value;
  const parent = kind === 'parent-workflow-step-end';
  if (!parent && kind !== 'workflow-finish') throw new TypeError('workflow terminal effect kind is invalid');
  const allowed = new Set([
    'version',
    'effectKey',
    'kind',
    'workflowName',
    'runId',
    'sourceEventKey',
    'terminalStatus',
    'recoveryEnvelopeHash',
    'payloadHash',
    'createdAt',
    ...(parent ? ['parentWorkflowName', 'parentRunId', 'parentStepId', 'parentExecutionPath'] : []),
  ]);
  const required = [
    'version',
    'kind',
    'workflowName',
    'runId',
    'sourceEventKey',
    'terminalStatus',
    'recoveryEnvelopeHash',
    ...(parent ? ['parentWorkflowName', 'parentRunId', 'parentStepId', 'parentExecutionPath'] : []),
  ];
  if (
    Object.keys(descriptors).some(key => !allowed.has(key)) ||
    required.some(key => !Object.prototype.hasOwnProperty.call(descriptors, key))
  ) {
    throw new TypeError('workflow terminal effect contains unknown or missing fields');
  }
  if (descriptors.version!.value !== 1) throw new TypeError('workflow terminal effect version must be 1');
  const terminalStatus = descriptors.terminalStatus!.value;
  if (terminalStatus !== 'success' && terminalStatus !== 'failed' && terminalStatus !== 'canceled') {
    throw new TypeError('workflow terminal effect status is invalid');
  }
  const common = {
    version: 1 as const,
    kind,
    workflowName: validateWorkflowTerminalStructuralString(
      descriptors.workflowName!.value,
      'workflow terminal effect workflowName',
    ),
    runId: validateWorkflowTerminalStructuralString(descriptors.runId!.value, 'workflow terminal effect runId'),
    sourceEventKey: validateWorkflowTerminalStructuralString(
      descriptors.sourceEventKey!.value,
      'workflow terminal effect sourceEventKey',
      1_024,
    ),
    terminalStatus,
    recoveryEnvelopeHash: validateWorkflowTerminalStructuralString(
      descriptors.recoveryEnvelopeHash!.value,
      'workflow terminal effect recoveryEnvelopeHash',
      256,
    ) as `sha256:${string}`,
  };
  if (!/^sha256:[a-f0-9]{64}$/.test(common.recoveryEnvelopeHash)) {
    throw new TypeError('workflow terminal effect recovery envelope hash is invalid');
  }
  if (!parent) return common as IntegrityInput;
  return {
    ...common,
    kind,
    parentWorkflowName: validateWorkflowTerminalStructuralString(
      descriptors.parentWorkflowName!.value,
      'workflow terminal effect parentWorkflowName',
    ),
    parentRunId: validateWorkflowTerminalStructuralString(
      descriptors.parentRunId!.value,
      'workflow terminal effect parentRunId',
    ),
    parentStepId: validateWorkflowTerminalStructuralString(
      descriptors.parentStepId!.value,
      'workflow terminal effect parentStepId',
    ),
    parentExecutionPath: canonicalPath(descriptors.parentExecutionPath!.value),
  } as IntegrityInput;
}

function hashFramedParts(domain: string, parts: readonly string[]): string {
  const hash = createHash('sha256');
  for (const part of [domain, ...parts]) {
    const bytes = Buffer.from(part, 'utf8');
    const length = Buffer.allocUnsafe(4);
    length.writeUInt32BE(bytes.length);
    hash.update(length);
    hash.update(bytes);
  }
  return hash.digest('hex');
}

/** @internal Recomputes the canonical identity and payload digests for terminal evidence. */
export function getWorkflowTerminalEffectIntegrity(input: unknown): { effectKey: string; payloadHash: string } {
  const effect = materializeIntegrityInput(input);
  const destinationParts =
    effect.kind === 'parent-workflow-step-end'
      ? [
          effect.parentWorkflowName,
          effect.parentRunId,
          effect.parentStepId,
          String(effect.parentExecutionPath.length),
          ...effect.parentExecutionPath.map(String),
        ]
      : [effect.workflowName, effect.runId];
  const identityParts = [
    String(effect.version),
    effect.workflowName,
    effect.runId,
    effect.sourceEventKey,
    effect.kind,
    ...destinationParts,
  ];
  const payloadParts = [...identityParts, effect.terminalStatus, effect.recoveryEnvelopeHash];
  return {
    effectKey: `wte:v1:${hashFramedParts('mastra.workflow-terminal-effect.identity.v1', identityParts)}`,
    payloadHash: `sha256:${hashFramedParts('mastra.workflow-terminal-effect.payload.v1', payloadParts)}`,
  };
}

/** @internal Fails closed when persisted terminal evidence does not match its framed identity. */
export function validateWorkflowTerminalEffectIntegrity(effect: unknown): void {
  const descriptors = dataDescriptors(effect);
  const expected = getWorkflowTerminalEffectIntegrity(effect);
  const effectKey = descriptors.effectKey?.value;
  const payloadHash = descriptors.payloadHash?.value;
  if (
    typeof effectKey !== 'string' ||
    typeof payloadHash !== 'string' ||
    effectKey !== expected.effectKey ||
    payloadHash !== expected.payloadHash
  ) {
    throw new TypeError('Invalid workflow terminal effect integrity');
  }
}
