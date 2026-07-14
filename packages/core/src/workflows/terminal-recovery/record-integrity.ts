import { Buffer } from 'node:buffer';
import { createHash } from 'node:crypto';
import { validateWorkflowTerminalStructuralString } from '../terminal-continuation/graph-fingerprint';
import type { WorkflowTerminalEffectRecord, WorkflowTerminalSnapshotRecord } from '../types';

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

/** @internal Authenticates a retained terminal record, including explicit resource presence. */
export function getWorkflowTerminalSnapshotRecordHash(
  retained: Pick<
    WorkflowTerminalSnapshotRecord,
    'version' | 'workflowName' | 'runId' | 'resourceId' | 'terminalStatus' | 'envelopeHash' | 'createdAt'
  >,
): `sha256:${string}` {
  if (retained.version !== 1) throw new TypeError('Invalid workflow terminal snapshot version');
  validateWorkflowTerminalStructuralString(retained.workflowName, 'workflowName', 512);
  validateWorkflowTerminalStructuralString(retained.runId, 'runId', 512);
  if (retained.resourceId !== undefined) {
    validateWorkflowTerminalStructuralString(retained.resourceId, 'resourceId', 512);
  }
  if (!['success', 'failed', 'canceled'].includes(retained.terminalStatus)) {
    throw new TypeError('Invalid workflow terminal snapshot status');
  }
  if (!/^sha256:[a-f0-9]{64}$/.test(retained.envelopeHash)) {
    throw new TypeError('Invalid workflow terminal recovery envelope hash');
  }
  if (!Number.isSafeInteger(retained.createdAt) || retained.createdAt < 0) {
    throw new TypeError('Invalid workflow terminal snapshot createdAt');
  }
  const resourceParts =
    retained.resourceId === undefined ? ['resource-id-absent'] : ['resource-id-present', retained.resourceId];
  return `sha256:${hashFramedParts('mastra.workflow-terminal-snapshot-record.v1', [
    String(retained.version),
    retained.workflowName,
    retained.runId,
    retained.terminalStatus,
    retained.envelopeHash,
    String(retained.createdAt),
    ...resourceParts,
  ])}`;
}

/** @internal Fails closed when any authenticated retained-record field was altered. */
export function validateWorkflowTerminalSnapshotRecordIntegrity(retained: WorkflowTerminalSnapshotRecord): void {
  if (retained.recordHash !== getWorkflowTerminalSnapshotRecordHash(retained)) {
    throw new TypeError('Invalid workflow terminal snapshot record integrity');
  }
}

/** @internal Binds a structural producer intent to its authenticated retained payload and ancestry source. */
export function validateWorkflowTerminalEffectRecoveryLink(
  effect: WorkflowTerminalEffectRecord,
  retained: WorkflowTerminalSnapshotRecord,
): void {
  if (
    effect.workflowName !== retained.workflowName ||
    effect.runId !== retained.runId ||
    effect.terminalStatus !== retained.terminalStatus ||
    effect.recoveryEnvelopeHash !== retained.envelopeHash ||
    effect.retainedRecordHash !== retained.recordHash ||
    effect.resourceId !== retained.resourceId
  ) {
    throw new TypeError('Invalid workflow terminal effect recovery link');
  }

  const immediate = retained.envelope.ancestry[0];
  if (effect.kind === 'workflow-finish') {
    if (immediate) throw new TypeError('Invalid workflow terminal effect recovery link');
    return;
  }
  if (!immediate) throw new TypeError('Invalid workflow terminal effect recovery link');
  const sourcePath =
    immediate.source.kind === 'step'
      ? immediate.source.executionPath
      : [...immediate.source.containerPath, immediate.source.iterationIndex];
  if (
    immediate.childWorkflowName !== effect.workflowName ||
    immediate.childRunId !== effect.runId ||
    immediate.parentWorkflowName !== effect.parentWorkflowName ||
    immediate.parentRunId !== effect.parentRunId ||
    immediate.source.stepId !== effect.parentStepId ||
    sourcePath.length !== effect.parentExecutionPath.length ||
    sourcePath.some((entry, index) => entry !== effect.parentExecutionPath[index])
  ) {
    throw new TypeError('Invalid workflow terminal effect recovery link');
  }
}
