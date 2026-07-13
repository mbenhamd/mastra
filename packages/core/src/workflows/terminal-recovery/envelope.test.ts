import { describe, expect, it, vi } from 'vitest';
import { createWorkflowTerminalGraphFingerprint } from '../terminal-continuation/graph-fingerprint';
import type { SerializedStepFlowEntry } from '../types';
import {
  copyWorkflowTerminalRecoveryAncestry,
  copyWorkflowTerminalRecoveryEnvelope,
  getWorkflowTerminalRecoveryAncestryHash,
  getWorkflowTerminalRecoveryEnvelopeHash,
  materializeWorkflowTerminalRecoveryAncestry,
  materializeWorkflowTerminalRecoveryEnvelope,
  validateWorkflowTerminalRecoveryEnvelope,
  validateWorkflowTerminalRecoveryEnvelopeIntegrity,
  validateWorkflowTerminalRecoveryGraphBinding,
} from './envelope';

const childGraph: SerializedStepFlowEntry[] = [];
const parentGraph: SerializedStepFlowEntry[] = [{ type: 'step', step: { id: 'nested', component: 'WORKFLOW' } }];
const grandparentGraph: SerializedStepFlowEntry[] = [{ type: 'step', step: { id: 'parent', component: 'WORKFLOW' } }];

function frame(overrides: Record<string, unknown> = {}) {
  return {
    version: 1,
    childWorkflowName: 'child',
    childRunId: 'child-run',
    parentWorkflowName: 'parent',
    parentRunId: 'parent-run',
    parentGraphFingerprint: createWorkflowTerminalGraphFingerprint(parentGraph),
    source: { kind: 'step', stepId: 'nested', executionPath: [0] },
    inputPointer: { kind: 'parent-source-payload', stepId: 'nested' },
    resultPointer: { kind: 'retained-terminal-result', workflowName: 'child', runId: 'child-run' },
    resumeMetadata: { wasResume: false, resumeSteps: [] },
    ...overrides,
  };
}

function input(overrides: Record<string, unknown> = {}) {
  return {
    version: 1,
    workflowName: 'child',
    runId: 'child-run',
    terminalStatus: 'success',
    executionMode: 'continuous',
    terminalResult: { status: 'success', output: { answer: 42 }, startedAt: 10, endedAt: 20 },
    finalState: {},
    requestContextPatch: { tenantId: 'tenant-a' },
    childGraphFingerprint: createWorkflowTerminalGraphFingerprint(childGraph),
    ancestry: [frame()],
    ...overrides,
  };
}

describe('workflow terminal recovery envelope', () => {
  it('materializes, copies, hashes, and binds a canonical envelope', () => {
    const envelope = materializeWorkflowTerminalRecoveryEnvelope(input());
    const hash = getWorkflowTerminalRecoveryEnvelopeHash(envelope);
    expect(hash).toMatch(/^sha256:[a-f0-9]{64}$/);
    expect(copyWorkflowTerminalRecoveryEnvelope(envelope)).toEqual(envelope);
    expect(copyWorkflowTerminalRecoveryEnvelope(envelope)).not.toBe(envelope);
    expect(() =>
      validateWorkflowTerminalRecoveryEnvelope(envelope, {
        workflowName: 'child',
        runId: 'child-run',
        terminalStatus: 'success',
        envelopeHash: hash,
      }),
    ).not.toThrow();
    expect(() =>
      validateWorkflowTerminalRecoveryEnvelopeIntegrity({ version: 1, envelopeHash: hash, envelope }),
    ).not.toThrow();
    expect(() =>
      validateWorkflowTerminalRecoveryEnvelopeIntegrity(
        { version: 1, envelopeHash: hash, envelope },
        { envelopeHash: `sha256:${'0'.repeat(64)}` },
      ),
    ).toThrow(/binding mismatch/);
  });

  it('produces stable hashes independent of input key order', () => {
    const left = materializeWorkflowTerminalRecoveryEnvelope(input({ finalState: { z: 1, a: 2 } }));
    const right = materializeWorkflowTerminalRecoveryEnvelope(input({ finalState: { a: 2, z: 1 } }));
    expect(getWorkflowTerminalRecoveryEnvelopeHash(left)).toBe(getWorkflowTerminalRecoveryEnvelopeHash(right));
  });

  it('keeps the durable envelope and ancestry digest vectors stable', () => {
    const envelope = materializeWorkflowTerminalRecoveryEnvelope(input({ ancestry: [] }));
    expect(getWorkflowTerminalRecoveryEnvelopeHash(envelope)).toBe(
      'sha256:741f468a681ee5c497bb1a9ddd117b24c7c18ae0c2c9e10e25a15ce5fdd3bbe3',
    );

    const ancestry = materializeWorkflowTerminalRecoveryAncestry([frame()]);
    expect(getWorkflowTerminalRecoveryAncestryHash(ancestry)).toBe(
      'sha256:5351dc4169f4084014348159061b4ffbe076ef6e47d9b891a4f052d7c9cc3a9e',
    );
  });

  it('normalizes native failed errors before hashing', () => {
    const error = Object.assign(new Error('boom'), { code: 'E_CHILD' });
    const envelope = materializeWorkflowTerminalRecoveryEnvelope(
      input({ terminalStatus: 'failed', terminalResult: { status: 'failed', error } }),
    );
    expect(envelope.terminalResult.error).toMatchObject({ name: 'Error', message: 'boom', code: 'E_CHILD' });
    expect(getWorkflowTerminalRecoveryEnvelopeHash(envelope)).toMatch(/^sha256:/);
  });

  it('enforces terminal result and request-context policy', () => {
    expect(() =>
      materializeWorkflowTerminalRecoveryEnvelope(
        input({ terminalStatus: 'failed', terminalResult: { status: 'failed' } }),
      ),
    ).toThrow(/requires an error/);
    for (const error of ['failure', { message: 'missing name' }, { name: 'Error', message: 'failure', stack: 42 }]) {
      expect(() =>
        materializeWorkflowTerminalRecoveryEnvelope(
          input({ terminalStatus: 'failed', terminalResult: { status: 'failed', error } }),
        ),
      ).toThrow(/serialized error|string name and message|must be a string/);
    }
    expect(() =>
      materializeWorkflowTerminalRecoveryEnvelope(
        input({ terminalResult: { status: 'success', error: { message: 'contradiction' } } }),
      ),
    ).toThrow(/non-failed result/);
    expect(() =>
      materializeWorkflowTerminalRecoveryEnvelope(input({ requestContextPatch: { mastra__authToken: 'secret' } })),
    ).toThrow(/framework credential/);
    expect(
      materializeWorkflowTerminalRecoveryEnvelope(input({ requestContextPatch: undefined })).requestContextPatch,
    ).toEqual({});
    expect(() => materializeWorkflowTerminalRecoveryEnvelope(input({ requestContextPatch: null }))).toThrow(
      /must be an object/,
    );
  });

  it('validates ancestry continuity, identity pointers, and cycles', () => {
    const parentFrame = frame();
    const rootFrame = frame({
      childWorkflowName: 'parent',
      childRunId: 'parent-run',
      parentWorkflowName: 'root',
      parentRunId: 'root-run',
      parentGraphFingerprint: createWorkflowTerminalGraphFingerprint(grandparentGraph),
      source: { kind: 'step', stepId: 'parent', executionPath: [0] },
      inputPointer: { kind: 'parent-source-payload', stepId: 'parent' },
      resultPointer: { kind: 'retained-terminal-result', workflowName: 'parent', runId: 'parent-run' },
    });
    const ancestry = materializeWorkflowTerminalRecoveryAncestry([parentFrame, rootFrame]);
    expect(copyWorkflowTerminalRecoveryAncestry(ancestry)).toEqual(ancestry);
    expect(getWorkflowTerminalRecoveryAncestryHash(ancestry)).toMatch(/^sha256:/);

    expect(() =>
      materializeWorkflowTerminalRecoveryAncestry([
        parentFrame,
        {
          ...rootFrame,
          childRunId: 'wrong',
          resultPointer: { kind: 'retained-terminal-result', workflowName: 'parent', runId: 'wrong' },
        },
      ]),
    ).toThrow(/not continuous/);
    expect(() =>
      materializeWorkflowTerminalRecoveryAncestry([
        parentFrame,
        {
          ...rootFrame,
          parentWorkflowName: 'child',
          parentRunId: 'child-run',
        },
      ]),
    ).toThrow(/cycle/);
    expect(() =>
      materializeWorkflowTerminalRecoveryAncestry([
        frame({ inputPointer: { kind: 'parent-source-payload', stepId: 'wrong' } }),
      ]),
    ).toThrow(/does not match source/);
  });

  it('rejects accessors without executing them', () => {
    const getter = vi.fn(() => 'child');
    const hostile = frame();
    Object.defineProperty(hostile, 'childWorkflowName', { enumerable: true, get: getter });
    expect(() => materializeWorkflowTerminalRecoveryAncestry([hostile])).toThrow(/data fields/);
    expect(getter).not.toHaveBeenCalled();
  });

  it('binds child and parent fingerprints and resolves the parent source', () => {
    const envelope = materializeWorkflowTerminalRecoveryEnvelope(input());
    expect(() =>
      validateWorkflowTerminalRecoveryGraphBinding(envelope, {
        childSerializedStepGraph: childGraph,
      }),
    ).not.toThrow();
    expect(() =>
      validateWorkflowTerminalRecoveryGraphBinding(envelope, {
        childSerializedStepGraph: childGraph,
        parentSerializedStepGraphs: [{ workflowName: 'parent', runId: 'parent-run', serializedStepGraph: parentGraph }],
      }),
    ).not.toThrow();
    expect(() =>
      validateWorkflowTerminalRecoveryGraphBinding(envelope, {
        childSerializedStepGraph: [{ type: 'step', step: { id: 'different' } }],
        parentSerializedStepGraphs: [{ workflowName: 'parent', runId: 'parent-run', serializedStepGraph: parentGraph }],
      }),
    ).toThrow(/child graph/);

    const wrongSource = materializeWorkflowTerminalRecoveryEnvelope(
      input({
        ancestry: [
          frame({
            source: { kind: 'step', stepId: 'wrong', executionPath: [0] },
            inputPointer: { kind: 'parent-source-payload', stepId: 'wrong' },
          }),
        ],
      }),
    );
    expect(() =>
      validateWorkflowTerminalRecoveryGraphBinding(wrongSource, {
        childSerializedStepGraph: childGraph,
        parentSerializedStepGraphs: [{ workflowName: 'parent', runId: 'parent-run', serializedStepGraph: parentGraph }],
      }),
    ).toThrow(/does not resolve/);

    const getter = vi.fn(() => childGraph);
    const hostileBinding = {};
    Object.defineProperty(hostileBinding, 'childSerializedStepGraph', { enumerable: true, get: getter });
    expect(() => validateWorkflowTerminalRecoveryGraphBinding(envelope, hostileBinding as never)).toThrow(
      /data fields/,
    );
    expect(getter).not.toHaveBeenCalled();
  });

  it('detects retained-envelope mutation through the canonical hash', () => {
    const envelope = materializeWorkflowTerminalRecoveryEnvelope(input());
    const envelopeHash = getWorkflowTerminalRecoveryEnvelopeHash(envelope);
    envelope.finalState.changed = true;
    expect(() => validateWorkflowTerminalRecoveryEnvelopeIntegrity({ version: 1, envelopeHash, envelope })).toThrow(
      /integrity mismatch/,
    );
  });
});
