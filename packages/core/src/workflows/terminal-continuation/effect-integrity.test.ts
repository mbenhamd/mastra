import { describe, expect, it, vi } from 'vitest';
import { validateWorkflowTerminalEffectIntegrity as validateLegacyStorageExport } from '../../storage/domains/workflows/terminalization';
import type { WorkflowTerminalEffectRecord } from '../types';
import { getWorkflowTerminalEffectIntegrity, validateWorkflowTerminalEffectIntegrity } from './effect-integrity';

const RECOVERY_ENVELOPE_HASH = `sha256:${'1'.repeat(64)}` as const;

function effect(): Extract<WorkflowTerminalEffectRecord, { kind: 'parent-workflow-step-end' }> {
  const identity = {
    version: 1 as const,
    workflowName: 'child',
    runId: 'child-run',
    sourceEventKey: 'event-1',
    kind: 'parent-workflow-step-end' as const,
    terminalStatus: 'success' as const,
    recoveryEnvelopeHash: RECOVERY_ENVELOPE_HASH,
    parentWorkflowName: 'parent',
    parentRunId: 'parent-run',
    parentStepId: 'nested',
    parentExecutionPath: [0],
  };
  return { ...identity, ...getWorkflowTerminalEffectIntegrity(identity), createdAt: 1 };
}

describe('workflow terminal effect integrity', () => {
  it('accepts its canonical framed identity and rejects syntactically valid substitutions', () => {
    const canonical = effect();
    expect(() => validateWorkflowTerminalEffectIntegrity(canonical)).not.toThrow();
    expect(() => validateWorkflowTerminalEffectIntegrity({ ...canonical, parentRunId: 'different-parent' })).toThrow(
      /integrity/,
    );
    expect(() => validateWorkflowTerminalEffectIntegrity({ ...canonical, terminalStatus: 'failed' })).toThrow(
      /integrity/,
    );
    expect(() =>
      validateWorkflowTerminalEffectIntegrity({ ...canonical, effectKey: `wte:v1:${'0'.repeat(64)}` }),
    ).toThrow(/integrity/);
    expect(() =>
      validateWorkflowTerminalEffectIntegrity({ ...canonical, payloadHash: `sha256:${'0'.repeat(64)}` }),
    ).toThrow(/integrity/);
  });

  it('frames paths and identifiers without concatenation ambiguity', () => {
    const canonical = effect();
    const changedPath = { ...canonical, parentExecutionPath: [0, 1] };
    const changedId = { ...canonical, parentStepId: 'nested:0' };
    expect(getWorkflowTerminalEffectIntegrity(changedPath)).not.toEqual(getWorkflowTerminalEffectIntegrity(canonical));
    expect(getWorkflowTerminalEffectIntegrity(changedId)).not.toEqual(getWorkflowTerminalEffectIntegrity(canonical));
  });

  it('preserves the pre-extraction persisted identity vectors for both effect kinds', () => {
    const root = {
      version: 1 as const,
      kind: 'workflow-finish' as const,
      workflowName: 'root-β',
      runId: 'run:1',
      sourceEventKey: 'evt/✓',
      terminalStatus: 'failed' as const,
      recoveryEnvelopeHash: RECOVERY_ENVELOPE_HASH,
      createdAt: 7,
      effectKey: 'wte:v1:3596b803a4c8fd49eec6d4b43851ce46bf8c27d86cf7f02263c3aab3b3f5e705',
      payloadHash: 'sha256:6e30dabb6e83ba46780d40df60146c6ddcc0b6458f1f0b1f6bff1f54770aff70',
    };
    const parent = {
      version: 1 as const,
      kind: 'parent-workflow-step-end' as const,
      workflowName: 'child-β',
      runId: 'run:2',
      sourceEventKey: 'evt/✓',
      terminalStatus: 'success' as const,
      recoveryEnvelopeHash: RECOVERY_ENVELOPE_HASH,
      parentWorkflowName: 'parent-δ',
      parentRunId: 'parent:1',
      parentStepId: 'nested-λ',
      parentExecutionPath: [12, 3],
      createdAt: 8,
      effectKey: 'wte:v1:a2259834ba6e16c819189443c3eb7061a81fa56ca03ec58d5197b5f09d961034',
      payloadHash: 'sha256:0238cfee8559a4fbb56b34e6ad2b9905888aae53d57d0456bf3fd87a17bb3fab',
    };
    expect(getWorkflowTerminalEffectIntegrity(root)).toEqual({
      effectKey: root.effectKey,
      payloadHash: root.payloadHash,
    });
    expect(getWorkflowTerminalEffectIntegrity(parent)).toEqual({
      effectKey: parent.effectKey,
      payloadHash: parent.payloadHash,
    });
    expect(() => validateWorkflowTerminalEffectIntegrity(root)).not.toThrow();
    expect(() => validateLegacyStorageExport(root)).not.toThrow();
    expect(() => validateWorkflowTerminalEffectIntegrity(parent)).not.toThrow();
    expect(() => validateLegacyStorageExport(parent)).not.toThrow();
  });

  it('rejects accessors, exotic prototypes, symbols, sparse paths, and negative zero without executing code', () => {
    const getter = vi.fn(() => 'different-parent');
    const hostile = { ...effect() } as Record<string, unknown>;
    Object.defineProperty(hostile, 'parentRunId', { enumerable: true, get: getter });
    expect(() => validateWorkflowTerminalEffectIntegrity(hostile)).toThrow(/accessor/);
    expect(getter).not.toHaveBeenCalled();

    const customMap = [...effect().parentExecutionPath] as number[] & { map: () => never };
    customMap.map = vi.fn(() => {
      throw new Error('must not execute');
    });
    expect(() => getWorkflowTerminalEffectIntegrity({ ...effect(), parentExecutionPath: customMap })).toThrow(
      /dense and data-only/,
    );
    expect(customMap.map).not.toHaveBeenCalled();

    const sparse = Array(2);
    sparse[1] = 0;
    expect(() => getWorkflowTerminalEffectIntegrity({ ...effect(), parentExecutionPath: sparse })).toThrow(/dense/);
    expect(() => getWorkflowTerminalEffectIntegrity({ ...effect(), parentExecutionPath: [-0] })).toThrow(
      /invalid index/,
    );
    expect(() => getWorkflowTerminalEffectIntegrity(Object.assign(new (class Effect {})(), effect()))).toThrow(
      /plain data object/,
    );
    const symbol = { ...effect(), [Symbol('hidden')]: true };
    expect(() => getWorkflowTerminalEffectIntegrity(symbol)).toThrow(/symbol/);
    expect(() => getWorkflowTerminalEffectIntegrity({ ...effect(), unknown: true })).toThrow(/unknown/);
  });
});
