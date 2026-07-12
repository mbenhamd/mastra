import type { WorkflowTerminalSha256 } from '../terminal-continuation/types';
import type { SerializedStepFlowEntry } from '../types';

export type WorkflowTerminalRecoveryStatus = 'success' | 'failed' | 'canceled';

export type WorkflowTerminalCanonicalJsonPrimitive = null | boolean | number | string;
export type WorkflowTerminalCanonicalJsonValue =
  | WorkflowTerminalCanonicalJsonPrimitive
  | WorkflowTerminalCanonicalJsonValue[]
  | WorkflowTerminalCanonicalJsonObject;
export type WorkflowTerminalCanonicalJsonObject = {
  [key: string]: WorkflowTerminalCanonicalJsonValue;
};

export type WorkflowTerminalRecoverySource =
  | {
      kind: 'step';
      stepId: string;
      executionPath: number[];
    }
  | {
      kind: 'foreach-iteration';
      stepId: string;
      containerPath: number[];
      iterationIndex: number;
    };

export interface WorkflowTerminalRecoveryParentFrameV1 {
  version: 1;
  childWorkflowName: string;
  childRunId: string;
  parentWorkflowName: string;
  parentRunId: string;
  parentGraphFingerprint: WorkflowTerminalSha256;
  source: WorkflowTerminalRecoverySource;
  inputPointer: {
    kind: 'parent-source-payload';
    stepId: string;
  };
  resultPointer: {
    kind: 'retained-terminal-result';
    workflowName: string;
    runId: string;
  };
  resumeMetadata: {
    wasResume: boolean;
    resumeSteps: string[];
  };
}

export type WorkflowTerminalRecoveryAncestryV1 = WorkflowTerminalRecoveryParentFrameV1[];

export interface WorkflowTerminalRecoveryEnvelopeV1 {
  version: 1;
  workflowName: string;
  runId: string;
  terminalStatus: WorkflowTerminalRecoveryStatus;
  executionMode: 'continuous';
  terminalResult: WorkflowTerminalCanonicalJsonObject;
  finalState: WorkflowTerminalCanonicalJsonObject;
  requestContextPatch: WorkflowTerminalCanonicalJsonObject;
  childGraphFingerprint: WorkflowTerminalSha256;
  ancestry: WorkflowTerminalRecoveryAncestryV1;
}

/**
 * Producer-facing shape. Request context may be absent/undefined; both mean an
 * empty patch. Every other field is required and is materialized from data
 * descriptors without invoking getters or `toJSON`.
 */
export interface WorkflowTerminalRecoveryEnvelopeInputV1 {
  version: 1;
  workflowName: string;
  runId: string;
  terminalStatus: WorkflowTerminalRecoveryStatus;
  executionMode: 'continuous';
  terminalResult: unknown;
  finalState: unknown;
  requestContextPatch?: unknown;
  childGraphFingerprint: WorkflowTerminalSha256;
  ancestry: unknown;
}

export interface WorkflowTerminalRecoveryEnvelopeExpectedBinding {
  workflowName?: string;
  runId?: string;
  terminalStatus?: WorkflowTerminalRecoveryStatus;
  childGraphFingerprint?: WorkflowTerminalSha256;
  envelopeHash?: WorkflowTerminalSha256;
}

export interface WorkflowTerminalRecoveryEnvelopeRecordV1 {
  version: 1;
  envelopeHash: WorkflowTerminalSha256;
  envelope: WorkflowTerminalRecoveryEnvelopeV1;
}

export interface WorkflowTerminalRecoveryGraphBinding {
  childSerializedStepGraph: SerializedStepFlowEntry[];
  parentSerializedStepGraphs?: Array<{
    workflowName: string;
    runId: string;
    serializedStepGraph: SerializedStepFlowEntry[];
  }>;
}
