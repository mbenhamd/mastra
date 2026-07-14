import type { WorkflowRunState, WorkflowTerminalEffectRecord } from '../types';
import type { WorkflowTerminalParentContinuationContract, WorkflowTerminalSha256 } from './types';

/** @internal Structural authority accepted by the pure parent continuation planner. */
export interface WorkflowTerminalParentPlannerInputV1 {
  version: 1;
  effect: Extract<WorkflowTerminalEffectRecord, { kind: 'parent-workflow-step-end' }>;
  parentRevision: string;
  parentSnapshot: WorkflowRunState;
  evaluatedDecision?: WorkflowTerminalEvaluatedLoopDecisionV1;
}

/** @internal Exact callback request derived from one locked parent revision. */
export interface WorkflowTerminalLoopDecisionRequestV1 {
  version: 1;
  kind: 'loop-condition';
  decisionKey: WorkflowTerminalSha256;
  loopType: 'dowhile' | 'dountil';
  previousIterationCount: number;
}

/** @internal Data-only callback result; the planner recomputes and verifies its decision key. */
export interface WorkflowTerminalEvaluatedLoopDecisionV1 {
  version: 1;
  kind: 'loop-condition';
  decisionKey: WorkflowTerminalSha256;
  conditionResult: boolean;
}

export type WorkflowTerminalParentPlannerResult = WorkflowTerminalParentContinuationContract;
