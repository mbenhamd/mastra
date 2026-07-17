import type { WorkflowRunStatus } from '../types';

/** @internal Shared by the atomic patch, planner, and eventual runtime foreach reader. */
export const WORKFLOW_TERMINAL_FOREACH_STATE_KEY = 'terminalIterationStates';
/** @internal Exact child-run ownership for concurrent foreach iterations. */
export const WORKFLOW_TERMINAL_FOREACH_RUN_KEY = 'iterationRunIds';
/** @internal Per-index JSON recovery payloads for label-directed foreach resume. */
export const WORKFLOW_TERMINAL_FOREACH_SUSPEND_PAYLOAD_KEY = 'iterationSuspendPayloads';

/** @internal Structural digest. Runtime validators enforce the exact lowercase SHA-256 form. */
export type WorkflowTerminalSha256 = `sha256:${string}`;

/** @internal Only terminal child states enter parent terminal continuation. */
export type WorkflowTerminalChildStatus = 'success' | 'failed' | 'canceled';

/** @internal Per-step recovery is intentionally deferred to PF-1800. */
export type WorkflowTerminalExecutionMode = 'continuous';

/** @internal Parent states that may still accept a child result. */
export type WorkflowTerminalActiveParentStatus = Extract<WorkflowRunStatus, 'running' | 'waiting' | 'suspended'>;

/** @internal Parent states for which an already-terminal no-op is truthful. */
export type WorkflowTerminalFinalParentStatus = 'success' | 'failed' | 'canceled' | 'tripwire' | 'bailed' | 'skipped';

/** @internal Canonicalizers always return a dense isolated array. */
export type WorkflowTerminalPath = number[];

/** @internal Exact location where the retained child terminal result is applied. */
export type WorkflowTerminalResultCoordinate =
  | { kind: 'step'; stepId: string; executionPath: WorkflowTerminalPath }
  | {
      kind: 'foreach-iteration';
      stepId: string;
      containerPath: WorkflowTerminalPath;
      iterationIndex: number;
    };

/** @internal A runnable graph entry. Containers intentionally do not invent a step ID. */
export type WorkflowTerminalContainerTarget<
  TContainer extends 'parallel' | 'conditional' | 'loop' | 'foreach' = 'parallel' | 'conditional' | 'loop' | 'foreach',
> = {
  kind: 'container';
  containerType: TContainer;
  executionPath: WorkflowTerminalPath;
};

export type WorkflowTerminalRunTarget =
  | { kind: 'step'; stepId: string; executionPath: WorkflowTerminalPath }
  | {
      kind: 'entry';
      entryType: 'sleep' | 'sleepUntil';
      entryId: string;
      executionPath: WorkflowTerminalPath;
    }
  | WorkflowTerminalContainerTarget;

/** @internal An exact foreach iteration that may be started next. */
export type WorkflowTerminalForeachTarget = {
  kind: 'foreach-iteration';
  stepId: string;
  containerPath: WorkflowTerminalPath;
  iterationIndex: number;
};

/** @internal Evaluated outside storage; stored structurally so replay never reruns the callback. */
export type WorkflowTerminalLoopDecision = {
  loopType: 'dowhile' | 'dountil';
  conditionResult: boolean;
  previousIterationCount: number;
  nextIterationCount: number;
};

/**
 * @internal One immediate framework action. This is not a raw PubSub event.
 * `finish-parent` must execute parent finalization before a
 * later integration publishes `workflow.end`.
 */
export type WorkflowTerminalContinuationAction =
  | { kind: 'run-entry'; reason: 'next-step'; target: WorkflowTerminalRunTarget }
  | {
      kind: 'run-entry';
      reason: 'loop-continue';
      target: WorkflowTerminalContainerTarget<'loop'>;
      loopDecision: WorkflowTerminalLoopDecision;
    }
  | { kind: 'run-entry'; reason: 'foreach-continue'; target: WorkflowTerminalForeachTarget }
  | {
      kind: 'complete-entry';
      reason: 'loop-exit';
      target: WorkflowTerminalContainerTarget<'loop'>;
      loopDecision: WorkflowTerminalLoopDecision;
    }
  | {
      kind: 'complete-entry';
      reason: 'parallel-continue';
      target: WorkflowTerminalContainerTarget<'parallel'>;
    }
  | {
      kind: 'complete-entry';
      reason: 'conditional-continue';
      target: WorkflowTerminalContainerTarget<'conditional'>;
    }
  | {
      kind: 'complete-entry';
      reason: 'foreach-complete';
      target: WorkflowTerminalContainerTarget<'foreach'>;
    }
  | { kind: 'fail-parent'; reason: 'parent-fail' }
  | { kind: 'finish-parent'; reason: 'parent-end' }
  | { kind: 'cancel-parent'; reason: 'child-canceled' }
  | {
      kind: 'suspend-parent';
      reason: 'branch-suspended';
      target: WorkflowTerminalContainerTarget<'parallel' | 'conditional'>;
    }
  | {
      kind: 'suspend-parent';
      reason: 'foreach-suspended';
      target: WorkflowTerminalContainerTarget<'foreach'>;
    }
  | {
      kind: 'wait';
      reason: 'parallel-aggregation' | 'conditional-aggregation' | 'foreach-aggregation';
      coordinate: Extract<WorkflowTerminalRunTarget, { kind: 'container' }>;
    }
  | { kind: 'noop'; reason: 'already-terminal' }
  | {
      kind: 'quarantine';
      reason: 'graph-conflict' | 'parent-conflict-exhausted' | 'plan-conflict';
      conflictDigest: WorkflowTerminalSha256;
    };

interface WorkflowTerminalMergePatchBase {
  kind: 'merge-child-terminal';
  resultWrite: 'source-coordinate';
  resultSource: 'retained-child-terminal-envelope';
  payloadWrite: 'preserve-parent-step-payload';
  metadataWrite: 'merge-child-and-bind-nested-run-id';
  stateWrite: 'replace-context-__state-from-retained-child';
  requestContextWrite: 'merge-from-retained-child';
  activeStepsWrite: 'derive-from-source-coordinate';
  snapshotTimestampWrite: 'storage-clock';
}

export type WorkflowTerminalPreservePatch = WorkflowTerminalMergePatchBase & {
  parentRunWrite: { kind: 'preserve' };
  loopWrite: { kind: 'preserve' };
};

export type WorkflowTerminalLoopPatch = WorkflowTerminalMergePatchBase & {
  parentRunWrite: { kind: 'preserve' };
  loopWrite: { kind: 'set-iteration'; stepId: string; iterationCount: number };
};

export type WorkflowTerminalFailedPatch = WorkflowTerminalMergePatchBase & {
  parentRunWrite: {
    kind: 'set';
    status: 'failed';
    resultSource: 'source-coordinate';
    activePathSource: 'source-coordinate';
  };
  loopWrite: { kind: 'preserve' };
};

export type WorkflowTerminalFinishPatch = WorkflowTerminalMergePatchBase & {
  parentRunWrite: {
    kind: 'set';
    status: 'success' | 'canceled';
    resultSource: 'source-coordinate';
    activePathSource: 'source-coordinate';
  };
  loopWrite: { kind: 'preserve' };
};

export type WorkflowTerminalSuspendPatch = WorkflowTerminalMergePatchBase & {
  parentRunWrite: {
    kind: 'set-suspended';
    resultSource: 'aggregate-container';
    activePathSource: 'source-coordinate';
    suspendedPathsSource: 'aggregate-container';
    resumeLabelsSource: 'aggregate-container';
  };
  loopWrite: { kind: 'preserve' };
};

/** @internal Declarative parent mutation; it never carries result/state/context payloads. */
export type WorkflowTerminalParentPatch =
  | { kind: 'none' }
  | WorkflowTerminalPreservePatch
  | WorkflowTerminalLoopPatch
  | WorkflowTerminalFailedPatch
  | WorkflowTerminalFinishPatch
  | WorkflowTerminalSuspendPatch;

interface WorkflowTerminalParentContinuationBase<
  TChild extends WorkflowTerminalChildStatus,
  TParent extends WorkflowRunStatus,
> {
  version: 1;
  terminalEffectKey: string;
  terminalEffectPayloadHash: WorkflowTerminalSha256;
  executionMode: WorkflowTerminalExecutionMode;
  expectedParentRevision: string;
  graphFingerprint: WorkflowTerminalSha256;
  childTerminalStatus: TChild;
  observedParentStatus: TParent;
  source: WorkflowTerminalResultCoordinate;
}

type OrdinaryAction = Exclude<
  WorkflowTerminalContinuationAction,
  | {
      kind: 'fail-parent' | 'finish-parent' | 'cancel-parent' | 'suspend-parent' | 'noop' | 'quarantine';
    }
  | { reason: 'loop-continue' | 'loop-exit' }
>;

/** @internal Strict graph-bound semantic input before the contract hash is attached. */
export type WorkflowTerminalParentContinuationSpec =
  | (WorkflowTerminalParentContinuationBase<'success', WorkflowTerminalActiveParentStatus> & {
      action: OrdinaryAction;
      patch: WorkflowTerminalPreservePatch;
    })
  | (WorkflowTerminalParentContinuationBase<'success', WorkflowTerminalActiveParentStatus> & {
      action: Extract<WorkflowTerminalContinuationAction, { reason: 'loop-continue' | 'loop-exit' }>;
      patch: WorkflowTerminalLoopPatch;
    })
  | (WorkflowTerminalParentContinuationBase<'failed', WorkflowTerminalActiveParentStatus> & {
      action: Extract<WorkflowTerminalContinuationAction, { kind: 'fail-parent' }>;
      patch: WorkflowTerminalFailedPatch;
    })
  | (WorkflowTerminalParentContinuationBase<'success', WorkflowTerminalActiveParentStatus> & {
      action: Extract<WorkflowTerminalContinuationAction, { kind: 'finish-parent' }>;
      patch: WorkflowTerminalFinishPatch;
    })
  | (WorkflowTerminalParentContinuationBase<'canceled', WorkflowTerminalActiveParentStatus> & {
      action: Extract<WorkflowTerminalContinuationAction, { kind: 'cancel-parent' }>;
      patch: WorkflowTerminalFinishPatch;
    })
  | (WorkflowTerminalParentContinuationBase<'success', WorkflowTerminalActiveParentStatus> & {
      action: Extract<WorkflowTerminalContinuationAction, { kind: 'suspend-parent' }>;
      patch: WorkflowTerminalSuspendPatch;
    })
  | (WorkflowTerminalParentContinuationBase<WorkflowTerminalChildStatus, WorkflowTerminalFinalParentStatus> & {
      action: Extract<WorkflowTerminalContinuationAction, { kind: 'noop' }>;
      patch: { kind: 'none' };
    })
  | (WorkflowTerminalParentContinuationBase<WorkflowTerminalChildStatus, WorkflowRunStatus> & {
      action: Extract<WorkflowTerminalContinuationAction, { kind: 'quarantine' }>;
      patch: { kind: 'none' };
    });

/** @internal Immutable structural contract stored by later PF-1771 work. */
export type WorkflowTerminalParentContinuationContract = WorkflowTerminalParentContinuationSpec & {
  contractHash: WorkflowTerminalSha256;
};
