import type { WorkflowRunState, WorkflowTerminalRecoveryAncestryV1 } from '../../../workflows';
import { createWorkflowTerminalGraphFingerprint } from '../../../workflows/terminal-continuation';

export function createTerminalRecoveryEnvelope({
  workflowName,
  runId,
  snapshot,
  terminalStatus,
  ancestry = [],
  terminalResult,
}: {
  workflowName: string;
  runId: string;
  snapshot: WorkflowRunState;
  terminalStatus: 'success' | 'failed' | 'canceled';
  ancestry?: WorkflowTerminalRecoveryAncestryV1;
  terminalResult?: unknown;
}) {
  return {
    version: 1 as const,
    workflowName,
    runId,
    terminalStatus,
    executionMode: 'continuous' as const,
    terminalResult:
      terminalResult ??
      (terminalStatus === 'failed'
        ? { status: terminalStatus, error: 'terminal test failure' }
        : { status: terminalStatus }),
    finalState: (snapshot.context?.__state as Record<string, unknown> | undefined) ?? snapshot.value ?? {},
    requestContextPatch: snapshot.requestContext ?? {},
    childGraphFingerprint: createWorkflowTerminalGraphFingerprint(snapshot.serializedStepGraph),
    ancestry,
  };
}
