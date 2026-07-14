import { createWorkflowTerminalGraphFingerprint } from '@mastra/core/storage';
import type { WorkflowRunState, WorkflowTerminalRecoveryAncestryV1 } from '@mastra/core/workflows';

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
        ? { status: terminalStatus, error: { name: 'Error', message: 'terminal test failure' } }
        : { status: terminalStatus }),
    finalState: (snapshot.context?.__state as Record<string, unknown> | undefined) ?? snapshot.value ?? {},
    requestContextPatch: snapshot.requestContext ?? {},
    childGraphFingerprint: createWorkflowTerminalGraphFingerprint(snapshot.serializedStepGraph),
    ancestry,
  };
}
