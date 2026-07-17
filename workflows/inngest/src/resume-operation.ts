import { materializeWorkflowResumeOperationHash } from '@mastra/core/storage';

export interface InngestWorkflowResumeOperation {
  workflowId: string;
  runId: string;
  resourceId?: string;
  inputData?: unknown;
  steps: string[];
  resumePayload?: unknown;
  resumePath?: number[];
  requestContext: Record<string, unknown>;
  outputOptions?: unknown;
  tracingOptions?: unknown;
  perStep: boolean;
  disableScorers?: boolean;
  format?: 'legacy' | 'vnext';
}

/** Bind every durable resume semantic while intentionally excluding `actor`. */
export function inngestWorkflowResumeOperationHash(operation: InngestWorkflowResumeOperation): `sha256:${string}` {
  return materializeWorkflowResumeOperationHash(operation);
}
