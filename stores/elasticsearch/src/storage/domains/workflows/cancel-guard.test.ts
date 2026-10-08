import { describe, expect, it } from 'vitest';

import { WorkflowsElasticSearch } from './index';

describe('WorkflowsElasticSearch cancellation request guards', () => {
  it.each([
    null,
    {
      version: 1 as const,
      requestId: 'abort-1',
      executionGeneration: 'generation-1',
      lifecycleResumeAttempt: 0,
      requestedAt: 1,
    },
  ])('rejects an unsupported request guard before adapter access (%s)', async expectedCancelRequest => {
    // No constructor or transport is installed: touching adapter state before
    // refusing this unsupported public guard would fail this assertion.
    const store = Object.create(WorkflowsElasticSearch.prototype) as WorkflowsElasticSearch;
    await expect(
      store.updateWorkflowState({
        workflowName: 'workflow-a',
        runId: 'run-1',
        opts: { status: 'canceled', expectedCancelRequest },
      }),
    ).rejects.toThrow('does not support expectedCancelRequest guards');
  });
});
