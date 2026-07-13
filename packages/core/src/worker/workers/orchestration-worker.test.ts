import { describe, expect, it, vi } from 'vitest';

import type { WorkerDeps } from '../worker';
import { OrchestrationWorker } from './orchestration-worker';

function createDeps() {
  const pubsub = {
    publish: vi.fn().mockResolvedValue(undefined),
    subscribe: vi.fn().mockResolvedValue(undefined),
    unsubscribe: vi.fn().mockResolvedValue(undefined),
    flush: vi.fn().mockResolvedValue(undefined),
  };
  const logger = {
    debug: vi.fn(),
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    trackException: vi.fn(),
  };
  const deps = {
    pubsub,
    storage: {},
    logger,
    mastra: {
      getWorkflow: vi.fn(),
      getLogger: vi.fn().mockReturnValue(logger),
      __shouldProcessWorkflowEvent: vi.fn().mockReturnValue(false),
    },
  } as unknown as WorkerDeps;

  return { deps, pubsub, logger };
}

describe('OrchestrationWorker foreign workflow ownership', () => {
  it('logs and absorbs acknowledgement failures', async () => {
    const { deps, pubsub, logger } = createDeps();
    const worker = new OrchestrationWorker();
    await worker.init(deps);
    await worker.start();

    const route = pubsub.subscribe.mock.calls[0]![1];
    const ackError = new Error('ack failed');
    const ack = vi.fn().mockRejectedValue(ackError);
    const nack = vi.fn();
    route(
      {
        type: 'workflow.start',
        runId: 'foreign-run',
        data: { workflowId: 'foreign-workflow', runId: 'foreign-run' },
      },
      ack,
      nack,
    );

    await vi.waitFor(() => {
      expect(logger.error).toHaveBeenCalledWith('OrchestrationWorker: error acking foreign event', {
        error: ackError,
      });
    });
    expect(nack).not.toHaveBeenCalled();
  });
});
