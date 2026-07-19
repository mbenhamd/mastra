import { TRANSIENT_EXECUTION_SYMBOL } from '@mastra/core/workflows/_constants';
import type { Client } from '@temporalio/client';
import { describe, expect, it } from 'vitest';
import { z } from 'zod';

import { createWorkflow } from './workflow';

describe('Temporal workflow createRun contract', () => {
  it('rejects transient execution inherited from a parent workflow', async () => {
    const workflow = createWorkflow(
      {
        id: 'durable-temporal-workflow',
        inputSchema: z.object({ value: z.string() }),
        outputSchema: z.object({ value: z.string() }),
      },
      { client: {} as Client, taskQueue: 'test' },
    );

    await expect(workflow.createRun({ [TRANSIENT_EXECUTION_SYMBOL]: true })).rejects.toThrow(
      'Temporal workflows cannot run inside transient workflows',
    );
  });
});
