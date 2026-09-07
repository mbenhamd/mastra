import { InMemoryDB, WorkflowsInMemory } from '@mastra/core/storage';
import { describe, it } from 'vitest';
import { expectWorkflowTerminalParentStorageContract } from './terminal-parent';
import { expectWorkflowTerminalStorageContract } from './terminalization';

describe('WorkflowsInMemory shared terminal storage contract', () => {
  function adapters() {
    const db = new InMemoryDB();
    return { primary: new WorkflowsInMemory({ db }), concurrent: new WorkflowsInMemory({ db }) };
  }

  it('preserves journal, outbox, receipt, and fencing semantics across handles', async () => {
    await expectWorkflowTerminalStorageContract({ ...adapters(), workflowName: 'terminal-contract-memory' });
  });

  it('applies a graph-bound child result and continuation once across handles', async () => {
    await expectWorkflowTerminalParentStorageContract({ ...adapters(), workflowName: 'terminal-parent-memory' });
  });
});
