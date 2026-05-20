---
'@mastra/core': patch
---

Added Harness desktop workspace policy evaluation for desktop hosts and other constrained executors.

Apps can now enforce file, command, network, and MCP access rules before starting work. The evaluator resolves file paths and command working directories against declared workspace roots, rejects path traversal and read-only root writes, applies deterministic `deny > ask > allow` precedence, and returns the matched rules and resolved paths for audit or approval UI.

```ts
import { evaluateWorkspacePolicy } from '@mastra/core/harness/v1';

const result = evaluateWorkspacePolicy(
  {
    roots: [{ id: 'project', path: '/workspace/project', writable: true }],
    defaultDecision: 'deny',
    rules: [{ kind: 'file', rootId: 'project', operation: 'write', decision: 'ask' }],
  },
  { kind: 'file', operation: 'write', path: 'src/index.ts', rootId: 'project' },
);

if (result.decision === 'deny') {
  throw new Error(result.reasons.join(', '));
}
```
