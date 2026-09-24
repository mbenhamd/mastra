---
'@mastra/core': minor
---

Added the fenced execution-closure contract for migration and recovery: a registered table set with `state`/`fence`/`authority`/`shared-resource` roles, guarded observational-memory and `(workflow_name, run_id)` run-pair scoping, and a versioned manifest whose verifier enforces complete table coverage, row digests, session metadata, pins, and payload consistency before any import may proceed.

```ts
import { verifyExecutionClosurePayload } from '@mastra/core/storage';

// Verify an exported payload before staging it into a destination store.
const result = verifyExecutionClosurePayload(payload.manifest, payload.rows);
if (!result.ok) {
  // Corrupt, incomplete, or hand-built payloads fail closed with a reason list.
  throw new Error(`execution closure rejected: ${result.mismatches.join('; ')}`);
}
```
