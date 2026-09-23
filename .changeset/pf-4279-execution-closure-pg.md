---
'@mastra/pg': minor
---

Added `exportExecutionClosure`/`importExecutionClosure` for fenced session migration and recovery: exports the harness session subtree, memory/OM, workflow snapshot and terminal lineage, and fence/authority evidence under one snapshot; import verifies the manifest, allocates or adopts the persisted destination session incarnations, rebinds fence rows, clears lease and claim authority, materializes workflow handoff evidence, restages projection intents, rebuilds pressure counters, and fails closed on tampered, incomplete, or conflicting payloads.

```ts
import { PostgresStore } from '@mastra/pg';

const source = new PostgresStore({ id: 'source', connectionString: sourceDsn });
const destination = new PostgresStore({ id: 'destination', connectionString: destinationDsn });

// Export a session subtree under one REPEATABLE READ snapshot.
const closure = await source.exportExecutionClosure({
  harnessName: 'default',
  sessionId: 'sess-123',
});
// closure.manifest.completeness: 'complete' | 'pinned'

// Stage it into the destination. Import is idempotent: an exact retry
// replays the persisted destination session incarnations, and a conflicting
// destination row fails closed instead of merging.
const result = await destination.importExecutionClosure(closure);
// result.incarnations: sessionId -> destination session incarnation token
```
