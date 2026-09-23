---
'@mastra/pg': minor
---

[PF-4279] Added `exportExecutionClosure`/`importExecutionClosure` for fenced session migration and recovery: exports the harness session subtree, memory/OM, workflow snapshot and terminal lineage, and fence/authority evidence under one snapshot; import verifies the manifest, allocates or adopts the persisted destination session incarnations, rebinds fence rows, clears lease and claim authority, materializes workflow handoff evidence, restages projection intents, rebuilds pressure counters, and fails closed on tampered, incomplete, or conflicting payloads.
