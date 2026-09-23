---
'@mastra/core': minor
---

[PF-4279] Added the fenced execution-closure contract for migration and recovery: a registered table set with `state`/`fence`/`authority`/`shared-resource` roles, guarded observational-memory and `(workflow_name, run_id)` run-pair scoping, and a versioned manifest whose verifier enforces complete table coverage, row digests, session metadata, pins, and payload consistency before any import may proceed.
