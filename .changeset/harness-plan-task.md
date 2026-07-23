---
'@mastra/core': minor
'@mastra/pg': minor
'@mastra/libsql': minor
---

Added HarnessPlanTask — a durable, arbitrary-depth, model-authored task tree for Harness v1. Agents can now build and continuously reshape a plan across long-horizon runs using the built-in `task_add`, `task_decompose`, `task_reparent`, `task_update`, `task_complete`, and `plan_task_check` tools. The tree is persisted (InMemory, Postgres, and LibSQL), survives restarts, and supports parent/child hierarchy, dependency edges (`blockedBy`), automatic parent-status rollup, cycle prevention, and a bounded "focused subtree" read so the model can stay oriented without reloading the whole tree.

Plan changes surface to UIs through the `papersflow.plan_task.updated` custom event (with incremental deltas) and a bounded `planTasks` summary on the session display state.

A subtree can also be delegated to a subagent: `task_delegate` hands a task (optionally with its subtree) to a child agent session, links it durably, and rolls the task up to completed/failed when the subagent finishes — recovering correctly across process restarts.
