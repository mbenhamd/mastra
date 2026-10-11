---
'@mastra/core': patch
---

Harness v1 no longer tells the model to call `plan_task_check` on a turn where it cannot. The durable plan summary is now omitted when the turn's permission policy denies `plan_task_check` or the mode excludes the Harness built-in tools.
