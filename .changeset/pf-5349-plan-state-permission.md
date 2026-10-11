---
'@mastra/core': patch
---

Fixed Harness v1 telling the model to call `plan_task_check` when it cannot. The model no longer receives the durable plan summary on a turn where the permission policy denies `plan_task_check` or the mode excludes the Harness built-in tools.
