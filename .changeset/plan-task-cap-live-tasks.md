---
'@mastra/core': patch
---

Count only live plan tasks toward the 100-task limit. Completed, cancelled and failed tasks no longer block a new plan.

Refuse any plan-task change that raises the live count above 100. This covers adds, decomposes, status updates and reparents that reopen finished tasks.

Limit each session to 1000 stored plan tasks.

Page `plan_task_check` through the whole stored plan, so the current plan stays reachable after a long history.
