---
'@mastra/core': patch
---

Harness plan tasks: the 100-task plan limit now counts only live tasks. Completed, cancelled and failed tasks no longer block a new plan, so a long thread can keep planning after it finishes earlier plans. A separate limit of 1000 stored tasks per session keeps whole-plan reads, resets and settlements bounded, and `plan_task_check` now pages through the full stored plan so the current plan stays reachable after the history.
