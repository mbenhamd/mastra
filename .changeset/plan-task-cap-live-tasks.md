---
'@mastra/core': patch
---

Fixed the 100-task plan limit counting finished tasks. Only live tasks (not completed, cancelled or failed) count now, so a long session can start a new plan after spending more than 100 tasks.

Changed every plan-task change (add, decompose, status update, reparent and delegate) to be refused when it would raise the live count above 100. A change that reopens finished tasks counts them as live. A delegation also reserves, as live, every task its settlement could reopen, so a settling child result never pushes the plan over the limit.

Added a limit of 1000 stored plan tasks per session. At that limit, adding more tasks is refused until the application resets the plan with `Session.resetPlanTasks()`; finishing existing tasks still works.

Fixed `plan_task_check` paging so it reads the whole stored plan and the current plan stays reachable after a long history.
