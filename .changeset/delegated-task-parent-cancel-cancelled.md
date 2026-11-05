---
'@mastra/core': patch
---

A delegated plan task now settles as `cancelled` when its parent cancels the subagent. This covers the `subagent` tool's cancel and `cancelSubagent`. The `subagent` tool's status view reports the task as `cancelled`. Timeouts, child failures, a parent Stop, cascaded cancels and cancels by anyone else still settle as `failed`. When several cancels race, the first one committed decides the outcome, live and after a restart.

Type change: `DelegatedSubagentOutcome` and the plan task view's delegation `status` now include `'cancelled'`.
