---
'@mastra/core': patch
---

A delegated plan task now settles as `cancelled` when its parent cancels the subagent through the `subagent` tool or `cancelSubagent`, and the `subagent` tool's status view reports such a task as `cancelled`. Timeouts, child failures, a parent Stop, cascaded cancels and cancels by anyone else still settle as `failed`.
