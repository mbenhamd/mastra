---
'@mastra/core': patch
---

A delegated plan task now settles as `cancelled` when its own parent cancels the subagent, through the `subagent` tool or `cancelSubagent`. Plan rollup treats it as skipped work, so the root no longer reports failed. Delegation timeouts, child failures and other cancels still settle as `failed`. A task that depends on the cancelled one stays blocked, because only a completed dependency releases it.
