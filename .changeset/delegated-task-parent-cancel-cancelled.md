---
'@mastra/core': patch
---

A delegated plan task now settles as `cancelled` after a cancel the parent requests through the `subagent` tool or `cancelSubagent`, whatever the reason. The child session's `cancelRequest` records a `targeted` marker for that cancel, so the result is the same after a restart. Plan rollup treats `cancelled` as skipped work, so the root no longer reports failed. Delegation timeouts, child failures, a parent Stop, cancels that cascade down from the parent's own cancellation, and cancels by anyone else still settle as `failed`. A task that depends on the cancelled one stays blocked, because only a completed dependency releases it.
