---
'@mastra/core': patch
---

Fixed `untilIdle` set only in an agent's `defaultOptions` being ignored by `stream()` and `resumeStream()`. The stream now waits for background work and continues, as it does when `untilIdle` is passed to the call.
