---
'@mastra/core': patch
---

Fixed custom `stopWhen` being ignored when `maxSteps` is set. Custom stop conditions now run alongside the max-step limit, so agents can stop early while still respecting the configured cap. Invalid `maxSteps` values are rejected. Closes #19007.
