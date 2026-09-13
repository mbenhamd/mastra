---
'@mastra/core': patch
---

Fixed lost workflow state during concurrent execution and unintended exposure of pruned workflow data. Canceled runs stop before starting another step, including when a storage acknowledgement is delayed.
