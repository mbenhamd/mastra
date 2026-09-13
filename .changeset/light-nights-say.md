---
'@mastra/core': patch
---

Fixed claimed thread owner retry, deduplication, run identity, lease handoff, and event acknowledgement handling. Awaited background tasks in the model loop now propagate suspension, wait for resume admission, and avoid queue deadlocks when an ancestor occupies the concurrency limit. Preserved successful void tool results during durable execution and restored the RunWithRawInput type export.
