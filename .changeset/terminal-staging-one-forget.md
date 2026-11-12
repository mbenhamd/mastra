---
'@mastra/core': patch
---

A harness session no longer keeps per-run bookkeeping for a settlement wake that was handed to recovery after its cancellation failed; recovery now releases it like any other settled run. There is no change to the public API.
