---
'@mastra/core': patch
---

Fixed harness sessions retaining state for a settlement wake that was handed to recovery after its cancellation failed. Recovery now releases it like any other settled run. Public APIs are unchanged.
