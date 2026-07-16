---
'@mastra/core': patch
---

Fixed concurrent evaluation items with explicit turns to reject conflicting thresholds and duplicate scorer IDs before target execution, then aggregate valid scores deterministically in data order.
