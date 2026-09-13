---
'@mastra/core': patch
---

Fixed late thread discovery subscriptions so timed-out requests release their subscription and do not publish after completion.
