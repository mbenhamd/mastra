---
'@mastra/core': patch
---

Fixed workflow snapshot redaction and concurrent step persistence. Canceled runs stop before starting another step, including when a storage acknowledgement is delayed.
