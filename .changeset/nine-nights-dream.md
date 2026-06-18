---
'@mastra/core': patch
---

Fixed an issue where stream errors could display as `[object Object]`.
Subscribed thread stream errors now show the actual error message.
