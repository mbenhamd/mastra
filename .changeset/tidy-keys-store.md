---
'@mastra/core': patch
---

Fixed workflow snapshot merges so steps with reserved JavaScript property names, such as `__proto__`, keep their results correctly.
