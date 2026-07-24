---
'@mastra/memory': patch
---

Avoid repeated provider timeouts when Observational Memory recounts the same attachment by persisting its local fallback with a bounded retry window.
