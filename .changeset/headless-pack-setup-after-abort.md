---
'@mastra/code-sdk': patch
---

Fixed a headless run continuing to apply a model pack after it was aborted or timed out. Once the run settles, setup stops before its next write, so it no longer switches subagent models or the session model.
