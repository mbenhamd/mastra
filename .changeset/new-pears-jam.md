---
'@mastra/core': patch
---

- Added one bounded, response-only recovery attempt after silent tool-result endings, with tools disabled.
- Added matching error receipts for aborted in-flight tools.
- Improved abort handling by briefly waiting for already-visible tools to emit authoritative terminal errors before falling back to synthetic abort events.
