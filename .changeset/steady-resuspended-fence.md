---
'@mastra/core': patch
---

Fixed Harness replacement-mode runs losing their tool restrictions when they suspend again after a resume. A re-suspended run keeps the tools it was admitted with. A later resume after a restart still refuses tools passed only for the original call and offers no newly registered tools.
