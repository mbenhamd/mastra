---
'@mastra/pg': patch
---

Fixed stale replica reads losing observational memory, bypassing background task limits or recovery, and delivering dismissed notifications. These execution decisions now read current writer state.
