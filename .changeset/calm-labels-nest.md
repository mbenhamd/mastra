---
'@mastra/core': patch
---

Fix resume labels in nested evented workflows, including parent routing and suspended steps inside parallel branches and foreach loops. Invalid or oversized labels now fail without exposing label details, duplicate labels at different suspended coordinates are quarantined, and a label-selected foreach iteration cannot be overridden by a conflicting index.
