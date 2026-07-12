---
'@mastra/core': patch
---

Fix resume labels in nested evented workflows, including parent routing and suspended steps inside parallel branches and foreach loops. Invalid labels now fail without exposing label details.
