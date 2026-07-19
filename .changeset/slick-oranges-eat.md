---
'@mastra/core': patch
---

Fixed dynamic memory functions being called more than once within a single Agent turn, which could give the model, tools, processors, and message persistence different memory instances. Each turn now resolves memory once and reuses that result throughout.
