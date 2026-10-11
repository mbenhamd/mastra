---
'@mastra/core': patch
---

Fixed agents with output processors delivering an answer twice after a processor retry. When a processor rejected a response and the model repaired it, the repaired step was read as empty, so a Harness session treated the turn as silent, asked the model again, and streamed and saved the answer twice. The repaired step now keeps its own text and tool results.
