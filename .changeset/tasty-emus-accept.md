---
'@mastra/core': patch
---

Fixed agents with output processors delivering an answer twice after a processor retry. When a processor rejected a response after a tool call and the model repaired it, the repaired step was read as empty. A Harness session then treated the turn as silent, asked the model again, and streamed and saved the answer twice. The repaired step now keeps its own text.

A step's `tripwire` data also gains `rolledBack: true` when its retry was granted and its response was removed from the conversation. `retry` still only says that a retry was requested.
