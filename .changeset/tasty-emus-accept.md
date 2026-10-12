---
'@mastra/core': patch
---

Fixed agents with output processors delivering an answer twice after a processor retry. When a processor rejected a response and the model repaired it, the repaired step was read as empty. A Harness session then treated the turn as silent, asked the model again, and streamed and saved the answer twice. Each step's content is now read from what that step wrote, so the repaired step keeps its own text and tool results. This also covers text that follows a provider-executed tool result in the same response, and history pruned by a processor.

A step's `tripwire` data also gains `rolledBack: true` when its retry was granted and its response was removed from the conversation. `retry` still only says that a retry was requested.
