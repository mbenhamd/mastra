---
'@mastra/core': patch
---

Continue the agentic loop after finishReason `length` only when every local tool call has a complete, parseable payload so the next iteration can consume the result. Truncated streamed JSON no longer becomes an executable empty object.
