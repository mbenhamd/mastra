---
'@mastra/core': minor
---

Added opt-in tool-call filtering for messages saved by MessageHistory, plus configurable model-output byte limits for MessageHistory and ToolCallFilter.

```ts
new MessageHistory({
  storage,
  toolCallFilter: {
    preserveModelOutput: true,
    maxModelOutputBytes: 16 * 1024,
  },
})

new ToolCallFilter({
  preserveModelOutput: true,
  maxModelOutputBytes: 4096,
})
```
