---
'@mastra/core': minor
---

Added opt-in tool-call filtering for MessageHistory persistence and bounded compact model-output preservation for both MessageHistory and ToolCallFilter.

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
