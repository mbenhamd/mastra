---
'@mastra/core': minor
---

Added opt-in tool-call filtering for MessageHistory persistence and bounded compact model-output preservation for both MessageHistory and ToolCallFilter.

```ts
new ToolCallFilter({
  preserveModelOutput: true,
  maxModelOutputBytes: 4096,
})
```
