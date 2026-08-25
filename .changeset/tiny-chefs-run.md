---
'@mastra/core': minor
---

Added optional limits for tools loaded by `ToolSearchProcessor`. Cap how many tools the agent can load and how many bytes their input schemas can add to the prompt. A search or load that would exceed a limit is refused, while tools already loaded stay available.

```typescript
import { ToolSearchProcessor } from '@mastra/core/processors'

const toolSearch = new ToolSearchProcessor({
  tools,
  budget: {
    maxToolCount: 12,
    maxToolSchemaBytes: 48_000,
  },
})
```
