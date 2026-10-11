---
'@mastra/core': patch
---

Added `ToolResultSupersedeProcessor`, which keeps only the newest result for a resource in model context. When a tool reads the same resource again, every earlier result with the same key is replaced by a short pointer in the next request, so repeated reads stop being re-sent on every step.

It is opt-in per tool, and you choose the key. The rewrite applies to the provider request only; the message list, memory and UI history keep the full results. Errors, results a tool classified as failures, and results that passed through an approval are never replaced.

```ts
import { ToolResultSupersedeProcessor } from '@mastra/core/processors';

const agent = new Agent({
  // ...
  inputProcessors: [
    new ToolResultSupersedeProcessor({
      tools: {
        read_file: { key: ({ input }) => (input as { path?: string }).path },
      },
    }),
  ],
});
```
