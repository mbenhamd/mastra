---
'@mastra/core': patch
---

Harden Working Memory prompt context against stored instructions, support bounded prompt data, and skip empty read-only context.

```ts
const memoryConfig = {
  options: {
    workingMemory: { enabled: true, maxDataBytes: 16_384 },
  },
};
```
