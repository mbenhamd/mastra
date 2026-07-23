---
'@mastra/memory': patch
---

Harden legacy Working Memory rendering against stored instructions, enforce configured prompt-data bounds, decode one prompt-escaping layer before tool persistence, and omit empty read-only context without narrowing the existing Core peer range.

```ts
const memory = new Memory({
  options: {
    workingMemory: { enabled: true, maxDataBytes: 16_384 },
  },
});
```
