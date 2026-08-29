---
'@mastra/core': major
---

Changed dynamic workflow schemas to fail closed against an admitted JSON Schema 2020-12 subset before they are saved. Unknown or unimplemented keywords are now rejected with JSON Pointer evidence. Historical rows that cannot rehydrate losslessly are quarantined at boot instead of executing through `z.any()`.

**Before**

```ts
await mastra.addDynamicWorkflow({
  id: 'legacy',
  inputSchema: { type: 'object', minProperties: 1 },
  outputSchema: { type: 'object' },
  graph: [{ type: 'tool', id: 'passthrough-tool', toolId: 'passthrough-tool' }],
});
```

`minProperties` was stored, then dropped on rehydrate, so the live validator was weaker than the saved schema.

**After**

```ts
await mastra.addDynamicWorkflow({
  id: 'legacy',
  inputSchema: {
    $schema: 'https://json-schema.org/draft/2020-12/schema',
    type: 'object',
    properties: { name: { type: 'string' } },
    required: ['name'],
    additionalProperties: false,
  },
  outputSchema: { type: 'object', properties: { name: { type: 'string' } }, additionalProperties: false },
  graph: [{ type: 'tool', id: 'passthrough-tool', toolId: 'passthrough-tool' }],
});
```

Unsupported historical rows stay in storage, appear on `listQuarantinedDynamicWorkflows()`, and throw if executed. Sibling workflows still boot.
