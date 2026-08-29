---
'@mastra/core': major
---

Improved dynamic workflow schema safety. Unsupported schemas are now rejected before storage with JSON Pointer evidence. Stored workflows whose schemas are unsupported are quarantined during startup instead of being registered or run.

**Before**

```ts
await mastra.addDynamicWorkflow({
  id: 'legacy',
  inputSchema: { type: 'object', minProperties: 1 },
  outputSchema: { type: 'object' },
  graph: [{ type: 'tool', id: 'passthrough-tool', toolId: 'passthrough-tool' }],
});
```

`minProperties` was stored, then ignored while creating the runtime validator, so the live validator was weaker than the saved schema.

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
