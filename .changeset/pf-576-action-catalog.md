---
"@mastra/core": minor
---

Added: Harness sessions now expose a read-only `actions` catalog that aggregates skill action metadata and MCP tool descriptors for desktop command palettes without adding execution or lifecycle controls.

```ts
const actions = await session.actions.list({ query: 'ticket', limit: 20 });
const mcpActions = await session.actions.search('workspace', { source: 'mcp-tool' });
await session.actions.refresh();
```
