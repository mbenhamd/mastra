---
"@mastra/core": minor
---

Harness sessions now expose a read-only MCP inventory catalog for desktop integrations.

```ts
const servers = session.mcp.listServers();
const filesystem = session.mcp.getServer('filesystem');
const tools = session.mcp.listTools('filesystem');
```

The catalog returns clone-safe snapshots of registered MCP server and tool descriptors. It is an integration inventory and does not imply tool execution permission.
