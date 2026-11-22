---
'@mastra/mcp': patch
---

MCP server event, payload and resume-data validation now waits for the JSON Schema validator to load, so a request right after startup is not refused as `JSON Schema validator unavailable`. MCP client tool inputs, which use the JSON Schema adapter bundled into `@mastra/mcp`, are covered too: tool execution in `@mastra/core` waits for every adapter copy in the process.
