---
'@mastra/mcp': patch
---

MCP server event, payload and resume-data validation now waits for the JSON Schema validator to load where code generation from strings is disallowed, so a request right after startup is not refused as `JSON Schema validator unavailable`. The JSON Schema adapter bundled into `@mastra/mcp`, which validates MCP client tool inputs, registers its load with the readiness functions, so callers that wait with `jsonSchemaValidatorReady()` wait for this copy too.
