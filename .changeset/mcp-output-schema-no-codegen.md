---
'@mastra/mcp': patch
---

Fixed MCP tools that declare an `outputSchema` failing in runtimes started with `--disallow-code-generation-from-strings`. The client rejected every call with "invalid outputSchema: Code generation from strings disallowed", because both the MCP SDK client and the structured-output check compiled the schema with Ajv's `new Function`. When no `jsonSchemaValidator` is configured, `MCPClient` now uses the SDK's `CfWorkerJsonSchemaValidator`, which never generates code, for both. A custom `jsonSchemaValidator` is still used as given. Output-schema error messages now use the cf-worker validator's wording.
