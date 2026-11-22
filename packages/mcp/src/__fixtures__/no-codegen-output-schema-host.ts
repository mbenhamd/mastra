import { MCPClient } from '../client/configuration.js';

// A host run under `--disallow-code-generation-from-strings` that calls MCP tools declaring an
// outputSchema and prints one JSON line per tool (PF-9855).
const [, , url, ...toolNames] = process.argv;
const mcp = new MCPClient({ id: 'no-codegen-host', servers: { stocks: { url: new URL(url!) } } });
try {
  const tools = await mcp.listTools();
  for (const name of toolNames) {
    try {
      const result = await tools[name]!.execute!({ symbol: 'ACME' }, {} as never);
      console.log(JSON.stringify({ name, result }));
    } catch (error) {
      console.log(JSON.stringify({ name, error: error instanceof Error ? error.message : String(error) }));
    }
  }
} finally {
  await mcp.disconnect();
}
