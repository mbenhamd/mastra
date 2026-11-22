/**
 * MCP tool output schemas under `node --disallow-code-generation-from-strings` (PF-9855)
 *
 * A host run with string code generation disallowed must still be able to call an MCP tool
 * that declares an `outputSchema`, and must still have the tool's `structuredContent` checked
 * against it. The client runs in a child process with the real flag; the server runs here.
 */

import { spawn } from 'node:child_process';
import { createServer } from 'node:http';
import type { Server as HttpServer } from 'node:http';
import type { AddressInfo } from 'node:net';
import { fileURLToPath } from 'node:url';
import { toNodeHandler } from '@modelcontextprotocol/node';
import { McpServer, createMcpHandler } from '@modelcontextprotocol/server';
import type { CallToolResult } from '@modelcontextprotocol/server';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { z } from 'zod';

const hostScript = fileURLToPath(new URL('../__fixtures__/no-codegen-output-schema-host.ts', import.meta.url));

let httpServer: HttpServer;
let mcpServer: McpServer;
let baseUrl: URL;

beforeAll(async () => {
  httpServer = createServer();
  mcpServer = new McpServer({ name: 'stock-server', version: '1.0.0' }, { capabilities: { tools: {} } });
  const outputSchema = { symbol: z.string(), price: z.number() };
  mcpServer.registerTool(
    'quote',
    { description: 'Returns a stock quote', inputSchema: { symbol: z.string() }, outputSchema },
    async ({ symbol }): Promise<CallToolResult> => ({
      content: [{ type: 'text', text: `${symbol} 12.5` }],
      structuredContent: { symbol, price: 12.5 },
    }),
  );
  // The server reports a price the schema forbids, so the client-side check must reject it.
  mcpServer.registerTool(
    'badQuote',
    { description: 'Returns a malformed stock quote', inputSchema: { symbol: z.string() }, outputSchema },
    async ({ symbol }): Promise<CallToolResult> =>
      ({
        content: [{ type: 'text', text: `${symbol} ?` }],
        structuredContent: { symbol, price: 'unknown' },
      }) as CallToolResult,
  );
  const handler = toNodeHandler(createMcpHandler(() => mcpServer.server, { legacy: 'reject' }));
  httpServer.on('request', (req, res) => handler(req, res));
  baseUrl = await new Promise<URL>(resolve => {
    httpServer.listen(0, '127.0.0.1', () => {
      const { port } = httpServer.address() as AddressInfo;
      resolve(new URL(`http://127.0.0.1:${port}/mcp`));
    });
  });
});

afterAll(async () => {
  await mcpServer.close().catch(() => {});
  httpServer.close();
});

function runHost(toolNames: string[]): Promise<Array<{ name: string; result?: unknown; error?: string }>> {
  const host = spawn(
    process.execPath,
    ['--disallow-code-generation-from-strings', '--import', 'tsx', hostScript, baseUrl.href, ...toolNames],
    { stdio: ['ignore', 'pipe', 'pipe'] },
  );
  let stdout = '';
  let stderr = '';
  host.stdout.on('data', chunk => (stdout += chunk));
  host.stderr.on('data', chunk => (stderr += chunk));
  return new Promise((resolve, reject) => {
    host.on('error', reject);
    host.on('close', code => {
      if (code !== 0) return reject(new Error(`host exited with ${code}: ${stderr}`));
      resolve(
        stdout
          .split('\n')
          .filter(line => line.startsWith('{'))
          .map(line => JSON.parse(line)),
      );
    });
  });
}

describe('MCP tool output schema without string code generation', () => {
  it('returns valid structured content and rejects content that breaks the output schema', async () => {
    const [quote, badQuote] = await runHost(['stocks_quote', 'stocks_badQuote']);

    expect(quote).toEqual({ name: 'stocks_quote', result: { symbol: 'ACME', price: 12.5 } });
    expect(badQuote!.name).toBe('stocks_badQuote');
    expect(badQuote!.error ?? JSON.stringify(badQuote!.result)).toMatch(/price/);
    expect(badQuote!.error ?? JSON.stringify(badQuote!.result)).not.toMatch(/Code generation/);
  }, 60_000);
});
