import { anthropic } from '@ai-sdk/anthropic-v5';
import { openai } from '@ai-sdk/openai-v6';
import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod/v4';
import { getBuiltToolFGAResourceId } from '../../auth/ee/fga-check';
import { noopLogger } from '../../logger';
import { SpanType } from '../../observability';
import type { AnySpan } from '../../observability';
import { RequestContext } from '../../request-context';
import { createTool } from '../../tools';
import { isProviderDefinedTool, isVercelTool } from '../toolchecks';
import { CoreToolBuilder } from './builder';

describe('CoreToolBuilder recovery fingerprint', () => {
  it('defers expensive recovery identity work until durable metadata is read', () => {
    class RuntimeOnlyPolicy extends RegExp {
      override test(value: string): boolean {
        return value.startsWith('internal:') && super.test(value);
      }
    }

    const originalTool = createTool({
      id: 'lazy-recovery-fingerprint',
      description: 'Proves ordinary tool conversion does not eagerly fingerprint runtime-only configuration.',
      providerOptions: { policy: new RuntimeOnlyPolicy('reports', 'u') } as any,
      execute: async () => ({ ok: true }),
    });
    const builtTool = new CoreToolBuilder({
      originalTool,
      options: {
        name: 'lazy-recovery-fingerprint',
        logger: noopLogger,
      },
    }).build();

    expect(builtTool.id).toBe('lazy-recovery-fingerprint');
    expect(() => builtTool.recoveryFingerprint).toThrow(
      'Cannot create a durable recovery fingerprint for RegExp subclass',
    );
  });

  it('binds durable recovery identity to the terminal-result policy', () => {
    const execute = async () => ({ ok: true, answer: 'done' });
    const build = (isSuccess: (output: { ok: boolean; answer: string }) => boolean) => {
      const originalTool = createTool({
        id: 'terminal-recovery-fingerprint',
        description: 'Return a terminal result.',
        outputSchema: z.object({ ok: z.boolean(), answer: z.string() }),
        terminalResult: {
          isSuccess,
          outputSchema: z.object({ answer: z.string() }),
          project: output => ({ answer: output.answer }),
        },
        execute,
      });
      return new CoreToolBuilder({
        originalTool,
        options: { name: 'terminal-recovery-fingerprint', logger: noopLogger },
      }).build();
    };

    const acceptsOk = build(output => output.ok);
    const acceptsNonEmpty = build(output => output.answer.length > 0);

    expect(acceptsOk.terminalResult).toBeDefined();
    expect(acceptsOk.recoveryFingerprint).not.toBe(acceptsNonEmpty.recoveryFingerprint);
  });

  it('uses an explicit original fingerprint as the durable implementation version', () => {
    const build = (version: string, execute: () => Promise<{ source: string }>) => {
      const originalTool = createTool({
        id: 'explicit-recovery-version',
        description: 'Uses a deployment-stable durable version.',
        execute,
      });
      originalTool.recoveryFingerprint = version;
      return new CoreToolBuilder({
        originalTool,
        options: {
          name: 'explicit-recovery-version',
          logger: noopLogger,
          requestContext: new RequestContext(),
        },
      }).build();
    };

    const first = build('explicit-recovery-version:v1', async () => ({ source: 'first transform' }));
    const equivalentReplica = build('explicit-recovery-version:v1', async () => ({ source: 'second transform' }));
    const changedVersion = build('explicit-recovery-version:v2', async () => ({ source: 'first transform' }));

    expect(first.recoveryFingerprint).toBe(equivalentReplica.recoveryFingerprint);
    expect(first.recoveryFingerprint).not.toBe(changedVersion.recoveryFingerprint);
  });
});

describe('CoreToolBuilder FGA', () => {
  it('executes tools without FGA when only auth/server config is present', async () => {
    const execute = vi.fn().mockResolvedValue({ result: 'ok' });
    const testTool = createTool({
      id: 'search',
      description: 'Search',
      inputSchema: z.object({ query: z.string() }),
      execute,
    });
    const requestContext = new RequestContext();
    requestContext.set('user', { id: 'user-1' });

    const builder = new CoreToolBuilder({
      originalTool: testTool,
      options: {
        name: 'search',
        logger: {
          debug: vi.fn(),
          warn: vi.fn(),
          error: vi.fn(),
          trackException: vi.fn(),
        } as any,
        requestContext,
        mastra: {
          getServer: () => ({ auth: {} }),
        } as any,
      },
    });

    const builtTool = builder.build();
    expect(getBuiltToolFGAResourceId(builtTool)).toBe('search');
    expect(builtTool).not.toHaveProperty('_mastraFgaResourceId');
    await expect(builtTool.execute!({ query: 'docs' }, { toolCallId: 'call-1', messages: [] })).resolves.toEqual({
      result: 'ok',
    });
    expect(execute).toHaveBeenCalledWith(
      { query: 'docs' },
      expect.objectContaining({
        mastra: expect.any(Object),
        requestContext,
      }),
    );
  });

  it('checks tool execution FGA before executing a tool', async () => {
    const execute = vi.fn().mockResolvedValue({ result: 'ok' });
    const testTool = createTool({
      id: 'search',
      description: 'Search',
      inputSchema: z.object({ query: z.string() }),
      execute,
    });
    const user = { id: 'user-1' };
    const requestContext = new RequestContext();
    requestContext.set('user', user);
    const fgaProvider = {
      require: vi.fn().mockResolvedValue(undefined),
    };

    const builder = new CoreToolBuilder({
      originalTool: testTool,
      options: {
        name: 'search',
        agentId: 'agent-1',
        agentName: 'Agent 1',
        runId: 'run-1',
        threadId: 'thread-1',
        resourceId: 'tenant-1',
        logger: {
          debug: vi.fn(),
          warn: vi.fn(),
          error: vi.fn(),
          trackException: vi.fn(),
        } as any,
        requestContext,
        mastra: {
          getServer: () => ({ fga: fgaProvider }),
        } as any,
      },
    });

    const builtTool = builder.build();
    expect(getBuiltToolFGAResourceId(builtTool)).toBe('agent-1:search');
    expect(builtTool).not.toHaveProperty('_mastraFgaResourceId');
    await builtTool.execute!({ query: 'docs' }, { toolCallId: 'call-1', messages: [] });

    expect(fgaProvider.require).toHaveBeenCalledWith(user, {
      resource: { type: 'tool', id: 'agent-1:search' },
      permission: 'tools:execute',
      context: expect.objectContaining({
        resourceId: 'tenant-1',
        requestContext,
        metadata: expect.objectContaining({
          toolName: 'search',
          agentId: 'agent-1',
          agentName: 'Agent 1',
          runId: 'run-1',
          threadId: 'thread-1',
          executionResourceId: 'tenant-1',
        }),
      }),
    });
    expect(execute).toHaveBeenCalled();
  });

  it('binds provenance to the final buildV5 object and not to arbitrary clones', () => {
    const testTool = createTool({
      id: 'search',
      description: 'Search',
      inputSchema: z.object({ query: z.string() }),
      execute: async () => ({ result: 'ok' }),
    });
    const builder = new CoreToolBuilder({
      originalTool: testTool,
      options: { name: 'search', agentId: 'agent-1' },
    });

    const builtTool = builder.buildV5();

    expect(getBuiltToolFGAResourceId(builtTool)).toBe('agent-1:search');
    expect(builtTool).not.toHaveProperty('_mastraFgaResourceId');
    expect(getBuiltToolFGAResourceId({ ...builtTool })).toBeUndefined();
  });

  it('fails closed when FGA is configured and a tool executes without a user', async () => {
    const execute = vi.fn().mockResolvedValue({ result: 'ok' });
    const testTool = createTool({
      id: 'search',
      description: 'Search',
      inputSchema: z.object({ query: z.string() }),
      execute,
    });
    const fgaProvider = {
      require: vi.fn().mockResolvedValue(undefined),
    };

    const builder = new CoreToolBuilder({
      originalTool: testTool,
      options: {
        name: 'search',
        logger: {
          debug: vi.fn(),
          warn: vi.fn(),
          error: vi.fn(),
          trackException: vi.fn(),
        } as any,
        requestContext: new RequestContext(),
        mastra: {
          getServer: () => ({ fga: fgaProvider }),
        } as any,
      },
    });

    const builtTool = builder.build();
    await expect(builtTool.execute!({ query: 'docs' }, { toolCallId: 'call-1', messages: [] })).rejects.toThrow(
      'authenticated user is required',
    );
    expect(fgaProvider.require).not.toHaveBeenCalled();
    expect(execute).not.toHaveBeenCalled();
  });

  it('bypasses membership resolution for a tenant-scoped trusted actor', async () => {
    const execute = vi.fn().mockResolvedValue({ result: 'ok' });
    const testTool = createTool({
      id: 'search',
      description: 'Search',
      inputSchema: z.object({ query: z.string() }),
      execute,
    });
    const requestContext = new RequestContext();
    requestContext.set('organizationId', 'org-1');
    const fgaProvider = {
      require: vi.fn().mockResolvedValue(undefined),
    };

    const builder = new CoreToolBuilder({
      originalTool: testTool,
      options: {
        name: 'search',
        logger: {
          debug: vi.fn(),
          warn: vi.fn(),
          error: vi.fn(),
          trackException: vi.fn(),
        } as any,
        requestContext,
        mastra: {
          getServer: () => ({ fga: fgaProvider }),
        } as any,
      },
    });

    const actor = { actorKind: 'system', sourceWorkflow: 'nightly-workflow' } as const;
    const builtTool = builder.build();
    await builtTool.execute!(
      { query: 'docs' },
      {
        toolCallId: 'call-1',
        messages: [],
        actor,
      },
    );

    expect(fgaProvider.require).not.toHaveBeenCalled();
    expect(execute).toHaveBeenCalledWith(
      { query: 'docs' },
      expect.objectContaining({
        actor,
      }),
    );
  });
});

describe('MCP Tool Tracing', () => {
  it('should use MCP_TOOL_CALL span type when tool has mcpMetadata', async () => {
    const testTool = createTool({
      id: 'mcp-server_list-files',
      description: 'List files in a directory',
      inputSchema: z.object({ path: z.string() }),
      mcpMetadata: {
        serverName: 'filesystem-server',
        serverVersion: '1.2.0',
      },
      execute: async inputData => ({ files: [inputData.path] }),
    });

    const mockToolSpan = {
      end: vi.fn(),
      error: vi.fn(),
      update: vi.fn(),
    };

    const mockAgentSpan = {
      createChildSpan: vi.fn().mockReturnValue(mockToolSpan),
    } as unknown as AnySpan;

    const builder = new CoreToolBuilder({
      originalTool: testTool,
      options: {
        name: 'mcp-server_list-files',
        logger: {
          debug: vi.fn(),
          warn: vi.fn(),
          error: vi.fn(),
          trackException: vi.fn(),
        } as any,
        description: 'List files in a directory',
        requestContext: new RequestContext(),
        tracingContext: { currentSpan: mockAgentSpan },
      },
    });

    const builtTool = builder.build();
    expect(getBuiltToolFGAResourceId(builtTool)).toBe('["filesystem-server","mcp-server_list-files"]');
    expect(builtTool).not.toHaveProperty('_mastraFgaResourceId');
    await builtTool.execute!({ path: '/tmp' }, { toolCallId: 'test-call-id', messages: [] });

    expect(mockAgentSpan.createChildSpan).toHaveBeenCalledWith(
      expect.objectContaining({
        type: SpanType.MCP_TOOL_CALL,
        name: "mcp_tool: 'mcp-server_list-files' on 'filesystem-server'",
        attributes: {
          toolCallId: 'test-call-id',
          mcpServer: 'filesystem-server',
          serverVersion: '1.2.0',
          toolDescription: 'List files in a directory',
          toolCallId: 'test-call-id',
        },
      }),
    );

    expect((mockAgentSpan.createChildSpan as ReturnType<typeof vi.fn>).mock.calls[0]?.[0]).not.toHaveProperty('input');
    expect(mockToolSpan.update).toHaveBeenCalledWith({ input: { path: '/tmp' } });
    expect(mockToolSpan.end).toHaveBeenCalledWith({ attributes: { success: true }, output: { files: ['/tmp'] } });
  });

  it('should use TOOL_CALL span type for tools without mcpMetadata', async () => {
    const testTool = createTool({
      id: 'regular-tool',
      description: 'A regular tool',
      inputSchema: z.object({ value: z.string() }),
      execute: async inputData => ({ result: inputData.value }),
    });

    const mockToolSpan = {
      end: vi.fn(),
      error: vi.fn(),
      update: vi.fn(),
    };

    const mockAgentSpan = {
      createChildSpan: vi.fn().mockReturnValue(mockToolSpan),
    } as unknown as AnySpan;

    const builder = new CoreToolBuilder({
      originalTool: testTool,
      options: {
        name: 'regular-tool',
        logger: {
          debug: vi.fn(),
          warn: vi.fn(),
          error: vi.fn(),
          trackException: vi.fn(),
        } as any,
        description: 'A regular tool',
        requestContext: new RequestContext(),
        tracingContext: { currentSpan: mockAgentSpan },
      },
    });

    const builtTool = builder.build();
    await builtTool.execute!({ value: 'test' }, { toolCallId: 'test-call-id', messages: [] });

    expect(mockAgentSpan.createChildSpan).toHaveBeenCalledWith(
      expect.objectContaining({
        type: SpanType.TOOL_CALL,
        name: "tool: 'regular-tool'",
        attributes: {
          toolCallId: 'test-call-id',
          toolDescription: 'A regular tool',
          toolType: 'tool',
          toolCallId: 'test-call-id',
        },
      }),
    );
    expect((mockAgentSpan.createChildSpan as ReturnType<typeof vi.fn>).mock.calls[0]?.[0]).not.toHaveProperty('input');
    expect(mockToolSpan.update).toHaveBeenCalledWith({ input: { value: 'test' } });
  });

  it('should handle mcpMetadata with missing serverVersion', async () => {
    const testTool = createTool({
      id: 'mcp_read-resource',
      description: 'Read a resource',
      inputSchema: z.object({ uri: z.string() }),
      mcpMetadata: {
        serverName: 'my-mcp-server',
      },
      execute: async inputData => ({ data: inputData.uri }),
    });

    const mockToolSpan = {
      end: vi.fn(),
      error: vi.fn(),
      update: vi.fn(),
    };

    const mockAgentSpan = {
      createChildSpan: vi.fn().mockReturnValue(mockToolSpan),
    } as unknown as AnySpan;

    const builder = new CoreToolBuilder({
      originalTool: testTool,
      options: {
        name: 'mcp_read-resource',
        logger: {
          debug: vi.fn(),
          warn: vi.fn(),
          error: vi.fn(),
          trackException: vi.fn(),
        } as any,
        description: 'Read a resource',
        requestContext: new RequestContext(),
        tracingContext: { currentSpan: mockAgentSpan },
      },
    });

    const builtTool = builder.build();
    await builtTool.execute!({ uri: 'file:///test' }, { toolCallId: 'test-call-id', messages: [] });

    const spanArgs = (mockAgentSpan.createChildSpan as ReturnType<typeof vi.fn>).mock.calls[0][0];
    expect(spanArgs.type).toBe(SpanType.MCP_TOOL_CALL);
    expect(spanArgs.attributes).toEqual({
      toolCallId: 'test-call-id',
      mcpServer: 'my-mcp-server',
      serverVersion: undefined,
      toolDescription: 'Read a resource',
      toolCallId: 'test-call-id',
    });
    expect(spanArgs.name).toBe("mcp_tool: 'mcp_read-resource' on 'my-mcp-server'");
  });

  it('should not use MCP_TOOL_CALL for Vercel tools even with mcpMetadata-like properties', async () => {
    const vercelTool = {
      description: 'A vercel tool',
      parameters: z.object({ input: z.string() }),
      mcpMetadata: { serverName: 'fake' },
      execute: async (args: any) => ({ output: args.input }),
    };

    const mockToolSpan = {
      end: vi.fn(),
      error: vi.fn(),
      update: vi.fn(),
    };

    const mockAgentSpan = {
      createChildSpan: vi.fn().mockReturnValue(mockToolSpan),
    } as unknown as AnySpan;

    const builder = new CoreToolBuilder({
      originalTool: vercelTool as any,
      options: {
        name: 'vercel-tool',
        logger: {
          debug: vi.fn(),
          warn: vi.fn(),
          error: vi.fn(),
          trackException: vi.fn(),
        } as any,
        description: 'A vercel tool',
        requestContext: new RequestContext(),
        tracingContext: { currentSpan: mockAgentSpan },
      },
    });

    const builtTool = builder.build();
    await builtTool.execute!({ input: 'test' }, { toolCallId: 'test-call-id', messages: [] });

    expect(mockAgentSpan.createChildSpan).toHaveBeenCalledWith(
      expect.objectContaining({
        type: SpanType.TOOL_CALL,
        name: "tool: 'vercel-tool'",
      }),
    );

    const spanArgs = (mockAgentSpan.createChildSpan as ReturnType<typeof vi.fn>).mock.calls[0][0];
    expect(spanArgs.attributes).not.toHaveProperty('mcpServer');
    expect(spanArgs.attributes).not.toHaveProperty('serverVersion');
  });

  describe('requireApproval Handling', () => {
    it('exposes the runtime input validator without executing the tool', () => {
      const execute = vi.fn(async (input: { value: string }) => input);
      const testTool = createTool({
        id: 'approval-preflight-tool',
        description: 'A tool whose input must be valid before approval.',
        inputSchema: z.object({ value: z.string().trim().min(1) }),
        execute,
      });

      const builtTool = new CoreToolBuilder({
        originalTool: testTool,
        options: {
          name: 'approval-preflight-tool',
          requireApproval: true,
        },
      }).build();

      expect(builtTool.validateInput?.({ value: 42 })).toMatchObject({
        error: {
          error: true,
          message: expect.stringContaining('approval-preflight-tool'),
        },
      });
      expect(builtTool.validateInput?.({ value: ' valid ' })).toEqual({
        data: { value: 'valid' },
      });
      expect(execute).not.toHaveBeenCalled();
    });

    it('should correctly handle function in this.options.requireApproval', () => {
      const needsApprovalFn = (input: any) => input.value === 'secret';
      const testTool = {
        id: 'test-tool',
        description: 'A test tool',
        inputSchema: z.object({ value: z.string() }),
        execute: async (input: any) => input,
      };

      const builder = new CoreToolBuilder({
        originalTool: testTool as any,
        options: {
          name: 'test-tool',
          requireApproval: needsApprovalFn,
        },
      });

      const builtTool = builder.build();

      // requireApproval should be true to trigger logic in tool-call-step
      expect(builtTool.requireApproval).toBe(true);
      // needsApprovalFn should be correctly assigned from options
      expect((builtTool as any).needsApprovalFn).toBe(needsApprovalFn);
    });

    it('should correctly handle boolean in this.options.requireApproval', () => {
      const testTool = {
        id: 'test-tool',
        description: 'A test tool',
        inputSchema: z.object({ value: z.string() }),
        execute: async (input: any) => input,
      };

      const builder = new CoreToolBuilder({
        originalTool: testTool as any,
        options: {
          name: 'test-tool',
          requireApproval: true,
        },
      });

      const builtTool = builder.build();
      expect(builtTool.requireApproval).toBe(true);
      expect((builtTool as any).needsApprovalFn).toBeUndefined();
    });

    it('should preserve a needsApprovalFn attached directly to the tool instance (MCP shape)', () => {
      // MCP tools wrap a server-level requireToolApproval function and attach it as
      // `needsApprovalFn` on the tool while keeping `requireApproval` as a boolean.
      const needsApprovalFn = (args: any) => args.value === 'secret';
      const testTool = {
        id: 'mcp-test-tool',
        description: 'An MCP-style test tool',
        inputSchema: z.object({ value: z.string() }),
        execute: async (input: any) => input,
        requireApproval: true,
        needsApprovalFn,
      };

      const builder = new CoreToolBuilder({
        originalTool: testTool as any,
        options: {
          name: 'mcp-test-tool',
          // Mirrors the agent passing the tool's boolean requireApproval into options.
          requireApproval: true,
        },
      });

      const builtTool = builder.build();

      // requireApproval stays true so tool-call-step evaluates the function.
      expect(builtTool.requireApproval).toBe(true);
      // The directly-attached function must survive conversion.
      expect((builtTool as any).needsApprovalFn).toBe(needsApprovalFn);
    });

    it('should not override an options-derived needsApprovalFn with the instance one', () => {
      const optionsFn = (input: any) => input.value === 'fromOptions';
      const instanceFn = (input: any) => input.value === 'fromInstance';
      const testTool = {
        id: 'precedence-tool',
        description: 'A tool with both function sources',
        inputSchema: z.object({ value: z.string() }),
        execute: async (input: any) => input,
        needsApprovalFn: instanceFn,
      };

      const builder = new CoreToolBuilder({
        originalTool: testTool as any,
        options: {
          name: 'precedence-tool',
          requireApproval: optionsFn,
        },
      });

      const builtTool = builder.build();

      expect(builtTool.requireApproval).toBe(true);
      // Options-derived function wins; the instance fallback only fills gaps.
      expect((builtTool as any).needsApprovalFn).toBe(optionsFn);
    });
  });
});

describe('Provider-defined Tool Handling', () => {
  it('binds the selected identity to build and buildV5 provider-tool objects', () => {
    const providerTool = {
      type: 'provider-defined' as const,
      id: 'provider.search' as const,
      description: 'Provider search',
      inputSchema: z.object({ query: z.string() }),
      execute: vi.fn().mockResolvedValue({ result: 'ok' }),
    };
    const createBuilder = () =>
      new CoreToolBuilder({
        originalTool: providerTool as any,
        options: { name: 'search', agentId: 'agent-1' },
      });

    const builtTool = createBuilder().build();
    const builtV5Tool = createBuilder().buildV5();

    expect(builtTool.execute).toEqual(expect.any(Function));
    expect(getBuiltToolFGAResourceId(builtTool)).toBe('agent-1:search');
    expect(getBuiltToolFGAResourceId(builtV5Tool)).toBe('agent-1:search');
    expect(builtTool).not.toHaveProperty('_mastraFgaResourceId');
    expect(builtV5Tool).not.toHaveProperty('_mastraFgaResourceId');
  });

  it('should not crash when autoResumeSuspendedTools is enabled with openai.tools.webSearch()', () => {
    const webSearchTool = openai.tools.webSearch({});

    // Verify this is actually a provider-defined tool (v5 uses 'provider-defined', v6 uses 'provider')
    expect(['provider-defined', 'provider']).toContain(webSearchTool.type);
    expect(webSearchTool.id).toBe('openai.web_search');

    // Verify isProviderDefinedTool detects it correctly
    expect(isProviderDefinedTool(webSearchTool)).toBe(true);
    // Verify isVercelTool does NOT match (so the schema extension code path would be entered without the fix)
    expect(isVercelTool(webSearchTool as any)).toBe(false);

    // This should not throw - previously it crashed with:
    // TypeError: Cannot read properties of undefined (reading 'jsonSchema')
    // because provider-defined tools have a lazy inputSchema that doesn't conform to standard schemas
    expect(() => {
      new CoreToolBuilder({
        originalTool: webSearchTool,
        options: {
          name: 'web_search',
          logger: {
            debug: vi.fn(),
            warn: vi.fn(),
            error: vi.fn(),
            trackException: vi.fn(),
          } as any,
          description: 'Search the web',
          requestContext: new RequestContext(),
        },
        autoResumeSuspendedTools: true,
      });
    }).not.toThrow();
  });
});

describe('CoreToolBuilder strict', () => {
  it('should pass through strict when building a tool', () => {
    const strictTool = createTool({
      id: 'strict-tool',
      description: 'A tool with strict input generation',
      strict: true,
      inputSchema: z.object({ city: z.string() }),
      execute: async ({ city }) => ({ result: city }),
    });

    const builder = new CoreToolBuilder({
      originalTool: strictTool,
      options: {
        name: 'strict-tool',
        logger: console as any,
        description: 'A tool with strict input generation',
        requestContext: new RequestContext(),
        tracingContext: {},
      },
    });

    const builtTool = builder.build();

    expect(builtTool.strict).toBe(true);
  });

  it('should pass through strict via buildV5()', () => {
    const strictTool = createTool({
      id: 'strict-tool-v5',
      description: 'A tool with strict input generation for V5',
      strict: true,
      inputSchema: z.object({ query: z.string() }),
      execute: async ({ query }) => ({ result: query }),
    });

    const builder = new CoreToolBuilder({
      originalTool: strictTool,
      options: {
        name: 'strict-tool-v5',
        logger: console as any,
        description: 'A tool with strict input generation for V5',
        requestContext: new RequestContext(),
        tracingContext: {},
      },
    });

    const builtTool = builder.buildV5();

    expect((builtTool as any).strict).toBe(true);
  });

  it('should preserve provider name in buildV5() for versioned provider-defined tools', () => {
    // Uses the real Anthropic V5 webSearch tool where the ID is versioned
    // ("anthropic.web_search_20250305") but the model-facing name is "web_search".
    // Without the fix, buildV5() would derive "web_search_20250305" from the ID,
    // which breaks V6 provider bidirectional tool name mapping.
    const providerTool = anthropic.tools.webSearch_20250305({});

    const builder = new CoreToolBuilder({
      originalTool: providerTool as any,
      options: {
        name: 'search',
        logger: console as any,
        description: providerTool.description ?? 'Search the web',
        requestContext: new RequestContext(),
        tracingContext: {},
      },
    });

    const builtTool = builder.buildV5();

    expect((builtTool as any).name).toBe('web_search');
    expect((builtTool as any).id).toBe('anthropic.web_search_20250305');
  });
});

describe('CoreToolBuilder background task schema injection', () => {
  it('does not crash re-building a tool whose inputSchema has a refinement (zod v4)', () => {
    const refinedTool = createTool({
      id: 'refined_tool',
      description: 'tool whose input schema carries a .refine()',
      inputSchema: z
        .object({ a: z.string().optional(), b: z.string().optional() })
        .refine(d => !!d.a || !!d.b, { message: 'pass a or b' }),
      execute: async () => ({ ok: true }),
    });

    const build = () =>
      new CoreToolBuilder({
        originalTool: refinedTool,
        options: { name: 'refined_tool', requestContext: new RequestContext() },
        backgroundTaskEnabled: true,
      }).build();

    // The builder mutates originalTool.inputSchema, so the second build re-injects
    // `_background` onto the already-refined schema. With `.extend()` Zod v4 threw
    // "Cannot overwrite keys on object schemas containing refinements"; safeExtend fixes it.
    expect(() => build()).not.toThrow();
    expect(() => build()).not.toThrow();
    expect((refinedTool.inputSchema as z.ZodTypeAny).safeParse({}).success).toBe(false);
  });
});

describe('CoreToolBuilder requestContext merge', () => {
  it('preserves invocation option accessors without evaluating compatibility getters', async () => {
    const legacyGetter = vi.fn(() => {
      throw new Error('deprecated compatibility getter was evaluated');
    });
    const mcpContext = { serverName: 'test-server' };
    let receivedInvocationOptions: Record<string, unknown> | undefined;

    const testTool = {
      description: 'Read canonical MCP context',
      inputSchema: z.object({}),
      execute: async (_args: unknown, invocationOptions: Record<string, unknown>) => {
        receivedInvocationOptions = invocationOptions;
        return { result: 'ok' };
      },
    };
    const builtTool = new CoreToolBuilder({
      originalTool: testTool as any,
      options: { name: 'mcp_context_tool', requestContext: new RequestContext() },
    }).build();
    const invocationOptions = {
      toolCallId: 'call-1',
      messages: [],
      mcp: mcpContext,
    } as any;
    Object.defineProperty(invocationOptions, 'elicitation', {
      enumerable: true,
      get: legacyGetter,
    });

    await expect(builtTool.execute!({}, invocationOptions)).resolves.toEqual({ result: 'ok' });

    expect(legacyGetter).not.toHaveBeenCalled();
    expect(receivedInvocationOptions?.mcp).toBe(mcpContext);
    expect(Object.getOwnPropertyDescriptor(receivedInvocationOptions, 'elicitation')?.get).toBe(legacyGetter);
  });

  it('preserves requestContext identity when closure and exec contexts are the same instance', async () => {
    const sharedRC = new RequestContext();
    sharedRC.set('initial-key', 'initial-value');

    const receivedCtx: { requestContext?: RequestContext } = {};
    const execute = vi.fn().mockImplementation((_args: unknown, ctx: any) => {
      receivedCtx.requestContext = ctx.requestContext;
      ctx.requestContext.set('tool-write-key', 'tool-write-value');
      return { result: 'ok' };
    });

    const testTool = createTool({
      id: 'test_tool',
      description: 'Test',
      inputSchema: z.object({}),
      execute,
    });

    const builder = new CoreToolBuilder({
      originalTool: testTool,
      options: {
        name: 'test_tool',
        logger: noopLogger,
        requestContext: sharedRC,
      },
    });

    const builtTool = builder.build();
    await builtTool.execute!({}, { toolCallId: 'call-1', messages: [], requestContext: sharedRC });

    expect(receivedCtx.requestContext).toBe(sharedRC);
    expect(sharedRC.get('tool-write-key')).toBe('tool-write-value');
  });

  it('preserves live infrastructure values while execution application keys override', async () => {
    // Simulate an evented workflow wire round trip: the harness slot survives,
    // but JSON serialization strips its live callbacks.
    const harnessCtx = {
      harnessId: 'h-1',
      getState: () => ({ tasks: [] }),
      setState: vi.fn(),
      updateState: vi.fn(),
    };

    const closureRC = new RequestContext();
    closureRC.set('harness', harnessCtx);
    closureRC.set('serializable-key', 'from-closure');

    const wireProjection = JSON.parse(JSON.stringify(closureRC.toJSON())) as Record<string, unknown>;
    const execRC = new RequestContext(Object.entries(wireProjection));
    execRC.set('serializable-key', 'from-exec');
    execRC.set('workflow-only-key', 42);
    expect((execRC.get('harness') as any).harnessId).toBe('h-1');
    expect((execRC.get('harness') as any).updateState).toBeUndefined();

    const receivedCtx: { requestContext?: RequestContext } = {};
    const execute = vi.fn().mockImplementation((_args: unknown, ctx: any) => {
      receivedCtx.requestContext = ctx.requestContext;
      return { result: 'ok' };
    });

    const testTool = createTool({
      id: 'task_write',
      description: 'Write tasks',
      inputSchema: z.object({ tasks: z.array(z.string()) }),
      execute,
    });

    const builder = new CoreToolBuilder({
      originalTool: testTool,
      options: {
        name: 'task_write',
        logger: { debug: vi.fn(), warn: vi.fn(), error: vi.fn(), trackException: vi.fn() } as any,
        requestContext: closureRC,
      },
    });

    const builtTool = builder.build();
    await builtTool.execute!({ tasks: ['a'] }, { toolCallId: 'call-1', messages: [], requestContext: execRC });

    const merged = receivedCtx.requestContext!;
    // Live infrastructure value from the closure is preserved.
    expect(merged.get('harness')).toBe(harnessCtx);
    expect((merged.get('harness') as any).updateState).toBe(harnessCtx.updateState);
    // Execution-time values retain the documented per-call precedence.
    expect(merged.get('serializable-key')).toBe('from-exec');
    // Exec-only key is preserved
    expect(merged.get('workflow-only-key')).toBe(42);
  });

  it('accepts an execution-time infrastructure value when the build context has no owner', async () => {
    const closureRC = new RequestContext();
    closureRC.set('tenantId', 'build-tenant');
    const execRC = new RequestContext();
    const user = { id: 'exec-user' };
    execRC.set('user', user);

    let receivedRequestContext: RequestContext | undefined;
    const testTool = createTool({
      id: 'read-user',
      description: 'Read user',
      inputSchema: z.object({}),
      execute: async (_args, context) => {
        receivedRequestContext = context.requestContext;
        return { result: 'ok' };
      },
    });
    const builtTool = new CoreToolBuilder({
      originalTool: testTool,
      options: { name: 'read-user', requestContext: closureRC },
    }).build();

    await builtTool.execute!({}, { toolCallId: 'call-1', messages: [], requestContext: execRC });

    expect(receivedRequestContext?.get('user')).toBe(user);
    expect(receivedRequestContext?.get('tenantId')).toBe('build-tenant');
  });

  it('falls back to closure RC when exec RC is empty', async () => {
    const closureRC = new RequestContext();
    closureRC.set('harness', { harnessId: 'h-1' });

    const receivedCtx: { requestContext?: RequestContext } = {};
    const execute = vi.fn().mockImplementation((_args: unknown, ctx: any) => {
      receivedCtx.requestContext = ctx.requestContext;
      return { result: 'ok' };
    });

    const testTool = createTool({
      id: 'test_tool',
      description: 'Test',
      inputSchema: z.object({}),
      execute,
    });

    const builder = new CoreToolBuilder({
      originalTool: testTool,
      options: {
        name: 'test_tool',
        logger: { debug: vi.fn(), warn: vi.fn(), error: vi.fn(), trackException: vi.fn() } as any,
        requestContext: closureRC,
      },
    });

    const builtTool = builder.build();
    await builtTool.execute!({}, { toolCallId: 'call-1', messages: [] });

    // With no exec RC, closure RC is used directly
    expect(receivedCtx.requestContext!.get('harness')).toEqual({ harnessId: 'h-1' });
  });
});

describe('resume input normalization', () => {
  const buildStrictReparseTool = (received: unknown[]) => {
    const inputSchema = z.object({
      name: z.string(),
      projectId: z.string().optional(),
    });
    const originalTool = createTool({
      id: 'strict-reparse',
      description: 'Re-parses its input with the original zod schema, like product workspace suspension tools.',
      inputSchema,
      execute: async input => {
        received.push(input);
        // The tool-side re-parse that threw on resumed legs before the fix:
        // OpenAI strict compat materializes .optional() fields as null, and the
        // resumed leg used to skip the builder's null-stripping normalization.
        const params = inputSchema.parse(input);
        return { ok: true, params };
      },
    });
    return new CoreToolBuilder({
      originalTool,
      options: { name: 'strict-reparse', logger: noopLogger },
    }).build();
  };

  it('normalizes provider null-for-optional on the initial leg', async () => {
    const received: unknown[] = [];
    const builtTool = buildStrictReparseTool(received);
    const result = await builtTool.execute!({ name: 'demo', projectId: null }, {
      toolCallId: 'call-initial',
      messages: [],
    } as any);
    expect(result).toMatchObject({ ok: true, params: { name: 'demo' } });
  });

  it('normalizes provider null-for-optional on the resumed leg too', async () => {
    const received: unknown[] = [];
    const builtTool = buildStrictReparseTool(received);
    const result = await builtTool.execute!({ name: 'demo', projectId: null }, {
      toolCallId: 'call-resumed',
      messages: [],
      resumeData: { approved: true },
    } as any);
    expect(result).toMatchObject({ ok: true, params: { name: 'demo' } });
    expect(received[0]).not.toHaveProperty('projectId');
  });

  it('keeps raw args on resumed legs whose replayed args fail validation', async () => {
    const received: unknown[] = [];
    const inputSchema = z.object({ task: z.string() }).strict();
    const originalTool = createTool({
      id: 'delegated-resume',
      description: 'Delegated resumes replay control fields the schema does not know.',
      inputSchema,
      execute: async input => {
        received.push(input);
        return { ok: true };
      },
    });
    const builtTool = new CoreToolBuilder({
      originalTool,
      options: { name: 'delegated-resume', logger: noopLogger },
    }).build();
    const result = await builtTool.execute!({ task: 'continue', suspendedToolRunId: 'run-1' }, {
      toolCallId: 'call-delegated',
      messages: [],
      resumeData: { answer: 'yes' },
    } as any);
    expect(result).toMatchObject({ ok: true });
    // Validation fails on the unknown control field, so the raw replayed args
    // must reach the tool unchanged (pre-fix behavior preserved).
    expect(received[0]).toMatchObject({ task: 'continue', suspendedToolRunId: 'run-1' });
  });

  // Same public API as RequestContext but a different prototype — simulates an
  // RC constructed by a duplicate @mastra/core copy (bundlers, monorepos),
  // where `instanceof RequestContext` is false (#19772).
  class ForeignRequestContext {
    private map = new Map<string, unknown>();
    set(key: string, value: unknown) {
      this.map.set(key, value);
    }
    get(key: string) {
      return this.map.get(key);
    }
    has(key: string) {
      return this.map.has(key);
    }
    entries() {
      return this.map.entries();
    }
    size() {
      return this.map.size;
    }
  }

  it('uses a foreign-copy exec requestContext when no closure requestContext is provided', async () => {
    const foreignRC = new ForeignRequestContext();
    foreignRC.set('exec-key', 'exec-value');
    expect(foreignRC instanceof RequestContext).toBe(false);

    const receivedCtx: { requestContext?: RequestContext } = {};
    const execute = vi.fn().mockImplementation((_args: unknown, ctx: any) => {
      receivedCtx.requestContext = ctx.requestContext;
      return { result: 'ok' };
    });

    const testTool = createTool({
      id: 'test_tool',
      description: 'Test',
      inputSchema: z.object({}),
      execute,
    });

    const builder = new CoreToolBuilder({
      originalTool: testTool,
      options: {
        name: 'test_tool',
        logger: noopLogger,
      },
    });

    const builtTool = builder.build();
    await builtTool.execute!({}, { toolCallId: 'call-1', messages: [], requestContext: foreignRC as any });

    expect(receivedCtx.requestContext!.get('exec-key')).toBe('exec-value');
  });

  it('merges a foreign-copy exec requestContext with the closure requestContext', async () => {
    const closureRC = new RequestContext();
    closureRC.set('shared-key', 'from-closure');
    closureRC.set('closure-only-key', 'closure-only');

    const foreignRC = new ForeignRequestContext();
    foreignRC.set('shared-key', 'from-exec');
    foreignRC.set('exec-only-key', 42);
    expect(foreignRC instanceof RequestContext).toBe(false);

    const receivedCtx: { requestContext?: RequestContext } = {};
    const execute = vi.fn().mockImplementation((_args: unknown, ctx: any) => {
      receivedCtx.requestContext = ctx.requestContext;
      return { result: 'ok' };
    });

    const testTool = createTool({
      id: 'test_tool',
      description: 'Test',
      inputSchema: z.object({}),
      execute,
    });

    const builder = new CoreToolBuilder({
      originalTool: testTool,
      options: {
        name: 'test_tool',
        logger: noopLogger,
        requestContext: closureRC,
      },
    });

    const builtTool = builder.build();
    await builtTool.execute!({}, { toolCallId: 'call-1', messages: [], requestContext: foreignRC as any });

    const merged = receivedCtx.requestContext!;
    // Exec-only entries from the foreign copy are preserved
    expect(merged.get('exec-only-key')).toBe(42);
    // Execution-time application values win just as they do for a same-copy
    // RequestContext; only closure-owned infrastructure keys are protected.
    expect(merged.get('shared-key')).toBe('from-exec');
    expect(merged.get('closure-only-key')).toBe('closure-only');
  });

  it('passes a same-copy exec requestContext through by identity when no closure RC exists', async () => {
    const execRC = new RequestContext();
    execRC.set('exec-key', 'exec-value');

    const receivedCtx: { requestContext?: RequestContext } = {};
    const execute = vi.fn().mockImplementation((_args: unknown, ctx: any) => {
      receivedCtx.requestContext = ctx.requestContext;
      return { result: 'ok' };
    });

    const testTool = createTool({
      id: 'test_tool',
      description: 'Test',
      inputSchema: z.object({}),
      execute,
    });

    const builder = new CoreToolBuilder({
      originalTool: testTool,
      options: {
        name: 'test_tool',
        logger: noopLogger,
      },
    });

    const builtTool = builder.build();
    await builtTool.execute!({}, { toolCallId: 'call-1', messages: [], requestContext: execRC });

    expect(receivedCtx.requestContext).toBe(execRC);
  });
});

describe('CoreToolBuilder execution failures', () => {
  it('keeps author-returned ValidationError-shaped output as successful data', async () => {
    const authorOutput = {
      error: true as const,
      message: 'domain result, not framework validation',
      validationErrors: { errors: ['domain-status'], fields: {} },
    };
    const testTool = createTool({
      id: 'validation-shaped-output',
      description: 'Returns a domain object that resembles a validation error.',
      inputSchema: z.object({}),
      execute: async () => authorOutput,
    });
    const mockToolSpan = { end: vi.fn(), error: vi.fn(), update: vi.fn() };
    const mockAgentSpan = { createChildSpan: vi.fn().mockReturnValue(mockToolSpan) } as unknown as AnySpan;
    const builtTool = new CoreToolBuilder({
      originalTool: testTool,
      options: { name: 'validation-shaped-output', logger: noopLogger, tracingContext: { currentSpan: mockAgentSpan } },
    }).build();

    await expect(builtTool.execute!({}, { toolCallId: 'call-1', messages: [] })).resolves.toBe(authorOutput);
    expect(mockToolSpan.end).toHaveBeenCalledWith({ output: authorOutput, attributes: { success: true } });
    expect(mockToolSpan.error).not.toHaveBeenCalled();
  });

  it('validates suspension data exactly once before delegating', async () => {
    const transform = vi.fn((value: { answer: string }) => ({ answer: value.answer.toUpperCase() }));
    const suspend = vi.fn().mockResolvedValue(undefined);
    const testTool = createTool({
      id: 'single-suspend-validation',
      description: 'Suspends once.',
      inputSchema: z.object({}),
      suspendSchema: z.object({ answer: z.string() }).transform(transform),
      execute: async (_input, context) => {
        const suspendTool = (context as any).agent?.suspend ?? (context as any).suspend;
        await suspendTool({ answer: 'raw' });
      },
    });
    const builtTool = new CoreToolBuilder({
      originalTool: testTool,
      options: { name: 'single-suspend-validation', logger: noopLogger },
    }).build();

    await expect(
      builtTool.execute!({}, { toolCallId: 'call-1', messages: [], suspend } as any),
    ).resolves.toBeUndefined();
    expect(transform).toHaveBeenCalledTimes(1);
    // Suspension validation is a gate, not a transform contract.
    expect(suspend).toHaveBeenCalledWith({ answer: 'raw' }, expect.any(Object));
  });

  it('rejects invalid suspension data without invoking the delegate or exposing it to telemetry', async () => {
    const marker = 'SENSITIVE_INVALID_SUSPEND';
    const suspend = vi.fn().mockResolvedValue(undefined);
    const logger = { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn(), trackException: vi.fn() };
    const mockToolSpan = { end: vi.fn(), error: vi.fn(), update: vi.fn() };
    const mockAgentSpan = { createChildSpan: vi.fn().mockReturnValue(mockToolSpan) } as unknown as AnySpan;
    const testTool = createTool({
      id: 'invalid-suspend',
      description: 'Attempts an invalid suspension.',
      inputSchema: z.object({}),
      suspendSchema: z.object({ approved: z.boolean() }),
      execute: async (_input, context) => {
        const suspendTool = (context as any).agent?.suspend ?? (context as any).suspend;
        await suspendTool({ approved: marker });
      },
    });
    const builtTool = new CoreToolBuilder({
      originalTool: testTool,
      options: {
        name: 'invalid-suspend',
        logger: logger as any,
        tracingContext: { currentSpan: mockAgentSpan },
      },
    }).build();

    const result = await builtTool.execute!({}, { toolCallId: 'call-1', messages: [], suspend } as any);
    expect(result).toMatchObject({ error: true });
    expect(result.message).toContain(marker);
    expect(suspend).not.toHaveBeenCalled();
    expect(mockToolSpan.end).toHaveBeenCalledWith({ output: { error: true }, attributes: { success: false } });
    expect(
      JSON.stringify({ logs: Object.values(logger).map(mock => mock.mock.calls), spans: mockToolSpan }),
    ).not.toContain(marker);
  });

  it.each([false, 0, '', null])('treats falsy resume data %j as present', async resumeData => {
    const execute = vi.fn();
    const testTool = createTool({
      id: 'falsy-resume',
      description: 'Requires structured resume data.',
      inputSchema: z.object({}),
      resumeSchema: z.object({ approved: z.boolean() }),
      execute,
    });
    const builtTool = new CoreToolBuilder({
      originalTool: testTool,
      options: { name: 'falsy-resume', logger: noopLogger },
    }).build();

    await expect(
      builtTool.execute!({}, { toolCallId: 'call-1', messages: [], resumeData } as any),
    ).resolves.toMatchObject({ error: true });
    expect(execute).not.toHaveBeenCalled();
  });

  it('rejects invalid resume data before attaching it to telemetry', async () => {
    const marker = 'SENSITIVE_INVALID_RESUME';
    const execute = vi.fn();
    const logger = { debug: vi.fn(), info: vi.fn(), warn: vi.fn(), error: vi.fn(), trackException: vi.fn() };
    const mockToolSpan = { end: vi.fn(), error: vi.fn(), update: vi.fn() };
    const mockAgentSpan = { createChildSpan: vi.fn().mockReturnValue(mockToolSpan) } as unknown as AnySpan;
    const testTool = createTool({
      id: 'invalid-resume',
      description: 'Requires structured resume data.',
      inputSchema: z.object({}),
      resumeSchema: z.object({ approved: z.boolean() }),
      execute,
    });
    const builtTool = new CoreToolBuilder({
      originalTool: testTool,
      options: {
        name: 'invalid-resume',
        logger: logger as any,
        tracingContext: { currentSpan: mockAgentSpan },
      },
    }).build();

    const result = await builtTool.execute!({}, {
      toolCallId: 'call-1',
      messages: [],
      resumeData: { approved: marker },
    } as any);

    expect(result).toMatchObject({ error: true });
    expect(result.message).toContain(marker);
    expect(execute).not.toHaveBeenCalled();
    expect(mockToolSpan.update).not.toHaveBeenCalled();
    expect(mockToolSpan.end).toHaveBeenCalledWith({ output: { error: true }, attributes: { success: false } });
    expect(
      JSON.stringify({ logger: Object.values(logger).map(mock => mock.mock.calls), spans: mockToolSpan }),
    ).not.toContain(marker);
  });

  it('keeps invalid input in the returned validation error but excludes it from logs and spans', async () => {
    const marker = 'SENSITIVE_INVALID_TOOL_INPUT';
    const execute = vi.fn();
    const testTool = createTool({
      id: 'invalid_input_tool',
      description: 'Rejects string content',
      inputSchema: z.object({ content: z.number() }),
      execute,
    });
    const logger = {
      debug: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
      trackException: vi.fn(),
    };
    const mockToolSpan = {
      end: vi.fn(),
      error: vi.fn(),
      update: vi.fn(),
    };
    const mockAgentSpan = {
      createChildSpan: vi.fn().mockReturnValue(mockToolSpan),
    } as unknown as AnySpan;

    const builtTool = new CoreToolBuilder({
      originalTool: testTool,
      options: {
        name: 'invalid_input_tool',
        logger: logger as any,
        tracingContext: { currentSpan: mockAgentSpan },
      },
    }).build();

    const result = await builtTool.execute!({ content: marker } as any, { toolCallId: 'call-1', messages: [] });

    expect(result).toMatchObject({ error: true });
    expect(result.message).toContain(marker);
    expect(execute).not.toHaveBeenCalled();
    expect(mockToolSpan.update).not.toHaveBeenCalled();
    expect(mockToolSpan.end).toHaveBeenCalledWith({ output: { error: true }, attributes: { success: false } });

    const telemetry = {
      logger: Object.values(logger).flatMap(mock => mock.mock.calls),
      spans: [
        (mockAgentSpan.createChildSpan as ReturnType<typeof vi.fn>).mock.calls,
        mockToolSpan.update.mock.calls,
        mockToolSpan.end.mock.calls,
        mockToolSpan.error.mock.calls,
      ],
    };
    expect(JSON.stringify(telemetry)).not.toContain(marker);
  });

  it('does not copy raw tool args into logs, error details, or exception metadata', async () => {
    const marker = 'SENSITIVE_REPORT_CONTENT';
    const testTool = createTool({
      id: 'failing_tool',
      description: 'Always throws',
      inputSchema: z.object({ content: z.string() }),
      execute: async () => {
        throw new Error('boom');
      },
    });
    const logger = {
      debug: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
      trackException: vi.fn(),
    };

    const builder = new CoreToolBuilder({
      originalTool: testTool,
      options: {
        name: 'failing_tool',
        logger: logger as any,
      },
    });

    const builtTool = builder.build();
    let thrown: any;
    try {
      await builtTool.execute!({ content: marker }, { toolCallId: 'call-1', messages: [] });
    } catch (err) {
      thrown = err;
    }

    expect(thrown?.id).toBe('TOOL_EXECUTION_FAILED');
    expect(thrown.details.argsJson).toBeUndefined();
    expect(JSON.stringify(thrown.details)).not.toContain(marker);

    expect(logger.trackException).toHaveBeenCalledTimes(1);
    const metadata = logger.trackException.mock.calls[0]?.[1];
    expect(metadata).not.toHaveProperty('args');
    expect(JSON.stringify(metadata)).not.toContain(marker);

    for (const level of ['debug', 'info', 'warn', 'error'] as const) {
      for (const call of logger[level].mock.calls) {
        expect(JSON.stringify(call)).not.toContain(marker);
      }
    }
  });
});
