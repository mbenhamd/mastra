/**
 * DurableAgent RequestContext Tests
 *
 * Tests for RequestContext reserved keys and security features.
 * Validates that middleware can securely set resourceId and threadId
 * via reserved keys that take precedence over client-provided values.
 */

import type { LanguageModelV2 } from '@ai-sdk/provider-v5';
import { MockLanguageModelV2, convertArrayToReadableStream } from '@internal/ai-sdk-v5/test';
import { describe, it, expect, beforeEach, afterEach } from 'vitest';
import { z } from 'zod';
import { EventEmitterPubSub } from '../../../events/event-emitter';
import {
  MASTRA_AUTH_TOKEN_KEY,
  MASTRA_RESOURCE_ID_KEY,
  MASTRA_THREAD_ID_KEY,
  RequestContext,
} from '../../../request-context';
import { createTool } from '../../../tools';
import { Agent } from '../../agent';
import { createDurableAgent } from '../create-durable-agent';
import { globalRunRegistry } from '../run-registry';

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * Creates a simple text model
 */
function createTextModel(text: string) {
  return new MockLanguageModelV2({
    doGenerate: async () => ({
      rawCall: { rawPrompt: null, rawSettings: {} },
      finishReason: 'stop',
      usage: { inputTokens: 10, outputTokens: 20, totalTokens: 30 },
      content: [{ type: 'text', text }],
      warnings: [],
    }),
    doStream: async () => ({
      stream: convertArrayToReadableStream([
        { type: 'stream-start', warnings: [] },
        { type: 'response-metadata', id: 'id-0', modelId: 'mock-model-id', timestamp: new Date(0) },
        { type: 'text-start', id: 'text-1' },
        { type: 'text-delta', id: 'text-1', delta: text },
        { type: 'text-end', id: 'text-1' },
        {
          type: 'finish',
          finishReason: 'stop',
          usage: { inputTokens: 10, outputTokens: 20, totalTokens: 30 },
        },
      ]),
      rawCall: { rawPrompt: null, rawSettings: {} },
      warnings: [],
    }),
  });
}

// ============================================================================
// DurableAgent RequestContext Tests
// ============================================================================

describe('DurableAgent RequestContext reserved keys', () => {
  let pubsub: EventEmitterPubSub;

  beforeEach(() => {
    pubsub = new EventEmitterPubSub();
  });

  afterEach(async () => {
    await pubsub.close();
  });

  describe('basic RequestContext handling', () => {
    it('rejects an until-idle stream before resolving dynamic memory', async () => {
      let memoryResolverCalls = 0;
      const baseAgent = new Agent({
        id: 'until-idle-preflight-agent',
        name: 'Until Idle Preflight Agent',
        instructions: 'Test authorization ordering.',
        model: createTextModel('must not run') as LanguageModelV2,
        requestContextSchema: z.object({ principal: z.string() }),
        memory: () => {
          memoryResolverCalls += 1;
          return undefined;
        },
      });
      const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

      await expect(
        durableAgent.stream('Hello', {
          untilIdle: true,
          requestContext: new RequestContext(),
        }),
      ).rejects.toThrow();

      expect(memoryResolverCalls).toBe(0);
    });

    it('uses dynamic-default request context for durable preparation preflight and persistence', async () => {
      const defaultRequestContext = new RequestContext();
      defaultRequestContext.set('principal', 'default-user');
      const baseAgent = new Agent({
        id: 'default-context-prepare-agent',
        name: 'Default Context Prepare Agent',
        instructions: 'Test default context preparation.',
        model: createTextModel('prepared') as LanguageModelV2,
        requestContextSchema: z.object({ principal: z.string() }),
        defaultOptions: () => ({ requestContext: defaultRequestContext }),
      });
      const durableAgent = createDurableAgent({
        agent: baseAgent,
        pubsub,
        durableRequestContextKeys: ['principal'],
      });

      const prepared = await durableAgent.prepare('Hello');

      expect(prepared.workflowInput.requestContextEntries).toMatchObject({
        principal: 'default-user',
      });
      prepared.cleanup();
    });

    it('should accept requestContext option in prepare', async () => {
      const mockModel = createTextModel('Hello!');

      const baseAgent = new Agent({
        id: 'request-context-agent',
        name: 'RequestContext Agent',
        instructions: 'Test requestContext',
        model: mockModel as LanguageModelV2,
      });
      const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

      const requestContext = new RequestContext();
      requestContext.set('customKey', 'customValue');

      const result = await durableAgent.prepare('Hello', {
        requestContext,
      });

      expect(result.runId).toBeDefined();
      result.cleanup();
    });

    it('should accept requestContext option in stream', async () => {
      const mockModel = createTextModel('Hello!');

      const baseAgent = new Agent({
        id: 'stream-request-context-agent',
        name: 'Stream RequestContext Agent',
        instructions: 'Test requestContext',
        model: mockModel as LanguageModelV2,
      });
      const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

      const requestContext = new RequestContext();
      requestContext.set('userInfo', { role: 'admin' });

      const { runId, cleanup } = await durableAgent.stream('Hello', {
        requestContext,
      });

      expect(runId).toBeDefined();
      await globalRunRegistry.get(runId)?.workflowExecution;
      cleanup();
    });
  });

  describe('reserved keys for security', () => {
    it('should use mastra__resourceId and mastra__threadId from RequestContext', async () => {
      const mockModel = createTextModel('Hello!');

      const baseAgent = new Agent({
        id: 'reserved-keys-agent',
        name: 'Reserved Keys Agent',
        instructions: 'Test reserved keys',
        model: mockModel as LanguageModelV2,
      });
      const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

      const requestContext = new RequestContext();
      requestContext.set(MASTRA_RESOURCE_ID_KEY, 'context-user-123');
      requestContext.set(MASTRA_THREAD_ID_KEY, 'context-thread-456');

      const result = await durableAgent.prepare('Hello', {
        requestContext,
        // Not passing memory options - should use RequestContext values
      });

      expect(result.runId).toBeDefined();
      // The requestContext is passed through for runtime use
      result.cleanup();
    });

    it('should handle RequestContext with memory options', async () => {
      const mockModel = createTextModel('Hello!');

      const baseAgent = new Agent({
        id: 'context-memory-agent',
        name: 'Context Memory Agent',
        instructions: 'Test context with memory',
        model: mockModel as LanguageModelV2,
      });
      const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

      const requestContext = new RequestContext();
      requestContext.set(MASTRA_RESOURCE_ID_KEY, 'middleware-user');
      requestContext.set(MASTRA_THREAD_ID_KEY, 'middleware-thread');

      const result = await durableAgent.prepare('Hello', {
        requestContext,
        memory: {
          thread: 'body-thread',
          resource: 'body-resource',
        },
      });

      // Reserved middleware identity is canonical during preparation as well as runtime.
      expect(result.threadId).toBe('middleware-thread');
      expect(result.resourceId).toBe('middleware-user');
      expect(result.workflowInput.state).toMatchObject({
        threadId: 'middleware-thread',
        resourceId: 'middleware-user',
      });
      expect(result.workflowInput.messageListState.memoryInfo).toEqual({
        threadId: 'middleware-thread',
        resourceId: 'middleware-user',
      });
      result.cleanup();
    });
  });

  describe('RequestContext with tools', () => {
    it('should pass requestContext to tool execute', async () => {
      let receivedRequestContext: unknown = undefined;
      let releaseFirstCall!: () => void;
      const firstCallGate = new Promise<void>(resolve => {
        releaseFirstCall = resolve;
      });

      // Model that calls a tool on first invocation, then returns text
      let callCount = 0;
      const mockModel = new MockLanguageModelV2({
        doStream: async () => {
          callCount++;
          if (callCount === 1) {
            await firstCallGate;
            return {
              stream: convertArrayToReadableStream([
                { type: 'stream-start', warnings: [] },
                { type: 'response-metadata', id: 'id-0', modelId: 'mock-model-id', timestamp: new Date(0) },
                {
                  type: 'tool-call',
                  toolCallType: 'function',
                  toolCallId: 'call-1',
                  toolName: 'contextTool',
                  input: JSON.stringify({ data: 'test' }),
                  providerExecuted: false,
                },
                {
                  type: 'finish',
                  finishReason: 'tool-calls',
                  usage: { inputTokens: 10, outputTokens: 20, totalTokens: 30 },
                },
              ]),
              rawCall: { rawPrompt: null, rawSettings: {} },
              warnings: [],
            };
          }
          return {
            stream: convertArrayToReadableStream([
              { type: 'stream-start', warnings: [] },
              { type: 'response-metadata', id: 'id-1', modelId: 'mock-model-id', timestamp: new Date(0) },
              { type: 'text-start', id: 'text-1' },
              { type: 'text-delta', id: 'text-1', delta: 'Done' },
              { type: 'text-end', id: 'text-1' },
              {
                type: 'finish',
                finishReason: 'stop',
                usage: { inputTokens: 10, outputTokens: 5, totalTokens: 15 },
              },
            ]),
            rawCall: { rawPrompt: null, rawSettings: {} },
            warnings: [],
          };
        },
      });

      const contextTool = createTool({
        id: 'contextTool',
        description: 'A tool that captures requestContext',
        inputSchema: z.object({ data: z.string() }),
        execute: async (input, context) => {
          // Capture the requestContext passed to the tool
          receivedRequestContext = context?.requestContext;
          return { data: input.data };
        },
      });

      const baseAgent = new Agent({
        id: 'tool-context-agent',
        name: 'Tool Context Agent',
        instructions: 'Use tools with context',
        model: mockModel as LanguageModelV2,
        tools: { contextTool },
      });
      const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

      const requestContext = new RequestContext();
      const policy = { role: 'admin' };
      requestContext.set('userId', 'user-123');
      requestContext.set('policy', policy);

      // Stream to actually execute the tool
      const { cleanup } = await durableAgent.stream('Use the tool', {
        requestContext,
      });
      policy.role = 'user';
      requestContext.set('userId', 'substituted-user');
      releaseFirstCall();

      // Wait for execution to complete
      await new Promise(resolve => setTimeout(resolve, 200));
      cleanup();

      // Verify requestContext was passed through to tool.execute()
      expect(receivedRequestContext).toBeDefined();
      expect((receivedRequestContext as RequestContext).get('userId')).toBe('user-123');
      expect((receivedRequestContext as RequestContext).get('policy')).toEqual({ role: 'admin' });
    });
  });

  describe('RequestContext serialization', () => {
    it('snapshots JSON-safe requestContext entries for parity with the non-durable agent', async () => {
      const mockModel = createTextModel('Hello!');

      const baseAgent = new Agent({
        id: 'serialize-context-agent',
        name: 'Serialize Context Agent',
        instructions: 'Test serialization',
        model: mockModel as LanguageModelV2,
      });
      const durableAgent = createDurableAgent({
        agent: baseAgent,
        pubsub,
        durableRequestContextKeys: ['userId'],
      });

      const requestContext = new RequestContext();
      requestContext.set('userId', 'user-123');
      requestContext.set('MastraMemory', {
        thread: { id: 'parent-thread' },
        resourceId: 'parent-resource',
        memoryConfig: { readOnly: false },
      });
      // Non-allowlisted values remain live-only, regardless of whether they
      // are serializable.
      requestContext.set('liveHandle', () => 'not-serializable');

      const result = await durableAgent.prepare('Hello', {
        requestContext,
      });

      // The serializable subset of requestContext is snapshotted on workflow
      // input so durable scorers can see customContext, mirroring the
      // non-durable agent which forwards Object.fromEntries(requestContext.entries())
      // to scorers. The full RequestContext (which can hold live handles) is
      // not serialized — it stays on the run registry.
      //
      // The snapshot is taken *before* preparation mutates the request context
      // (e.g. adding MASTRA_VERSIONS_KEY / MastraMemory), so persisted
      // customContext must reflect only caller-provided entries.
      const entries = (result.workflowInput as { requestContextEntries?: Record<string, unknown> })
        .requestContextEntries;
      expect(entries).toEqual({ userId: 'user-123' });
      result.cleanup();
    });

    it('never persists auth tokens or arbitrary application secrets by default', async () => {
      const baseAgent = new Agent({
        id: 'secure-context-agent',
        name: 'Secure Context Agent',
        instructions: 'Test secure durable context defaults',
        model: createTextModel('Hello!') as LanguageModelV2,
      });
      const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });
      const requestContext = new RequestContext();
      requestContext.set(MASTRA_AUTH_TOKEN_KEY, 'Bearer raw-secret-token');
      requestContext.set('credentials', {
        accessToken: 'nested-secret-token',
        refreshToken: 'nested-refresh-token',
      });
      requestContext.set('userId', 'user-123');

      const result = await durableAgent.prepare('Hello', { requestContext });

      expect(result.workflowInput.requestContextEntries).toBeUndefined();
      expect(result.workflowInput.requiredRequestContextCapabilities).toEqual({ authToken: true });
      expect(globalRunRegistry.get(result.runId)?.requestContext?.get(MASTRA_AUTH_TOKEN_KEY)).toBe(
        'Bearer raw-secret-token',
      );
      result.cleanup();
    });

    it('persists only explicitly allowlisted non-secret application references', async () => {
      const baseAgent = new Agent({
        id: 'allowlisted-context-agent',
        name: 'Allowlisted Context Agent',
        instructions: 'Test explicit durable context persistence',
        model: createTextModel('Hello!') as LanguageModelV2,
      });
      const durableAgent = createDurableAgent({
        agent: baseAgent,
        pubsub,
        durableRequestContextKeys: ['userId', 'connectionRef'],
      });
      const requestContext = new RequestContext();
      requestContext.set('userId', 'user-123');
      requestContext.set('connectionRef', { provider: 'drive', id: 'connection-456' });
      requestContext.set('credentials', { accessToken: 'must-not-persist' });

      const result = await durableAgent.prepare('Hello', { requestContext });

      expect(result.workflowInput.requestContextEntries).toEqual({
        userId: 'user-123',
        connectionRef: { provider: 'drive', id: 'connection-456' },
      });
      result.cleanup();
    });

    it('rejects infrastructure keys even when explicitly allowlisted', async () => {
      const baseAgent = new Agent({
        id: 'forbidden-context-agent',
        name: 'Forbidden Context Agent',
        instructions: 'Test forbidden durable context persistence',
        model: createTextModel('Hello!') as LanguageModelV2,
      });
      const durableAgent = createDurableAgent({
        agent: baseAgent,
        pubsub,
        durableRequestContextKeys: [MASTRA_AUTH_TOKEN_KEY],
      });
      const requestContext = new RequestContext();
      requestContext.set(MASTRA_AUTH_TOKEN_KEY, 'Bearer raw-secret-token');

      await expect(durableAgent.prepare('Hello', { requestContext })).rejects.toMatchObject({
        id: 'DURABLE_AGENT_REQUEST_CONTEXT_INFRASTRUCTURE_KEY_FORBIDDEN',
      });
    });

    it.each([
      'credentials',
      'accessToken',
      'refresh_token',
      'oauthToken',
      'token',
      'apiKey',
      'accessKey',
      'clientSecret',
      'private-key',
      'signingKey',
      'headers',
      'password',
    ])('rejects credential-like key %s even when explicitly allowlisted', async credentialKey => {
      const baseAgent = new Agent({
        id: `forbidden-credential-context-${credentialKey}`,
        name: 'Forbidden Credential Context Agent',
        instructions: 'Test forbidden durable credential persistence',
        model: createTextModel('Hello!') as LanguageModelV2,
      });
      const durableAgent = createDurableAgent({
        agent: baseAgent,
        pubsub,
        durableRequestContextKeys: [credentialKey],
      });
      const requestContext = new RequestContext();
      requestContext.set(credentialKey, 'must-not-persist');

      await expect(durableAgent.prepare('Hello', { requestContext })).rejects.toMatchObject({
        id: 'DURABLE_AGENT_REQUEST_CONTEXT_CREDENTIAL_KEY_FORBIDDEN',
      });
    });

    it('does not mistake non-credential token metadata for a credential', async () => {
      const baseAgent = new Agent({
        id: 'safe-token-metadata-context-agent',
        name: 'Safe Token Metadata Context Agent',
        instructions: 'Test non-credential durable context persistence',
        model: createTextModel('Hello!') as LanguageModelV2,
      });
      const durableAgent = createDurableAgent({
        agent: baseAgent,
        pubsub,
        durableRequestContextKeys: ['tokenBudget', 'connectionId'],
      });
      const requestContext = new RequestContext();
      requestContext.set('tokenBudget', 4_096);
      requestContext.set('connectionId', 'connection-123');

      const result = await durableAgent.prepare('Hello', { requestContext });
      expect(result.workflowInput.requestContextEntries).toEqual({
        tokenBudget: 4_096,
        connectionId: 'connection-123',
      });
      result.cleanup();
    });

    it('fails closed when an allowlisted value is not serializable or exceeds its bound', async () => {
      const baseAgent = new Agent({
        id: 'invalid-context-agent',
        name: 'Invalid Context Agent',
        instructions: 'Test bounded durable context persistence',
        model: createTextModel('Hello!') as LanguageModelV2,
      });
      const durableAgent = createDurableAgent({
        agent: baseAgent,
        pubsub,
        durableRequestContextKeys: ['applicationRef'],
      });
      const nonSerializableContext = new RequestContext();
      nonSerializableContext.set('applicationRef', () => 'live-only');

      await expect(durableAgent.prepare('Hello', { requestContext: nonSerializableContext })).rejects.toMatchObject({
        id: 'DURABLE_AGENT_REQUEST_CONTEXT_VALUE_NOT_SERIALIZABLE',
      });

      const oversizedContext = new RequestContext();
      oversizedContext.set('applicationRef', 'x'.repeat(8_193));
      await expect(durableAgent.prepare('Hello', { requestContext: oversizedContext })).rejects.toMatchObject({
        id: 'DURABLE_AGENT_REQUEST_CONTEXT_VALUE_TOO_LARGE',
      });
    });
  });
});

describe('DurableAgent RequestContext edge cases', () => {
  let pubsub: EventEmitterPubSub;

  beforeEach(() => {
    pubsub = new EventEmitterPubSub();
  });

  afterEach(async () => {
    await pubsub.close();
  });

  it('should handle empty RequestContext', async () => {
    const mockModel = createTextModel('Hello!');

    const baseAgent = new Agent({
      id: 'empty-context-agent',
      name: 'Empty Context Agent',
      instructions: 'Test empty context',
      model: mockModel as LanguageModelV2,
    });
    const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

    const requestContext = new RequestContext();
    // Empty context - no values set

    const result = await durableAgent.prepare('Hello', {
      requestContext,
    });

    expect(result.runId).toBeDefined();
    result.cleanup();
  });

  it('should handle RequestContext with complex values', async () => {
    const mockModel = createTextModel('Hello!');

    const baseAgent = new Agent({
      id: 'complex-context-agent',
      name: 'Complex Context Agent',
      instructions: 'Test complex context',
      model: mockModel as LanguageModelV2,
    });
    const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

    const requestContext = new RequestContext();
    requestContext.set('user', {
      id: 'user-123',
      roles: ['admin', 'user'],
      metadata: {
        lastLogin: new Date().toISOString(),
        preferences: { theme: 'dark' },
      },
    });

    const result = await durableAgent.prepare('Hello', {
      requestContext,
    });

    expect(result.runId).toBeDefined();
    result.cleanup();
  });

  it('should handle undefined requestContext', async () => {
    const mockModel = createTextModel('Hello!');

    const baseAgent = new Agent({
      id: 'undefined-context-agent',
      name: 'Undefined Context Agent',
      instructions: 'Test undefined context',
      model: mockModel as LanguageModelV2,
    });
    const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

    const result = await durableAgent.prepare('Hello', {
      // requestContext is not provided
    });

    expect(result.runId).toBeDefined();
    result.cleanup();
  });

  it('should handle RequestContext with special characters in keys', async () => {
    const mockModel = createTextModel('Hello!');

    const baseAgent = new Agent({
      id: 'special-keys-agent',
      name: 'Special Keys Agent',
      instructions: 'Test special keys',
      model: mockModel as LanguageModelV2,
    });
    const durableAgent = createDurableAgent({ agent: baseAgent, pubsub });

    const requestContext = new RequestContext();
    requestContext.set('key-with-dashes', 'value1');
    requestContext.set('key_with_underscores', 'value2');
    requestContext.set('key.with.dots', 'value3');

    const result = await durableAgent.prepare('Hello', {
      requestContext,
    });

    expect(result.runId).toBeDefined();
    result.cleanup();
  });
});
