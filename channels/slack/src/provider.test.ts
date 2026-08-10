import { createHmac } from 'node:crypto';
import { AgentChannels } from '@mastra/core/channels';
import { describe, it, expect, vi } from 'vitest';

import { SlackProvider, stripTrailingSlash, resolveSlackAdapterConfig } from './provider';

describe('controller channels replacement', () => {
  it('closes the replaced AgentControllerChannels exactly once before setChannels', async () => {
    // A controller that already has channels (e.g. a persistent Discord
    // gateway adapter configured directly on the controller). When Slack
    // restores an installation for it, the provider builds a fresh
    // AgentControllerChannels — the replaced instance must be closed first or
    // its gateway connections and thread subscriptions leak.
    const closeSpy = vi.fn();
    const existingChannels = {
      adapters: { discord: { name: 'discord' } },
      channelConfig: {
        adapters: { discord: { name: 'discord' } },
        userName: 'controller-1',
      },
      close: closeSpy,
    };
    const setChannelsSpy = vi.fn();
    const controller = {
      id: 'controller-1',
      getChannels: () => existingChannels,
      setChannels: setChannelsSpy,
    };

    const installationRecord = {
      id: 'install-1',
      platform: 'slack',
      agentId: 'controller-1',
      status: 'active',
      webhookId: 'wh-1',
      configHash: 'hash-1',
      createdAt: new Date(),
      updatedAt: new Date(),
      data: {
        ownerType: 'agentController',
        appId: 'A0001',
        clientId: 'client-1',
        clientSecret: 'client-secret-1',
        signingSecret: 'signing-secret-1',
        teamId: 'T0001',
        botToken: 'xoxb-test-token',
        botUserId: 'U0001',
      },
    };
    const channelsStorage = {
      getConfig: async () => null,
      listInstallations: async () => [installationRecord],
    };
    const mastra = {
      getStorage: () => ({
        init: async () => {},
        getStore: async () => channelsStorage,
      }),
      getServer: () => undefined,
      getAgentControllerById: () => controller,
    };

    const initializeSpy = vi.spyOn(AgentChannels.prototype, 'initialize').mockResolvedValue(undefined);
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
    try {
      const provider = new SlackProvider({ state: {} as never });
      provider.__attach(mastra as never);
      await provider.initialize();
    } finally {
      initializeSpy.mockRestore();
      errorSpy.mockRestore();
      logSpy.mockRestore();
    }

    // The stale instance was closed exactly once, BEFORE the replacement was
    // installed on the controller.
    expect(closeSpy).toHaveBeenCalledTimes(1);
    expect(setChannelsSpy).toHaveBeenCalledTimes(1);
    expect(closeSpy.mock.invocationCallOrder[0]!).toBeLessThan(setChannelsSpy.mock.invocationCallOrder[0]!);

    // Superset merge preserved: the replacement still carries the existing
    // Discord adapter alongside the new Slack adapter.
    const replacement = setChannelsSpy.mock.calls[0]![0] as {
      adapters: Record<string, unknown>;
    };
    expect(Object.keys(replacement.adapters).sort()).toEqual(['discord', 'slack']);
  });
});

describe('agent channels replacement', () => {
  it('awaits the replaced AgentChannels close before setChannels', async () => {
    let releaseClose!: () => void;
    const closeFinished = new Promise<void>(resolve => {
      releaseClose = resolve;
    });
    let markCloseStarted!: () => void;
    const closeStarted = new Promise<void>(resolve => {
      markCloseStarted = resolve;
    });
    const closeSpy = vi.fn(async () => {
      markCloseStarted();
      await closeFinished;
    });
    const existingChannels = { adapters: {}, channelConfig: { adapters: {} }, close: closeSpy };
    let currentChannels: unknown = existingChannels;
    const setChannelsSpy = vi.fn(channels => {
      currentChannels = channels;
    });
    const agent = {
      id: 'agent-1',
      name: 'Agent One',
      getChannels: () => currentChannels,
      setChannels: setChannelsSpy,
      getDescription: () => 'Agent One',
    };
    const installationRecord = {
      id: 'install-1',
      platform: 'slack',
      agentId: 'agent-1',
      status: 'active',
      webhookId: 'wh-1',
      configHash: 'hash-1',
      createdAt: new Date(),
      updatedAt: new Date(),
      data: {
        ownerType: 'agent',
        appId: 'A0001',
        clientId: 'client-1',
        clientSecret: 'client-secret-1',
        signingSecret: 'signing-secret-1',
        teamId: 'T0001',
        botToken: 'xoxb-test-token',
        botUserId: 'U0001',
      },
    };
    const channelsStorage = {
      getConfig: async () => null,
      listInstallations: async () => [installationRecord],
    };
    const mastra = {
      getStorage: () => ({
        init: async () => {},
        getStore: async () => channelsStorage,
      }),
      getServer: () => undefined,
      getAgentById: () => agent,
    };

    const initializeSpy = vi.spyOn(AgentChannels.prototype, 'initialize').mockResolvedValue(undefined);
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
    try {
      const provider = new SlackProvider({ state: {} as never });
      provider.__attach(mastra as never);
      const initializing = provider.initialize();
      await closeStarted;
      expect(setChannelsSpy).not.toHaveBeenCalled();
      releaseClose();
      await initializing;
    } finally {
      initializeSpy.mockRestore();
      errorSpy.mockRestore();
      logSpy.mockRestore();
    }

    expect(closeSpy).toHaveBeenCalledTimes(1);
    expect(setChannelsSpy).toHaveBeenCalledTimes(1);
    expect(closeSpy.mock.invocationCallOrder[0]!).toBeLessThan(setChannelsSpy.mock.invocationCallOrder[0]!);
  });
});

describe('concurrent agent channels replacement', () => {
  it('shares one close/create/initialize/install transition across concurrent webhook requests', async () => {
    let releaseClose!: () => void;
    let markCloseStarted!: () => void;
    const closeFinished = new Promise<void>(resolve => {
      releaseClose = resolve;
    });
    const closeStarted = new Promise<void>(resolve => {
      markCloseStarted = resolve;
    });
    let blockClose = false;
    const closeSpy = vi.fn(async () => {
      if (!blockClose) return;
      markCloseStarted();
      await closeFinished;
    });
    const staleChannels = { adapters: {}, channelConfig: { adapters: {} }, close: closeSpy };
    let currentChannels: any = staleChannels;
    const setChannelsSpy = vi.fn(channels => {
      currentChannels = channels;
    });
    const agent = {
      id: 'agent-concurrent',
      name: 'Concurrent Agent',
      getChannels: () => currentChannels,
      setChannels: setChannelsSpy,
      getDescription: () => 'Concurrent Agent',
    };
    const installationRecord = {
      id: 'install-concurrent',
      platform: 'slack',
      agentId: 'agent-concurrent',
      status: 'active',
      webhookId: 'wh-concurrent',
      configHash: 'hash-concurrent',
      createdAt: new Date(),
      updatedAt: new Date(),
      data: {
        ownerType: 'agent',
        appId: 'A0002',
        clientId: 'client-2',
        clientSecret: 'client-secret-2',
        signingSecret: 'signing-secret-2',
        teamId: 'T0002',
        botToken: 'xoxb-concurrent-token',
        botUserId: 'U0002',
      },
    };
    const channelsStorage = {
      getConfig: async () => null,
      listInstallations: async () => [installationRecord],
      getInstallationByWebhookId: async () => installationRecord,
    };
    const mastra = {
      getStorage: () => ({
        init: async () => {},
        getStore: async () => channelsStorage,
      }),
      getServer: () => undefined,
      getAgentById: () => agent,
    };

    const initializeSpy = vi.spyOn(AgentChannels.prototype, 'initialize').mockResolvedValue(undefined);
    const webhookSpy = vi
      .spyOn(AgentChannels.prototype, 'handleWebhookEvent')
      .mockResolvedValue(new Response(JSON.stringify({ ok: true })));
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
    try {
      const provider = new SlackProvider({ state: {} as never });
      provider.__attach(mastra as never);
      await provider.initialize();

      // Recreate a stale owner after startup so two route requests observe the
      // same instance while its close is deliberately held at a barrier.
      currentChannels = staleChannels;
      closeSpy.mockClear();
      setChannelsSpy.mockClear();
      initializeSpy.mockClear();
      blockClose = true;

      const eventRoute = provider.getRoutes().find(route => route.path === '/slack/events/:webhookId')!;
      const eventHandler = await eventRoute.createHandler!({ mastra } as never);
      const body = JSON.stringify({ type: 'event_callback', event: { type: 'message' } });
      const timestamp = String(Math.floor(Date.now() / 1000));
      const signature = `v0=${createHmac('sha256', 'signing-secret-2')
        .update(`v0:${timestamp}:${body}`)
        .digest('hex')}`;
      const makeContext = () => {
        const headers = new Headers({
          'content-type': 'application/json',
          'x-slack-request-timestamp': timestamp,
          'x-slack-signature': signature,
        });
        return {
          req: {
            url: 'https://example.test/slack/events/wh-concurrent',
            method: 'POST',
            raw: { headers },
            param: (name: string) => (name === 'webhookId' ? 'wh-concurrent' : undefined),
            text: async () => body,
            header: (name: string) => headers.get(name) ?? undefined,
          },
          json: (value: unknown, status = 200) =>
            new Response(JSON.stringify(value), { status, headers: { 'content-type': 'application/json' } }),
        };
      };

      const first = eventHandler(makeContext() as never);
      await closeStarted;
      const second = eventHandler(makeContext() as never);
      releaseClose();
      await Promise.all([first, second]);

      expect(closeSpy).toHaveBeenCalledTimes(1);
      expect(initializeSpy).toHaveBeenCalledTimes(1);
      expect(setChannelsSpy).toHaveBeenCalledTimes(1);
      expect(webhookSpy).toHaveBeenCalledTimes(2);
    } finally {
      initializeSpy.mockRestore();
      webhookSpy.mockRestore();
      errorSpy.mockRestore();
      logSpy.mockRestore();
    }
  });
});

describe('connect object form', () => {
  it('requires a name when connecting without an agent id', async () => {
    const provider = new SlackProvider();
    await expect(
      // @ts-expect-error deliberately omitting the required name to exercise the runtime guard
      provider.connect({ id: 'controller-1' }),
    ).rejects.toThrow(/"name" is required/);
  });
});

describe('stripTrailingSlash', () => {
  it('removes a single trailing slash', () => {
    expect(stripTrailingSlash('https://mastra-demo.calebbarnes.ca/')).toBe('https://mastra-demo.calebbarnes.ca');
  });

  it('removes multiple trailing slashes', () => {
    expect(stripTrailingSlash('https://example.com///')).toBe('https://example.com');
  });

  it('leaves a URL without a trailing slash unchanged', () => {
    expect(stripTrailingSlash('https://example.com')).toBe('https://example.com');
  });

  it('preserves path segments and only strips the trailing slash', () => {
    expect(stripTrailingSlash('https://example.com/base/')).toBe('https://example.com/base');
  });

  it('produces a clean OAuth callback URL when joined', () => {
    const baseUrl = stripTrailingSlash('https://mastra-demo.calebbarnes.ca/');
    expect(`${baseUrl}/slack/oauth/callback`).toBe('https://mastra-demo.calebbarnes.ca/slack/oauth/callback');
  });
});

describe('resolveSlackAdapterConfig', () => {
  it('carries textFormat through to the resolved adapter config', () => {
    const resolved = resolveSlackAdapterConfig({ textFormat: 'plain' });
    expect(resolved.textFormat).toBe('plain');
  });

  it('omits textFormat when unset so the core default governs', () => {
    const resolved = resolveSlackAdapterConfig({});
    expect('textFormat' in resolved).toBe(false);
  });

  it('maps top-level and nested legacy cards before applying Slack defaults', () => {
    expect(resolveSlackAdapterConfig({ cards: false } as any).toolDisplay).toBe('text');
    expect(resolveSlackAdapterConfig({ adapterConfig: { cards: true } } as any).toolDisplay).toBe('cards');
  });

  it('maps a legacy formatToolCall callback and strips legacy fields from the resolved config', () => {
    const formatToolCall = vi.fn(() => ({ text: 'legacy result' }));
    const resolved = resolveSlackAdapterConfig({ formatToolCall } as any);
    const render = resolved.toolDisplay as (event: unknown) => unknown;

    expect(
      render({
        kind: 'result',
        toolName: 'read_file',
        displayName: 'Read file',
        args: { path: '/tmp/a' },
        result: 'ok',
      }),
    ).toEqual({ kind: 'post', message: { text: 'legacy result' } });
    expect(formatToolCall).toHaveBeenCalledWith({
      toolName: 'Read file',
      args: { path: '/tmp/a' },
      result: 'ok',
      isError: undefined,
    });
    expect('cards' in resolved).toBe(false);
    expect('formatToolCall' in resolved).toBe(false);
  });

  it('keeps explicit modern toolDisplay authoritative over legacy fields', () => {
    const resolved = resolveSlackAdapterConfig({ toolDisplay: 'grouped', cards: false } as any);
    expect(resolved.toolDisplay).toBe('grouped');
  });
});
