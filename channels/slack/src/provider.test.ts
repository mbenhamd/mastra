import { describe, it, expect, vi } from 'vitest';

import { SlackProvider, stripTrailingSlash } from './provider';

describe('controller channels replacement', () => {
  it('closes the replaced AgentControllerChannels exactly once before setChannels', async () => {
    // A controller that already has channels (e.g. a persistent Discord
    // gateway adapter configured directly on the controller). When Slack
    // restores an installation for it, the provider builds a fresh
    // AgentControllerChannels — the replaced instance must be closed first or
    // its gateway connections and thread subscriptions leak.
    const closeSpy = vi.fn();
    const existingChannels = {
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

    // Custom `state` keeps AgentControllerChannels.initialize() off Mastra
    // storage; initialize failures are caught per-installation by the restore
    // loop anyway, so the close/replace assertions never depend on chat-sdk.
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {});
    const logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
    try {
      const provider = new SlackProvider({ state: {} as never });
      provider.__attach(mastra as never);
      await provider.initialize();
    } finally {
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
