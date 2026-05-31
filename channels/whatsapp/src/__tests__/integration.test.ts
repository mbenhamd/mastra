import { createHmac } from 'node:crypto';
import { Agent } from '@mastra/core/agent';
import type { ChannelProvider } from '@mastra/core/channels';
import { Harness } from '@mastra/core/harness/v1';
import type {
  HarnessChannelConfig,
  HarnessChannelRouteContext,
  HarnessChannelTransportRequest,
} from '@mastra/core/harness/v1';
import { Mastra } from '@mastra/core';
import { InMemoryStore } from '@mastra/core/storage';
import { describe, it, expect } from 'vitest';

import { WhatsAppHarnessAdapter, WHATSAPP_PLATFORM } from '../harness-adapter';
import type { WhatsAppWebhookPayload } from '../types';

// -----------------------------------------------------------------------------
// Integration coverage.
//
// This wires a REAL `WhatsAppHarnessAdapter` into a REAL `Harness` + `Mastra`
// (public surface only) and drives a real signed WhatsApp Cloud API webhook all
// the way through `harness.handleChannelInboundRequest(...)` to FULL admission
// (`kind:'ok'` → `status:'queued'`). The in-memory `HarnessStorage` is reached
// through `InMemoryStore` from `@mastra/core/storage` (its `stores.harness`
// domain), exactly like the Slack adapter's integration test.
// -----------------------------------------------------------------------------

const APP_SECRET = 'integration-app-secret';
const PHONE_NUMBER_ID = '555000111';

function adapterConfig() {
  return {
    appSecret: APP_SECRET,
    accessToken: 'integration-access-token',
    phoneNumberId: PHONE_NUMBER_ID,
    verifyToken: 'integration-verify-token',
  };
}

function whatsappProvider(): ChannelProvider {
  return { id: WHATSAPP_PLATFORM, getRoutes: () => [] };
}

function channelConfig(adapter: WhatsAppHarnessAdapter): HarnessChannelConfig {
  return {
    providerId: WHATSAPP_PLATFORM,
    platform: WHATSAPP_PLATFORM,
    adapter,
    ingress: {
      // per-user-resource binding: WhatsApp DMs are 1:1, so the wa_id is the
      // natural resource key.
      resolveResource: async ctx => ({ resourceId: ctx.externalThreadId, mode: 'per-user-resource' }),
    },
  };
}

function messageEventPayload(overrides: { messageId?: string; body?: string } = {}): WhatsAppWebhookPayload {
  return {
    object: 'whatsapp_business_account',
    entry: [
      {
        id: 'WABA-INT',
        changes: [
          {
            field: 'messages',
            value: {
              messaging_product: 'whatsapp',
              metadata: { phone_number_id: PHONE_NUMBER_ID, display_phone_number: '15550009999' },
              contacts: [{ wa_id: '14155551234', profile: { name: 'Grace Hopper' } }],
              messages: [
                {
                  from: '14155551234',
                  id: overrides.messageId ?? 'wamid.INT-1',
                  timestamp: '1700001234',
                  type: 'text',
                  text: { body: overrides.body ?? 'integration hello' },
                },
              ],
            },
          },
        ],
      },
    ],
  };
}

/** Compute the WhatsApp `X-Hub-Signature-256` over the raw bytes, like Meta does. */
function signWhatsApp(rawBody: string, secret = APP_SECRET): string {
  return `sha256=${createHmac('sha256', secret).update(Buffer.from(rawBody, 'utf8')).digest('hex')}`;
}

/** Build a transport request carrying a correctly-signed WhatsApp JSON body. */
function signedRequest(
  payload: WhatsAppWebhookPayload,
  opts: { signature?: string; secret?: string } = {},
): HarnessChannelTransportRequest {
  const rawBody = JSON.stringify(payload);
  const signature = opts.signature ?? signWhatsApp(rawBody, opts.secret);
  return {
    method: 'POST',
    path: '/harness/primary/channels/whatsapp/inbound',
    headers: {
      'content-type': 'application/json',
      'x-hub-signature-256': signature,
    },
    rawBody,
  };
}

// ---------------------------------------------------------------------------
// 1. Adapter wired into the real route context (envelope projection layer)
// ---------------------------------------------------------------------------

describe('WhatsAppHarnessAdapter — real Harness/Mastra route context', () => {
  function setup() {
    const adapter = new WhatsAppHarnessAdapter(adapterConfig());
    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      channels: { whatsapp: channelConfig(adapter) },
    });
    new Mastra({
      agents: { default: new Agent({ id: 'default', name: 'default', instructions: 'test', model: 'openai/gpt-4o-mini' as never }) },
      channels: { whatsapp: whatsappProvider() },
      harnesses: { primary: harness },
    });
    return { harness, adapter };
  }

  // The harness resolves the WhatsApp ChannelProvider against the platform tag.
  // This is the registry boundary `handleChannelInboundRequest` crosses before
  // it invokes the adapter's `verifyInbound`.
  function harnessRouteContext(harness: Harness): HarnessChannelRouteContext {
    const provider = harness.mastra.getChannelProvider(WHATSAPP_PLATFORM);
    expect(provider).toBeDefined();
    return {
      harnessName: 'primary',
      channelId: 'whatsapp',
      providerId: WHATSAPP_PLATFORM,
      platform: WHATSAPP_PLATFORM,
      provider: provider!,
      route: 'inbound',
    };
  }

  it('registers the WhatsApp channel + provider so the route context resolves', () => {
    const { harness } = setup();
    const provider = harness.mastra.getChannelProvider(WHATSAPP_PLATFORM);
    expect(provider?.id).toBe(WHATSAPP_PLATFORM);
  });

  it('admits a correctly-signed inbound through the real route context → ingress envelope', async () => {
    const { harness, adapter } = setup();
    const payload = messageEventPayload();
    const req = signedRequest(payload);
    const ctx = harnessRouteContext(harness);

    const envelope = await adapter.verifyInbound(
      {
        method: 'POST',
        path: '/harness/primary/channels/whatsapp/inbound',
        headers: req.headers,
        rawBody: req.rawBody,
        body: payload,
      } as never,
      ctx,
    );

    // The envelope is admission-ready: every field the §14.2 admission core /
    // `resolveResource` consumes is present and correct.
    expect(envelope).toMatchObject({
      platform: WHATSAPP_PLATFORM,
      conversationKind: 'dm',
      trigger: 'message',
      externalTenantId: 'WABA-INT',
      externalChannelId: PHONE_NUMBER_ID,
      externalThreadId: '14155551234',
      externalMessageId: 'wamid.INT-1',
      content: 'integration hello',
      actor: { platformUserId: '14155551234', displayName: 'Grace Hopper' },
      receivedAt: 1700001234 * 1000,
    });

    // resolveResource (the channel's ingress policy) keys on externalThreadId.
    const config = (harness as unknown as { _channelRegistry: { getConfig: (id: string) => HarnessChannelConfig } })._channelRegistry.getConfig('whatsapp');
    const resolved = await config.ingress.resolveResource({ ...envelope, harnessName: 'primary', channelId: 'whatsapp', providerId: WHATSAPP_PLATFORM } as never);
    expect(resolved).toEqual({ resourceId: '14155551234', mode: 'per-user-resource' });
  });

  it('rejects a tampered inbound at the adapter verification boundary', async () => {
    const { harness, adapter } = setup();
    const payload = messageEventPayload();
    const rawBody = JSON.stringify(payload);
    const signature = signWhatsApp(rawBody);
    const ctx = harnessRouteContext(harness);
    const tampered = rawBody.replace('integration hello', 'tampered');

    // The harness wraps this throw into verify_failed/401 (redacted message).
    await expect(
      adapter.verifyInbound(
        {
          method: 'POST',
          path: '/inbound',
          headers: { 'x-hub-signature-256': signature },
          rawBody: tampered,
          body: JSON.parse(tampered),
        } as never,
        ctx,
      ),
    ).rejects.toThrow(/signature verification failed/i);
  });
});

// ---------------------------------------------------------------------------
// 2. Integration — real Harness driven by a real signed WhatsApp webhook
//    all the way to admission (ingress → admission → queued)
// ---------------------------------------------------------------------------

describe('WhatsAppHarnessAdapter integration (ingress → admission via Harness)', () => {
  function setup() {
    const adapter = new WhatsAppHarnessAdapter(adapterConfig());
    const composite = new InMemoryStore();
    const storage = composite.stores.harness;
    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage } as never,
      channels: { whatsapp: channelConfig(adapter) },
    });
    new Mastra({
      agents: { default: new Agent({ id: 'default', name: 'default', instructions: 'test', model: 'openai/gpt-4o-mini' as never }) },
      storage: composite,
      channels: { whatsapp: whatsappProvider() },
      harnesses: { primary: harness },
    });
    return { harness, adapter };
  }

  it('flows a real signed WhatsApp message through to a record-only ACK (202 received)', async () => {
    const { harness } = setup();
    const req = signedRequest(messageEventPayload());
    const result = await harness.handleChannelInboundRequest('whatsapp', req as never);
    expect(result).toMatchObject({ kind: 'ok', ackStatus: 202, status: 'received', duplicate: false });
  });

  it('flows a real signed WhatsApp message through full admission (200 queued) with continueAdmission', async () => {
    const { harness } = setup();
    const req = signedRequest(messageEventPayload());
    const result = await harness.handleChannelInboundRequest('whatsapp', req as never, { continueAdmission: true });
    expect(result).toMatchObject({ kind: 'ok', ackStatus: 200, status: 'queued', duplicate: false });
    expect((result as { sessionId?: string }).sessionId).toMatch(/^chs:/);
  });

  it('rejects a tampered webhook with verify_failed (401), redacting the raw cause', async () => {
    const { harness } = setup();
    const req = signedRequest(messageEventPayload());
    req.rawBody = String(req.rawBody) + 'X';
    const result = await harness.handleChannelInboundRequest('whatsapp', req as never);
    expect(result).toMatchObject({ kind: 'verify_failed', httpStatus: 401, error: { code: 'harness.permission_denied' } });
    const message = (result as { error: { message: string } }).error.message;
    expect(message).not.toContain('signature');
  });

  it('treats an exact provider retry of the same signed event as a duplicate', async () => {
    const { harness } = setup();
    const payload = messageEventPayload();
    const first = await harness.handleChannelInboundRequest('whatsapp', signedRequest(payload) as never, { continueAdmission: true });
    const second = await harness.handleChannelInboundRequest('whatsapp', signedRequest(payload) as never, { continueAdmission: true });
    expect(first).toMatchObject({ kind: 'ok', duplicate: false });
    expect(second).toMatchObject({ kind: 'ok', duplicate: true });
  });
});
