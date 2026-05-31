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

function interactiveReplyPayload(
  reply:
    | { kind: 'button'; id: string; title: string }
    | { kind: 'list'; id: string; title: string; description?: string },
  overrides: { messageId?: string } = {},
): WhatsAppWebhookPayload {
  const interactive =
    reply.kind === 'button'
      ? { type: 'button_reply', button_reply: { id: reply.id, title: reply.title } }
      : {
          type: 'list_reply',
          list_reply: { id: reply.id, title: reply.title, ...(reply.description !== undefined ? { description: reply.description } : {}) },
        };
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
                  id: overrides.messageId ?? 'wamid.INT-INTERACTIVE',
                  timestamp: '1700001234',
                  type: 'interactive',
                  interactive,
                } as never,
              ],
            },
          },
        ],
      },
    ],
  };
}

/** A status-callback (delivery receipt) payload — NOT chat ingress; must be rejected. */
function statusCallbackPayload(): WhatsAppWebhookPayload {
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
              statuses: [{ id: 'wamid.INT-1', status: 'delivered', timestamp: '1700001234', recipient_id: '14155551234' }],
            },
          },
        ],
      },
    ],
  };
}

/** A non-chat message type (a reaction) — must be rejected (not a chat turn). */
function reactionPayload(): WhatsAppWebhookPayload {
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
                  id: 'wamid.INT-REACT',
                  timestamp: '1700001234',
                  type: 'reaction',
                  reaction: { message_id: 'wamid.INT-1', emoji: '👍' },
                } as never,
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

  it('maps an interactive button_reply into a chat ingress envelope (title as content, id in raw)', async () => {
    const { harness, adapter } = setup();
    const payload = interactiveReplyPayload({ kind: 'button', id: 'opt-yes', title: 'Yes, proceed' });
    const req = signedRequest(payload);
    const ctx = harnessRouteContext(harness);

    const envelope = await adapter.verifyInbound(
      { method: 'POST', path: '/inbound', headers: req.headers, rawBody: req.rawBody, body: payload } as never,
      ctx,
    );

    expect(envelope).toMatchObject({
      platform: WHATSAPP_PLATFORM,
      conversationKind: 'dm',
      trigger: 'message',
      externalTenantId: 'WABA-INT',
      externalChannelId: PHONE_NUMBER_ID,
      externalThreadId: '14155551234',
      externalMessageId: 'wamid.INT-INTERACTIVE',
      content: 'Yes, proceed',
      actor: { platformUserId: '14155551234', displayName: 'Grace Hopper' },
      receivedAt: 1700001234 * 1000,
    });
    // The developer-set reply id rides along on the raw payload for menu routing.
    const rawMsg = (envelope.raw as WhatsAppWebhookPayload).entry[0]!.changes[0]!.value.messages![0]! as never as {
      interactive: { button_reply: { id: string } };
    };
    expect(rawMsg.interactive.button_reply.id).toBe('opt-yes');
  });

  it('maps an interactive list_reply into a chat ingress envelope (title as content, id+description in raw)', async () => {
    const { harness, adapter } = setup();
    const payload = interactiveReplyPayload({ kind: 'list', id: 'row-2', title: 'Standard shipping', description: '3-5 days' });
    const req = signedRequest(payload);
    const ctx = harnessRouteContext(harness);

    const envelope = await adapter.verifyInbound(
      { method: 'POST', path: '/inbound', headers: req.headers, rawBody: req.rawBody, body: payload } as never,
      ctx,
    );

    expect(envelope).toMatchObject({
      platform: WHATSAPP_PLATFORM,
      conversationKind: 'dm',
      trigger: 'message',
      externalThreadId: '14155551234',
      externalMessageId: 'wamid.INT-INTERACTIVE',
      content: 'Standard shipping',
      actor: { platformUserId: '14155551234' },
    });
    const rawMsg = (envelope.raw as WhatsAppWebhookPayload).entry[0]!.changes[0]!.value.messages![0]! as never as {
      interactive: { list_reply: { id: string; description?: string } };
    };
    expect(rawMsg.interactive.list_reply.id).toBe('row-2');
    expect(rawMsg.interactive.list_reply.description).toBe('3-5 days');
  });

  it('rejects a status-callback payload as non-chat ingress (delivery receipts are not turns)', async () => {
    const { harness, adapter } = setup();
    const payload = statusCallbackPayload();
    const req = signedRequest(payload);
    const ctx = harnessRouteContext(harness);
    await expect(
      adapter.verifyInbound(
        { method: 'POST', path: '/inbound', headers: req.headers, rawBody: req.rawBody, body: payload } as never,
        ctx,
      ),
    ).rejects.toThrow(/no inbound chat message/i);
  });

  it('rejects a non-chat message type (reaction) as non-chat ingress', async () => {
    const { harness, adapter } = setup();
    const payload = reactionPayload();
    const req = signedRequest(payload);
    const ctx = harnessRouteContext(harness);
    await expect(
      adapter.verifyInbound(
        { method: 'POST', path: '/inbound', headers: req.headers, rawBody: req.rawBody, body: payload } as never,
        ctx,
      ),
    ).rejects.toThrow(/no inbound chat message/i);
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

  // -------------------------------------------------------------------------
  // SIGNAL delivery: a real signed WhatsApp webhook STEERS an active run as a
  // signal (delivery:'signal', signalId persisted) instead of being queued as a
  // separate turn. The delivery mode is chosen by the ingress POLICY
  // (resolveResource → admission.delivery), NOT by the adapter — so the same
  // verified WhatsApp envelope reaches the §14.2 signal admission path.
  // Mirrors packages/core/src/harness/v1/harness.test.ts:5050-5075.
  // -------------------------------------------------------------------------

  /** Capture the latest durable inbox row per id (storage exposes no get-by-id). */
  function setupSignal() {
    const adapter = new WhatsAppHarnessAdapter(adapterConfig());
    const composite = new InMemoryStore();
    const storage = composite.stores.harness;
    const rows = new Map<string, { delivery?: string; runId?: string; signalId?: string; queuedItemId?: string; acceptedAt?: number; status: string }>();
    const realUpdate = (storage as unknown as { updateChannelInboxItem: (...a: never[]) => Promise<void> }).updateChannelInboxItem.bind(storage);
    (storage as unknown as { updateChannelInboxItem: unknown }).updateChannelInboxItem = async (record: { id: string } & Record<string, unknown>, opts: { claimId: string }) => {
      await realUpdate(record as never, opts as never);
      rows.set(record.id, { ...(record as never) });
    };
    const channel: HarnessChannelConfig = {
      providerId: WHATSAPP_PLATFORM,
      platform: WHATSAPP_PLATFORM,
      adapter,
      ingress: {
        // The POLICY selects signal delivery (steer an active run), independent of the adapter.
        resolveResource: async ctx => ({ resourceId: ctx.externalThreadId, mode: 'per-user-resource', admission: { delivery: 'signal' } }),
      },
    };
    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage } as never,
      channels: { whatsapp: channel },
    });
    new Mastra({
      agents: { default: new Agent({ id: 'default', name: 'default', instructions: 'test', model: 'openai/gpt-4o-mini' as never }) },
      storage: composite,
      channels: { whatsapp: whatsappProvider() },
      harnesses: { primary: harness },
    });
    return { harness, rows };
  }

  it("admits a real signed WhatsApp webhook as a SIGNAL (delivery:'signal', signalId persisted) when the policy selects signal delivery", async () => {
    const { harness, rows } = setupSignal();
    const req = signedRequest(messageEventPayload());

    const result = await harness.handleChannelInboundRequest('whatsapp', req as never, { continueAdmission: true });

    // A signal admission accepts synchronously (200) with status 'accepted', NOT 'queued'.
    expect(result).toMatchObject({ kind: 'ok', ackStatus: 200, status: 'accepted', duplicate: false });
    expect((result as { sessionId?: string }).sessionId).toMatch(/^chs:/);
    // queue-only field is absent on a signal admission.
    expect((result as { queuedItemId?: string }).queuedItemId).toBeUndefined();

    const inboxItemId = (result as { inboxItemId: string }).inboxItemId;
    const row = rows.get(inboxItemId);
    expect(row?.delivery).toBe('signal');
    expect(typeof row?.runId).toBe('string');
    expect(typeof row?.signalId).toBe('string');
    expect(row?.acceptedAt).toBeDefined();
    // a signal row never carries a queued item id.
    expect(row?.queuedItemId).toBeUndefined();
  });
});
