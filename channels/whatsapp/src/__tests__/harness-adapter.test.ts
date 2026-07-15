import { createHmac } from 'node:crypto';
import { describe, it, expect, vi } from 'vitest';

import { WhatsAppHarnessAdapter, WHATSAPP_PLATFORM } from '../harness-adapter';
import type { WhatsAppWebhookPayload } from '../types';

const APP_SECRET = 'test-app-secret';
const ACCESS_TOKEN = 'test-access-token';
const PHONE_NUMBER_ID = '123456789';

function makeAdapter(overrides: Partial<ConstructorParameters<typeof WhatsAppHarnessAdapter>[0]> = {}) {
  return new WhatsAppHarnessAdapter({
    appSecret: APP_SECRET,
    accessToken: ACCESS_TOKEN,
    phoneNumberId: PHONE_NUMBER_ID,
    verifyToken: 'my-verify-token',
    ...overrides,
  });
}

function textPayload(): WhatsAppWebhookPayload {
  return {
    object: 'whatsapp_business_account',
    entry: [
      {
        id: 'WABA-100',
        changes: [
          {
            field: 'messages',
            value: {
              messaging_product: 'whatsapp',
              metadata: { phone_number_id: PHONE_NUMBER_ID, display_phone_number: '15550001111' },
              contacts: [{ wa_id: '14155550000', profile: { name: 'Ada Lovelace' } }],
              messages: [
                {
                  from: '14155550000',
                  id: 'wamid.ABC123',
                  timestamp: '1700000000',
                  type: 'text',
                  text: { body: 'hello from whatsapp' },
                },
              ],
            },
          },
        ],
      },
    ],
  };
}

function sign(body: string): string {
  return `sha256=${createHmac('sha256', APP_SECRET).update(Buffer.from(body, 'utf8')).digest('hex')}`;
}

function inboundRequest(payload: unknown, opts: { sign?: boolean; signWith?: string } = {}) {
  const rawBody = JSON.stringify(payload);
  const signature =
    opts.sign === false
      ? undefined
      : `sha256=${createHmac('sha256', opts.signWith ?? APP_SECRET)
          .update(Buffer.from(rawBody, 'utf8'))
          .digest('hex')}`;
  return {
    method: 'POST',
    path: '/harness/primary/channels/whatsapp/inbound',
    headers: signature ? { 'X-Hub-Signature-256': signature } : {},
    rawBody,
    body: payload,
  };
}

const routeCtx = {
  harnessName: 'primary',
  channelId: 'whatsapp',
  providerId: 'whatsapp',
  platform: WHATSAPP_PLATFORM,
  provider: { id: 'whatsapp', getRoutes: () => [] },
  route: 'inbound' as const,
};

describe('WhatsAppHarnessAdapter.verifyInbound', () => {
  it('maps a correctly-signed text message into a full ChannelIngressEnvelope', async () => {
    const adapter = makeAdapter();
    const payload = textPayload();
    const envelope = await adapter.verifyInbound(inboundRequest(payload) as never, routeCtx as never);

    expect(envelope).toMatchObject({
      platform: 'whatsapp',
      conversationKind: 'dm',
      trigger: 'message',
      externalTenantId: 'WABA-100',
      externalChannelId: PHONE_NUMBER_ID,
      externalThreadId: '14155550000',
      externalMessageId: 'wamid.ABC123',
      content: 'hello from whatsapp',
      receivedAt: 1700000000 * 1000,
      actor: { platformUserId: '14155550000', displayName: 'Ada Lovelace' },
    });
    expect(envelope.raw).toEqual(payload);
  });

  it('verifies the signature over the EXACT raw bytes, not a re-serialization', async () => {
    const adapter = makeAdapter();
    const payload = textPayload();
    // Raw body with extra whitespace; signature computed over THESE bytes.
    const rawBody = JSON.stringify(payload, null, 2);
    const request = {
      method: 'POST',
      path: '/inbound',
      headers: { 'x-hub-signature-256': sign(rawBody) },
      rawBody,
      body: payload,
    };
    const envelope = await adapter.verifyInbound(request as never, routeCtx as never);
    expect(envelope.externalMessageId).toBe('wamid.ABC123');
  });

  it('throws on a tampered body (signature over original bytes)', async () => {
    const adapter = makeAdapter();
    const payload = textPayload();
    const original = JSON.stringify(payload);
    const tampered = original.replace('hello from whatsapp', 'evil injected text');
    const request = {
      method: 'POST',
      path: '/inbound',
      headers: { 'x-hub-signature-256': sign(original) },
      rawBody: tampered,
      body: JSON.parse(tampered),
    };
    await expect(adapter.verifyInbound(request as never, routeCtx as never)).rejects.toThrow(
      /signature verification failed/i,
    );
  });

  it('throws on a signature from the wrong app secret', async () => {
    const adapter = makeAdapter();
    const req = inboundRequest(textPayload(), { signWith: 'attacker-secret' });
    await expect(adapter.verifyInbound(req as never, routeCtx as never)).rejects.toThrow(
      /signature verification failed/i,
    );
  });

  it('throws on a missing signature header', async () => {
    const adapter = makeAdapter();
    const req = inboundRequest(textPayload(), { sign: false });
    await expect(adapter.verifyInbound(req as never, routeCtx as never)).rejects.toThrow(
      /signature verification failed/i,
    );
  });

  it('throws on a header missing the sha256= prefix', async () => {
    const adapter = makeAdapter();
    const payload = textPayload();
    const rawBody = JSON.stringify(payload);
    const hex = createHmac('sha256', APP_SECRET).update(Buffer.from(rawBody, 'utf8')).digest('hex');
    const request = {
      method: 'POST',
      path: '/inbound',
      headers: { 'x-hub-signature-256': hex }, // no sha256= prefix
      rawBody,
      body: payload,
    };
    await expect(adapter.verifyInbound(request as never, routeCtx as never)).rejects.toThrow(
      /signature verification failed/i,
    );
  });

  it('omits actor.displayName when no contact profile name is present', async () => {
    const adapter = makeAdapter();
    const payload = textPayload();
    payload.entry[0]!.changes[0]!.value.contacts = [{ wa_id: '14155550000' }];
    const envelope = await adapter.verifyInbound(inboundRequest(payload) as never, routeCtx as never);
    expect(envelope.actor).toEqual({ platformUserId: '14155550000' });
  });

  it('throws (no ingress) for a status-callback payload', async () => {
    const adapter = makeAdapter();
    const payload: WhatsAppWebhookPayload = {
      object: 'whatsapp_business_account',
      entry: [
        {
          id: 'WABA-100',
          changes: [
            {
              field: 'messages',
              value: {
                metadata: { phone_number_id: PHONE_NUMBER_ID },
                statuses: [
                  { id: 'wamid.X', status: 'delivered', timestamp: '1700000000', recipient_id: '14155550000' },
                ],
              },
            },
          ],
        },
      ],
    };
    await expect(adapter.verifyInbound(inboundRequest(payload) as never, routeCtx as never)).rejects.toThrow(
      /no inbound chat message/i,
    );
  });

  it('throws (no ingress) for a non-chat message type (image)', async () => {
    const adapter = makeAdapter();
    const payload = textPayload();
    payload.entry[0]!.changes[0]!.value.messages = [
      { from: '14155550000', id: 'wamid.IMG', timestamp: '1700000000', type: 'image' },
    ];
    await expect(adapter.verifyInbound(inboundRequest(payload) as never, routeCtx as never)).rejects.toThrow(
      /no inbound chat message/i,
    );
  });

  it('throws (no ingress) for a reaction message type', async () => {
    const adapter = makeAdapter();
    const payload = textPayload();
    payload.entry[0]!.changes[0]!.value.messages = [
      {
        from: '14155550000',
        id: 'wamid.REACT',
        timestamp: '1700000000',
        type: 'reaction',
        reaction: { message_id: 'wamid.X', emoji: '👍' },
      } as never,
    ];
    await expect(adapter.verifyInbound(inboundRequest(payload) as never, routeCtx as never)).rejects.toThrow(
      /no inbound chat message/i,
    );
  });

  it('throws when metadata.phone_number_id does not match the configured number', async () => {
    const adapter = makeAdapter();
    const payload = textPayload();
    payload.entry[0]!.changes[0]!.value.metadata = { phone_number_id: 'someone-elses-number' };
    await expect(adapter.verifyInbound(inboundRequest(payload) as never, routeCtx as never)).rejects.toThrow(
      /phone_number_id mismatch/i,
    );
  });

  it('parses the payload from the signed bytes, ignoring a mutated request.body', async () => {
    const adapter = makeAdapter();
    const signedPayload = textPayload();
    const rawBody = JSON.stringify(signedPayload);
    // A different object than the signed bytes (as a mutating middleware might supply).
    const mutatedBody = textPayload();
    mutatedBody.entry[0]!.changes[0]!.value.messages![0]!.text = { body: 'evil injected text' };
    const request = {
      method: 'POST',
      path: '/inbound',
      headers: { 'x-hub-signature-256': sign(rawBody) },
      rawBody,
      body: mutatedBody,
    };
    const envelope = await adapter.verifyInbound(request as never, routeCtx as never);
    expect(envelope.content).toBe('hello from whatsapp');
  });

  it('maps an interactive button_reply into a chat envelope (title as content, id in raw)', async () => {
    const adapter = makeAdapter();
    const payload = textPayload();
    payload.entry[0]!.changes[0]!.value.messages = [
      {
        from: '14155550000',
        id: 'wamid.BTN',
        timestamp: '1700000000',
        type: 'interactive',
        interactive: { type: 'button_reply', button_reply: { id: 'opt-yes', title: 'Yes, proceed' } },
      } as never,
    ];
    const envelope = await adapter.verifyInbound(inboundRequest(payload) as never, routeCtx as never);
    expect(envelope).toMatchObject({
      platform: 'whatsapp',
      conversationKind: 'dm',
      trigger: 'message',
      externalThreadId: '14155550000',
      externalMessageId: 'wamid.BTN',
      content: 'Yes, proceed',
      actor: { platformUserId: '14155550000', displayName: 'Ada Lovelace' },
    });
    const raw = (envelope.raw as WhatsAppWebhookPayload).entry[0]!.changes[0]!.value.messages![0]! as never as {
      interactive: { button_reply: { id: string } };
    };
    expect(raw.interactive.button_reply.id).toBe('opt-yes');
  });

  it('maps an interactive list_reply into a chat envelope (title as content, id+description in raw)', async () => {
    const adapter = makeAdapter();
    const payload = textPayload();
    payload.entry[0]!.changes[0]!.value.messages = [
      {
        from: '14155550000',
        id: 'wamid.LIST',
        timestamp: '1700000000',
        type: 'interactive',
        interactive: {
          type: 'list_reply',
          list_reply: { id: 'row-2', title: 'Standard shipping', description: '3-5 days' },
        },
      } as never,
    ];
    const envelope = await adapter.verifyInbound(inboundRequest(payload) as never, routeCtx as never);
    expect(envelope).toMatchObject({
      externalMessageId: 'wamid.LIST',
      content: 'Standard shipping',
    });
    const raw = (envelope.raw as WhatsAppWebhookPayload).entry[0]!.changes[0]!.value.messages![0]! as never as {
      interactive: { list_reply: { id: string; description?: string } };
    };
    expect(raw.interactive.list_reply.id).toBe('row-2');
    expect(raw.interactive.list_reply.description).toBe('3-5 days');
  });
});

describe('WhatsAppHarnessAdapter.verifyWebhookChallenge', () => {
  it('echoes the challenge for a matching verify token', () => {
    const adapter = makeAdapter({ verifyToken: 'tok-1' });
    const result = adapter.verifyWebhookChallenge({
      method: 'GET',
      path: '/inbound',
      headers: {},
      query: { 'hub.mode': 'subscribe', 'hub.verify_token': 'tok-1', 'hub.challenge': 'CHAL' },
    } as never);
    expect(result).toEqual({ ok: true, challenge: 'CHAL' });
  });

  it('parses the challenge from the URL when query is not pre-parsed', () => {
    const adapter = makeAdapter({ verifyToken: 'tok-1' });
    const result = adapter.verifyWebhookChallenge({
      method: 'GET',
      path: '/inbound',
      headers: {},
      url: 'https://example.com/inbound?hub.mode=subscribe&hub.verify_token=tok-1&hub.challenge=URLCHAL',
    } as never);
    expect(result).toEqual({ ok: true, challenge: 'URLCHAL' });
  });

  it('rejects a mismatched verify token', () => {
    const adapter = makeAdapter({ verifyToken: 'tok-1' });
    const result = adapter.verifyWebhookChallenge({
      method: 'GET',
      path: '/inbound',
      headers: {},
      query: { 'hub.mode': 'subscribe', 'hub.verify_token': 'WRONG', 'hub.challenge': 'x' },
    } as never);
    expect(result).toEqual({ ok: false, reason: 'token_mismatch' });
  });
});

describe('WhatsAppHarnessAdapter.deliver', () => {
  function outboxItem(payload: unknown) {
    return {
      target: { platform: 'whatsapp', externalThreadId: '14155550000' },
      payload,
    } as never;
  }

  const deliveryCtx = {} as never;

  it('POSTs the right URL, auth header, and body, returning messages[0].id', async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ messages: [{ id: 'wamid.SENT-1' }] }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      }),
    );
    const adapter = makeAdapter({ fetch: fetchMock as never, apiVersion: 'v21.0' });

    const result = await adapter.deliver(outboxItem({ text: 'reply body' }), deliveryCtx);

    expect(result.providerMessageId).toBe('wamid.SENT-1');
    expect(result.providerReceipt).toEqual({ providerMessageId: 'wamid.SENT-1' });

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, init] = fetchMock.mock.calls[0]!;
    expect(url).toBe(`https://graph.facebook.com/v21.0/${PHONE_NUMBER_ID}/messages`);
    expect(init.method).toBe('POST');
    expect(init.headers.Authorization).toBe(`Bearer ${ACCESS_TOKEN}`);
    expect(JSON.parse(init.body)).toEqual({
      messaging_product: 'whatsapp',
      recipient_type: 'individual',
      to: '14155550000',
      type: 'text',
      text: { body: 'reply body' },
    });
  });

  it('accepts a bare-string payload', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(new Response(JSON.stringify({ messages: [{ id: 'wamid.S2' }] }), { status: 200 }));
    const adapter = makeAdapter({ fetch: fetchMock as never });
    const result = await adapter.deliver(outboxItem('plain string body'), deliveryCtx);
    expect(result.providerMessageId).toBe('wamid.S2');
    expect(JSON.parse(fetchMock.mock.calls[0]![1].body).text.body).toBe('plain string body');
  });

  it('surfaces a Graph API error response as a throw', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(
        new Response(JSON.stringify({ error: { message: 'Invalid OAuth access token', code: 190 } }), { status: 401 }),
      );
    const adapter = makeAdapter({ fetch: fetchMock as never });
    await expect(adapter.deliver(outboxItem({ text: 'x' }), deliveryCtx)).rejects.toThrow(
      /WhatsApp send failed \(HTTP 401\).*Invalid OAuth access token.*code 190/,
    );
  });

  it('uses the configured api version and base url', async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValue(new Response(JSON.stringify({ messages: [{ id: 'm' }] }), { status: 200 }));
    const adapter = makeAdapter({
      fetch: fetchMock as never,
      apiVersion: 'v19.0',
      graphApiBaseUrl: 'https://graph.facebook.example/',
    });
    await adapter.deliver(outboxItem({ text: 'x' }), deliveryCtx);
    expect(fetchMock.mock.calls[0]![0]).toBe(`https://graph.facebook.example/v19.0/${PHONE_NUMBER_ID}/messages`);
  });

  it('declares at-least-once delivery semantics', () => {
    const adapter = makeAdapter();
    expect(adapter.deliverySemantics).toBe('at-least-once');
  });
});
