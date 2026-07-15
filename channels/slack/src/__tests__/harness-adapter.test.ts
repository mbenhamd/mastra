import { createHmac } from 'crypto';

import { Mastra } from '@mastra/core/mastra';
import { Harness } from '@mastra/core/harness/v1';
import type {
  ChannelOutboxItem,
  HarnessChannelDeliveryContext,
  HarnessChannelRouteContext,
  HarnessChannelTransportRequest,
} from '@mastra/core/harness/v1';
import type { ChannelProvider } from '@mastra/core/channels';
import { InMemoryStore } from '@mastra/core/storage';
import { Agent } from '@mastra/core/agent';
import { describe, it, expect, vi } from 'vitest';

import { SlackHarnessAdapter, SlackInboundVerificationError, SlackUrlVerificationChallenge } from '../harness-adapter';

const SIGNING_SECRET = 'test-signing-secret';
const BOT_TOKEN = 'xoxb-test-token';
const BOT_USER_ID = 'U0BOT';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Compute the `v0=` Slack signature for a body + timestamp, like Slack does. */
function signSlack(body: string, timestamp: string, secret = SIGNING_SECRET): string {
  const base = `v0:${timestamp}:${body}`;
  return `v0=${createHmac('sha256', secret).update(base).digest('hex')}`;
}

/** Build a transport request carrying a correctly-signed Slack JSON body. */
function signedRequest(
  payload: unknown,
  opts: { timestamp?: string; secret?: string; signature?: string; signAt?: string } = {},
): HarnessChannelTransportRequest {
  const body = JSON.stringify(payload);
  const timestamp = opts.timestamp ?? String(Math.floor(Date.now() / 1000));
  const signature = opts.signature ?? signSlack(body, opts.signAt ?? timestamp, opts.secret);
  return {
    method: 'POST',
    path: '/harness/primary/channels/support/inbound',
    headers: {
      'content-type': 'application/json',
      'x-slack-signature': signature,
      'x-slack-request-timestamp': timestamp,
    },
    rawBody: body,
  };
}

const routeCtx = {
  harnessName: 'primary',
  channelId: 'support',
  providerId: 'slack',
  platform: 'slack',
  route: 'inbound',
} as unknown as HarnessChannelRouteContext;

const deliveryCtx = {} as unknown as HarnessChannelDeliveryContext;

/** A `fetch`-shaped mock so `.mock.calls[0]` is typed as `[url, init]`. */
function mockFetch(responder: () => Response) {
  return vi.fn((_url: string | URL | Request, _init?: RequestInit): Promise<Response> => Promise.resolve(responder()));
}

function messageEventPayload(overrides: Record<string, unknown> = {}) {
  return {
    type: 'event_callback',
    team_id: 'T123',
    api_app_id: 'A123',
    event_id: 'Ev123',
    event_time: 1700000000,
    event: {
      type: 'message',
      channel: 'C123',
      channel_type: 'channel',
      user: 'U999',
      text: 'hello agent',
      ts: '1700000000.000100',
      client_msg_id: 'cmid-1',
      team: 'T123',
      ...overrides,
    },
  };
}

// ---------------------------------------------------------------------------
// 1. verifyInbound
// ---------------------------------------------------------------------------

describe('SlackHarnessAdapter.verifyInbound', () => {
  const adapter = new SlackHarnessAdapter({
    signingSecret: SIGNING_SECRET,
    botToken: BOT_TOKEN,
    botUserId: BOT_USER_ID,
  });

  it('maps a correctly-signed channel message into a full ingress envelope', async () => {
    const payload = messageEventPayload();
    const env = await adapter.verifyInbound(signedRequest(payload), routeCtx);

    expect(env).toMatchObject({
      platform: 'slack',
      conversationKind: 'channel',
      trigger: 'message',
      externalTenantId: 'T123',
      externalChannelId: 'C123',
      externalThreadId: '1700000000.000100', // ts (no thread_ts → starts thread)
      externalMessageId: 'Ev123', // event_id wins
      content: 'hello agent',
      actor: { platformUserId: 'U999' },
      receivedAt: 1700000000 * 1000,
    });
    expect(env.raw).toEqual(payload);
  });

  it('maps an app_mention to trigger "mention"', async () => {
    const payload = messageEventPayload({ type: 'app_mention', text: '<@U0BOT> hi' });
    const env = await adapter.verifyInbound(signedRequest(payload), routeCtx);
    expect(env.trigger).toBe('mention');
    expect(env.content).toBe('<@U0BOT> hi');
  });

  it('classifies a DM (channel_type "im") as conversationKind "dm"', async () => {
    const env = await adapter.verifyInbound(signedRequest(messageEventPayload({ channel_type: 'im' })), routeCtx);
    expect(env.conversationKind).toBe('dm');
  });

  it('classifies a reply (thread_ts != ts) as conversationKind "thread" and threads on thread_ts', async () => {
    const env = await adapter.verifyInbound(
      signedRequest(messageEventPayload({ thread_ts: '1699999999.000001', ts: '1700000000.000100' })),
      routeCtx,
    );
    expect(env.conversationKind).toBe('thread');
    expect(env.externalThreadId).toBe('1699999999.000001');
  });

  it('falls back externalMessageId to client_msg_id then ts when event_id is absent', async () => {
    const payload = messageEventPayload();
    delete (payload as any).event_id;
    const env = await adapter.verifyInbound(signedRequest(payload), routeCtx);
    expect(env.externalMessageId).toBe('cmid-1');
  });

  it('throws on a tampered body (signature no longer matches)', async () => {
    const req = signedRequest(messageEventPayload());
    // Mutate the body AFTER signing so the HMAC no longer verifies.
    req.rawBody = String(req.rawBody) + ' ';
    await expect(adapter.verifyInbound(req, routeCtx)).rejects.toBeInstanceOf(SlackInboundVerificationError);
  });

  it('throws on a wrong signature', async () => {
    const req = signedRequest(messageEventPayload(), { signature: 'v0=deadbeef' });
    await expect(adapter.verifyInbound(req, routeCtx)).rejects.toBeInstanceOf(SlackInboundVerificationError);
  });

  it('throws on an expired timestamp (>5 min skew)', async () => {
    const expired = String(Math.floor(Date.now() / 1000) - 600);
    // Sign over the expired timestamp so the signature itself is valid; only the
    // replay window check (inside verifySlackRequest) should reject it.
    const req = signedRequest(messageEventPayload(), { timestamp: expired, signAt: expired });
    await expect(adapter.verifyInbound(req, routeCtx)).rejects.toBeInstanceOf(SlackInboundVerificationError);
  });

  it('throws on missing signature headers (non-handshake)', async () => {
    const req = signedRequest(messageEventPayload());
    delete req.headers['x-slack-signature'];
    await expect(adapter.verifyInbound(req, routeCtx)).rejects.toBeInstanceOf(SlackInboundVerificationError);
  });

  it('surfaces the url_verification challenge (signed)', async () => {
    const req = signedRequest({ type: 'url_verification', challenge: 'abc123' });
    await expect(adapter.verifyInbound(req, routeCtx)).rejects.toMatchObject({
      name: 'SlackUrlVerificationChallenge',
      challenge: 'abc123',
    });
  });

  it('accepts the unsigned setup-time url_verification handshake', async () => {
    const req: HarnessChannelTransportRequest = {
      method: 'POST',
      path: '/inbound',
      headers: { 'content-type': 'application/json' },
      rawBody: JSON.stringify({ type: 'url_verification', challenge: 'setup-ping' }),
    };
    await expect(adapter.verifyInbound(req, routeCtx)).rejects.toBeInstanceOf(SlackUrlVerificationChallenge);
  });

  it("emits an empty-content envelope for the bot's own message (no agent turn)", async () => {
    const env = await adapter.verifyInbound(signedRequest(messageEventPayload({ user: BOT_USER_ID })), routeCtx);
    expect(env.content).toBe('');
    // Routing identity preserved so the delivery is still recorded/deduped.
    expect(env.externalChannelId).toBe('C123');
  });

  it('emits empty-content for a bot_id-stamped message', async () => {
    const env = await adapter.verifyInbound(
      signedRequest(messageEventPayload({ bot_id: 'B1', user: undefined })),
      routeCtx,
    );
    expect(env.content).toBe('');
  });

  it('emits empty-content for a non message/app_mention event', async () => {
    const env = await adapter.verifyInbound(
      signedRequest({
        type: 'event_callback',
        team_id: 'T1',
        event_id: 'Ev9',
        event: { type: 'reaction_added', item: {} },
      }),
      routeCtx,
    );
    expect(env.content).toBe('');
  });
});

// ---------------------------------------------------------------------------
// 2. deliver
// ---------------------------------------------------------------------------

describe('SlackHarnessAdapter.deliver', () => {
  function outboxItem(overrides: Partial<ChannelOutboxItem> = {}): ChannelOutboxItem {
    return {
      id: 'ob-1',
      harnessName: 'primary',
      channelId: 'support',
      providerId: 'slack',
      bindingId: 'b1',
      bindingGeneration: 1,
      idempotencyKey: 'k1',
      resourceId: 'r1',
      threadId: 't1',
      target: {
        platform: 'slack',
        externalTenantId: 'T123',
        externalChannelId: 'C123',
        externalThreadId: '1700000000.000100',
      },
      kind: 'assistant-message',
      operationKind: 'message-create',
      payload: { text: 'reply from agent' },
      payloadHash: 'h1',
      deliverySemantics: 'at-least-once',
      status: 'claimed',
      attempts: 0,
      createdAt: 1,
      updatedAt: 1,
      ...overrides,
    } as ChannelOutboxItem;
  }

  it('posts to the right channel + thread and returns the ts as providerMessageId', async () => {
    const fetchImpl = mockFetch(
      () =>
        new Response(JSON.stringify({ ok: true, ts: '1700000000.000200', channel: 'C123' }), {
          status: 200,
          headers: { 'content-type': 'application/json' },
        }),
    );
    const adapter = new SlackHarnessAdapter({ signingSecret: SIGNING_SECRET, botToken: BOT_TOKEN, fetchImpl });

    const result = await adapter.deliver(outboxItem(), deliveryCtx);

    expect(fetchImpl).toHaveBeenCalledOnce();
    const [url, init] = fetchImpl.mock.calls[0]!;
    expect(url).toBe('https://slack.com/api/chat.postMessage');
    expect(init!.headers).toMatchObject({ Authorization: `Bearer ${BOT_TOKEN}` });
    const sent = JSON.parse(init!.body as string);
    expect(sent).toMatchObject({ channel: 'C123', text: 'reply from agent', thread_ts: '1700000000.000100' });

    expect(result.providerMessageId).toBe('1700000000.000200');
    expect(result.providerReceipt).toMatchObject({ providerMessageId: '1700000000.000200', providerThreadId: 'C123' });
  });

  it('accepts a bare string payload', async () => {
    const fetchImpl = mockFetch(() => new Response(JSON.stringify({ ok: true, ts: '1.2' }), { status: 200 }));
    const adapter = new SlackHarnessAdapter({ signingSecret: SIGNING_SECRET, botToken: BOT_TOKEN, fetchImpl });
    await adapter.deliver(outboxItem({ payload: 'plain text' }), deliveryCtx);
    const sent = JSON.parse(fetchImpl.mock.calls[0]![1]!.body as string);
    expect(sent.text).toBe('plain text');
  });

  it('throws when Slack returns ok:false', async () => {
    const fetchImpl = mockFetch(
      () => new Response(JSON.stringify({ ok: false, error: 'channel_not_found' }), { status: 200 }),
    );
    const adapter = new SlackHarnessAdapter({ signingSecret: SIGNING_SECRET, botToken: BOT_TOKEN, fetchImpl });
    await expect(adapter.deliver(outboxItem(), deliveryCtx)).rejects.toThrow(/channel_not_found/);
  });

  it('throws when the target has no channel', async () => {
    const fetchImpl = mockFetch(() => new Response('{}', { status: 200 }));
    const adapter = new SlackHarnessAdapter({ signingSecret: SIGNING_SECRET, botToken: BOT_TOKEN, fetchImpl });
    await expect(
      adapter.deliver(
        outboxItem({ target: { platform: 'slack', externalThreadId: 't', externalChannelId: undefined } }),
        deliveryCtx,
      ),
    ).rejects.toThrow(/externalChannelId/);
    expect(fetchImpl).not.toHaveBeenCalled();
  });

  it('declares at-least-once delivery semantics', () => {
    const adapter = new SlackHarnessAdapter({ signingSecret: SIGNING_SECRET, botToken: BOT_TOKEN });
    expect(adapter.deliverySemantics).toBe('at-least-once');
  });
});

// ---------------------------------------------------------------------------
// 3. Integration — real Harness driven by a real signed Slack webhook
// ---------------------------------------------------------------------------

describe('SlackHarnessAdapter integration (ingress → admission via Harness)', () => {
  function makeChannelProvider(id = 'slack'): ChannelProvider {
    return { id, getRoutes: () => [] };
  }

  function setup() {
    const adapter = new SlackHarnessAdapter({
      signingSecret: SIGNING_SECRET,
      botToken: BOT_TOKEN,
      botUserId: BOT_USER_ID,
    });
    const composite = new InMemoryStore();
    const storage = composite.stores.harness;
    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage } as any,
      channels: {
        support: {
          providerId: 'slack',
          platform: 'slack',
          adapter,
          ingress: {
            resolveResource: async () => ({ resourceId: 'resource-1', mode: 'shared-resource' }),
          },
        },
      },
    });
    new Mastra({
      agents: {
        default: new Agent({
          id: 'default',
          name: 'default',
          instructions: 'test',
          model: 'openai/gpt-4o-mini' as any,
        }),
      },
      storage: composite,
      channels: { slack: makeChannelProvider('slack') },
      harnesses: { primary: harness },
    });
    return { harness, adapter };
  }

  it('flows a real signed Slack message through to a record-only ACK (202 received)', async () => {
    const { harness } = setup();
    const req = signedRequest(messageEventPayload());
    const result = await harness.handleChannelInboundRequest('support', req as any);
    expect(result).toMatchObject({ kind: 'ok', ackStatus: 202, status: 'received', duplicate: false });
  });

  it('flows a real signed Slack message through full admission (200 queued) with continueAdmission', async () => {
    const { harness } = setup();
    const req = signedRequest(messageEventPayload());
    const result = await harness.handleChannelInboundRequest('support', req as any, { continueAdmission: true });
    expect(result).toMatchObject({ kind: 'ok', ackStatus: 200, status: 'queued', duplicate: false });
    expect((result as { sessionId?: string }).sessionId).toMatch(/^chs:/);
  });

  it('rejects a tampered webhook with verify_failed (401), redacting the raw cause', async () => {
    const { harness } = setup();
    const req = signedRequest(messageEventPayload());
    req.rawBody = String(req.rawBody) + 'X';
    const result = await harness.handleChannelInboundRequest('support', req as any);
    expect(result).toMatchObject({
      kind: 'verify_failed',
      httpStatus: 401,
      error: { code: 'harness.permission_denied' },
    });
    const message = (result as { error: { message: string } }).error.message;
    expect(message).not.toContain('signature');
  });

  it('treats an exact provider retry of the same signed event as a duplicate', async () => {
    const { harness } = setup();
    const payload = messageEventPayload();
    const first = await harness.handleChannelInboundRequest('support', signedRequest(payload) as any, {
      continueAdmission: true,
    });
    const second = await harness.handleChannelInboundRequest('support', signedRequest(payload) as any, {
      continueAdmission: true,
    });
    expect(first).toMatchObject({ kind: 'ok', duplicate: false });
    expect(second).toMatchObject({ kind: 'ok', duplicate: true });
  });

  // -------------------------------------------------------------------------
  // SIGNAL delivery: a real signed Slack webhook STEERS an active run as a
  // signal (delivery:'signal', signalId persisted) instead of being queued as a
  // separate turn. The delivery mode is chosen by the ingress POLICY
  // (resolveResource → admission.delivery), NOT by the adapter — so the same
  // verified Slack envelope reaches the §14.2 signal admission path.
  // Mirrors packages/core/src/harness/v1/harness.test.ts:5050-5075.
  // -------------------------------------------------------------------------

  /** Capture the latest durable inbox row per id (storage exposes no get-by-id). */
  function setupSignal() {
    const adapter = new SlackHarnessAdapter({
      signingSecret: SIGNING_SECRET,
      botToken: BOT_TOKEN,
      botUserId: BOT_USER_ID,
    });
    const composite = new InMemoryStore();
    const storage = composite.stores.harness;
    const rows = new Map<
      string,
      {
        delivery?: string;
        runId?: string;
        signalId?: string;
        queuedItemId?: string;
        acceptedAt?: number;
        status: string;
      }
    >();
    const realUpdate = (
      storage as unknown as { updateChannelInboxItem: (...a: any[]) => Promise<void> }
    ).updateChannelInboxItem.bind(storage);
    (storage as unknown as { updateChannelInboxItem: unknown }).updateChannelInboxItem = async (
      record: { id: string } & Record<string, unknown>,
      opts: { claimId: string },
    ) => {
      await realUpdate(record as any, opts as any);
      rows.set(record.id, { ...(record as any) });
    };
    const harness = new Harness({
      modes: [{ id: 'default', agentId: 'default' }],
      defaultModeId: 'default',
      sessions: { storage } as any,
      channels: {
        support: {
          providerId: 'slack',
          platform: 'slack',
          adapter,
          ingress: {
            // The POLICY selects signal delivery (steer an active run), independent of the adapter.
            resolveResource: async () => ({
              resourceId: 'resource-1',
              mode: 'shared-resource',
              admission: { delivery: 'signal' },
            }),
          },
        },
      },
    });
    new Mastra({
      agents: {
        default: new Agent({
          id: 'default',
          name: 'default',
          instructions: 'test',
          model: 'openai/gpt-4o-mini' as any,
        }),
      },
      storage: composite,
      channels: { slack: makeChannelProvider('slack') },
      harnesses: { primary: harness },
    });
    return { harness, rows };
  }

  it("admits a real signed Slack webhook as a SIGNAL (delivery:'signal', signalId persisted) when the policy selects signal delivery", async () => {
    const { harness, rows } = setupSignal();
    const req = signedRequest(messageEventPayload());

    const result = await harness.handleChannelInboundRequest('support', req as any, { continueAdmission: true });

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
