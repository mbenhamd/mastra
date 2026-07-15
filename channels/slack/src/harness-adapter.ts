import type {
  ChannelConversationKind,
  ChannelIngressEnvelope,
  ChannelIngressTrigger,
  ChannelOutboxItem,
  ChannelProviderDeliveryReceipt,
  HarnessChannelAdapter,
  HarnessChannelDeliveryContext,
  HarnessChannelRouteContext,
  HarnessChannelTransportRequest,
} from '@mastra/core/harness/v1';

import { verifySlackRequest } from './crypto';

const SLACK_API_BASE = 'https://slack.com/api';
const SLACK_API_TIMEOUT_MS = 30_000;
const PLATFORM = 'slack';

/**
 * Error thrown by {@link SlackHarnessAdapter.verifyInbound} when a Slack request
 * fails signature/timestamp verification. The harness maps any throw from
 * `verifyInbound` onto a `verify_failed` (401) result and REDACTS this message
 * before it crosses the public wire envelope (see §13.3f.1), so the detail here
 * is for server-side logs only.
 */
export class SlackInboundVerificationError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'SlackInboundVerificationError';
  }
}

/**
 * Sentinel thrown to signal that the inbound request is the Slack Events API
 * `url_verification` handshake and carries a challenge that the transport layer
 * must echo back verbatim. The HTTP route that owns the response surface should
 * catch this and reply `{ challenge }` with a 200. It is NOT a verification
 * failure — the request was correctly signed (or is the unsigned setup ping
 * Slack sends before a signing secret is configured).
 */
export class SlackUrlVerificationChallenge extends Error {
  readonly challenge: string;
  constructor(challenge: string) {
    super('slack url_verification challenge');
    this.name = 'SlackUrlVerificationChallenge';
    this.challenge = challenge;
  }
}

export interface SlackHarnessAdapterConfig {
  /**
   * Slack app signing secret. Used to verify the `X-Slack-Signature` HMAC over
   * the raw request body + `X-Slack-Request-Timestamp`. Required for
   * `verifyInbound`.
   */
  signingSecret: string;

  /**
   * Slack bot token (`xoxb-…`) used as the `Authorization: Bearer` credential
   * for outbound `chat.postMessage`. Required for `deliver`.
   */
  botToken: string;

  /**
   * The bot's own Slack user id (e.g. `U0123BOT`). When set, inbound events
   * authored by this user are ignored (mapped to an empty/no-op turn) so the
   * agent does not react to its own posts. Slack also stamps bot posts with
   * `bot_id`, which is filtered independently.
   */
  botUserId?: string;

  /**
   * Optional `fetch` override (for tests). Defaults to the global `fetch`.
   */
  fetchImpl?: typeof fetch;

  /**
   * Optional clock override (epoch ms) for deterministic timestamp checks in
   * tests. Defaults to `Date.now`.
   */
  now?: () => number;
}

/**
 * Slack Events API envelope shapes we care about. Slack sends many more fields;
 * we narrow only what the ingress projection reads.
 */
interface SlackEventCallback {
  type: 'event_callback';
  team_id?: string;
  api_app_id?: string;
  event_id?: string;
  event_time?: number;
  authorizations?: Array<{ team_id?: string; user_id?: string; is_bot?: boolean }>;
  event: SlackMessageEvent;
}

interface SlackUrlVerification {
  type: 'url_verification';
  challenge: string;
}

interface SlackMessageEvent {
  type: string; // 'message' | 'app_mention' | …
  subtype?: string; // 'bot_message', 'message_changed', etc.
  channel?: string;
  channel_type?: string; // 'im' | 'mpim' | 'channel' | 'group'
  user?: string;
  bot_id?: string;
  text?: string;
  ts?: string;
  thread_ts?: string;
  client_msg_id?: string;
  team?: string;
  app_id?: string;
}

type SlackEnvelopeBody = SlackEventCallback | SlackUrlVerification | { type?: string; [k: string]: unknown };

function headerValue(headers: Record<string, string | string[]>, name: string): string | undefined {
  // Transport may deliver header keys in any case. Match case-insensitively and
  // collapse array-valued headers to their first entry.
  const lower = name.toLowerCase();
  for (const [key, value] of Object.entries(headers)) {
    if (key.toLowerCase() === lower) {
      return Array.isArray(value) ? value[0] : value;
    }
  }
  return undefined;
}

function rawBodyToString(rawBody: HarnessChannelTransportRequest['rawBody']): string | undefined {
  if (rawBody === undefined) return undefined;
  if (typeof rawBody === 'string') return rawBody;
  return Buffer.from(rawBody).toString('utf8');
}

/**
 * A REAL {@link HarnessChannelAdapter} for Slack. It wraps the existing
 * `channels/slack` provider primitives (`verifySlackRequest` for inbound HMAC
 * verification) and a minimal `chat.postMessage` Web API call for delivery,
 * so a Slack workspace webhook flows through `harness.handleChannelInboundRequest`
 * into durable admission, and harness-queued outbox items post back to Slack.
 *
 * Scope: this adapter covers the Events API `message` / `app_mention` ingress
 * path and assistant-message delivery. Interactive actions (`verifyAction`) and
 * non-message events are intentionally out of scope here.
 */
export class SlackHarnessAdapter implements HarnessChannelAdapter {
  readonly #signingSecret: string;
  readonly #botToken: string;
  readonly #botUserId?: string;
  readonly #fetch: typeof fetch;
  readonly #now: () => number;

  /**
   * Slack `chat.postMessage` is at-least-once from the harness's point of view.
   * Slack assigns the canonical message `ts` server-side and offers no caller-
   * supplied idempotency key, so a retried POST after a lost ACK produces a
   * DUPLICATE message with a NEW `ts` — the harness cannot dedupe it natively.
   * `at-least-once` tells the outbox dispatcher to treat a delivery whose ACK was
   * lost as possibly-delivered and to rely on the harness's own idempotency-key
   * de-dup at enqueue time rather than any provider-side guarantee.
   */
  readonly deliverySemantics: 'at-least-once' = 'at-least-once';

  constructor(config: SlackHarnessAdapterConfig) {
    this.#signingSecret = config.signingSecret;
    this.#botToken = config.botToken;
    this.#botUserId = config.botUserId;
    this.#fetch = config.fetchImpl ?? fetch;
    this.#now = config.now ?? (() => Date.now());
  }

  /**
   * Verify a Slack inbound webhook and project it into a {@link ChannelIngressEnvelope}.
   *
   * Verification (throws on failure → harness maps to `verify_failed`/401):
   *  - Requires `X-Slack-Signature` + `X-Slack-Request-Timestamp` headers and a
   *    raw body. `verifySlackRequest` recomputes the `v0=` HMAC over
   *    `v0:{timestamp}:{rawBody}` and rejects a tampered body, a wrong signature,
   *    or a timestamp outside Slack's 5-minute replay window.
   *
   * `url_verification` handshake: if the (signed, or unsigned setup-ping) body is
   * a `url_verification` event, this throws {@link SlackUrlVerificationChallenge}
   * carrying the challenge for the transport to echo. Slack sends this ping
   * BEFORE a signing secret is in place, so it is accepted without a signature
   * when no signature headers are present.
   *
   * Event mapping (`event_callback` → envelope):
   *  - platform           = 'slack'
   *  - externalTenantId   = team_id
   *  - externalChannelId  = event.channel
   *  - externalThreadId   = event.thread_ts ?? event.ts   (a top-level message
   *                         starts a thread keyed by its own ts)
   *  - externalMessageId  = event_id ?? client_msg_id ?? ts (stable idempotency key)
   *  - content            = event.text
   *  - actor.platformUserId = event.user
   *  - trigger            = 'mention' for app_mention, else 'message'
   *  - conversationKind   = 'dm' (channel_type 'im'), 'group-dm' (mpim),
   *                         'thread' (has thread_ts != ts), else 'channel'
   *  - receivedAt         = event_time*1000 ?? now
   *  - raw                = the full parsed payload
   *
   * Bot's own / non-message events: an event authored by this bot (matching
   * `botUserId`, carrying a `bot_id`, or with the `bot_message` subtype), an
   * edit/delete subtype, or a non message/app_mention event is NOT a user turn.
   * We surface it as an empty-content envelope (content '', threadId/messageId
   * preserved when present) so the harness records/dedupes the delivery without
   * waking the agent on noise. Throwing would map to a 401 and make Slack retry.
   */
  async verifyInbound(
    request: HarnessChannelTransportRequest,
    _ctx: HarnessChannelRouteContext,
  ): Promise<ChannelIngressEnvelope> {
    const rawBody = rawBodyToString(request.rawBody);
    if (rawBody === undefined) {
      throw new SlackInboundVerificationError('missing raw request body');
    }

    const signature = headerValue(request.headers, 'x-slack-signature');
    const timestamp = headerValue(request.headers, 'x-slack-request-timestamp');

    // Parse first so we can recognise the unsigned setup-time url_verification ping.
    let payload: SlackEnvelopeBody;
    try {
      payload = JSON.parse(rawBody) as SlackEnvelopeBody;
    } catch {
      throw new SlackInboundVerificationError('malformed JSON body');
    }

    const isUrlVerification = payload.type === 'url_verification';

    if (!signature || !timestamp) {
      // Slack's initial Events API request-URL handshake can arrive before the
      // signing secret is configured. Accept ONLY the url_verification handshake
      // without a signature; everything else must be signed.
      if (isUrlVerification) {
        throw new SlackUrlVerificationChallenge((payload as SlackUrlVerification).challenge);
      }
      throw new SlackInboundVerificationError('missing X-Slack-Signature or X-Slack-Request-Timestamp header');
    }

    const valid = verifySlackRequest({
      signingSecret: this.#signingSecret,
      timestamp,
      body: rawBody,
      signature,
    });
    if (!valid) {
      // verifySlackRequest folds both a tampered/incorrect signature AND an
      // expired (>5min) timestamp into `false`; both are verification failures.
      throw new SlackInboundVerificationError(
        'Slack signature verification failed (bad signature or expired timestamp)',
      );
    }

    if (isUrlVerification) {
      throw new SlackUrlVerificationChallenge((payload as SlackUrlVerification).challenge);
    }

    if (payload.type !== 'event_callback') {
      // Signed but not an event we model (e.g. app_uninstalled wrapper). Record a
      // no-op delivery rather than 401 (which would make Slack retry).
      return this.#emptyEnvelope(payload, request);
    }

    const callback = payload as SlackEventCallback;
    const event = callback.event;
    const teamId = callback.team_id ?? event?.team;

    if (!event || (event.type !== 'message' && event.type !== 'app_mention')) {
      return this.#emptyEnvelope(payload, request, teamId, event);
    }

    // Ignore the bot's own posts and message edits/deletes — not user turns.
    const isBotAuthored =
      (this.#botUserId !== undefined && event.user === this.#botUserId) ||
      event.bot_id !== undefined ||
      event.subtype === 'bot_message';
    const isEditOrDelete =
      event.subtype === 'message_changed' || event.subtype === 'message_deleted' || event.subtype === 'message_replied';

    const ts = event.ts ?? String(this.#now() / 1000);
    const threadTs = event.thread_ts ?? ts;
    const externalMessageId = callback.event_id ?? event.client_msg_id ?? ts;
    const receivedAt = callback.event_time !== undefined ? callback.event_time * 1000 : this.#now();

    if (isBotAuthored || isEditOrDelete) {
      return {
        platform: PLATFORM,
        conversationKind: this.#conversationKind(event),
        trigger: event.type === 'app_mention' ? 'mention' : 'message',
        externalTenantId: teamId,
        externalChannelId: event.channel,
        externalThreadId: threadTs,
        externalMessageId,
        content: '',
        receivedAt,
        raw: payload,
      };
    }

    const trigger: ChannelIngressTrigger = event.type === 'app_mention' ? 'mention' : 'message';

    return {
      platform: PLATFORM,
      conversationKind: this.#conversationKind(event),
      trigger,
      externalTenantId: teamId,
      externalChannelId: event.channel,
      externalThreadId: threadTs,
      externalMessageId,
      content: event.text ?? '',
      ...(event.user !== undefined ? { actor: { platformUserId: event.user } } : {}),
      receivedAt,
      raw: payload,
    };
  }

  /**
   * Deliver a harness outbox item to Slack via `chat.postMessage`.
   *
   * The target channel/thread come from `item.target` (the harness-owned routing
   * target), and the message body from `item.payload`. We accept either a bare
   * string payload or a `{ text, blocks? }` object so callers can enqueue plain
   * text or Block Kit. Returns the Slack message `ts` as `providerMessageId` plus
   * a receipt carrying the channel as `providerThreadId` and the raw `ts`.
   */
  async deliver(
    item: ChannelOutboxItem,
    _ctx: HarnessChannelDeliveryContext,
  ): Promise<{ providerMessageId?: string; providerReceipt?: ChannelProviderDeliveryReceipt }> {
    const channel = item.target.externalChannelId;
    if (!channel) {
      throw new Error('Slack deliver: outbox item target is missing externalChannelId');
    }

    const { text, blocks } = this.#payloadToMessage(item.payload);
    if (text === undefined && blocks === undefined) {
      throw new Error('Slack deliver: outbox payload has neither text nor blocks');
    }

    const body: Record<string, unknown> = { channel };
    if (text !== undefined) body.text = text;
    if (blocks !== undefined) body.blocks = blocks;
    // Post into the thread the conversation belongs to. Slack ignores thread_ts
    // when it equals a top-level message ts that is itself the root, which is the
    // desired "reply in thread / start thread" behaviour.
    if (item.target.externalThreadId) body.thread_ts = item.target.externalThreadId;

    const response = await this.#fetch(`${SLACK_API_BASE}/chat.postMessage`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        Authorization: `Bearer ${this.#botToken}`,
      },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(SLACK_API_TIMEOUT_MS),
    });

    const data = (await response.json()) as {
      ok: boolean;
      error?: string;
      ts?: string;
      channel?: string;
      message?: { ts?: string };
    };

    if (!data.ok) {
      throw new Error(`Slack chat.postMessage failed: ${data.error ?? 'unknown_error'}`);
    }

    const ts = data.ts ?? data.message?.ts;
    return {
      providerMessageId: ts,
      providerReceipt: {
        providerMessageId: ts,
        providerThreadId: data.channel ?? channel,
        metadata: { channel: data.channel ?? channel },
      },
    };
  }

  #conversationKind(event: SlackMessageEvent): ChannelConversationKind {
    if (event.channel_type === 'im') return 'dm';
    if (event.channel_type === 'mpim') return 'group-dm';
    // A message carrying a thread_ts that differs from its own ts is a reply in
    // an existing thread.
    if (event.thread_ts !== undefined && event.thread_ts !== event.ts) return 'thread';
    return 'channel';
  }

  #emptyEnvelope(
    payload: SlackEnvelopeBody,
    request: HarnessChannelTransportRequest,
    teamId?: string,
    event?: SlackMessageEvent,
  ): ChannelIngressEnvelope {
    const ts = event?.ts ?? String(this.#now() / 1000);
    return {
      platform: PLATFORM,
      conversationKind: event ? this.#conversationKind(event) : 'channel',
      trigger: event?.type === 'app_mention' ? 'mention' : 'message',
      externalTenantId: teamId,
      externalChannelId: event?.channel,
      externalThreadId: event?.thread_ts ?? ts,
      externalMessageId: (payload as SlackEventCallback).event_id ?? event?.client_msg_id ?? event?.ts ?? ts,
      content: '',
      receivedAt:
        (payload as SlackEventCallback).event_time !== undefined
          ? (payload as SlackEventCallback).event_time! * 1000
          : (request.receivedAt ?? this.#now()),
      raw: payload,
    };
  }

  #payloadToMessage(payload: ChannelOutboxItem['payload']): { text?: string; blocks?: unknown } {
    if (typeof payload === 'string') return { text: payload };
    if (payload && typeof payload === 'object' && !Array.isArray(payload)) {
      const obj = payload as Record<string, unknown>;
      const text = typeof obj.text === 'string' ? obj.text : undefined;
      const blocks = Array.isArray(obj.blocks) ? obj.blocks : undefined;
      return { text, blocks };
    }
    return {};
  }
}
