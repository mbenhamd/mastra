import type {
  ChannelDeliverySemantics,
  ChannelIngressEnvelope,
  ChannelOutboxItem,
  ChannelProviderDeliveryReceipt,
  HarnessChannelAdapter,
  HarnessChannelDeliveryContext,
  HarnessChannelRouteContext,
  HarnessChannelTransportRequest,
} from '@mastra/core/harness/v1';

import { verifyWebhookChallenge, verifyWhatsAppSignature, type WebhookChallengeResult } from './crypto';
import {
  DEFAULT_GRAPH_API_BASE_URL,
  DEFAULT_WHATSAPP_API_VERSION,
  type WhatsAppAdapterConfig,
  type WhatsAppSendBody,
  type WhatsAppSendResponse,
  type WhatsAppGraphErrorResponse,
  type WhatsAppWebhookPayload,
} from './types';

/** The `platform` tag emitted on every WhatsApp ingress envelope. */
export const WHATSAPP_PLATFORM = 'whatsapp';

/**
 * Harness channel adapter for the WhatsApp Cloud API (Meta Graph API).
 *
 * - Inbound: verifies `X-Hub-Signature-256` over the raw request bytes, then maps
 *   the first inbound text message into a {@link ChannelIngressEnvelope}.
 * - Outbound: POSTs a text message to `/<version>/<phone_number_id>/messages`.
 *
 * The GET webhook-verification handshake is NOT part of the harness POST inbound
 * route; use {@link WhatsAppHarnessAdapter.verifyWebhookChallenge} from your GET
 * route. See `crypto.ts` for details.
 */
export class WhatsAppHarnessAdapter implements HarnessChannelAdapter {
  readonly #config: WhatsAppAdapterConfig;

  /**
   * §13/§14 delivery semantics. The Cloud API `/messages` send performs NO
   * server-side dedupe — there is no client message id / idempotency key on the
   * send call, and a retried POST produces a SECOND WhatsApp message with a new
   * id. So the only honest contract is `at-least-once`: the harness outbox may
   * deliver a message more than once under retry, and downstream must tolerate
   * that. (We do not claim `native-idempotency` or `client-message-id`, which
   * would be a correctness lie for this API.)
   */
  readonly deliverySemantics: ChannelDeliverySemantics = 'at-least-once';

  constructor(config: WhatsAppAdapterConfig) {
    if (!config.appSecret) throw new Error('WhatsAppHarnessAdapter: `appSecret` is required');
    if (!config.accessToken) throw new Error('WhatsAppHarnessAdapter: `accessToken` is required');
    if (!config.phoneNumberId) throw new Error('WhatsAppHarnessAdapter: `phoneNumberId` is required');
    this.#config = config;
  }

  /**
   * Verify + project an inbound WhatsApp webhook POST into a ChannelIngressEnvelope.
   *
   * THROWS on signature mismatch / malformed header / wrong app secret — the
   * harness maps the throw to `verify_failed`/401. THROWS on a payload that
   * carries no inbound text message (status callbacks, non-text message types,
   * unrelated change fields) so it is not admitted as a chat turn.
   */
  async verifyInbound(
    request: HarnessChannelTransportRequest,
    _ctx: HarnessChannelRouteContext,
  ): Promise<ChannelIngressEnvelope> {
    const signature = this.#headerValue(request.headers, 'x-hub-signature-256');
    const rawBody = this.#rawBody(request);

    const valid = verifyWhatsAppSignature({ appSecret: this.#config.appSecret, rawBody, signature });
    if (!valid) {
      // Generic message — the harness redacts adapter error text on the public
      // envelope anyway, but we avoid leaking secret/header hints regardless.
      throw new Error('WhatsApp inbound signature verification failed');
    }

    const payload = this.#parsePayload(rawBody, request.body);
    return this.#projectFirstTextMessage(payload);
  }

  /**
   * Convenience wrapper for the GET webhook-verification handshake. Call this
   * from your GET route; it is NOT part of the harness inbound (POST) contract.
   * Returns the challenge to echo back on success.
   */
  verifyWebhookChallenge(request: HarnessChannelTransportRequest): WebhookChallengeResult {
    if (!this.#config.verifyToken) {
      return { ok: false, reason: 'token_mismatch' };
    }
    // Prefer parsed query; fall back to parsing the URL's search string.
    const query = request.query ?? this.#queryFromUrl(request.url);
    return verifyWebhookChallenge({ verifyToken: this.#config.verifyToken, query });
  }

  /**
   * Deliver an outbound text message via the Graph API.
   * POST `<base>/<version>/<phone_number_id>/messages`
   * Authorization: Bearer <accessToken>
   */
  async deliver(
    item: ChannelOutboxItem,
    _ctx: HarnessChannelDeliveryContext,
  ): Promise<{ providerMessageId?: string; providerReceipt?: ChannelProviderDeliveryReceipt }> {
    const to = item.target.externalThreadId;
    const text = this.#extractText(item.payload);

    const body: WhatsAppSendBody = {
      messaging_product: 'whatsapp',
      recipient_type: 'individual',
      to,
      type: 'text',
      text: { body: text },
    };

    const url = this.#sendUrl();
    const doFetch = this.#config.fetch ?? globalThis.fetch;
    const response = await doFetch(url, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${this.#config.accessToken}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
    });

    if (!response.ok) {
      const errText = await this.#safeReadGraphError(response);
      throw new Error(`WhatsApp send failed (HTTP ${response.status}): ${errText}`);
    }

    const json = (await response.json()) as WhatsAppSendResponse;
    const providerMessageId = json.messages?.[0]?.id;
    return {
      providerMessageId,
      providerReceipt: { providerMessageId },
    };
  }

  // -------------------------------------------------------------------------
  // Internals
  // -------------------------------------------------------------------------

  #sendUrl(): string {
    const base = (this.#config.graphApiBaseUrl ?? DEFAULT_GRAPH_API_BASE_URL).replace(/\/+$/, '');
    const version = this.#config.apiVersion ?? DEFAULT_WHATSAPP_API_VERSION;
    return `${base}/${version}/${this.#config.phoneNumberId}/messages`;
  }

  #rawBody(request: HarnessChannelTransportRequest): Uint8Array | string {
    if (request.rawBody !== undefined) return request.rawBody;
    // No raw bytes available (e.g. a transport that only parsed JSON). Fall back
    // to a canonical re-serialization; this can fail signature verification if
    // the upstream JSON had different whitespace, so transports SHOULD provide
    // rawBody for WhatsApp.
    if (typeof request.body === 'string') return request.body;
    return JSON.stringify(request.body ?? {});
  }

  #parsePayload(rawBody: Uint8Array | string, parsedBody: unknown): WhatsAppWebhookPayload {
    if (parsedBody && typeof parsedBody === 'object') {
      return parsedBody as WhatsAppWebhookPayload;
    }
    const text = typeof rawBody === 'string' ? rawBody : Buffer.from(rawBody).toString('utf8');
    return JSON.parse(text) as WhatsAppWebhookPayload;
  }

  #projectFirstTextMessage(payload: WhatsAppWebhookPayload): ChannelIngressEnvelope {
    if (payload.object !== 'whatsapp_business_account') {
      throw new Error(`WhatsApp inbound: unexpected object '${String(payload.object)}'`);
    }
    for (const entry of payload.entry ?? []) {
      for (const change of entry.changes ?? []) {
        // `field: 'messages'` carries both inbound messages and status callbacks.
        if (change.field !== 'messages') continue;
        const value = change.value ?? {};
        const messages = value.messages ?? [];
        const message = messages.find(m => m.type === 'text' && m.text?.body !== undefined);
        if (!message) continue;

        const from = message.from;
        const contact = value.contacts?.[0];
        const displayName = contact?.profile?.name;
        const tsSeconds = Number(message.timestamp);
        const receivedAt = Number.isFinite(tsSeconds) ? tsSeconds * 1000 : Date.now();

        return {
          platform: WHATSAPP_PLATFORM,
          // WhatsApp Cloud API conversations are strictly 1:1 (a user <-> business
          // number), so the conversation is a direct message.
          conversationKind: 'dm',
          trigger: 'message',
          externalTenantId: entry.id, // WhatsApp Business Account id
          externalChannelId: value.metadata?.phone_number_id, // business phone-number id
          externalThreadId: from, // per-user conversation == the user's wa_id
          externalMessageId: message.id,
          content: message.text!.body,
          actor: {
            platformUserId: from,
            ...(displayName !== undefined ? { displayName } : {}),
          },
          receivedAt,
          raw: payload,
        };
      }
    }

    // No inbound text message: status callbacks (value.statuses[]) and non-text
    // message types (image/audio/document/...) are NOT chat-turn ingress. We
    // throw so the harness does not admit a turn. Status callbacks should be
    // handled by a dedicated delivery-receipt path, not the ingress route.
    throw new Error('WhatsApp inbound: no inbound text message to admit');
  }

  #extractText(payload: ChannelOutboxItem['payload']): string {
    if (typeof payload === 'string') return payload;
    if (payload && typeof payload === 'object' && !Array.isArray(payload)) {
      const obj = payload as Record<string, unknown>;
      // Common shapes: { text: '...' } or { text: { body: '...' } } or { body: '...' }.
      if (typeof obj.text === 'string') return obj.text;
      if (obj.text && typeof obj.text === 'object') {
        const body = (obj.text as Record<string, unknown>).body;
        if (typeof body === 'string') return body;
      }
      if (typeof obj.body === 'string') return obj.body;
      if (typeof obj.content === 'string') return obj.content;
    }
    throw new Error('WhatsApp deliver: outbox payload has no text body');
  }

  #headerValue(headers: Record<string, string | string[]>, name: string): string | undefined {
    // Transport header maps are not guaranteed lowercased — normalize.
    for (const [key, value] of Object.entries(headers)) {
      if (key.toLowerCase() === name) {
        return Array.isArray(value) ? value[0] : value;
      }
    }
    return undefined;
  }

  #queryFromUrl(url: string | undefined): Record<string, string | string[]> | undefined {
    if (!url) return undefined;
    try {
      const parsed = new URL(url, 'http://localhost');
      const out: Record<string, string | string[]> = {};
      for (const [k, v] of parsed.searchParams) out[k] = v;
      return out;
    } catch {
      return undefined;
    }
  }

  async #safeReadGraphError(response: Response): Promise<string> {
    try {
      const json = (await response.json()) as WhatsAppGraphErrorResponse;
      if (json.error?.message) {
        return `${json.error.message}${json.error.code !== undefined ? ` (code ${json.error.code})` : ''}`;
      }
      return JSON.stringify(json);
    } catch {
      return response.statusText || 'unknown error';
    }
  }
}
