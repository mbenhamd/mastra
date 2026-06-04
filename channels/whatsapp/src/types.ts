/**
 * Configuration + wire-shape types for the WhatsApp Cloud API channel adapter.
 *
 * References:
 * - Webhooks: https://developers.facebook.com/docs/whatsapp/cloud-api/webhooks/payload-examples
 * - Request validation (X-Hub-Signature-256):
 *   https://developers.facebook.com/docs/graph-api/webhooks/getting-started#validating-payloads
 * - Sending messages:
 *   https://developers.facebook.com/docs/whatsapp/cloud-api/reference/messages
 */

/** Default Meta Graph API version used when one is not supplied via config. */
export const DEFAULT_WHATSAPP_API_VERSION = 'v21.0';

/** Default Graph API host. Overridable in tests / for regional endpoints. */
export const DEFAULT_GRAPH_API_BASE_URL = 'https://graph.facebook.com';

export interface WhatsAppAdapterConfig {
  /**
   * Meta App Secret. Used to verify the `X-Hub-Signature-256` header over the
   * EXACT raw request bytes. This is NOT the same as the access token.
   */
  appSecret: string;
  /**
   * Bearer token (system-user or app access token) used on outbound Graph API
   * `/messages` calls.
   */
  accessToken: string;
  /**
   * The WhatsApp Business phone-number id that sends outbound messages. This is
   * the `<phone_number_id>` path segment on the Graph API send URL. Inbound
   * payloads also carry it under `value.metadata.phone_number_id`.
   */
  phoneNumberId: string;
  /**
   * Token configured on the Meta webhook subscription. Used by the GET
   * verification handshake (`hub.verify_token`). Optional — only required if
   * `verifyWebhookChallenge` is used.
   */
  verifyToken?: string;
  /** Graph API version, e.g. `v21.0`. Defaults to {@link DEFAULT_WHATSAPP_API_VERSION}. */
  apiVersion?: string;
  /** Graph API base URL. Defaults to {@link DEFAULT_GRAPH_API_BASE_URL}. */
  graphApiBaseUrl?: string;
  /**
   * Injectable fetch — defaults to the global `fetch`. Lets tests stub the
   * Graph API without monkey-patching globals.
   */
  fetch?: typeof fetch;
}

// ---------------------------------------------------------------------------
// Inbound webhook payload shapes (the subset we map).
// ---------------------------------------------------------------------------

/**
 * A WhatsApp Cloud API interactive reply: the user tapping a quick-reply button
 * (`button_reply`) or selecting a list row (`list_reply`). Both carry the
 * developer-supplied `id` (the routing key set when the menu was sent) and the
 * human-readable `title`; `list_reply` may also carry a `description`. See
 * https://developers.facebook.com/docs/whatsapp/cloud-api/webhooks/payload-examples#received-callback-from-a-customer-clicking-a-reply-button
 */
export interface WhatsAppInteractiveReply {
  type: 'button_reply' | 'list_reply' | string;
  button_reply?: { id: string; title: string };
  list_reply?: { id: string; title: string; description?: string };
}

export interface WhatsAppTextMessage {
  from: string;
  id: string;
  timestamp: string;
  type: string;
  text?: { body: string };
  // `type:'interactive'` button/list replies (the user tapped a reply button or
  // picked a list row). These ARE chat-turn ingress, mapped via the reply title.
  interactive?: WhatsAppInteractiveReply;
  // image/audio/document/etc. carry their own keys; we only read `text` /
  // `interactive` here.
  [key: string]: unknown;
}

export interface WhatsAppContact {
  wa_id: string;
  profile?: { name?: string };
}

export interface WhatsAppStatus {
  id: string;
  status: string;
  timestamp: string;
  recipient_id: string;
  [key: string]: unknown;
}

export interface WhatsAppChangeValue {
  messaging_product?: string;
  metadata?: { phone_number_id?: string; display_phone_number?: string };
  contacts?: WhatsAppContact[];
  messages?: WhatsAppTextMessage[];
  statuses?: WhatsAppStatus[];
}

export interface WhatsAppChange {
  field: string;
  value: WhatsAppChangeValue;
}

export interface WhatsAppEntry {
  id: string;
  changes: WhatsAppChange[];
}

export interface WhatsAppWebhookPayload {
  object: string;
  entry: WhatsAppEntry[];
}

// ---------------------------------------------------------------------------
// Outbound send shapes.
// ---------------------------------------------------------------------------

export interface WhatsAppSendBody {
  messaging_product: 'whatsapp';
  recipient_type: 'individual';
  to: string;
  type: 'text';
  text: { body: string };
}

export interface WhatsAppSendResponse {
  messaging_product?: string;
  contacts?: Array<{ input: string; wa_id: string }>;
  messages?: Array<{ id: string }>;
}

export interface WhatsAppGraphErrorResponse {
  error?: {
    message?: string;
    type?: string;
    code?: number;
    error_subcode?: number;
    fbtrace_id?: string;
  };
}
