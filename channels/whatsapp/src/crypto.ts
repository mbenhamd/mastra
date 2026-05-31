import { createHmac, timingSafeEqual } from 'node:crypto';

/**
 * Verify the `X-Hub-Signature-256` header Meta sends on WhatsApp Cloud API
 * webhooks. Meta computes `sha256=<hex>` = HMAC-SHA256(rawBody, APP_SECRET) over
 * the EXACT raw bytes of the request body. We must verify over those same bytes —
 * re-serializing a parsed JSON object would change whitespace/key-order and break
 * the comparison.
 *
 * @see https://developers.facebook.com/docs/graph-api/webhooks/getting-started#validating-payloads
 *
 * Returns `true` on a valid signature, `false` otherwise (malformed header, wrong
 * length, mismatch, wrong secret). The adapter turns `false` into a throw so the
 * harness can map it to `verify_failed`/401.
 */
export function verifyWhatsAppSignature(params: {
  appSecret: string;
  /** The EXACT raw request body bytes (or the UTF-8 string of those bytes). */
  rawBody: Uint8Array | string;
  /** The `X-Hub-Signature-256` header value, e.g. `sha256=ab12...`. */
  signature: string | undefined;
}): boolean {
  const { appSecret, rawBody, signature } = params;
  if (!signature || !signature.startsWith('sha256=')) {
    return false;
  }
  const provided = signature.slice('sha256='.length);

  const bodyBuf = typeof rawBody === 'string' ? Buffer.from(rawBody, 'utf8') : Buffer.from(rawBody);
  const expectedHex = createHmac('sha256', appSecret).update(bodyBuf).digest('hex');

  // Compare the hex strings via timing-safe equality. Mismatched lengths make
  // timingSafeEqual throw, which we treat as a non-match.
  const providedBuf = Buffer.from(provided, 'utf8');
  const expectedBuf = Buffer.from(expectedHex, 'utf8');
  if (providedBuf.length !== expectedBuf.length) {
    return false;
  }
  try {
    return timingSafeEqual(providedBuf, expectedBuf);
  } catch {
    return false;
  }
}

/**
 * Result of the GET webhook-verification handshake. On success the caller must
 * echo `challenge` back as the HTTP 200 body (plain text); on failure it must
 * return 403.
 *
 * The harness inbound route is POST-only, so this handshake lives OUTSIDE
 * `verifyInbound`. Wire it up in whatever HTTP layer terminates the GET
 * `?hub.mode=subscribe&hub.verify_token=...&hub.challenge=...` request that Meta
 * issues once, when you (re)subscribe the webhook. See the adapter's
 * `verifyWebhookChallenge` convenience wrapper.
 *
 * @see https://developers.facebook.com/docs/graph-api/webhooks/getting-started#verification-requests
 */
export type WebhookChallengeResult =
  | { ok: true; challenge: string }
  | { ok: false; reason: 'mode_mismatch' | 'token_mismatch' | 'missing_challenge' };

/**
 * Validate a GET webhook-verification handshake against the configured verify
 * token. `query` is the parsed query string (string-or-array values, matching
 * `HarnessChannelTransportRequest.query`).
 */
export function verifyWebhookChallenge(params: {
  verifyToken: string;
  query: Record<string, string | string[]> | undefined;
}): WebhookChallengeResult {
  const { verifyToken, query } = params;
  const mode = firstQueryValue(query?.['hub.mode']);
  const token = firstQueryValue(query?.['hub.verify_token']);
  const challenge = firstQueryValue(query?.['hub.challenge']);

  if (mode !== 'subscribe') {
    return { ok: false, reason: 'mode_mismatch' };
  }
  if (!constantTimeStringEqual(token, verifyToken)) {
    return { ok: false, reason: 'token_mismatch' };
  }
  if (challenge === undefined) {
    return { ok: false, reason: 'missing_challenge' };
  }
  return { ok: true, challenge };
}

function firstQueryValue(value: string | string[] | undefined): string | undefined {
  if (value === undefined) return undefined;
  return Array.isArray(value) ? value[0] : value;
}

/** Timing-safe string equality that does not leak length via early return. */
function constantTimeStringEqual(a: string | undefined, b: string): boolean {
  if (a === undefined) return false;
  const aBuf = Buffer.from(a, 'utf8');
  const bBuf = Buffer.from(b, 'utf8');
  if (aBuf.length !== bBuf.length) return false;
  try {
    return timingSafeEqual(aBuf, bBuf);
  } catch {
    return false;
  }
}
