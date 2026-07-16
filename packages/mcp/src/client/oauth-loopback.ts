// Matches the entire 127.0.0.0/8 range in dotted-quad form. `URL` normalizes
// IPv4 hosts to four octets (so `127.1` becomes `127.0.0.1`), so anchoring the
// pattern is enough — and it rejects lookalikes like `127.evil.com` that a
// prefix check would wrongly accept and leak the authorization code to.
const LOOPBACK_IPV4 = /^127\.(?:\d{1,3})\.(?:\d{1,3})\.(?:\d{1,3})$/;

/**
 * Whether a hostname is an RFC 8252 loopback address accepted for an OAuth
 * callback URL.
 */
export function isLoopbackHostname(hostname: string): boolean {
  return hostname === 'localhost' || hostname === '[::1]' || hostname === '::1' || LOOPBACK_IPV4.test(hostname);
}
