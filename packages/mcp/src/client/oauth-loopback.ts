import { isIP } from 'node:net';

const LOOPBACK_CALLBACK_REQUIREMENTS =
  'OAuth callback redirect URL must use HTTP and a loopback IP literal (127.0.0.0/8 or ::1) without userinfo or a fragment.';

export interface NormalizedLoopbackCallbackUrl {
  /**
   * Canonical URL form produced by the platform URL parser.
   */
  url: URL;

  /**
   * Canonical IP literal without IPv6 brackets, suitable for server.listen().
   */
  hostname: string;
}

/**
 * Parses and validates an RFC 8252 loopback callback URL without relying on
 * hostname resolution. URL parsing canonicalizes supported IPv4 and IPv6
 * literal spellings before the address-family and loopback checks run.
 */
export function normalizeLoopbackCallbackUrl(redirectUrl: string | URL): NormalizedLoopbackCallbackUrl {
  const source = redirectUrl.toString();
  let url: URL;
  try {
    url = new URL(source);
  } catch {
    throw new Error(LOOPBACK_CALLBACK_REQUIREMENTS);
  }

  const hostname = url.hostname.replace(/^\[|\]$/g, '');
  const addressFamily = isIP(hostname);
  const isLoopbackIpv4 = addressFamily === 4 && hostname.startsWith('127.');
  const isLoopbackIpv6 = addressFamily === 6 && hostname === '::1';

  if (
    url.protocol !== 'http:' ||
    (!isLoopbackIpv4 && !isLoopbackIpv6) ||
    url.username !== '' ||
    url.password !== '' ||
    source.includes('#')
  ) {
    throw new Error(LOOPBACK_CALLBACK_REQUIREMENTS);
  }

  return { url, hostname };
}

/**
 * Whether two verified loopback callback URLs identify the same callback
 * endpoint apart from their port. Query parameters remain part of the target:
 * this helper returns true only when the port is the sole differing component.
 */
export function hasSameLoopbackCallbackTarget(left: string | URL, right: string | URL): boolean {
  try {
    const normalizedLeft = normalizeLoopbackCallbackUrl(left);
    const normalizedRight = normalizeLoopbackCallbackUrl(right);

    return (
      normalizedLeft.url.protocol === normalizedRight.url.protocol &&
      normalizedLeft.hostname === normalizedRight.hostname &&
      normalizedLeft.url.pathname === normalizedRight.url.pathname &&
      normalizedLeft.url.search === normalizedRight.url.search
    );
  } catch {
    return false;
  }
}
