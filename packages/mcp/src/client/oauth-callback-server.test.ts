/**
 * Tests for the loopback OAuth callback server.
 *
 * Uses real HTTP requests for socket-level behavior (binding, fallback,
 * one-shot semantics, releasing the port). A narrow createServer seam covers
 * default-port and address-reconciliation states that cannot be forced
 * reliably through the operating system.
 */

import { EventEmitter } from 'node:events';
import { createServer } from 'node:http';
import type * as NodeHttp from 'node:http';
import type { Server as HttpServer } from 'node:http';
import { connect } from 'node:net';
import type { Socket } from 'node:net';

import { describe, it, expect, afterEach, vi } from 'vitest';

// Track every HTTP server the callback helper creates so a test can reach the
// bound instance and emit a post-bind 'error' on it.
const createdServers: HttpServer[] = [];
type CreateServerOverride = (...args: Parameters<typeof NodeHttp.createServer>) => HttpServer;
let createServerOverride: CreateServerOverride | undefined;
vi.mock('node:http', async () => {
  const actual = await vi.importActual<typeof NodeHttp>('node:http');
  return {
    ...actual,
    createServer: (...args: Parameters<typeof actual.createServer>) => {
      const server = createServerOverride ? createServerOverride(...args) : actual.createServer(...args);
      createdServers.push(server);
      return server;
    },
  };
});

import type { OAuthCallbackServer } from './oauth-callback-server.js';
import { createOAuthCallbackServer, getCallbackUrlCandidates } from './oauth-callback-server.js';
import { hasSameLoopbackCallbackTarget } from './oauth-loopback.js';

const STATE = 'expected-state';

/**
 * Finds a port that is currently free by binding an ephemeral port and
 * releasing it. Keeps tests independent of hardcoded port availability.
 */
async function getFreePort(hostname = '127.0.0.1'): Promise<number> {
  return new Promise((resolve, reject) => {
    const probe = createServer();
    probe.once('error', reject);
    probe.listen(0, hostname, () => {
      const address = probe.address();
      if (!address || typeof address === 'string') {
        reject(new Error('Failed to probe for a free port'));
        return;
      }
      probe.close(() => resolve(address.port));
    });
  });
}

function occupyPort(port: number): Promise<HttpServer> {
  return new Promise((resolve, reject) => {
    const blocker = createServer();
    blocker.once('error', reject);
    blocker.listen(port, '127.0.0.1', () => resolve(blocker));
  });
}

function closeServer(server: HttpServer): Promise<void> {
  return new Promise((resolve, reject) => {
    server.close(error => (error ? reject(error) : resolve()));
  });
}

function mockNextServerBinding(
  actualPort: number,
  portsInUse: number[] = [],
  address: { address: string; family: 'IPv4' | 'IPv6' } = { address: '127.0.0.1', family: 'IPv4' },
) {
  const server = new EventEmitter() as unknown as HttpServer;
  const listen = vi.fn((port: number, _hostname: string) => {
    queueMicrotask(() => {
      if (portsInUse.includes(port)) {
        server.emit('error', Object.assign(new Error(`Port ${port} is in use`), { code: 'EADDRINUSE' }));
      } else {
        server.emit('listening');
      }
    });
    return server;
  });

  server.listen = listen as unknown as HttpServer['listen'];
  server.address = vi.fn(() => ({ ...address, port: actualPort }));
  server.closeIdleConnections = vi.fn();
  server.closeAllConnections = vi.fn();
  server.close = vi.fn((callback?: (error?: Error) => void) => {
    queueMicrotask(() => callback?.());
    return server;
  }) as unknown as HttpServer['close'];

  createServerOverride = () => server;
  return { listen };
}

describe('getCallbackUrlCandidates', () => {
  it('returns the preferred URL followed by sequential fallback ports', () => {
    const candidates = getCallbackUrlCandidates('http://127.0.0.1:5533/oauth/callback');

    expect(candidates).toHaveLength(11);
    expect(candidates[0]!.toString()).toBe('http://127.0.0.1:5533/oauth/callback');
    expect(candidates.map(url => Number(url.port))).toEqual([
      5533, 5534, 5535, 5536, 5537, 5538, 5539, 5540, 5541, 5542, 5543,
    ]);
    expect(candidates.every(url => url.pathname === '/oauth/callback')).toBe(true);
  });

  it('stops the fallback range at the maximum valid port', () => {
    // URL.port silently ignores out-of-range assignments, so without the cap
    // the overflowing candidates would keep the previous (duplicate) port.
    const candidates = getCallbackUrlCandidates('http://127.0.0.1:65533/oauth/callback');

    expect(candidates.map(url => Number(url.port))).toEqual([65533, 65534, 65535]);
  });

  it('keeps the default HTTP port as numeric binding metadata', () => {
    const candidates = getCallbackUrlCandidates('http://127.0.0.1/oauth/callback');

    expect(candidates).toHaveLength(11);
    expect(candidates[0]!.toString()).toBe('http://127.0.0.1/oauth/callback');
    expect(candidates[0]!.port).toBe('');
    expect(candidates[1]!.port).toBe('81');
    expect(candidates.at(-1)!.port).toBe('90');
  });

  it('preserves explicit port 0 as an ephemeral binding request', () => {
    const candidates = getCallbackUrlCandidates('http://127.0.0.1:0/oauth/callback');

    expect(candidates.map(url => Number(url.port))).toEqual([0]);
  });

  it('normalizes verified IPv4 and IPv6 loopback literals', () => {
    expect(getCallbackUrlCandidates('http://127.1:0/oauth/callback')[0]!.toString()).toBe(
      'http://127.0.0.1:0/oauth/callback',
    );
    expect(getCallbackUrlCandidates('http://[0:0:0:0:0:0:0:1]:0/oauth/callback')[0]!.toString()).toBe(
      'http://[::1]:0/oauth/callback',
    );
  });

  it.each(['https://127.0.0.1/oauth/callback', 'ftp://127.0.0.1/oauth/callback'])(
    'rejects a non-HTTP callback URL: %s',
    redirectUrl => {
      expect(() => getCallbackUrlCandidates(redirectUrl)).toThrow(/must use HTTP and a loopback IP literal/);
    },
  );

  it.each(['http://localhost/oauth/callback', 'http://example.com/oauth/callback'])(
    'rejects a hostname instead of resolving it: %s',
    redirectUrl => {
      expect(() => getCallbackUrlCandidates(redirectUrl)).toThrow(/must use HTTP and a loopback IP literal/);
    },
  );

  it.each([
    'http://user@127.0.0.1:5533/oauth/callback',
    'http://user:secret@127.0.0.1:5533/oauth/callback',
    'http://127.0.0.1:5533/oauth/callback#fragment',
    'http://127.0.0.1:5533/oauth/callback#',
  ])('rejects callback URL userinfo and fragments: %s', redirectUrl => {
    expect(() => getCallbackUrlCandidates(redirectUrl)).toThrow(/without userinfo or a fragment/);
  });

  it('matches persisted ephemeral callbacks only when the port is the sole difference', () => {
    const configured = 'http://127.0.0.1:0/oauth/callback?tenant=one';
    const prior = 'http://127.0.0.1:43123/oauth/callback?tenant=one';

    expect(hasSameLoopbackCallbackTarget(configured, prior)).toBe(true);
    expect(hasSameLoopbackCallbackTarget(prior, 'http://127.0.0.2:43124/oauth/callback?tenant=one')).toBe(false);
    expect(hasSameLoopbackCallbackTarget(prior, 'http://127.0.0.1:43124/other?tenant=one')).toBe(false);
    expect(hasSameLoopbackCallbackTarget(prior, 'http://127.0.0.1:43124/oauth/callback?tenant=two')).toBe(false);
  });
});

describe('createOAuthCallbackServer', () => {
  let callbackServer: OAuthCallbackServer | undefined;

  afterEach(async () => {
    await callbackServer?.close().catch(() => {});
    callbackServer = undefined;
    createServerOverride = undefined;
    createdServers.length = 0;
  });

  async function startCallbackServer(): Promise<OAuthCallbackServer> {
    const port = await getFreePort();
    callbackServer = await createOAuthCallbackServer({
      redirectUrl: `http://127.0.0.1:${port}/oauth/callback`,
      state: STATE,
    });
    return callbackServer;
  }

  it('captures the authorization code from the callback request', async () => {
    const server = await startCallbackServer();

    const pending = server.waitForCode();
    const response = await fetch(`${server.url}?code=auth-code-123&state=${STATE}`);
    const body = await response.text();

    expect(response.status).toBe(200);
    expect(body).toContain('close this tab');
    expect(body).not.toContain('auth-code-123');
    await expect(pending).resolves.toEqual({ code: 'auth-code-123', state: STATE });
  });

  it('ignores requests without a matching state so they cannot settle the flow', async () => {
    const server = await startCallbackServer();
    const pending = server.waitForCode();

    // Neither a forged code nor a forged denial (the state authenticates the
    // redirect) may settle the pending flow.
    const forgedCode = await fetch(`${server.url}?code=forged-code&state=wrong-state`);
    expect(forgedCode.status).toBe(400);
    const forgedDenial = await fetch(`${server.url}?error=access_denied`);
    expect(forgedDenial.status).toBe(400);

    const genuine = await fetch(`${server.url}?code=auth-code-123&state=${STATE}`);
    expect(genuine.status).toBe(200);
    await expect(pending).resolves.toEqual({ code: 'auth-code-123', state: STATE });
  });

  it('rejects on a state-matching OAuth error response, including the description', async () => {
    const server = await startCallbackServer();

    const pending = expect(server.waitForCode()).rejects.toThrow(/access_denied.*User denied the request/);
    const response = await fetch(
      `${server.url}?error=access_denied&error_description=${encodeURIComponent('User denied the request')}&state=${STATE}`,
    );

    expect(response.status).toBe(400);
    await pending;
  });

  it('rejects when no callback arrives before the timeout', async () => {
    const server = await startCallbackServer();

    await expect(server.waitForCode({ timeoutMs: 50 })).rejects.toThrow(/Timed out waiting for OAuth callback/);
  });

  it('is one-shot: subsequent callback requests receive 410', async () => {
    const server = await startCallbackServer();

    const pending = server.waitForCode();
    await fetch(`${server.url}?code=auth-code-123&state=${STATE}`);
    await pending;

    const replay = await fetch(`${server.url}?code=another-code&state=${STATE}`);
    expect(replay.status).toBe(410);
  });

  it('rejects pending waitForCode when closed before a code arrives', async () => {
    const server = await startCallbackServer();

    const pending = server.waitForCode();
    await server.close();

    await expect(pending).rejects.toThrow(/closed before receiving an authorization code/);
  });

  it('falls back to the next port when the preferred port is in use', async () => {
    const preferredPort = await getFreePort();
    const blocker = await occupyPort(preferredPort);

    try {
      callbackServer = await createOAuthCallbackServer({
        redirectUrl: `http://127.0.0.1:${preferredPort}/oauth/callback`,
        state: STATE,
      });

      expect(callbackServer.port).toBe(preferredPort + 1);
      expect(callbackServer.url.toString()).toBe(`http://127.0.0.1:${preferredPort + 1}/oauth/callback`);
    } finally {
      await closeServer(blocker);
    }
  });

  it('binds port 80 when the HTTP redirect URL omits a port', async () => {
    const { listen } = mockNextServerBinding(80);

    callbackServer = await createOAuthCallbackServer({
      redirectUrl: 'http://127.0.0.1/oauth/callback',
      state: STATE,
    });

    expect(listen).toHaveBeenCalledWith(80, '127.0.0.1');
    expect(callbackServer.port).toBe(80);
    expect(callbackServer.url.toString()).toBe('http://127.0.0.1/oauth/callback');
  });

  it('falls back sequentially when the default HTTP port is in use', async () => {
    const { listen } = mockNextServerBinding(81, [80]);

    callbackServer = await createOAuthCallbackServer({
      redirectUrl: 'http://127.0.0.1/oauth/callback',
      state: STATE,
    });

    expect(listen.mock.calls).toEqual([
      [80, '127.0.0.1'],
      [81, '127.0.0.1'],
    ]);
    expect(callbackServer.port).toBe(81);
    expect(callbackServer.url.toString()).toBe('http://127.0.0.1:81/oauth/callback');
  });

  it('supports explicit port 0 and reports the actual ephemeral port', async () => {
    const { listen } = mockNextServerBinding(43123);

    callbackServer = await createOAuthCallbackServer({
      redirectUrl: 'http://127.0.0.1:0/oauth/callback',
      state: STATE,
    });

    expect(listen).toHaveBeenCalledWith(0, '127.0.0.1');
    expect(callbackServer.port).toBe(43123);
    expect(callbackServer.url.toString()).toBe('http://127.0.0.1:43123/oauth/callback');
  });

  it('binds a normalized IPv6 loopback literal without hostname resolution', async () => {
    const { listen } = mockNextServerBinding(43123, [], { address: '::1', family: 'IPv6' });

    callbackServer = await createOAuthCallbackServer({
      redirectUrl: 'http://[0:0:0:0:0:0:0:1]:0/oauth/callback',
      state: STATE,
    });

    expect(listen).toHaveBeenCalledWith(0, '::1');
    expect(callbackServer.port).toBe(43123);
    expect(callbackServer.url.toString()).toBe('http://[::1]:43123/oauth/callback');
  });

  it('reconciles the returned URL with the actual server address port', async () => {
    const { listen } = mockNextServerBinding(60999);

    callbackServer = await createOAuthCallbackServer({
      redirectUrl: 'http://127.0.0.1:5533/oauth/callback',
      state: STATE,
    });

    expect(listen).toHaveBeenCalledWith(5533, '127.0.0.1');
    expect(callbackServer.port).toBe(60999);
    expect(callbackServer.url.toString()).toBe('http://127.0.0.1:60999/oauth/callback');
  });

  it.each(['https://127.0.0.1/oauth/callback', 'ftp://127.0.0.1/oauth/callback'])(
    'rejects a non-HTTP callback URL before creating a server: %s',
    async redirectUrl => {
      await expect(createOAuthCallbackServer({ redirectUrl, state: STATE })).rejects.toThrow(
        /must use HTTP and a loopback IP literal/,
      );
      expect(createdServers).toHaveLength(0);
    },
  );

  it.each(['http://localhost/oauth/callback', 'http://example.com/oauth/callback'])(
    'rejects a callback hostname before creating a server: %s',
    async redirectUrl => {
      await expect(createOAuthCallbackServer({ redirectUrl, state: STATE })).rejects.toThrow(
        /must use HTTP and a loopback IP literal/,
      );
      expect(createdServers).toHaveLength(0);
    },
  );

  it.each([
    'http://user@127.0.0.1:5533/oauth/callback',
    'http://127.0.0.1:5533/oauth/callback#fragment',
    'http://127.0.0.1:5533/oauth/callback#',
  ])('rejects callback URL userinfo or a fragment before creating a server: %s', async redirectUrl => {
    await expect(createOAuthCallbackServer({ redirectUrl, state: STATE })).rejects.toThrow(
      /without userinfo or a fragment/,
    );
    expect(createdServers).toHaveLength(0);
  });

  it('binds the IPv4 loopback literal from the redirect URL', async () => {
    const hostname = '127.0.0.2';
    const port = await getFreePort(hostname);
    callbackServer = await createOAuthCallbackServer({
      redirectUrl: `http://${hostname}:${port}/oauth/callback`,
      state: STATE,
    });

    const pending = callbackServer.waitForCode();
    const response = await fetch(`http://${hostname}:${port}/oauth/callback?code=auth-code-123&state=${STATE}`);

    expect(response.status).toBe(200);
    await expect(pending).resolves.toEqual({ code: 'auth-code-123', state: STATE });
  });

  it('releases the port on close', async () => {
    const server = await startCallbackServer();
    const { port } = server;

    await server.close();
    callbackServer = undefined;

    const reclaimed = await occupyPort(port);
    await closeServer(reclaimed);
  });

  it('releases the port on close despite an idle keep-alive connection', async () => {
    const server = await startCallbackServer();
    const { port } = server;

    // Complete the flow over a raw socket that stays open afterwards, the way
    // a browser holds the callback connection alive after the response.
    const socket = await new Promise<Socket>((resolve, reject) => {
      const client = connect(port, '127.0.0.1', () => resolve(client));
      client.once('error', reject);
    });
    try {
      const pending = server.waitForCode();
      socket.write(
        `GET /oauth/callback?code=auth-code-123&state=${STATE} HTTP/1.1\r\nHost: 127.0.0.1:${port}\r\nConnection: keep-alive\r\n\r\n`,
      );
      await pending;

      // close() must not wait for the keep-alive socket's timeout to release
      // the port; without closeIdleConnections this close() hangs.
      await server.close();
      callbackServer = undefined;

      const reclaimed = await occupyPort(port);
      await closeServer(reclaimed);
    } finally {
      socket.destroy();
    }
  });

  it('bounds close when a peer leaves an active HTTP request incomplete', async () => {
    const server = await startCallbackServer();
    const { port } = server;
    const socket = await new Promise<Socket>((resolve, reject) => {
      const client = connect(port, '127.0.0.1', () => resolve(client));
      client.once('error', reject);
    });
    socket.on('error', () => {});

    try {
      // Omit the terminating blank line so Node keeps this request active
      // indefinitely rather than classifying the connection as idle.
      socket.write(`GET /oauth/callback HTTP/1.1\r\nHost: 127.0.0.1:${port}\r\n`);

      const closeResult = await Promise.race([
        server.close().then(() => 'closed' as const),
        new Promise<'timeout'>(resolve => setTimeout(() => resolve('timeout'), 1_000)),
      ]);
      expect(closeResult).toBe('closed');
      callbackServer = undefined;

      const reclaimed = await occupyPort(port);
      await closeServer(reclaimed);
    } finally {
      socket.destroy();
    }
  });

  it('settles the flow instead of crashing when the server errors after binding', async () => {
    const port = await getFreePort();
    createdServers.length = 0;
    callbackServer = await createOAuthCallbackServer({
      redirectUrl: `http://127.0.0.1:${port}/oauth/callback`,
      state: STATE,
    });

    const boundServer = createdServers.at(-1)!;
    expect(boundServer).toBeDefined();
    // The bind-time listeners are once()-based and self-remove, so after bind
    // the server has no 'error' listener. Emitting one with no handler would
    // throw and crash the host process; the persistent listener must absorb it
    // and reject the pending waitForCode instead.
    const pending = callbackServer.waitForCode();
    boundServer.emit('error', new Error('post-bind socket failure'));

    await expect(pending).rejects.toThrow(/post-bind socket failure/);
  });
});
