import { Busboy } from '@fastify/busboy';
import type { ToolsInput } from '@mastra/core/agent';
import type { Mastra } from '@mastra/core/mastra';
import type { RequestContext } from '@mastra/core/request-context';
import type { InMemoryTaskStore } from '@mastra/server/a2a/store';
import type { MCPHttpTransportResult, MCPSseTransportResult } from '@mastra/server/handlers/mcp';
import type { ParsedRequestParams, ServerRoute } from '@mastra/server/server-adapter';
import {
  MastraServer as MastraServerBase,
  checkRouteFGA,
  isZodError,
  normalizeQueryParams,
  redactSensitiveQueryParams,
  redactStreamChunk,
  serializeStreamChunk,
} from '@mastra/server/server-adapter';
import { errorCodes } from 'fastify';
import type { FastifyInstance, FastifyReply, FastifyRequest, preHandlerHookHandler, RouteHandlerMethod } from 'fastify';
export { createAuthMiddleware } from './auth-middleware';
export type { FastifyAuthMiddlewareOptions } from './auth-middleware';

type HasPermissionFn = (userPerms: string[], required: string) => boolean;
let _hasPermissionPromise: Promise<HasPermissionFn | undefined> | undefined;
function loadHasPermission(): Promise<HasPermissionFn | undefined> {
  if (!_hasPermissionPromise) {
    _hasPermissionPromise = import('@mastra/core/auth/ee')
      .then(m => m.hasPermission)
      .catch(() => {
        console.error(
          '[@mastra/fastify] Auth features require @mastra/core >= 1.6.0. Please upgrade: npm install @mastra/core@latest',
        );
        return undefined;
      });
  }
  return _hasPermissionPromise;
}

/**
 * Derive the request origin (scheme + authority) for URL construction.
 *
 * `request.protocol` and `request.host` are Fastify getters that honor
 * `trustProxy` (x-forwarded-proto / x-forwarded-host), so scheme and
 * authority come from the same trust domain, and `request.host` falls back
 * to the HTTP/2 `:authority` pseudo-header. Both can still be empty for
 * fully host-less requests (e.g. HTTP/1.0 without a Host header, where
 * `request.host` is `''`), so keep final fallbacks: derived URLs must never
 * contain `undefined` or an empty authority.
 */
function requestOrigin(request: FastifyRequest): string {
  const protocol = request.protocol || 'http';
  const host = request.host || 'localhost';
  return `${protocol}://${host}`;
}

/**
 * Convert Fastify request to Web API Request for cookie-based auth providers.
 */
function toWebRequest(request: FastifyRequest): globalThis.Request {
  const url = `${requestOrigin(request)}${request.url}`;

  const headers = new Headers();
  for (const [key, value] of Object.entries(request.headers)) {
    // HTTP/2 pseudo-headers (:method, :path, :authority, ...) are not legal
    // Web API header names — Headers.set() throws on them.
    if (value && !key.startsWith(':')) {
      if (Array.isArray(value)) {
        value.forEach(v => headers.append(key, v));
      } else {
        headers.set(key, value);
      }
    }
  }

  return new globalThis.Request(url, {
    method: request.method,
    headers,
  });
}

function isRequestAborted(rawRequest: FastifyRequest['raw']): boolean {
  // Fastify can emit request close after a POST body is fully consumed while
  // the response stream is still active, so only treat it as disconnect when
  // the request itself reports an abort or never completed.
  return rawRequest.aborted || rawRequest.readableAborted || !rawRequest.complete;
}

/**
 * Fastify's own factory default for `bodyLimit` (1 MiB). The multipart lane
 * cannot rely on Fastify's enforcement — the multipart content-type parser
 * deliberately never hands the payload to Fastify's byte counting — so the
 * aggregate cap mirrors what the JSON lane would have allowed on the same
 * server: route cap, else adapter-wide cap, else the server's `bodyLimit`.
 * `request.server.initialConfig.bodyLimit` normally supplies that last value;
 * this constant is the fallback for exotic instances that do not expose it.
 */
const FASTIFY_DEFAULT_BODY_LIMIT_BYTES = 1024 * 1024;

/**
 * Smallest wire cost of one multipart part: the delimiter line for a one-char
 * boundary (`--b\r\n`), a minimal `content-disposition` header, the blank
 * header/body separator, and the part's trailing CRLF. Used to derive busboy
 * part/field/file COUNT caps from the byte cap: a request with more than
 * `maxTotalBytes / MIN_MULTIPART_PART_WIRE_BYTES` parts necessarily also
 * breaches the aggregate byte cap, so the derived count limits can never
 * reject a request the aggregate accounting would have allowed — they are
 * fail-closed backstops against parser-state abuse, not the primary gate.
 */
const MIN_MULTIPART_PART_WIRE_BYTES = 30;

/**
 * The exact error the hardened non-multipart body-limit lane produces: Fastify
 * rejects an over-limit body with `FST_ERR_CTP_BODY_TOO_LARGE` (413) from its
 * content-type parser. Reusing the same error class means the multipart lane's
 * breaches flow through Fastify's default error handler and serialize to the
 * identical status/shape (`statusCode`/`code`/`error`/`message`).
 */
function createBodyTooLargeError(): Error {
  return new errorCodes.FST_ERR_CTP_BODY_TOO_LARGE();
}

function isBodyTooLargeError(error: Error): boolean {
  return (error as { code?: string }).code === 'FST_ERR_CTP_BODY_TOO_LARGE';
}

/**
 * A multipart request stream that closes before the body completed (client
 * abort). Distinct from a body-limit breach: surfaces as a bodyParseError
 * (400 attempt to a client that is usually already gone) rather than a 413.
 */
function createPrematureCloseError(): Error {
  return new Error('Multipart request stream closed before the body completed');
}

// Extend Fastify types to include Mastra context
declare module 'fastify' {
  interface FastifyRequest {
    mastra: Mastra;
    requestContext: RequestContext;
    registeredTools: ToolsInput;
    abortSignal: AbortSignal;
    taskStore: InMemoryTaskStore;
    customRouteAuthConfig?: Map<string, boolean>;
  }
}

export class MastraServer extends MastraServerBase<FastifyInstance, FastifyRequest, FastifyReply> {
  createContextMiddleware(): preHandlerHookHandler {
    return async (request: FastifyRequest, reply: FastifyReply) => {
      // Parse request context from request body and add to context
      let bodyRequestContext: Record<string, any> | undefined;
      let paramsRequestContext: Record<string, any> | undefined;

      // Parse request context from request body (POST/PUT)
      if (request.method === 'POST' || request.method === 'PUT') {
        const contentType = request.headers['content-type'];
        if (contentType?.includes('application/json') && request.body) {
          const body = request.body as { requestContext?: Record<string, any> };
          if (body.requestContext) {
            bodyRequestContext = body.requestContext;
          }
        }
      }

      // Parse request context from query params (GET)
      if (request.method === 'GET') {
        try {
          const query = request.query as Record<string, string>;
          const encodedRequestContext = query.requestContext;
          if (typeof encodedRequestContext === 'string') {
            // Try JSON first
            try {
              paramsRequestContext = JSON.parse(encodedRequestContext);
            } catch {
              // Fallback to base64(JSON)
              try {
                const json = Buffer.from(encodedRequestContext, 'base64').toString('utf-8');
                paramsRequestContext = JSON.parse(json);
              } catch {
                // ignore if still invalid
              }
            }
          }
        } catch {
          // ignore query parsing errors
        }
      }

      const requestContext = this.mergeRequestContext({ paramsRequestContext, bodyRequestContext });
      this.applyRequestMetadataToContext({
        requestContext,
        getHeader: name => {
          const value = request.headers[name.toLowerCase()];
          return Array.isArray(value) ? value[0] : value;
        },
      });

      // Set context in request object
      request.requestContext = requestContext;
      request.mastra = this.mastra;
      request.registeredTools = this.tools || {};
      if (this.taskStore) {
        request.taskStore = this.taskStore;
      }
      request.customRouteAuthConfig = this.customRouteAuthConfig;

      // Create abort controller for request cancellation
      const controller = new AbortController();
      request.raw.on('close', () => {
        if (isRequestAborted(request.raw)) {
          controller.abort();
        }
      });
      reply.raw.on('close', () => {
        // Response close fires for normal completion too; only abort if the
        // response did not finish successfully.
        if (!reply.raw.writableEnded) {
          controller.abort();
        }
      });
      request.abortSignal = controller.signal;
    };
  }

  async stream(
    route: ServerRoute,
    reply: FastifyReply,
    result: { fullStream: ReadableStream },
    request?: FastifyRequest,
  ): Promise<void> {
    // Capture headers set by plugins (e.g., @fastify/cors) BEFORE hijacking
    // reply.hijack() bypasses Fastify's response handling, so we need to preserve
    // any headers that were set by hooks/plugins and manually include them
    const rawHeaders = reply.getHeaders();
    // Filter out undefined values and conflicting headers (content-length, transfer-encoding)
    // Having both Content-Length and Transfer-Encoding: chunked violates RFC 7230
    const existingHeaders: Record<string, string | number | string[]> = {};
    for (const [key, value] of Object.entries(rawHeaders)) {
      if (value === undefined) continue;
      const lowerKey = key.toLowerCase();
      if (lowerKey === 'content-length' || lowerKey === 'transfer-encoding') continue;
      existingHeaders[key] = value;
    }

    // Hijack the reply to take control of the response
    // This is required when writing directly to reply.raw
    reply.hijack();

    const streamFormat = route.streamFormat || 'stream';

    // Write headers directly to the raw response, merging existing headers (like CORS)
    // with our stream-specific headers
    const sseHeaders =
      streamFormat === 'sse'
        ? {
            'Content-Type': 'text/event-stream',
            'Cache-Control': 'no-cache',
            Connection: 'keep-alive',
            'X-Accel-Buffering': 'no',
          }
        : {
            'Content-Type': 'text/plain',
          };

    reply.raw.writeHead(200, {
      ...existingHeaders,
      ...sseHeaders,
      'Transfer-Encoding': 'chunked',
    });

    if (streamFormat === 'sse' && route.sseFlushOnConnect) {
      reply.raw.write(': connected\n\n');
    }

    const readableStream = result instanceof ReadableStream ? result : result.fullStream;
    const reader = readableStream.getReader();

    let readerCanceled = false;
    const cancelReader = (reason: string) => {
      if (readerCanceled) return;
      readerCanceled = true;
      void reader.cancel(reason);
    };
    const cancelReaderOnResponseClose = () => cancelReader('request aborted');
    const cancelReaderOnRequestClose = () => {
      if (request && isRequestAborted(request.raw)) {
        cancelReader('request aborted');
      }
    };
    reply.raw.on('close', cancelReaderOnResponseClose);
    request?.raw.on('close', cancelReaderOnRequestClose);

    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        if (value) {
          if (streamFormat === 'sse' && typeof value === 'string' && value.startsWith(':')) {
            reply.raw.write(value);
            continue;
          }

          // Optionally redact sensitive data (system prompts, tool definitions, API keys) before sending to the client
          const shouldRedact = this.streamOptions?.redact ?? true;
          const outputValue = shouldRedact ? redactStreamChunk(value) : value;
          // A chunk that can't be serialized must not kill the stream — skip it and keep streaming
          const serialized = serializeStreamChunk(outputValue);
          if (!serialized.ok) {
            this.mastra.getLogger()?.error('Failed to serialize stream chunk, skipping', {
              path: route.path,
              chunkType: (outputValue as { type?: string })?.type,
              error: serialized.error.message,
            });
            continue;
          }
          if (streamFormat === 'sse') {
            reply.raw.write(`data: ${serialized.json}\n\n`);
          } else {
            reply.raw.write(serialized.json + '\x1E');
          }
        }
      }
    } catch (error) {
      this.mastra.getLogger()?.error('Error in stream processing', {
        error: error instanceof Error ? { message: error.message, stack: error.stack } : error,
      });
    } finally {
      reply.raw.off('close', cancelReaderOnResponseClose);
      request?.raw.off('close', cancelReaderOnRequestClose);
      if (!reply.raw.writableEnded && !reply.raw.destroyed) {
        reply.raw.end();
      }
    }
  }

  async getParams(route: ServerRoute, request: FastifyRequest): Promise<ParsedRequestParams> {
    const urlParams = (request.params || {}) as Record<string, string>;
    // Fastify's request.query can contain string | string[] for repeated params
    const queryParams = normalizeQueryParams((request.query || {}) as Record<string, unknown>);
    let body: unknown;
    let bodyParseError: { message: string } | undefined;
    let rawBody: Uint8Array | string | undefined;

    if (route.method === 'POST' || route.method === 'PUT' || route.method === 'PATCH' || route.method === 'DELETE') {
      const contentType = request.headers['content-type'] || '';

      // Routes that verify a provider signature over the EXACT bytes (e.g. channel
      // webhooks) opt out of parsing AND need the unparsed body. The JSON content-type
      // parser (registerContextMiddleware) leaves request.body as the raw Buffer for
      // these routes; surface it as `rawBody` and leave `body`/`bodyParseError`
      // undefined so a signed-but-not-strict-JSON payload is not rejected with a 400
      // before the signature is checked. Mirrors the Hono adapter's getParams.
      if (route.skipBodyParse) {
        if (Buffer.isBuffer(request.body)) {
          rawBody = new Uint8Array(request.body.buffer, request.body.byteOffset, request.body.byteLength);
        } else if (typeof request.body === 'string') {
          rawBody = request.body;
        } else if (request.body instanceof Uint8Array) {
          rawBody = request.body;
        }
        return { urlParams, queryParams, body, bodyParseError, rawBody };
      }

      if (contentType.includes('multipart/form-data')) {
        try {
          // The multipart content-type parser deliberately ignores the payload
          // (registerContextMiddleware), so neither Fastify's server-wide
          // `bodyLimit` nor the per-route `bodyLimit` registered in
          // registerRoute() ever counts multipart bytes. Enforce the SAME cap
          // here as an AGGREGATE limit over the raw request stream, resolved
          // exactly like the non-multipart lane: route cap, else adapter-wide
          // cap, else the Fastify server's own bodyLimit.
          const maxTotalBytes =
            route.maxBodySize ??
            this.bodyLimitOptions?.maxSize ??
            request.server.initialConfig?.bodyLimit ??
            FASTIFY_DEFAULT_BODY_LIMIT_BYTES;
          body = await this.parseMultipartFormData(request, maxTotalBytes);
        } catch (error) {
          this.mastra.getLogger()?.error('Failed to parse multipart form data', {
            error: error instanceof Error ? { message: error.message, stack: error.stack } : error,
          });
          // Re-throw body-limit breaches so they escape the route handler and
          // surface through Fastify's default error handler with the exact 413
          // status/shape of the hardened non-multipart lane
          // (FST_ERR_CTP_BODY_TOO_LARGE); let others fall through to validation
          if (error instanceof Error && (isBodyTooLargeError(error) || error.message.toLowerCase().includes('size'))) {
            throw error;
          }
          bodyParseError = {
            message: error instanceof Error ? error.message : 'Failed to parse multipart form data',
          };
        }
      } else {
        body = request.body;
      }
    }

    return { urlParams, queryParams, body, bodyParseError, rawBody };
  }

  /**
   * Parse multipart/form-data using @fastify/busboy.
   * Converts file uploads to Buffers and parses JSON field values.
   *
   * Enforces `maxTotalBytes` as an AGGREGATE cap over the ENTIRE multipart
   * payload — every part body, boundary, and part header — by accounting the
   * raw request stream, because Fastify's own body-limit lane never engages for
   * multipart (the registered content-type parser ignores the payload). Busboy
   * per-file/per-field size caps and derived part/field/file count caps are
   * fail-closed backstops behind the aggregate accounting. A breach rejects
   * with the same `FST_ERR_CTP_BODY_TOO_LARGE` (413) the hardened
   * non-multipart lane produces.
   *
   * @param request - The Fastify request object
   * @param maxTotalBytes - Maximum aggregate payload size in bytes
   */
  private parseMultipartFormData(request: FastifyRequest, maxTotalBytes: number): Promise<Record<string, unknown>> {
    return new Promise((resolve, reject) => {
      // Same pre-buffer short-circuit Fastify applies in its own body-limit
      // lane: an honest Content-Length above the cap is rejected before a
      // single payload byte is read. Chunked or absent Content-Length falls
      // through to the streaming accounting below (NaN comparisons are false).
      const declaredLength = Number(request.headers['content-length']);
      if (declaredLength > maxTotalBytes) {
        reject(createBodyTooLargeError());
        return;
      }

      // A request stream that was already torn down (client aborted while an
      // earlier await was in flight) will never emit 'data'/'end'/'error'/
      // 'close' again — busboy would never finish and this promise would pend
      // forever. Fail it before registering any listeners.
      if (request.raw.destroyed || request.raw.readableAborted) {
        reject(createPrematureCloseError());
        return;
      }

      const result: Record<string, unknown> = {};
      const partCountLimit = Math.floor(maxTotalBytes / MIN_MULTIPART_PART_WIRE_BYTES) + 2;

      const busboy = new Busboy({
        headers: {
          'content-type': request.headers['content-type'] as string,
        },
        limits: {
          // A single file or field can never legitimately exceed the aggregate
          // cap, and @fastify/busboy's defaults (fieldSize 1 MiB; files,
          // fields, parts unlimited) must not outlive the route's cap.
          fileSize: maxTotalBytes,
          fieldSize: maxTotalBytes,
          files: partCountLimit,
          fields: partCountLimit,
          parts: partCountLimit,
        },
      });

      let settled = false;

      // Abort teardown, mirroring Fastify's hardened lane: stop observing and
      // consuming the request stream (no drain — once the 413 is flushed, Node
      // closes a connection whose request body was not fully read) and destroy
      // busboy so no further parts are parsed or buffered. unpipe BEFORE
      // destroy so the pipe never writes into a destroyed stream.
      const settleReject = (error: Error) => {
        if (settled) return;
        settled = true;
        request.raw.removeListener('data', onAggregateData);
        request.raw.removeListener('close', onRawClose);
        request.raw.unpipe(busboy);
        busboy.destroy();
        reject(error);
      };

      const settleResolve = () => {
        if (settled) return;
        settled = true;
        request.raw.removeListener('data', onAggregateData);
        request.raw.removeListener('close', onRawClose);
        resolve(result);
      };

      const abortTooLarge = () => settleReject(createBodyTooLargeError());

      // AGGREGATE accounting. Registered BEFORE .pipe() so it observes every
      // chunk ahead of busboy and the breach aborts at the earliest byte.
      let aggregateBytes = 0;
      const onAggregateData = (chunk: Buffer) => {
        aggregateBytes += chunk.length;
        if (aggregateBytes > maxTotalBytes) {
          abortTooLarge();
        }
      };

      busboy.on(
        'file',
        (fieldname: string, file: NodeJS.ReadableStream, _filename: string, _encoding: string, _mimetype: string) => {
          const chunks: Buffer[] = [];

          file.on('data', (chunk: Buffer) => {
            chunks.push(chunk);
          });

          // Per-file cap breach (backstop behind the aggregate accounting).
          file.on('limit', abortTooLarge);

          // Busboy re-emits parse errors on the in-flight file stream; without
          // a listener that becomes an unhandled 'error' crash. The busboy
          // 'error' handler below owns the rejection.
          file.on('error', () => {});

          file.on('end', () => {
            if (!settled) {
              result[fieldname] = Buffer.concat(chunks);
            }
          });
        },
      );

      busboy.on('field', (fieldname: string, value: string, _fieldnameTruncated: boolean, valueTruncated: boolean) => {
        // Fail closed on a truncated field: silently keeping the truncated
        // prefix would hand the handler a different body than the client
        // sent. With fieldSize == maxTotalBytes truncation implies the
        // aggregate cap is breached as well.
        if (valueTruncated) {
          abortTooLarge();
          return;
        }
        // Try to parse JSON strings (like 'options')
        try {
          result[fieldname] = JSON.parse(value);
        } catch {
          result[fieldname] = value;
        }
      });

      // Fail closed when a derived count cap trips instead of silently
      // truncating the part stream (busboy's default is to ignore the rest).
      busboy.on('partsLimit', abortTooLarge);
      busboy.on('filesLimit', abortTooLarge);
      busboy.on('fieldsLimit', abortTooLarge);

      busboy.on('finish', settleResolve);

      busboy.on('error', (error: Error) => {
        settleReject(error);
      });

      // A request stream that errors mid-parse (client abort) must tear the
      // parse down instead of leaving an unhandled 'error' and a forever-
      // pending promise.
      request.raw.once('error', (error: Error) => {
        settleReject(error);
      });

      // Premature close WITHOUT an 'error' event (e.g. the socket torn down
      // after the response side detached, or a destroy() without an error):
      // busboy never emits 'finish', so this promise would pend forever. On
      // normal completion 'end' fires first (readableEnded is true by the
      // time 'close' arrives), so this backstop never rejects a completed
      // body; after any settle it is a guarded no-op.
      const onRawClose = () => {
        if (!settled && !request.raw.readableEnded) {
          settleReject(createPrematureCloseError());
        }
      };
      request.raw.once('close', onRawClose);

      request.raw.on('data', onAggregateData);
      // Pipe the raw request to busboy
      request.raw.pipe(busboy);
    });
  }

  /**
   * Aggregate body cap for multipart requests to `skipBodyParse` routes.
   *
   * skipBodyParse routes return from getParams() before parseMultipartFormData,
   * and the multipart content-type parser deliberately never hands the payload
   * to Fastify's byte counting, so a multipart request to a skipBodyParse
   * route with a declared `maxBodySize` was entirely uncapped at the adapter
   * level. Applies to EXACTLY the `skipBodyParse && route.maxBodySize &&
   * multipart` combination; other content types on skipBodyParse routes stay
   * covered by Fastify's per-route `bodyLimit` (their parsers buffer through
   * Fastify's byte counting).
   *
   * An honest Content-Length above the cap throws the hardened lane's 413
   * before a payload byte is read. Otherwise a PASSIVE counter is attached via
   * `prependListener('data')` — unlike `.on('data')` this does not switch the
   * stream into flowing mode, so the unread stream reaches the handler exactly
   * as before (identical pause/pipe/read semantics, byte-exact) and the
   * counter only observes chunks the handler's own consumption causes to be
   * emitted. The moment the aggregate exceeds the cap the reply is rejected
   * with the SAME `FST_ERR_CTP_BODY_TOO_LARGE` 413 shape as the hardened lane
   * plus `connection: close` (mirroring Fastify's own content-type-parser
   * abort: the client may keep sending, so the connection must close). Node
   * then tears down the connection whose request body was never fully read,
   * which also terminates the handler's in-flight consumption — no hang. A
   * handler that already sent its own response (e.g. the harness channel
   * webhook's self-enforced rawBody cap) wins the race: the guard never
   * double-fires over a sent reply.
   */
  private installSkipBodyParseMultipartAggregateGuard(
    route: ServerRoute,
    request: FastifyRequest,
    reply: FastifyReply,
  ): void {
    if (!route.skipBodyParse || !route.maxBodySize) return;

    // Same body-bearing set as getParams()/the per-route bodyLimit lane.
    const method = String(request.method || '').toUpperCase();
    if (method !== 'POST' && method !== 'PUT' && method !== 'PATCH' && method !== 'DELETE') return;

    const contentType = request.headers['content-type'] || '';
    if (!contentType.includes('multipart/form-data')) return;

    const maxTotalBytes = route.maxBodySize;

    // Same pre-buffer short-circuit as the hardened multipart lane: an honest
    // Content-Length above the cap is rejected before a single payload byte is
    // read. Chunked or absent Content-Length falls through to the passive
    // accounting below (NaN comparisons are false).
    const declaredLength = Number(request.headers['content-length']);
    if (declaredLength > maxTotalBytes) {
      throw createBodyTooLargeError();
    }

    let aggregateBytes = 0;
    const onAggregateData = (chunk: Buffer | string) => {
      // A handler may setEncoding() before consuming; count BYTES either way
      // (same dance as Fastify's own body-limit accounting).
      aggregateBytes += typeof chunk === 'string' ? Buffer.byteLength(chunk) : chunk.length;
      if (aggregateBytes <= maxTotalBytes) return;
      // Teardown discipline of the hardened lane: stop observing first, and
      // never double-fire over a reply the handler already sent (the handler's
      // own enforcement won the race and owns its stream's lifecycle).
      request.raw.removeListener('data', onAggregateData);
      if (reply.sent) return;
      void reply.header('connection', 'close');
      // Destroy the half-read request stream, but only after the 413 has left
      // the process: a finished response is DETACHED from the connection's
      // abort path (Node's `resOnFinish` shifts the request out of
      // `state.incoming` before the socket teardown), so without an explicit
      // destroy an in-flight handler consumer (pipe/for-await) would wait
      // forever on a stream nobody will ever abort. Deferring to the reply's
      // 'close' means the destroy can never race the 413 flush.
      reply.raw.once('close', () => {
        if (!request.raw.readableEnded && !request.raw.destroyed) {
          request.raw.destroy(createBodyTooLargeError());
        }
      });
      void reply.send(createBodyTooLargeError());
    };
    request.raw.prependListener('data', onAggregateData);
  }

  async sendResponse(
    route: ServerRoute,
    reply: FastifyReply,
    result: unknown,
    request?: FastifyRequest,
    prefix?: string,
  ): Promise<void> {
    const resolvedPrefix = prefix ?? this.prefix ?? '';

    // Apply refresh headers from transparent session refresh (e.g. Set-Cookie after token refresh)
    if (result && typeof result === 'object' && '__refreshHeaders' in result) {
      const refreshHeaders = (result as any).__refreshHeaders as Record<string, string>;
      for (const [key, value] of Object.entries(refreshHeaders)) {
        reply.header(key, value);
      }
      delete (result as any).__refreshHeaders;
    }

    if (route.responseType === 'json') {
      await reply.send(result);
    } else if (route.responseType === 'stream') {
      await this.stream(route, reply, result as { fullStream: ReadableStream }, request);
    } else if (route.responseType === 'datastream-response') {
      // Handle AI SDK / harness Response objects (e.g. the SSE session-events route):
      // pipe the Web ReadableStream body to the Node response. We write directly to
      // `reply.raw`, so we must hijack the reply and `writeHead` the status + headers
      // ourselves — headers set via `reply.header()` are NOT flushed once we bypass
      // Fastify's send path, which would drop the `text/event-stream` content-type.
      // This mirrors the `stream()` method above. Preserve headers already set by
      // hooks/plugins (e.g. @fastify/cors), then merge the Response's own headers.
      const fetchResponse = result as globalThis.Response;

      const rawHeaders = reply.getHeaders();
      const mergedHeaders: Record<string, string | number | string[]> = {};
      for (const [key, value] of Object.entries(rawHeaders)) {
        if (value === undefined) continue;
        const lowerKey = key.toLowerCase();
        // Drop framing headers that conflict with chunked streaming.
        if (lowerKey === 'content-length' || lowerKey === 'transfer-encoding') continue;
        mergedHeaders[key] = value;
      }
      fetchResponse.headers.forEach((value, key) => {
        const lowerKey = key.toLowerCase();
        if (lowerKey === 'content-length' || lowerKey === 'transfer-encoding') return;
        mergedHeaders[key] = value;
      });

      reply.hijack();

      if (fetchResponse.body) {
        reply.raw.writeHead(fetchResponse.status, mergedHeaders);
        const reader = fetchResponse.body.getReader();
        let readerCanceled = false;

        const cancelReader = (reason: string) => {
          if (readerCanceled) return;
          readerCanceled = true;
          void reader.cancel(reason);
        };

        const cancelReaderOnResponseClose = () => cancelReader('request aborted');
        const cancelReaderOnRequestClose = () => {
          if (request && isRequestAborted(request.raw)) {
            cancelReader('request aborted');
          }
        };

        const onResError = (err: unknown) => {
          this.mastra.getLogger()?.error('Error writing datastream response', {
            error: err instanceof Error ? { message: err.message, stack: err.stack } : err,
          });
          cancelReader('response write error');
        };
        reply.raw.once('error', onResError);
        reply.raw.on('close', cancelReaderOnResponseClose);
        request?.raw.on('close', cancelReaderOnRequestClose);

        try {
          while (true) {
            const { done, value } = await reader.read();
            if (done) break;
            reply.raw.write(value);
          }
        } catch (error) {
          this.mastra.getLogger()?.error('Error in datastream processing', {
            error: error instanceof Error ? { message: error.message, stack: error.stack } : error,
          });
        } finally {
          reply.raw.off('error', onResError);
          reply.raw.off('close', cancelReaderOnResponseClose);
          request?.raw.off('close', cancelReaderOnRequestClose);
          if (!reply.raw.writableEnded && !reply.raw.destroyed) {
            reply.raw.end();
          }
        }
      } else {
        // No body (e.g. a 204/redirect Response): we already hijacked, so emit the
        // status + headers and end the raw response ourselves.
        reply.raw.writeHead(fetchResponse.status, mergedHeaders);
        reply.raw.end();
      }
    } else if (route.responseType === 'mcp-http') {
      // MCP Streamable HTTP transport - request is required
      if (!request) {
        await reply.status(500).send({ error: 'Request object required for MCP transport' });
        return;
      }

      const { server, httpPath, mcpOptions: routeMcpOptions } = result as MCPHttpTransportResult;

      try {
        // Hijack the response to bypass Fastify's response handling
        // This is required when we write directly to reply.raw
        reply.hijack();

        // Attach parsed body to raw request so MCP server's readJsonBody can use it
        // Fastify consumes the body stream, so we need to provide the pre-parsed body
        const rawReq = request.raw as typeof request.raw & { body?: unknown };
        if (request.body !== undefined) {
          rawReq.body = request.body;
        }

        // Merge class-level mcpOptions with route-specific options (route takes precedence)
        const options = { ...this.mcpOptions, ...routeMcpOptions };

        await server.startHTTP({
          url: new URL(request.url, requestOrigin(request)),
          httpPath: `${resolvedPrefix}${httpPath}`,
          req: rawReq,
          res: reply.raw,
          options: Object.keys(options).length > 0 ? options : undefined,
        });
        // Response handled by startHTTP
      } catch {
        if (!reply.raw.headersSent) {
          reply.raw.writeHead(500, { 'Content-Type': 'application/json' });
          reply.raw.end(
            JSON.stringify({
              jsonrpc: '2.0',
              error: { code: -32603, message: 'Internal server error' },
              id: null,
            }),
          );
        }
      }
    } else if (route.responseType === 'mcp-sse') {
      // MCP SSE transport - request is required
      if (!request) {
        await reply.status(500).send({ error: 'Request object required for MCP transport' });
        return;
      }

      const { server, ssePath, messagePath } = result as MCPSseTransportResult;

      try {
        // Hijack the response to bypass Fastify's response handling
        // This is required when we write directly to reply.raw for SSE
        reply.hijack();

        // Attach parsed body to raw request so MCP server's readJsonBody can use it
        // Fastify consumes the body stream, so we need to provide the pre-parsed body
        const rawReq = request.raw as typeof request.raw & { body?: unknown };
        if (request.body !== undefined) {
          rawReq.body = request.body;
        }

        await server.startSSE({
          url: new URL(request.url, requestOrigin(request)),
          ssePath: `${resolvedPrefix}${ssePath}`,
          messagePath: `${resolvedPrefix}${messagePath}`,
          req: rawReq,
          res: reply.raw,
        });
        // Response handled by startSSE
      } catch {
        if (!reply.raw.headersSent) {
          reply.raw.writeHead(500, { 'Content-Type': 'application/json' });
          reply.raw.end(JSON.stringify({ error: 'Error handling MCP SSE request' }));
        }
      }
    } else {
      reply.status(500);
    }
  }

  async registerRoute(
    app: FastifyInstance,
    route: ServerRoute,
    { prefix: prefixParam }: { prefix?: string } = {},
  ): Promise<void> {
    // Default prefix to this.prefix if not provided, or empty string
    const prefix = prefixParam ?? this.prefix ?? '';

    const fullPath = `${prefix}${route.path}`;

    // Convert Express-style :param to Fastify-style :param (they're the same, but ensure consistency)
    const fastifyPath = fullPath;

    // Define the route handler
    const handler: RouteHandlerMethod = async (request: FastifyRequest, reply: FastifyReply) => {
      // Build the WHATWG Request at most once per LANE per request, and only
      // when it is actually read. Constructing undici Headers + Request costs
      // several microseconds, and most requests never touch it: with no auth
      // configured `checkRouteAuth` returns before reading `context.request`,
      // and most handlers never read `ctx.request` — those paths construct
      // zero Requests.
      //
      // The auth lane gets its OWN memoized instance, separate from the
      // handler-visible one: auth callbacks (authenticateToken / legacy
      // authorize) are handed the constructed Request with its LIVE Headers
      // and may mutate it, and the handler's ctx.request must never observe
      // those mutations (pre-memoization, every read got a fresh copy). Within
      // the auth lane a single instance is shared across authenticateToken and
      // authorize — constructed lazily, once.
      let webRequest: globalThis.Request | undefined;
      const getWebRequest = () => (webRequest ??= toWebRequest(request));
      let authWebRequest: globalThis.Request | undefined;
      const getAuthWebRequest = () => (authWebRequest ??= toWebRequest(request));

      // Check route-level authentication/authorization
      const authError = await this.checkRouteAuth(route, {
        path: String(request.url.split('?')[0] || '/'),
        method: String(request.method || 'GET'),
        getHeader: name => request.headers[name.toLowerCase()] as string | undefined,
        getQuery: name => (request.query as Record<string, string>)[name],
        requestContext: request.requestContext,
        get request() {
          return getAuthWebRequest();
        },
        buildAuthorizeContext: getAuthWebRequest,
      });

      if (authError) {
        // Apply any refresh headers (e.g. Set-Cookie from transparent session refresh)
        if (authError.headers) {
          for (const [key, value] of Object.entries(authError.headers)) {
            void reply.header(key, value);
          }
        }

        // If this is an auth error (not just a success-with-headers), return error response
        if (authError.error) {
          return reply.status(authError.status).send({ error: authError.error });
        }
      }

      // `skipBodyParse` routes return from getParams() before the multipart
      // aggregate lane, so a multipart request to a skipBodyParse route with a
      // declared `maxBodySize` would otherwise reach its handler with NO
      // adapter-level aggregate cap (Fastify's own bodyLimit never engages for
      // the ignore-the-payload multipart parser either). Guard exactly that
      // combination. May throw the hardened lane's 413 (honest Content-Length
      // above the cap), which propagates through Fastify's default error
      // handler exactly like a getParams() body-limit breach.
      this.installSkipBodyParseMultipartAggregateGuard(route, request, reply);

      const params = await this.getParams(route, request);

      // Return 400 Bad Request if body parsing failed (e.g., malformed multipart data)
      if (params.bodyParseError) {
        return reply.status(400).send({
          error: 'Invalid request body',
          issues: [{ field: 'body', message: params.bodyParseError.message }],
        });
      }

      if (params.queryParams) {
        try {
          params.queryParams = await this.parseQueryParams(route, params.queryParams);
        } catch (error) {
          this.mastra.getLogger()?.error('Error parsing query params', {
            error: error instanceof Error ? { message: error.message, stack: error.stack } : error,
          });
          if (isZodError(error)) {
            const { status, body } = this.resolveValidationError(route, error, 'query');
            return reply.status(status).send(body);
          }
          return reply.status(400).send({
            error: 'Invalid query parameters',
            issues: [{ field: 'unknown', message: error instanceof Error ? error.message : 'Unknown error' }],
          });
        }
      }

      if (params.body) {
        try {
          params.body = await this.parseBody(route, params.body);
        } catch (error) {
          this.mastra.getLogger()?.error('Error parsing body', {
            error: error instanceof Error ? { message: error.message, stack: error.stack } : error,
          });
          if (isZodError(error)) {
            const { status, body } = this.resolveValidationError(route, error, 'body');
            return reply.status(status).send(body);
          }
          return reply.status(400).send({
            error: 'Invalid request body',
            issues: [{ field: 'unknown', message: error instanceof Error ? error.message : 'Unknown error' }],
          });
        }
      }

      // Parse path params through pathParamSchema for type coercion (e.g., z.coerce.number())
      if (params.urlParams) {
        try {
          params.urlParams = await this.parsePathParams(route, params.urlParams);
        } catch (error) {
          this.mastra.getLogger()?.error('Error parsing path params', {
            error: error instanceof Error ? { message: error.message, stack: error.stack } : error,
          });
          if (isZodError(error)) {
            const { status, body } = this.resolveValidationError(route, error, 'path');
            return reply.status(status).send(body);
          }
          return reply.status(400).send({
            error: 'Invalid path parameters',
            issues: [{ field: 'unknown', message: error instanceof Error ? error.message : 'Unknown error' }],
          });
        }
      }

      const handlerParams = {
        ...params.urlParams,
        ...params.queryParams,
        ...(typeof params.body === 'object' ? params.body : {}),
        requestContext: request.requestContext,
        mastra: this.mastra,
        registeredTools: request.registeredTools,
        taskStore: request.taskStore,
        abortSignal: request.abortSignal,
        routePrefix: prefix,
        serverRoutes: this.getServerRoutes(),
        getHeader: (name: string) => {
          const value = request.headers[name.toLowerCase()];
          return Array.isArray(value) ? value.join(',') : value;
        },
        // Full header set for handlers that must read every provider header (e.g. a
        // channel webhook reading its signature/timestamp headers). Mirrors Hono's
        // `getHeaders`. Fastify normalizes header names to lower-case already; drop
        // undefined values so the shape matches the transport-neutral contract.
        getHeaders: (): Record<string, string | string[]> => {
          const out: Record<string, string | string[]> = {};
          for (const [key, value] of Object.entries(request.headers)) {
            if (value !== undefined) out[key] = value;
          }
          return out;
        },
        // Unparsed request bytes for skipBodyParse routes (HMAC verification over the
        // exact bytes). Undefined for normally-parsed routes. Mirrors Hono's `rawBody`.
        rawBody: params.rawBody,
        requestBody: params.body,
        requestPathParams: params.urlParams,
        get request() {
          return getWebRequest();
        },
      };

      // Check route permission requirement (EE feature)
      // Uses convention-based permission derivation: permissions are auto-derived
      // from route path/method unless explicitly set or route is public
      const requestContext = request.requestContext;
      // Check if any auth is configured (studio or server) for RBAC
      const hasAuth = this.mastra.getStudio?.()?.auth || this.mastra.getServer()?.auth;
      if (hasAuth) {
        const hasPermission = await loadHasPermission();
        if (hasPermission) {
          const userPermissions = requestContext.get('mastra__userPermissions') as string[] | undefined;
          const permissionError = this.checkRoutePermission(route, userPermissions, hasPermission, requestContext);

          if (permissionError) {
            return reply.status(permissionError.status).send({
              error: permissionError.error,
              message: permissionError.message,
            });
          }
        }
      }

      // Check FGA authorization (EE feature)
      const fgaError = await checkRouteFGA(this.mastra, route, requestContext, {
        ...params.urlParams,
        ...params.queryParams,
        ...(typeof params.body === 'object' ? params.body : {}),
      });
      if (fgaError) {
        return reply.status(fgaError.status).send({ error: fgaError.error, message: fgaError.message });
      }

      try {
        const result = await route.handler(handlerParams);
        await this.sendResponse(route, reply, result, request, prefix);
      } catch (error) {
        const httpStatus = error && typeof error === 'object' && 'status' in error ? (error as any).status : undefined;
        const isClientError = typeof httpStatus === 'number' && httpStatus >= 400 && httpStatus < 500;
        if (!isClientError) {
          this.mastra.getLogger()?.error('Error calling handler', {
            error: error instanceof Error ? { message: error.message, stack: error.stack } : error,
            path: route.path,
            method: route.method,
          });
        }
        // Check if it's an HTTPException or MastraError with a status code
        let status = 500;
        if (error && typeof error === 'object') {
          // Check for direct status property (HTTPException)
          if ('status' in error) {
            status = (error as any).status;
          }
          // Check for MastraError with status in details
          else if (
            'details' in error &&
            error.details &&
            typeof error.details === 'object' &&
            'status' in error.details
          ) {
            status = (error.details as any).status;
          }
        }
        await reply.status(status).send({ error: error instanceof Error ? error.message : 'Unknown error' });
      }
    };

    // Resolve the per-route body size cap. A route-specific cap (`route.maxBodySize`)
    // takes precedence over the adapter-wide default and is honored INDEPENDENTLY of
    // `this.bodyLimitOptions`: a route that declares its own cap (e.g. the channel
    // webhook's 1 MiB) must get a real pre-buffer 413 even when the adapter was
    // constructed without global bodyLimitOptions (embedders/tests). Fastify's
    // per-route `bodyLimit` rejects with 413 before buffering the whole body, mirroring
    // the Hono adapter's per-route `bodyLimit` middleware.
    //
    // IMPORTANT: Fastify only enforces a per-route body cap when `bodyLimit` is a
    // TOP-LEVEL route option (`app.route({ bodyLimit })`); a `bodyLimit` nested under
    // `config` is silently ignored. `config` is reserved for `skipBodyParse`, which
    // the JSON content-type parser reads via `request.routeOptions.config` to capture
    // the raw bytes for this route instead of JSON-parsing them (see
    // registerContextMiddleware).
    // DELETE is included because DELETE requests may carry bodies too (#20015).
    const isBodyBearingMethod = ['POST', 'PUT', 'PATCH', 'DELETE'].includes(route.method.toUpperCase());
    const maxSize = isBodyBearingMethod ? (route.maxBodySize ?? this.bodyLimitOptions?.maxSize) : undefined;

    const config: { skipBodyParse?: boolean } | undefined = route.skipBodyParse ? { skipBodyParse: true } : undefined;
    const bodyLimit = maxSize || undefined;

    // Handle ALL method by registering for each HTTP method
    // Fastify doesn't support 'ALL' method natively like Express
    if (route.method.toUpperCase() === 'ALL') {
      // Only register the main HTTP methods that MCP actually uses
      // Skip HEAD/OPTIONS to avoid potential conflicts with Fastify's auto-generated routes
      const methods = ['GET', 'POST', 'PUT', 'DELETE', 'PATCH'] as const;
      for (const method of methods) {
        try {
          app.route({
            method,
            url: fastifyPath,
            handler,
            config,
            ...(bodyLimit !== undefined ? { bodyLimit } : {}),
          });
        } catch (err) {
          // Skip duplicate route errors - can happen if route is registered multiple times.
          // Match Fastify's stable error code instead of the English message; the code
          // exists across the entire supported peer range (verified in 5.8.4, 5.8.5,
          // 5.9.0, 5.10.0 and 5.11.0 — lib/errors.js FST_ERR_DUPLICATED_ROUTE).
          if (err instanceof Error && (err as { code?: string }).code === 'FST_ERR_DUPLICATED_ROUTE') {
            continue;
          }
          throw err;
        }
      }
    } else {
      app.route({
        method: route.method as 'GET' | 'POST' | 'PUT' | 'DELETE' | 'PATCH',
        url: fastifyPath,
        handler,
        config,
        ...(bodyLimit !== undefined ? { bodyLimit } : {}),
      });
    }
  }

  async registerCustomApiRoutes(): Promise<void> {
    if (!(await this.buildCustomRouteHandler())) return;

    const routes = this.customApiRoutes ?? this.mastra.getServer()?.apiRoutes ?? [];

    for (const route of routes) {
      // Create pseudo ServerRoute for auth checking
      const serverRoute: ServerRoute = {
        method: route.method as any,
        path: route.path,
        responseType: 'json',
        handler: async () => {},
        requiresAuth: route.requiresAuth,
        requiresPermission: route.requiresPermission,
        fga: route.fga,
      };

      const fastifyHandler: RouteHandlerMethod = async (request: FastifyRequest, reply: FastifyReply) => {
        // Same lazy memoized construction as registerRoute: at most one WHATWG
        // Request per request, built only when auth actually reads it.
        let webRequest: globalThis.Request | undefined;
        const getWebRequest = () => (webRequest ??= toWebRequest(request));

        // Per-route auth check (same pattern as registerRoute)
        const authError = await this.checkRouteAuth(serverRoute, {
          path: String(request.url.split('?')[0] || '/'),
          method: String(request.method || 'GET'),
          getHeader: name => request.headers[name.toLowerCase()] as string | undefined,
          getQuery: name => (request.query as Record<string, string>)[name],
          requestContext: request.requestContext,
          get request() {
            return getWebRequest();
          },
          buildAuthorizeContext: getWebRequest,
        });

        if (authError) {
          if (authError.headers) {
            for (const [key, value] of Object.entries(authError.headers)) {
              void reply.header(key, value);
            }
          }
          if (authError.error) {
            return reply.status(authError.status).send({ error: authError.error });
          }
        }

        const requestContext = request.requestContext;
        // Check if any auth is configured (studio or server) for RBAC
        const hasAuth = this.mastra.getStudio?.()?.auth || this.mastra.getServer()?.auth;
        if (hasAuth) {
          let hasPermission: ((userPerms: string[], required: string) => boolean) | undefined;
          try {
            ({ hasPermission } = await import('@mastra/core/auth/ee'));
          } catch {
            console.error(
              '[@mastra/fastify] Auth features require @mastra/core >= 1.6.0. Please upgrade: npm install @mastra/core@latest',
            );
          }

          if (hasPermission) {
            const userPermissions = requestContext.get('mastra__userPermissions') as string[] | undefined;
            const permissionError = this.checkRoutePermission(
              serverRoute,
              userPermissions,
              hasPermission,
              requestContext,
            );
            if (permissionError) {
              return reply.status(permissionError.status).send({
                error: permissionError.error,
                message: permissionError.message,
              });
            }
          }
        }

        // Check FGA authorization (EE feature)
        const fgaError = await checkRouteFGA(this.mastra, serverRoute, requestContext, {
          ...(request.params as Record<string, string>),
          ...(request.query as Record<string, string>),
          ...(typeof request.body === 'object' && request.body !== null
            ? (request.body as Record<string, unknown>)
            : {}),
        });
        if (fgaError) {
          return reply.status(fgaError.status).send({ error: fgaError.error, message: fgaError.message });
        }

        const response = await this.handleCustomRouteRequest(
          `${requestOrigin(request)}${request.url}`,
          request.method,
          request.headers as Record<string, string | string[] | undefined>,
          request.body,
          request.requestContext,
          request.abortSignal,
        );
        if (!response) {
          reply.status(404).send({ error: 'Not Found' });
          return;
        }
        // Merge headers set by Fastify hooks/plugins (e.g. @fastify/cors) into
        // the Fetch Response before hijacking. Otherwise writeCustomRouteResponse's
        // nodeRes.writeHead() overwrites them with only the response.headers set
        // by the custom route handler. Route-set headers win on conflict, except
        // for set-cookie which is always appended so plugin cookies survive
        // alongside handler cookies (distinct cookies, not a collision).
        // Skip framing headers (RFC 7230) — writeCustomRouteResponse /
        // Node's writeHead owns content-length and transfer-encoding.
        const existingHeaders = reply.getHeaders();
        for (const [key, value] of Object.entries(existingHeaders)) {
          if (value === undefined) continue;
          const lowerKey = key.toLowerCase();
          if (lowerKey === 'content-length' || lowerKey === 'transfer-encoding') continue;
          const isSetCookie = lowerKey === 'set-cookie';
          if (!isSetCookie && response.headers.has(key)) continue;
          if (Array.isArray(value)) {
            for (const item of value) response.headers.append(key, String(item));
          } else if (isSetCookie) {
            // set-cookie must always append so plugin cookies coexist with handler cookies.
            response.headers.append(key, String(value));
          } else {
            response.headers.set(key, String(value));
          }
        }
        reply.hijack();
        await this.writeCustomRouteResponse(response, reply.raw, request.abortSignal);
      };

      if (route.method === 'ALL') {
        const methods = ['GET', 'POST', 'PUT', 'DELETE', 'PATCH'] as const;
        for (const method of methods) {
          this.app.route({ method, url: route.path, handler: fastifyHandler });
        }
      } else {
        this.app.route({
          method: route.method as 'GET' | 'POST' | 'PUT' | 'DELETE' | 'PATCH',
          url: route.path,
          handler: fastifyHandler,
        });
      }
    }
  }

  registerContextMiddleware(): void {
    // Override the default JSON parser to allow empty bodies
    // This matches Express behavior where empty POST requests with Content-Type: application/json are allowed.
    //
    // Parse the body as a Buffer (not a string) so a `skipBodyParse` route can keep
    // the EXACT request bytes: a channel webhook signs the raw bytes, and a JSON
    // parse + re-serialize would break HMAC verification. The parser runs AFTER
    // routing, so `request.routeOptions.config.skipBodyParse` (set in registerRoute)
    // identifies the matched route. For such routes we hand the raw Buffer through
    // untouched and DO NOT reject on a parse failure — a signed-but-not-strict-JSON
    // payload carrying `content-type: application/json` must still reach the handler,
    // which captures it as `rawBody` in getParams() and defers parse/verification to
    // the route's adapter. This mirrors the Hono adapter, where the global context
    // middleware skips its JSON pre-parse for skipBodyParse routes and getParams
    // captures `rawBody` via `arrayBuffer()`.
    this.app.removeContentTypeParser('application/json');
    this.app.addContentTypeParser(
      'application/json',
      { parseAs: 'buffer' },
      (request: FastifyRequest, body: Buffer, done) => {
        const skipBodyParse = (request.routeOptions?.config as { skipBodyParse?: boolean } | undefined)?.skipBodyParse;
        if (skipBodyParse) {
          // Leave the raw bytes unparsed; getParams() reads request.body as the Buffer.
          done(null, body);
          return;
        }
        try {
          // Allow empty body
          if (!body || body.length === 0 || body.toString('utf8').trim() === '') {
            done(null, undefined);
            return;
          }
          const parsed = JSON.parse(body.toString('utf8'));
          done(null, parsed);
        } catch (err) {
          done(err as Error, undefined);
        }
      },
    );

    // Register content type parser for multipart/form-data
    // This allows Fastify to accept multipart requests without parsing them
    // We'll parse them manually in getParams using busboy
    this.app.addContentTypeParser('multipart/form-data', (_request, _payload, done) => {
      // Don't parse the body, we'll handle it manually with busboy
      done(null, undefined);
    });

    this.app.addHook('preHandler', this.createContextMiddleware());
  }

  registerAuthMiddleware(): void {
    // Auth is handled per-route in registerRoute() and registerCustomApiRoutes()
    // No global middleware needed
  }

  registerHttpLoggingMiddleware(): void {
    if (!this.httpLoggingConfig?.enabled) {
      return;
    }

    this.app.addHook('onRequest', async (request: FastifyRequest, reply: FastifyReply) => {
      const urlPath = request.url.split('?')[0]!;
      if (!this.shouldLogRequest(urlPath)) {
        return;
      }

      const start = Date.now();
      const method = request.method;
      const path = urlPath;

      reply.raw.once('finish', () => {
        const duration = Date.now() - start;
        const status = reply.statusCode;
        const level = this.httpLoggingConfig?.level || 'info';

        const logData: Record<string, any> = {
          method,
          path,
          status,
          duration: `${duration}ms`,
        };

        if (this.httpLoggingConfig?.includeQueryParams) {
          logData.query = redactSensitiveQueryParams(request.query as Record<string, unknown>);
        }

        if (this.httpLoggingConfig?.includeHeaders) {
          const headers = { ...request.headers };
          const redactHeaders = this.httpLoggingConfig.redactHeaders || [];
          redactHeaders.forEach((h: string) => {
            const key = h.toLowerCase();
            if (headers[key] !== undefined) {
              headers[key] = '[REDACTED]';
            }
          });
          logData.headers = headers;
        }

        this.logger[level](`${method} ${path} ${status} ${duration}ms`, logData);
      });
    });
  }
}
