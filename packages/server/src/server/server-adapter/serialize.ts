import { safeStringify } from '@mastra/core/utils';

export type SerializedStreamChunk = { ok: true; json: string } | { ok: false; error: Error };

/**
 * Serializes a stream chunk to JSON for wire transport.
 *
 * Fast path first: plain `JSON.stringify` stays on V8's native serializer.
 * `safeStringify` passes a replacer, which forces a JS callback per node plus
 * an O(depth) cycle scan — several times slower, and this runs once per chunk
 * in the adapters' stream write loops. Plain `JSON.stringify` throws on
 * exactly the inputs the safe path exists for (BigInt produced by zod
 * coercions/transforms in structuredOutput schemas, circular references), so
 * only those chunks take the replacer path. The two paths are byte-identical
 * for every serializable input (see the equivalence suite in
 * `serialize.test.ts`); `JSON.stringify` returns `undefined` for unsupported
 * top-level values, which maps to `'null'` to match `safeStringify`.
 *
 * A single bad chunk must never kill the whole stream — Studio relies on
 * later chunks (`workflow-step-result`, `workflow-finish`) to render run
 * state. Returns `{ ok: false, error }` if the chunk cannot be serialized at
 * all (e.g. a throwing getter/toJSON, which both paths throw on) — callers
 * should log the error, skip the chunk, and keep streaming.
 */
export function serializeStreamChunk(chunk: unknown): SerializedStreamChunk {
  try {
    let json: string;
    try {
      const fast = JSON.stringify(chunk);
      json = fast === undefined ? 'null' : fast;
    } catch {
      // Re-walks the chunk, so getters/toJSON run a second time on this path.
      // Chunk payloads must not rely on evaluate-once semantics; nothing the
      // framework constructs does.
      json = safeStringify(chunk);
    }
    return { ok: true, json };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error : new Error(String(error)) };
  }
}
