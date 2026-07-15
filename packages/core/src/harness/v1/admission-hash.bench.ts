/**
 * Harness v1 — ADMISSION HASHING benchmark (canonical-json.ts).
 *
 * `Session._computeMessageAdmissionHashes` (session.ts) computes a primary +
 * legacy-compatible hashes, each `sha256CanonicalJson(_messageAdmissionHashInput(...))`.
 * `_messageAdmissionHashInput` maps over every attachment (incl. a deep clone of
 * `metadata`), so cost scales with attachment count + metadata size, then
 * `canonicalJson` sorts keys and `sha256` digests the serialized form
 * (canonical-json.ts:71,90).
 *
 * `_messageAdmissionHashInput` is private, so this bench isolates the flagged
 * cost via the PUBLIC `sha256CanonicalJson`, fed structures shaped exactly like
 * the hash input the private method builds (small vs many-large-attachment).
 * Fully synchronous, zero harness/network state — the most deterministic of the
 * suite.
 *
 * `*.bench.ts` → ignored by `pnpm test`; runs only under `vitest bench`.
 *
 * Run:
 *   pnpm --filter ./packages/core exec vitest bench src/harness/v1/admission-hash.bench.ts --run
 */

import { bench, describe } from 'vitest';

import type { JsonValue } from '../../storage/domains/harness';
import { sha256CanonicalJson } from './canonical-json';

/** Build a hash-input shaped object mirroring `_messageAdmissionHashInput`. */
function hashInput(opts: { content: string; attachmentCount: number; metadataKeys: number }): JsonValue {
  const attachments: JsonValue[] = [];
  for (let i = 0; i < opts.attachmentCount; i++) {
    const metadata: Record<string, JsonValue> = {};
    for (let k = 0; k < opts.metadataKeys; k++) {
      // ~ a few KB of structured metadata per attachment when metadataKeys is large.
      metadata[`meta_key_${k}`] = {
        index: k,
        label: `attachment ${i} metadata field ${k}`,
        nested: { a: k * 2, b: `value-${i}-${k}`, flags: [true, false, k % 2 === 0] },
      };
    }
    attachments.push({
      attachmentId: `att-${i}`,
      resourceId: `res-${i}`,
      bytes: 1024 * (i + 1),
      sha256: `deadbeef${i.toString(16).padStart(8, '0')}`,
      kind: 'file',
      name: `file-${i}.pdf`,
      mimeType: 'application/pdf',
      ...(opts.metadataKeys > 0 ? { metadata } : {}),
    });
  }
  return {
    kind: 'signal',
    hashVersion: 2,
    content: opts.content,
    mode: 'default',
    model: 'mock-model-id',
    attachments,
  };
}

const SMALL = hashInput({ content: 'hello world', attachmentCount: 0, metadataKeys: 0 });
const ONE_SMALL_ATT = hashInput({ content: 'hello world', attachmentCount: 1, metadataKeys: 0 });
const MANY_LARGE_ATT = hashInput({ content: 'hello world', attachmentCount: 20, metadataKeys: 24 });

const BOUNDED = { time: 1000, iterations: 100, warmupIterations: 10, warmupTime: 200 } as const;

describe('Harness v1 admission hashing — sha256CanonicalJson', () => {
  bench(
    'small payload, 0 attachments',
    () => {
      sha256CanonicalJson(SMALL);
    },
    BOUNDED,
  );

  bench(
    '1 small attachment (ids only)',
    () => {
      sha256CanonicalJson(ONE_SMALL_ATT);
    },
    BOUNDED,
  );

  bench(
    '20 attachments, large metadata blob each',
    () => {
      // Mirrors the 3-hash compute (primary + 2 legacy-compatible) cost shape.
      sha256CanonicalJson(MANY_LARGE_ATT);
      sha256CanonicalJson(MANY_LARGE_ATT);
      sha256CanonicalJson(MANY_LARGE_ATT);
    },
    BOUNDED,
  );
});
