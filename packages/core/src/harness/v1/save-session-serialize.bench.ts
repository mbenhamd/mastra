/**
 * Harness v1 — STATE (RE)SERIALIZATION benchmark.
 *
 * The InMemory harness store persists a session via `cloneSessionRecord` →
 * `structuredClone` (storage/domains/harness/inmemory.ts:2849-2855) on every
 * `saveSession`. The PG adapter instead (re)serializes the record to jsonb, which
 * requires a live Postgres and is NOT measured here. This bench therefore times:
 *
 *   1. `structuredClone` of a large record blob — the actual InMemory write hot
 *      path (inmemory.ts:2854).
 *   2. `JSON.stringify` + `JSON.parse` of the same blob — a DOCUMENTED PROXY for
 *      the PG jsonb (re)serialization cost (true jsonb needs a Postgres-gated
 *      integration bench, intentionally out of this deterministic suite).
 *
 * The blob is generated deterministically (seeded counter, no randomness) and is
 * shaped like a heavy SessionRecord: large `pendingQueue`, many
 * `queueAdmissionReceipts`, a long transcript, and a sizeable custom `state`.
 *
 * `*.bench.ts` → ignored by `pnpm test`; runs only under `vitest bench`.
 *
 * Run:
 *   pnpm --filter ./packages/core exec vitest bench src/harness/v1/save-session-serialize.bench.ts --run
 */

import { bench, describe } from 'vitest';

/** Build a deterministic, ~SessionRecord-shaped heavy blob. */
function buildLargeRecord(opts: { queueItems: number; receipts: number; transcript: number }) {
  const pendingQueue = Array.from({ length: opts.queueItems }, (_unused, i) => ({
    queuedItemId: `q-${i}`,
    content: `queued message ${i} with a moderately long body to add serialization weight`,
    priority: i % 5,
    admissionId: `adm-${i}`,
    admissionHash: `deadbeef${i.toString(16).padStart(12, '0')}`,
    enqueuedAt: 1_700_000_000_000 + i,
    attachments: [{ attachmentId: `att-${i}`, resourceId: `res-${i}`, bytes: 1024 * (i + 1) }],
  }));

  const queueAdmissionReceipts: Record<string, unknown> = {};
  for (let i = 0; i < opts.receipts; i++) {
    queueAdmissionReceipts[`q-${i}`] = {
      status: i % 3 === 0 ? 'completed' : 'pending',
      admissionId: `adm-${i}`,
      admissionHash: `deadbeef${i.toString(16).padStart(12, '0')}`,
      updatedAt: 1_700_000_000_000 + i,
      result: i % 3 === 0 ? { text: `result ${i}`, usage: { totalTokens: 30 } } : undefined,
    };
  }

  const transcript = Array.from({ length: opts.transcript }, (_unused, i) => ({
    role: i % 2 === 0 ? 'user' : 'assistant',
    parts: [{ type: 'text', text: `transcript line ${i}: ` + 'lorem ipsum dolor sit amet '.repeat(4) }],
    createdAt: 1_700_000_000_000 + i,
  }));

  return {
    harnessName: 'default',
    id: 'bench-session',
    resourceId: 'bench-resource',
    threadId: 'bench-thread',
    origin: 'top-level',
    ownsThread: true,
    modeId: 'default',
    modelId: 'mock-model-id',
    subagentModelOverrides: {},
    permissionRules: {},
    sessionGrants: {},
    tokenUsage: { inputTokens: 1000, outputTokens: 2000, totalTokens: 3000 },
    pendingQueue,
    queueAdmissionReceipts,
    state: { transcript, scratch: { notes: 'x'.repeat(2048), counters: Array.from({ length: 256 }, (_u, i) => i) } },
    createdAt: 1_700_000_000_000,
    lastActivityAt: 1_700_000_100_000,
    version: 42,
  };
}

const RECORD = buildLargeRecord({ queueItems: 90, receipts: 90, transcript: 400 });

// Report approximate serialized size once at module load (visible in bench stderr/log).
// eslint-disable-next-line no-console
console.error(`[save-session-serialize.bench] approx JSON size: ${(JSON.stringify(RECORD).length / 1024).toFixed(1)} KB`);

const BOUNDED = { time: 1000, iterations: 100, warmupIterations: 10, warmupTime: 200 } as const;

describe('Harness v1 saveSession (re)serialization', () => {
  bench(
    'structuredClone large record (InMemory saveSession hot path)',
    () => {
      structuredClone(RECORD);
    },
    BOUNDED,
  );

  bench(
    'JSON.stringify + JSON.parse large record (jsonb proxy)',
    () => {
      JSON.parse(JSON.stringify(RECORD));
    },
    BOUNDED,
  );

  bench(
    'JSON.stringify only large record',
    () => {
      JSON.stringify(RECORD);
    },
    BOUNDED,
  );
});
