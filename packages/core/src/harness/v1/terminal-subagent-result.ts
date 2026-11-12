import { z } from 'zod';

import {
  materializeTerminalToolResult,
  tryFormatTerminalToolResultForModel,
} from '../../agent/message-list/terminal-tool-result';
import type { HarnessRunToolReceipt } from '../../storage/domains/harness';

export const HARNESS_SUBAGENT_DIRECT_ANSWER_KIND = 'subagent-direct-answer' as const;
export const HARNESS_SUBAGENT_OUTCOME_REPORT_KIND = 'subagent-outcome-report' as const;
export const HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID = 'report_subagent_outcome' as const;
/** Maximum UTF-8 bytes retained from a child answer in the parent transcript. */
export const MAX_HARNESS_SUBAGENT_RESULT_TEXT_BYTES = 48 * 1024;
/** Leave headroom for a complete subagent_end event under an 8 KiB wire cap. */
export const MAX_HARNESS_SUBAGENT_EVENT_TEXT_BYTES = 4 * 1024;
/**
 * Terminal-policy budget for the complete projected JSON value. This is
 * deliberately larger than the text budget so envelope keys/ids cannot make a
 * text value pass the predicate and then fail the terminal byte check.
 */
export const MAX_HARNESS_SUBAGENT_DIRECT_ANSWER_BYTES = 64 * 1024;
export const MAX_HARNESS_SUBAGENT_ERROR_MESSAGE_BYTES = 4 * 1024;
/** Maximum projected artifact metadata retained from a child's terminal tool result. */
export const MAX_HARNESS_SUBAGENT_TERMINAL_ARTIFACTS_BYTES = 12 * 1024;

const MAX_HARNESS_SUBAGENT_LABEL_LENGTH = 256;
const MAX_HARNESS_SUBAGENT_EVIDENCE_DESCRIPTION_LENGTH = 2 * 1024;
const MAX_HARNESS_SUBAGENT_EVIDENCE_REFERENCE_LENGTH = 2 * 1024;
const MAX_HARNESS_SUBAGENT_EVIDENCE_ITEMS = 24;
const MAX_HARNESS_SUBAGENT_EVENT_EVIDENCE_ITEMS = 1;
const MAX_HARNESS_SUBAGENT_EVENT_EVIDENCE_TEXT_BYTES = 256;
const MAX_HARNESS_SUBAGENT_EVENT_ISSUE_TEXT_BYTES = 512;
const MAX_HARNESS_SUBAGENT_TOOL_RECEIPTS = 4_096;
const MAX_HARNESS_SUBAGENT_TERMINAL_ARTIFACTS = 16;
const HARNESS_SUBAGENT_TEXT_TRUNCATION_MARKER = '\n\u2026 [truncated by Harness]';

const harnessSubagentTerminalArtifactsSchema = z
  .array(z.json())
  .min(1)
  .max(MAX_HARNESS_SUBAGENT_TERMINAL_ARTIFACTS)
  .superRefine((artifacts, ctx) => {
    if (
      new TextEncoder().encode(JSON.stringify(artifacts)).byteLength > MAX_HARNESS_SUBAGENT_TERMINAL_ARTIFACTS_BYTES
    ) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: `subagent terminal artifacts exceed ${MAX_HARNESS_SUBAGENT_TERMINAL_ARTIFACTS_BYTES} UTF-8 bytes`,
      });
    }
  });

export const harnessSubagentOutcomeSchema = z.enum(['completed', 'blocked', 'failed']);

export const harnessSubagentOutcomeEvidenceSchema = z
  .object({
    kind: z.enum(['tool-result', 'artifact', 'source', 'analysis']),
    description: z.string().min(1).max(MAX_HARNESS_SUBAGENT_EVIDENCE_DESCRIPTION_LENGTH),
    toolName: z
      .string()
      .min(1)
      .max(MAX_HARNESS_SUBAGENT_LABEL_LENGTH)
      .optional()
      .describe('For tool-result evidence: the exact name of the tool you called.'),
    toolCallId: z
      .string()
      .min(1)
      .max(1_024)
      .optional()
      .describe(
        'Optional framework tool-call id. Omit it unless the exact id was shown to you; never substitute a ' +
          'request, job, or provider id. The Harness binds tool-result evidence to its own record of your tool calls.',
      ),
    status: z
      .enum(['success', 'error'])
      .optional()
      .describe('For tool-result evidence: the observed result status of that tool call.'),
    reference: z.string().min(1).max(MAX_HARNESS_SUBAGENT_EVIDENCE_REFERENCE_LENGTH).optional(),
  })
  .strict()
  .superRefine((evidence, ctx) => {
    if (evidence.kind === 'tool-result' && (evidence.toolName === undefined || evidence.status === undefined)) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'tool-result evidence requires toolName and status',
      });
    }
  });

export const harnessSubagentOutcomeIssueSchema = z
  .object({
    code: z.string().min(1).max(MAX_HARNESS_SUBAGENT_LABEL_LENGTH),
    message: z.string().min(1).max(MAX_HARNESS_SUBAGENT_ERROR_MESSAGE_BYTES),
    retryable: z.boolean(),
  })
  .strict();

/**
 * Framework-owned semantic terminal contract for a subagent. Provider
 * `finishReason` only describes transport/model termination; this report says
 * whether the delegated task itself completed, is externally blocked, or
 * failed, and carries bounded evidence for the parent/recovery path.
 */
export const harnessSubagentOutcomeReportSchema = z
  .object({
    kind: z.literal(HARNESS_SUBAGENT_OUTCOME_REPORT_KIND),
    outcome: harnessSubagentOutcomeSchema,
    summary: z.string().min(1).max(MAX_HARNESS_SUBAGENT_RESULT_TEXT_BYTES),
    evidence: z.array(harnessSubagentOutcomeEvidenceSchema).min(1).max(MAX_HARNESS_SUBAGENT_EVIDENCE_ITEMS),
    issue: harnessSubagentOutcomeIssueSchema.optional(),
  })
  .strict()
  .superRefine((report, ctx) => {
    if (report.outcome === 'completed' && report.issue !== undefined) {
      ctx.addIssue({ code: z.ZodIssueCode.custom, message: 'completed reports cannot include issue' });
    }
    if (report.outcome !== 'completed' && report.issue === undefined) {
      ctx.addIssue({ code: z.ZodIssueCode.custom, message: 'blocked/failed reports require issue' });
    }
    if (new TextEncoder().encode(JSON.stringify(report)).byteLength > MAX_HARNESS_SUBAGENT_DIRECT_ANSWER_BYTES) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: `subagent outcome report exceeds ${MAX_HARNESS_SUBAGENT_DIRECT_ANSWER_BYTES} UTF-8 bytes`,
      });
    }
  });

export type HarnessSubagentOutcomeReport = z.infer<typeof harnessSubagentOutcomeReportSchema>;

const harnessSubagentUsageSchema = z
  .object({
    inputTokens: z.number().finite().nonnegative().optional(),
    outputTokens: z.number().finite().nonnegative().optional(),
    totalTokens: z.number().finite().nonnegative().optional(),
    reasoningTokens: z.number().finite().nonnegative().optional(),
    cachedInputTokens: z.number().finite().nonnegative().optional(),
    cacheCreationInputTokens: z.number().finite().nonnegative().optional(),
  })
  .strict();

/** Maximum distinct tools listed in a subagent result's tool trace. */
export const MAX_HARNESS_SUBAGENT_TOOL_TRACE_ENTRIES = 24;
/**
 * Serialized trace budget. It keeps a completed result with a near-maximal
 * summary and evidence inside the 64 KiB terminal envelope.
 */
export const MAX_HARNESS_SUBAGENT_TOOL_TRACE_BYTES = 2 * 1024;
/** Room kept free for the settlement and plan-task view wrapping a summary. */
const MAX_HARNESS_SUBAGENT_TOOL_TRACE_ENVELOPE_HEADROOM_BYTES = 4 * 1024;
const MAX_HARNESS_SUBAGENT_TOOL_TRACE_NAME_LENGTH = 128;

/**
 * Per-tool distinct-call and error counts from framework receipts, in the
 * order the framework recorded them. It tells the parent what the child
 * covered (and what failed) without re-searching; raw tool inputs and
 * outputs are never included.
 */
export const harnessSubagentToolTraceSchema = z
  .array(
    z
      .object({
        toolName: z.string().min(1).max(MAX_HARNESS_SUBAGENT_TOOL_TRACE_NAME_LENGTH),
        calls: z.number().int().positive(),
        errors: z.number().int().nonnegative(),
      })
      .strict(),
  )
  .min(1)
  .max(MAX_HARNESS_SUBAGENT_TOOL_TRACE_ENTRIES)
  .superRefine((trace, ctx) => {
    if (new Set(trace.map(entry => entry.toolName)).size !== trace.length) {
      ctx.addIssue({ code: z.ZodIssueCode.custom, message: 'subagent tool trace lists a tool more than once' });
    }
    if (trace.some(entry => entry.errors > entry.calls)) {
      ctx.addIssue({ code: z.ZodIssueCode.custom, message: 'subagent tool trace counts more errors than calls' });
    }
    if (new TextEncoder().encode(JSON.stringify(trace)).byteLength > MAX_HARNESS_SUBAGENT_TOOL_TRACE_BYTES) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: `subagent tool trace exceeds ${MAX_HARNESS_SUBAGENT_TOOL_TRACE_BYTES} UTF-8 bytes`,
      });
    }
  });

export type HarnessSubagentToolTrace = z.infer<typeof harnessSubagentToolTraceSchema>;

/**
 * The per-type budget that stopped a child before it reported an outcome
 * (`SubagentDefinition.maxSteps` / `maxTotalTokens`). Present only on a
 * `harness.subagent_budget_exhausted` error summary.
 */
export const harnessSubagentBudgetExhaustedSchema = z
  .object({
    budget: z.enum(['steps', 'tokens']),
    limit: z.number().int().positive(),
    used: z.number().finite().nonnegative(),
  })
  .strict();

export const harnessSubagentResultSummarySchema = z
  .object({
    status: z.enum(['success', 'error']),
    outcome: harnessSubagentOutcomeSchema.optional(),
    text: z.string(),
    textTruncated: z.boolean(),
    finishReason: z.string().min(1).max(MAX_HARNESS_SUBAGENT_LABEL_LENGTH),
    stepCount: z.number().int().nonnegative(),
    toolCallCount: z.number().int().nonnegative(),
    toolResultCount: z.number().int().nonnegative(),
    toolTrace: harnessSubagentToolTraceSchema.optional(),
    toolTraceTruncated: z.boolean().optional(),
    usage: harnessSubagentUsageSchema.optional(),
    budgetExhausted: harnessSubagentBudgetExhaustedSchema.optional(),
    artifacts: harnessSubagentTerminalArtifactsSchema.optional(),
    evidence: z.array(harnessSubagentOutcomeEvidenceSchema).max(MAX_HARNESS_SUBAGENT_EVIDENCE_ITEMS).optional(),
    evidenceTruncated: z.boolean().optional(),
    issue: harnessSubagentOutcomeIssueSchema.optional(),
    error: z
      .object({
        code: z.string().min(1).max(MAX_HARNESS_SUBAGENT_LABEL_LENGTH),
        message: z.string(),
        messageTruncated: z.boolean(),
      })
      .strict()
      .optional(),
  })
  .strict();

export type HarnessSubagentResultSummary = z.infer<typeof harnessSubagentResultSummarySchema>;

export const harnessSubagentDirectAnswerSchema = z
  .object({
    kind: z.literal(HARNESS_SUBAGENT_DIRECT_ANSWER_KIND),
    subagentSessionId: z.string().min(1).max(512),
    text: z.string().min(1),
    artifacts: harnessSubagentTerminalArtifactsSchema.optional(),
  })
  .strict()
  .superRefine((answer, ctx) => {
    if (new TextEncoder().encode(JSON.stringify(answer)).byteLength > MAX_HARNESS_SUBAGENT_DIRECT_ANSWER_BYTES) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: `subagent direct answer exceeds ${MAX_HARNESS_SUBAGENT_DIRECT_ANSWER_BYTES} UTF-8 bytes`,
      });
    }
  });

export type HarnessSubagentDirectAnswer = z.infer<typeof harnessSubagentDirectAnswerSchema>;

const terminalSubagentEnvelopeSchema = z
  .object({
    status: z.literal('success'),
    items: z.tuple([
      z
        .object({
          toolName: z.literal('spawn_subagent'),
          toolCallId: z.string().min(1).max(1_024),
          status: z.literal('success'),
          value: harnessSubagentDirectAnswerSchema,
        })
        .strict(),
    ]),
  })
  .strict();

const terminalSubagentOutcomeEnvelopeSchema = z
  .object({
    status: z.literal('success'),
    items: z.tuple([
      z
        .object({
          toolName: z.literal(HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID),
          toolCallId: z.string().min(1).max(1_024),
          status: z.literal('success'),
          value: harnessSubagentOutcomeReportSchema,
        })
        .strict(),
    ]),
  })
  .strict();

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

export type HarnessSubagentToolReceipt = HarnessRunToolReceipt;

const HARNESS_SUBAGENT_TOOL_RECEIPTS_FIELD = 'harnessToolReceipts' as const;
const HARNESS_SUBAGENT_TOOL_RECEIPTS_OVERFLOW_FIELD = 'harnessToolReceiptsOverflow' as const;

/**
 * Attach only framework-minted receipt identity/status to a run result. Raw
 * tool inputs and outputs stay outside this recovery evidence.
 */
export function withHarnessSubagentToolReceipts<T extends object>(
  value: T,
  receipts: readonly HarnessSubagentToolReceipt[],
  overflow = false,
): T {
  // Reserved fields are framework authority. Always discard any values already
  // present on the provider/output-shaped object before attaching the observed
  // state, including the empty state.
  const {
    [HARNESS_SUBAGENT_TOOL_RECEIPTS_FIELD]: _untrustedReceipts,
    [HARNESS_SUBAGENT_TOOL_RECEIPTS_OVERFLOW_FIELD]: _untrustedOverflow,
    ...safeValue
  } = value as T & {
    [HARNESS_SUBAGENT_TOOL_RECEIPTS_FIELD]?: unknown;
    [HARNESS_SUBAGENT_TOOL_RECEIPTS_OVERFLOW_FIELD]?: unknown;
  };
  return {
    ...safeValue,
    ...(receipts.length > 0
      ? {
          [HARNESS_SUBAGENT_TOOL_RECEIPTS_FIELD]: receipts.map(receipt => ({ ...receipt })),
        }
      : {}),
    ...(overflow ? { [HARNESS_SUBAGENT_TOOL_RECEIPTS_OVERFLOW_FIELD]: true } : {}),
  } as T;
}

const HARNESS_SUBAGENT_BUDGET_EXHAUSTED_FIELD = 'harnessSubagentBudgetExhausted' as const;

/**
 * Attach the framework's budget-stop record to a child run result, so durable
 * result evidence keeps the typed outcome for crash recovery. Any value already
 * on the provider-shaped object is discarded first.
 */
export function withHarnessSubagentBudgetExhausted<T extends object>(
  value: T,
  exhausted: z.infer<typeof harnessSubagentBudgetExhaustedSchema> | undefined,
): T {
  const { [HARNESS_SUBAGENT_BUDGET_EXHAUSTED_FIELD]: _untrusted, ...safeValue } = value as T & {
    [HARNESS_SUBAGENT_BUDGET_EXHAUSTED_FIELD]?: unknown;
  };
  return {
    ...safeValue,
    ...(exhausted !== undefined ? { [HARNESS_SUBAGENT_BUDGET_EXHAUSTED_FIELD]: { ...exhausted } } : {}),
  } as T;
}

/** Read the budget-stop record the framework attached to a child run result. */
export function parseHarnessSubagentBudgetExhausted(
  value: unknown,
): z.infer<typeof harnessSubagentBudgetExhaustedSchema> | undefined {
  if (!isRecord(value)) return undefined;
  const parsed = harnessSubagentBudgetExhaustedSchema.safeParse(value[HARNESS_SUBAGENT_BUDGET_EXHAUSTED_FIELD]);
  return parsed.success ? parsed.data : undefined;
}

function itemArray(value: unknown): unknown[] {
  if (Array.isArray(value)) return value;
  if (isRecord(value) && Array.isArray(value.items)) return value.items;
  return [];
}

function normalizeToolReceipt(value: unknown): HarnessSubagentToolReceipt | undefined {
  if (!isRecord(value)) return undefined;
  const payload = isRecord(value.payload) ? value.payload : undefined;
  const toolCallId =
    typeof value.toolCallId === 'string'
      ? value.toolCallId
      : typeof payload?.toolCallId === 'string'
        ? payload.toolCallId
        : undefined;
  const toolName =
    typeof value.toolName === 'string'
      ? value.toolName
      : typeof payload?.toolName === 'string'
        ? payload.toolName
        : undefined;
  if (!toolCallId || !toolName || toolName === HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID) return undefined;
  const explicitStatus = value.status === 'success' || value.status === 'error' ? value.status : undefined;
  const isError =
    typeof value.isError === 'boolean'
      ? value.isError
      : typeof payload?.isError === 'boolean'
        ? payload.isError
        : false;
  // A tool's own classification of a returned failure (`ToolAction.resultStatus`)
  // marks the call failed here too, so fallback tool results agree with the
  // framework receipt for the same call.
  const providerMetadata = isRecord(value.providerMetadata)
    ? value.providerMetadata
    : isRecord(payload?.providerMetadata)
      ? payload.providerMetadata
      : undefined;
  const mastraMetadata = isRecord(providerMetadata?.mastra) ? providerMetadata.mastra : undefined;
  // A running background placeholder is not the tool's result.
  if (isRecord(mastraMetadata?.backgroundTask) && mastraMetadata.backgroundTask.status === 'running') return undefined;
  const classifiedError = mastraMetadata?.resultStatus === 'error';
  return { toolCallId, toolName, status: explicitStatus ?? (isError || classifiedError ? 'error' : 'success') };
}

function collectToolReceipts(value: unknown): {
  receipts: HarnessSubagentToolReceipt[];
  overflow: boolean;
} {
  const source = isRecord(value) ? value : undefined;
  if (source === undefined) return { receipts: [], overflow: false };
  // Framework receipts are authoritative: the session records the last
  // framework result of each call. Run tool results only fill in calls the
  // receipts do not name; contradictory fallback results for one call stay
  // contradictory, so such a call is never bound.
  const framework = itemArray(source[HARNESS_SUBAGENT_TOOL_RECEIPTS_FIELD]).flatMap(candidate => {
    const receipt = normalizeToolReceipt(candidate);
    return receipt === undefined ? [] : [receipt];
  });
  const frameworkCalls = new Set(framework.map(receipt => `${receipt.toolCallId}\0${receipt.toolName}`));
  const fallbackCandidates = [...itemArray(source.toolResults)];
  for (const step of itemArray(source.steps)) {
    if (isRecord(step)) fallbackCandidates.push(...itemArray(step.toolResults));
  }
  const fallback = fallbackCandidates.flatMap(candidate => {
    const receipt = normalizeToolReceipt(candidate);
    return receipt === undefined || frameworkCalls.has(`${receipt.toolCallId}\0${receipt.toolName}`) ? [] : [receipt];
  });

  const receipts: HarnessSubagentToolReceipt[] = [];
  const exact = new Set<string>();
  for (const receipt of [...framework, ...fallback]) {
    const key = `${receipt.toolCallId}\0${receipt.toolName}\0${receipt.status}`;
    if (exact.has(key)) continue;
    exact.add(key);
    receipts.push(receipt);
    if (receipts.length > MAX_HARNESS_SUBAGENT_TOOL_RECEIPTS) {
      return { receipts: receipts.slice(0, MAX_HARNESS_SUBAGENT_TOOL_RECEIPTS), overflow: true };
    }
  }
  return {
    receipts,
    overflow: source[HARNESS_SUBAGENT_TOOL_RECEIPTS_OVERFLOW_FIELD] === true,
  };
}

/** Framework-minted receipts only, in recorded order; never run tool-result fallbacks. */
function frameworkToolReceipts(value: unknown): HarnessSubagentToolReceipt[] {
  if (!isRecord(value)) return [];
  return itemArray(value[HARNESS_SUBAGENT_TOOL_RECEIPTS_FIELD]).flatMap(candidate => {
    const receipt = normalizeToolReceipt(candidate);
    return receipt === undefined ? [] : [receipt];
  });
}

function truncateUtf8(value: string, maxBytes: number, marker = HARNESS_SUBAGENT_TEXT_TRUNCATION_MARKER) {
  const encoder = new TextEncoder();
  const encoded = encoder.encode(value);
  if (encoded.byteLength <= maxBytes) return { value, truncated: false };

  const markerBytes = encoder.encode(marker);
  const contentBudget = Math.max(0, maxBytes - markerBytes.byteLength);
  let end = contentBudget;
  // If the boundary lands inside a multibyte code point, exclude that entire
  // point. TextDecoder therefore never has to insert a replacement character.
  while (end > 0 && (encoded[end]! & 0xc0) === 0x80) end -= 1;
  const prefix = new TextDecoder().decode(encoded.subarray(0, end));
  return { value: `${prefix}${marker}`, truncated: true };
}

function boundedLabel(value: unknown, fallback: string): string {
  if (typeof value !== 'string' || value.length === 0) return fallback;
  return value.slice(0, MAX_HARNESS_SUBAGENT_LABEL_LENGTH);
}

function arrayLength(value: unknown): number {
  return Array.isArray(value) ? value.length : 0;
}

/** Shorten to at most `maxLength` UTF-16 units without splitting a surrogate pair. */
function truncateAtCodePoint(value: string, maxLength: number): string {
  if (value.length <= maxLength) return value;
  let result = '';
  for (const codePoint of value) {
    if (result.length + codePoint.length > maxLength) break;
    result += codePoint;
  }
  return result;
}

/** Bytes of the `"toolTraceTruncated":true,` member; below this no flag fits either. */
const HARNESS_SUBAGENT_TOOL_TRACE_FLAG_BYTES = 26;

/**
 * Only framework-minted receipts feed the trace. Run tool-result fallbacks
 * stay available to evidence verification but are provider-shaped, so they
 * never inflate what the parent is told the child did. A call is counted
 * once, and as an error when any receipt for it failed.
 */
function summarizeToolTrace(
  value: unknown,
  maxBytes: number,
): { trace?: HarnessSubagentToolTrace; truncated: boolean } {
  const source = isRecord(value) ? value : undefined;
  if (source === undefined) return { truncated: false };
  const callsByTool = new Map<string, Map<string, boolean>>();
  for (const candidate of itemArray(source[HARNESS_SUBAGENT_TOOL_RECEIPTS_FIELD])) {
    const receipt = normalizeToolReceipt(candidate);
    if (receipt === undefined) continue;
    const calls = callsByTool.get(receipt.toolName) ?? new Map<string, boolean>();
    calls.set(receipt.toolCallId, calls.get(receipt.toolCallId) === true || receipt.status === 'error');
    callsByTool.set(receipt.toolName, calls);
  }
  if (callsByTool.size === 0) return { truncated: false };
  const trace: HarnessSubagentToolTrace = [];
  const listedNames = new Set<string>();
  let shortenedOrSkipped = false;
  let bytes = 2;
  for (const [fullName, calls] of callsByTool) {
    if (trace.length >= MAX_HARNESS_SUBAGENT_TOOL_TRACE_ENTRIES) break;
    const toolName = truncateAtCodePoint(fullName, MAX_HARNESS_SUBAGENT_TOOL_TRACE_NAME_LENGTH);
    // A shortened name is flagged, and never merged into another tool's entry.
    if (toolName !== fullName) shortenedOrSkipped = true;
    if (listedNames.has(toolName)) continue;
    const entry = { toolName, calls: calls.size, errors: [...calls.values()].filter(Boolean).length };
    const entryBytes = new TextEncoder().encode(JSON.stringify(entry)).byteLength + 1;
    if (bytes + entryBytes > maxBytes) break;
    bytes += entryBytes;
    listedNames.add(toolName);
    trace.push(entry);
  }
  const overflow = source[HARNESS_SUBAGENT_TOOL_RECEIPTS_OVERFLOW_FIELD] === true;
  return {
    ...(trace.length > 0 ? { trace } : {}),
    truncated: overflow || shortenedOrSkipped || callsByTool.size > trace.length,
  };
}

const USAGE_KEYS = [
  'inputTokens',
  'outputTokens',
  'totalTokens',
  'reasoningTokens',
  'cachedInputTokens',
  'cacheCreationInputTokens',
] as const;

function summarizeUsage(value: unknown): z.infer<typeof harnessSubagentUsageSchema> | undefined {
  if (!isRecord(value)) return undefined;
  const usage: Record<string, number> = {};
  for (const key of USAGE_KEYS) {
    const tokenCount = value[key];
    if (typeof tokenCount === 'number' && Number.isFinite(tokenCount) && tokenCount >= 0) {
      usage[key] = tokenCount;
    }
  }
  return Object.keys(usage).length > 0 ? usage : undefined;
}

/**
 * Collapse a child FullOutput into the only shape allowed to cross back into
 * the parent tool result/event/history. Raw steps, messages, provider metadata,
 * request/response bodies, sources, files, and tool result bodies are
 * intentionally never copied.
 */
export function summarizeHarnessSubagentResult(
  value: unknown,
  options: {
    isError?: boolean;
    error?: { code: string; message: string };
    budgetExhausted?: z.infer<typeof harnessSubagentBudgetExhaustedSchema>;
  } = {},
): HarnessSubagentResultSummary {
  const record = isRecord(value) ? value : undefined;
  const completion = resolveHarnessSubagentTerminalCompletion(value);
  const report = completion?.report;
  const finishReason = boundedLabel(record?.finishReason, options.isError ? 'error' : 'unknown');
  const status =
    options.isError === true ||
    options.error !== undefined ||
    record?.error !== undefined ||
    (report !== undefined && report.outcome !== 'completed') ||
    (report === undefined && finishReason !== 'stop')
      ? 'error'
      : 'success';
  const text = truncateUtf8(
    report?.summary ?? (typeof record?.text === 'string' ? record.text : ''),
    MAX_HARNESS_SUBAGENT_RESULT_TEXT_BYTES,
  );

  let error: HarnessSubagentResultSummary['error'];
  if (status === 'error') {
    const projected = options.error ?? {
      code: report?.issue?.code ?? 'harness.subagent_failed',
      message: report?.issue?.message ?? 'The subagent did not complete successfully',
    };
    const message = truncateUtf8(projected.message, MAX_HARNESS_SUBAGENT_ERROR_MESSAGE_BYTES);
    error = {
      code: boundedLabel(projected.code, 'harness.subagent_failed'),
      message: message.value,
      messageTruncated: message.truncated,
    };
  }
  const usage = summarizeUsage(record?.totalUsage ?? record?.usage);
  const artifacts =
    status === 'success' && report?.outcome === 'completed'
      ? parseHarnessTerminalToolResultArtifacts(record?.terminalToolResult)
      : undefined;

  const draft: HarnessSubagentResultSummary = {
    status,
    ...(report ? { outcome: report.outcome } : {}),
    text: text.value,
    textTruncated: text.truncated,
    finishReason,
    stepCount: arrayLength(record?.steps),
    toolCallCount: arrayLength(record?.toolCalls),
    toolResultCount: arrayLength(record?.toolResults),
    ...(usage ? { usage } : {}),
    ...(status === 'error' && options.budgetExhausted ? { budgetExhausted: { ...options.budgetExhausted } } : {}),
    ...(artifacts ? { artifacts } : {}),
    ...(report ? { evidence: report.evidence.slice(0, completion!.requiredEvidenceCount) } : {}),
    ...(report?.issue ? { issue: report.issue } : {}),
    ...(error ? { error } : {}),
  };
  // Earlier receipts appended to a direct delivery's evidence only use room
  // the rest of the summary leaves inside the envelope the trace budget below
  // assumes, so they never make a settled task unreadable. A receipt that does
  // not fit is skipped and stays unproven; the required evidence always stays.
  const withEvidence =
    report === undefined
      ? draft
      : fitAppendedEvidence(
          draft,
          report.evidence.slice(completion!.requiredEvidenceCount),
          MAX_HARNESS_SUBAGENT_DIRECT_ANSWER_BYTES - MAX_HARNESS_SUBAGENT_TOOL_TRACE_ENVELOPE_HEADROOM_BYTES,
        );
  // A budget-stopped child's partial text is arbitrary interim prose whose JSON
  // escaping can double it; fit the serialized summary to the room the trace
  // budget below assumes, so the partial result never outgrows the envelope.
  const summary =
    withEvidence.budgetExhausted === undefined
      ? withEvidence
      : fitSummaryText(
          withEvidence,
          MAX_HARNESS_SUBAGENT_DIRECT_ANSWER_BYTES - MAX_HARNESS_SUBAGENT_TOOL_TRACE_ENVELOPE_HEADROOM_BYTES,
        );
  // The trace only uses room the rest of the summary leaves inside the 64 KiB
  // terminal envelope, keeping headroom for the settlement and plan-task view
  // that wrap it; a near-limit answer keeps no trace rather than breaking reads.
  const summaryBytes = new TextEncoder().encode(JSON.stringify(summary)).byteLength;
  const traceBudget = Math.min(
    MAX_HARNESS_SUBAGENT_TOOL_TRACE_BYTES,
    MAX_HARNESS_SUBAGENT_DIRECT_ANSWER_BYTES - MAX_HARNESS_SUBAGENT_TOOL_TRACE_ENVELOPE_HEADROOM_BYTES - summaryBytes,
  );
  // The key and flag bytes come out of the trace budget. The flag itself is
  // kept whenever entries were dropped and it fits the 64 KiB terminal
  // envelope: the parent must not read a missing trace as "no tools ran".
  // plan_task_check drops trace and flag if a wrapped view would not fit.
  const toolTrace = summarizeToolTrace(
    value,
    traceBudget - HARNESS_SUBAGENT_TOOL_TRACE_FLAG_BYTES - '"toolTrace":,'.length,
  );
  const flagFits = summaryBytes + HARNESS_SUBAGENT_TOOL_TRACE_FLAG_BYTES <= MAX_HARNESS_SUBAGENT_DIRECT_ANSWER_BYTES;
  return {
    ...summary,
    ...(toolTrace.trace ? { toolTrace: toolTrace.trace } : {}),
    ...(toolTrace.truncated && flagFits ? { toolTraceTruncated: true } : {}),
  };
}

function jsonBytes(value: unknown): number {
  return new TextEncoder().encode(JSON.stringify(value)).byteLength;
}

/**
 * Add `appended` evidence items to `summary.evidence` in order while the
 * serialized summary stays within `maxBytes`, skipping any item that does not
 * fit. The existing evidence array is non-empty, so each item adds a comma.
 */
function fitAppendedEvidence(
  summary: HarnessSubagentResultSummary,
  appended: HarnessSubagentOutcomeReport['evidence'],
  maxBytes: number,
): HarnessSubagentResultSummary {
  if (appended.length === 0 || summary.evidence === undefined) return summary;
  const evidence = [...summary.evidence];
  let bytes = jsonBytes(summary);
  for (const item of appended) {
    const itemBytes = jsonBytes(item) + 1;
    if (bytes + itemBytes > maxBytes) continue;
    bytes += itemBytes;
    evidence.push(item);
  }
  return { ...summary, evidence };
}

/**
 * Shorten `summary.text` until the serialized summary is at most `maxBytes`.
 * Each pass removes at least the excess in raw bytes, which removes at least
 * as many serialized bytes; the text is dropped if it still cannot fit.
 */
function fitSummaryText(summary: HarnessSubagentResultSummary, maxBytes: number): HarnessSubagentResultSummary {
  let fitted = summary;
  for (let pass = 0; pass < 8; pass += 1) {
    const excess = jsonBytes(fitted) - maxBytes;
    if (excess <= 0) return fitted;
    const textBytes = new TextEncoder().encode(fitted.text).byteLength;
    const shorter = truncateUtf8(fitted.text, Math.max(0, textBytes - excess));
    fitted = { ...fitted, text: shorter.value, textTruncated: true };
  }
  return jsonBytes(fitted) <= maxBytes ? fitted : { ...fitted, text: '', textTruncated: true };
}

/**
 * Derive the replay/activity event view from the already-sanitized tool result.
 * The parent tool retains up to 48 KiB for model continuation, while lifecycle
 * events carry only a 4 KiB preview so their terminal status is never dropped
 * merely because the child wrote a long answer.
 */
export function summarizeHarnessSubagentEventResult(
  summary: HarnessSubagentResultSummary,
): HarnessSubagentResultSummary {
  // The tool trace serves parent continuation; lifecycle events keep their
  // 4 KiB headroom for terminal status, as with artifacts.
  const {
    artifacts: _artifacts,
    toolTrace: _toolTrace,
    toolTraceTruncated: _toolTraceTruncated,
    ...eventSummary
  } = summary;
  const text = truncateUtf8(summary.text, MAX_HARNESS_SUBAGENT_EVENT_TEXT_BYTES);
  const evidence = summary.evidence?.slice(0, MAX_HARNESS_SUBAGENT_EVENT_EVIDENCE_ITEMS).map(item => ({
    ...item,
    description: truncateUtf8(item.description, MAX_HARNESS_SUBAGENT_EVENT_EVIDENCE_TEXT_BYTES, '').value,
    ...(item.toolName === undefined
      ? {}
      : { toolName: truncateUtf8(item.toolName, MAX_HARNESS_SUBAGENT_EVENT_EVIDENCE_TEXT_BYTES, '').value }),
    ...(item.reference === undefined
      ? {}
      : { reference: truncateUtf8(item.reference, MAX_HARNESS_SUBAGENT_EVENT_EVIDENCE_TEXT_BYTES, '').value }),
  }));
  const issue =
    summary.issue === undefined
      ? undefined
      : {
          ...summary.issue,
          message: truncateUtf8(summary.issue.message, MAX_HARNESS_SUBAGENT_EVENT_ISSUE_TEXT_BYTES).value,
        };
  const errorMessage =
    summary.error === undefined
      ? undefined
      : truncateUtf8(summary.error.message, MAX_HARNESS_SUBAGENT_EVENT_ISSUE_TEXT_BYTES);
  const error =
    summary.error === undefined || errorMessage === undefined
      ? undefined
      : {
          ...summary.error,
          message: errorMessage.value,
          messageTruncated: summary.error.messageTruncated || errorMessage.truncated,
        };
  const projected: HarnessSubagentResultSummary = {
    ...eventSummary,
    text: text.value,
    textTruncated: summary.textTruncated || text.truncated,
    ...(evidence === undefined ? {} : { evidence }),
    ...(summary.evidence !== undefined && summary.evidence.length > (evidence?.length ?? 0)
      ? { evidenceTruncated: true }
      : {}),
    ...(issue === undefined ? {} : { issue }),
    ...(error === undefined ? {} : { error }),
  };
  // A budget stop's event keeps its whole serialized summary inside the 4 KiB
  // event budget, not only its text.
  return projected.budgetExhausted === undefined
    ? projected
    : fitSummaryText(projected, MAX_HARNESS_SUBAGENT_EVENT_TEXT_BYTES);
}

function isBoundedAnswerText(text: string): boolean {
  return (
    text.trim().length > 0 &&
    !text.includes('\0') &&
    new TextEncoder().encode(text).byteLength <= MAX_HARNESS_SUBAGENT_RESULT_TEXT_BYTES
  );
}

/**
 * Parse the exact framework terminal envelope emitted by
 * `report_subagent_outcome`. Arbitrary nested objects are never searched, so a
 * domain tool result cannot spoof task settlement by containing similar keys.
 */
export function parseHarnessSubagentOutcomeReport(value: unknown): HarnessSubagentOutcomeReport | undefined {
  const record = isRecord(value) ? value : undefined;
  const candidate = record?.terminalToolResult ?? value;
  const parsed = terminalSubagentOutcomeEnvelopeSchema.safeParse(candidate);
  if (!parsed.success) return undefined;
  const report = parsed.data.items[0].value;
  if (new TextEncoder().encode(JSON.stringify(report)).byteLength > MAX_HARNESS_SUBAGENT_DIRECT_ANSWER_BYTES) {
    return undefined;
  }
  return report;
}

type HarnessSubagentOutcomeEvidence = HarnessSubagentOutcomeReport['evidence'][number];

/**
 * Bind every `tool-result` claim in a report to the framework-observed receipt
 * ledger. The model may describe evidence, but it cannot mint its identity or
 * status:
 *
 * - a claim that cites a `toolCallId` must match that exact call's recorded
 *   name and status;
 * - a claim without an id (models are never shown framework call ids) binds to
 *   the first recorded call, in execution order, with the same tool name and
 *   status that no other item already cites; when every such call is already
 *   cited, it re-binds to the first of them (the same receipt described
 *   twice). A call whose receipts disagree on name/status is never bound this
 *   way, and a claim with no matching call at all is rejected.
 *
 * Returns the report with every `tool-result` item carrying its bound
 * framework id, or undefined when any claim is not backed by the ledger.
 */
function bindHarnessSubagentOutcomeEvidence(
  report: HarnessSubagentOutcomeReport,
  receipts: readonly HarnessSubagentToolReceipt[],
): HarnessSubagentOutcomeReport | undefined {
  const byCallId = new Map<string, HarnessSubagentToolReceipt[]>();
  for (const receipt of receipts) {
    const existing = byCallId.get(receipt.toolCallId) ?? [];
    existing.push(receipt);
    byCallId.set(receipt.toolCallId, existing);
  }
  const unambiguousCalls = [...byCallId.entries()].flatMap(([toolCallId, candidates]) => {
    const [first] = candidates;
    return first !== undefined &&
      candidates.every(candidate => candidate.toolName === first.toolName && candidate.status === first.status)
      ? [{ toolCallId, toolName: first.toolName, status: first.status }]
      : [];
  });
  // Calls cited by exact id are already claimed; id-less claims prefer others.
  const claimed = new Set(
    report.evidence.flatMap(item =>
      item.kind === 'tool-result' && item.toolCallId !== undefined ? [item.toolCallId] : [],
    ),
  );

  const evidence: HarnessSubagentOutcomeEvidence[] = [];
  let citesSuccessfulTool = false;
  for (const item of report.evidence) {
    if (item.kind !== 'tool-result') {
      evidence.push(item);
      continue;
    }
    // The schema requires both for tool-result evidence; fail closed regardless.
    if (item.toolName === undefined || item.status === undefined) return undefined;
    if (item.toolCallId !== undefined) {
      const candidates = byCallId.get(item.toolCallId) ?? [];
      if (
        candidates.length === 0 ||
        candidates.some(candidate => candidate.toolName !== item.toolName || candidate.status !== item.status)
      ) {
        return undefined;
      }
      evidence.push(item);
    } else {
      const matches = (candidate: (typeof unambiguousCalls)[number]) =>
        candidate.toolName === item.toolName && candidate.status === item.status;
      // Prefer a call no other item cites. A further description of a receipt
      // already cited re-binds to it (PF-7506) instead of discarding the report.
      const call =
        unambiguousCalls.find(candidate => !claimed.has(candidate.toolCallId) && matches(candidate)) ??
        unambiguousCalls.find(matches);
      if (call === undefined) return undefined;
      claimed.add(call.toolCallId);
      evidence.push({ ...item, toolCallId: call.toolCallId });
    }
    if (item.status === 'success') citesSuccessfulTool = true;
  }

  // A completed tool-using run must cite at least one successful tool receipt.
  if (report.outcome === 'completed' && receipts.length > 0 && !citesSuccessfulTool) return undefined;
  // Bound ids come from the ledger, not the validated claim: re-check the
  // id length and aggregate byte budget so every consumer schema still holds.
  const bound = harnessSubagentOutcomeReportSchema.safeParse({ ...report, evidence });
  return bound.success ? bound.data : undefined;
}

/**
 * Return the bounded, framework-observed receipts cited by a terminal report.
 * The projection contains identity and status only; raw tool inputs/outputs are
 * intentionally excluded so durable adapters can retain verification evidence
 * without retaining provider/tool payloads. Receipts bound to id-less claims
 * are included, so a compacted result can still be re-verified.
 */
export function projectHarnessSubagentOutcomeReceipts(value: unknown): HarnessSubagentToolReceipt[] {
  const report = parseHarnessSubagentOutcomeReport(value);
  if (report === undefined) return [];
  const { receipts, overflow } = collectToolReceipts(value);
  if (overflow) return [];
  const bound = bindHarnessSubagentOutcomeEvidence(report, receipts);
  const citedIds = new Set(
    [...report.evidence, ...(bound?.evidence ?? [])].flatMap(evidence =>
      evidence.kind === 'tool-result' && evidence.toolCallId !== undefined ? [evidence.toolCallId] : [],
    ),
  );
  if (citedIds.size === 0) return [];
  return receipts.filter(receipt => citedIds.has(receipt.toolCallId));
}

/**
 * Validate semantic tool evidence against framework-observed execution receipts
 * (see `bindHarnessSubagentOutcomeEvidence`). Every `tool-result` item must bind
 * to a real recorded call with the claimed tool name and status, and a
 * completed tool-using run must cite at least one successful tool receipt.
 */
export function verifyHarnessSubagentOutcomeReport(value: unknown): HarnessSubagentOutcomeReport | undefined {
  const report = parseHarnessSubagentOutcomeReport(value);
  if (report === undefined) return undefined;

  const { receipts, overflow } = collectToolReceipts(value);
  if (overflow) return undefined;
  return bindHarnessSubagentOutcomeEvidence(report, receipts);
}

function parseGenericHarnessTerminalToolResult(value: unknown) {
  const record = isRecord(value) ? value : undefined;
  const candidate = record?.terminalToolResult ?? value;
  try {
    const terminal = materializeTerminalToolResult(candidate);
    // The dedicated report tool has stronger semantic and receipt rules above.
    // Never reinterpret a malformed report as an ordinary domain completion.
    if (terminal.items.some(item => item.toolName === HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID)) return undefined;
    return terminal;
  } catch {
    return undefined;
  }
}

/**
 * Preserve only the explicit, bounded `artifacts` projection from one validated
 * domain terminal tool result. The Harness never searches raw child output or
 * provider payloads for similarly named fields.
 */
export function parseHarnessTerminalToolResultArtifacts(
  value: unknown,
): z.infer<typeof harnessSubagentTerminalArtifactsSchema> | undefined {
  const terminal = parseGenericHarnessTerminalToolResult(value);
  if (terminal === undefined || terminal.items.length !== 1) return undefined;
  const projected = terminal.items[0]?.value;
  if (!isRecord(projected) || !Object.hasOwn(projected, 'artifacts')) return undefined;
  const parsed = harnessSubagentTerminalArtifactsSchema.safeParse(projected.artifacts);
  return parsed.success ? parsed.data : undefined;
}

/**
 * Materialize caller-visible text from a framework terminal envelope without
 * searching arbitrary nested objects. This accepts the Harness direct-answer
 * envelope and explicitly opted-in domain tools, but never the outcome-report
 * control tool.
 */
export function parseHarnessTerminalToolResultText(value: unknown): string | undefined {
  const directAnswer = parseHarnessSubagentTerminalResult(value);
  if (directAnswer !== undefined) return directAnswer.text;

  const terminal = parseGenericHarnessTerminalToolResult(value);
  if (terminal === undefined) return undefined;
  if (terminal.items.length === 1) {
    const projected = terminal.items[0]?.value;
    if (isRecord(projected) && typeof projected.text === 'string' && isBoundedAnswerText(projected.text)) {
      return projected.text;
    }
  }
  const text = tryFormatTerminalToolResultForModel(terminal);
  return text !== undefined && isBoundedAnswerText(text) ? text : undefined;
}

/**
 * Resolve a child's semantic completion from either the dedicated outcome tool
 * or another explicitly terminal domain tool. Generic terminal items are
 * accepted only when every identity/status matches a framework-observed tool
 * receipt; provider-shaped data cannot mint completion authority.
 */
export function verifyHarnessSubagentTerminalCompletion(value: unknown): HarnessSubagentOutcomeReport | undefined {
  return resolveHarnessSubagentTerminalCompletion(value)?.report;
}

/**
 * The verified completion plus how many leading evidence items are required:
 * every item of an outcome report, or the terminal items of a direct delivery.
 * Items after those are earlier framework receipts appended to a direct
 * delivery, which a summary may skip when they do not fit its envelope.
 */
function resolveHarnessSubagentTerminalCompletion(
  value: unknown,
): { report: HarnessSubagentOutcomeReport; requiredEvidenceCount: number } | undefined {
  const reported = verifyHarnessSubagentOutcomeReport(value);
  if (reported !== undefined) return { report: reported, requiredEvidenceCount: reported.evidence.length };

  const terminal = parseGenericHarnessTerminalToolResult(value);
  const summary = terminal === undefined ? undefined : parseHarnessTerminalToolResultText(terminal);
  if (terminal === undefined || summary === undefined) return undefined;

  const { receipts, overflow } = collectToolReceipts(value);
  if (overflow) return undefined;
  const byCallId = new Map<string, HarnessSubagentToolReceipt[]>();
  for (const receipt of receipts) {
    const existing = byCallId.get(receipt.toolCallId) ?? [];
    existing.push(receipt);
    byCallId.set(receipt.toolCallId, existing);
  }

  const seenCallIds = new Set<string>();
  const evidence: HarnessSubagentOutcomeReport['evidence'] = [];
  for (const item of terminal.items) {
    if (
      item.toolName.length > MAX_HARNESS_SUBAGENT_LABEL_LENGTH ||
      item.toolCallId.length > 1_024 ||
      seenCallIds.has(item.toolCallId)
    ) {
      return undefined;
    }
    seenCallIds.add(item.toolCallId);
    const candidates = byCallId.get(item.toolCallId) ?? [];
    if (candidates.length !== 1 || candidates[0]!.toolName !== item.toolName || candidates[0]!.status !== 'success') {
      return undefined;
    }
    evidence.push({
      kind: 'tool-result',
      toolName: item.toolName,
      toolCallId: item.toolCallId,
      status: 'success',
      description: `Framework-observed terminal result from ${item.toolName}.`,
    });
  }
  // The terminal items prove only the call that delivered the answer. The
  // child's other framework-minted receipts are Harness-observed too, so they
  // join the evidence in call order: a caller that requires every delegated
  // call to be proven can then verify a multi-call direct delivery. Run
  // tool-result fallbacks are never appended, and an ambiguous earlier call id
  // is skipped. Appending stops at the report's item bound and skips any
  // receipt that would break its byte bound, so a terminal-only report that
  // verified before still verifies; a call left out stays unproven for the
  // caller.
  const requiredEvidenceCount = evidence.length;
  const encoder = new TextEncoder();
  let reportBytes = encoder.encode(
    JSON.stringify({ kind: HARNESS_SUBAGENT_OUTCOME_REPORT_KIND, outcome: 'completed', summary, evidence }),
  ).byteLength;
  for (const receipt of frameworkToolReceipts(value)) {
    if (evidence.length >= MAX_HARNESS_SUBAGENT_EVIDENCE_ITEMS) break;
    if (seenCallIds.has(receipt.toolCallId) || byCallId.get(receipt.toolCallId)?.length !== 1) continue;
    const item: HarnessSubagentOutcomeReport['evidence'][number] = {
      kind: 'tool-result',
      toolName: receipt.toolName,
      toolCallId: receipt.toolCallId,
      status: receipt.status,
      description: `Framework-observed result from ${receipt.toolName}.`,
    };
    // One more array element serializes as a comma plus the item.
    const itemBytes = encoder.encode(JSON.stringify(item)).byteLength + 1;
    // Skip a receipt that does not fit; a smaller later one still may.
    if (reportBytes + itemBytes > MAX_HARNESS_SUBAGENT_DIRECT_ANSWER_BYTES) continue;
    reportBytes += itemBytes;
    seenCallIds.add(receipt.toolCallId);
    evidence.push(item);
  }

  const projected = harnessSubagentOutcomeReportSchema.safeParse({
    kind: HARNESS_SUBAGENT_OUTCOME_REPORT_KIND,
    outcome: 'completed',
    summary,
    evidence,
  });
  return projected.success ? { report: projected.data, requiredEvidenceCount } : undefined;
}

/**
 * Parse only the one framework-owned terminal shape that Harness v1 can render
 * as a direct subagent answer. This intentionally does not search arbitrary
 * nested objects for a `text` property.
 */
export function parseHarnessSubagentTerminalResult(value: unknown): HarnessSubagentDirectAnswer | undefined {
  const parsed = terminalSubagentEnvelopeSchema.safeParse(value);
  if (!parsed.success || !isBoundedAnswerText(parsed.data.items[0].value.text)) return undefined;
  return parsed.data.items[0].value;
}

/** Project a successful spawn tool output to its small, caller-facing terminal value. */
export function projectHarnessSpawnSubagentResult(output: unknown): HarnessSubagentDirectAnswer | undefined {
  if (!isRecord(output) || output.isError === true) return undefined;
  const subagentSessionId = output.subagentSessionId;
  if (typeof subagentSessionId !== 'string' || subagentSessionId.length === 0 || subagentSessionId.length > 512) {
    return undefined;
  }

  const parsed = harnessSubagentResultSummarySchema.safeParse(output.result);
  if (
    !parsed.success ||
    parsed.data.status !== 'success' ||
    (parsed.data.outcome !== undefined && parsed.data.outcome !== 'completed') ||
    (parsed.data.outcome === undefined && parsed.data.finishReason !== 'stop') ||
    parsed.data.textTruncated ||
    parsed.data.error !== undefined ||
    !isBoundedAnswerText(parsed.data.text)
  ) {
    return undefined;
  }

  return {
    kind: HARNESS_SUBAGENT_DIRECT_ANSWER_KIND,
    subagentSessionId,
    text: parsed.data.text,
    ...(parsed.data.artifacts === undefined ? {} : { artifacts: parsed.data.artifacts }),
  };
}

/** Give Harness callers the same authoritative text that its live/history views render. */
export function materializeHarnessTerminalText<T extends { text: string; terminalToolResult?: unknown }>(output: T): T {
  const text = parseHarnessTerminalToolResultText(output.terminalToolResult);
  return text === undefined ? output : { ...output, text };
}
