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
    toolName: z.string().min(1).max(MAX_HARNESS_SUBAGENT_LABEL_LENGTH).optional(),
    toolCallId: z.string().min(1).max(1_024).optional(),
    status: z.enum(['success', 'error']).optional(),
    reference: z.string().min(1).max(MAX_HARNESS_SUBAGENT_EVIDENCE_REFERENCE_LENGTH).optional(),
  })
  .strict()
  .superRefine((evidence, ctx) => {
    if (
      evidence.kind === 'tool-result' &&
      (evidence.toolName === undefined || evidence.toolCallId === undefined || evidence.status === undefined)
    ) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: 'tool-result evidence requires toolName, toolCallId, and status',
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
    usage: harnessSubagentUsageSchema.optional(),
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
  return { toolCallId, toolName, status: explicitStatus ?? (isError ? 'error' : 'success') };
}

function collectToolReceipts(value: unknown): {
  receipts: HarnessSubagentToolReceipt[];
  overflow: boolean;
} {
  const source = isRecord(value) ? value : undefined;
  if (source === undefined) return { receipts: [], overflow: false };
  const candidates = [...itemArray(source[HARNESS_SUBAGENT_TOOL_RECEIPTS_FIELD]), ...itemArray(source.toolResults)];
  for (const step of itemArray(source.steps)) {
    if (isRecord(step)) candidates.push(...itemArray(step.toolResults));
  }

  const receipts: HarnessSubagentToolReceipt[] = [];
  const exact = new Set<string>();
  for (const candidate of candidates) {
    const receipt = normalizeToolReceipt(candidate);
    if (receipt === undefined) continue;
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
  } = {},
): HarnessSubagentResultSummary {
  const record = isRecord(value) ? value : undefined;
  const report = verifyHarnessSubagentTerminalCompletion(value);
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

  return {
    status,
    ...(report ? { outcome: report.outcome } : {}),
    text: text.value,
    textTruncated: text.truncated,
    finishReason,
    stepCount: arrayLength(record?.steps),
    toolCallCount: arrayLength(record?.toolCalls),
    toolResultCount: arrayLength(record?.toolResults),
    ...(usage ? { usage } : {}),
    ...(artifacts ? { artifacts } : {}),
    ...(report ? { evidence: report.evidence } : {}),
    ...(report?.issue ? { issue: report.issue } : {}),
    ...(error ? { error } : {}),
  };
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
  const { artifacts: _artifacts, ...eventSummary } = summary;
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
  return {
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

/**
 * Return the bounded, framework-observed receipts cited by a terminal report.
 * The projection contains identity and status only; raw tool inputs/outputs are
 * intentionally excluded so durable adapters can retain verification evidence
 * without retaining provider/tool payloads.
 */
export function projectHarnessSubagentOutcomeReceipts(value: unknown): HarnessSubagentToolReceipt[] {
  const report = parseHarnessSubagentOutcomeReport(value);
  if (report === undefined) return [];
  const citedIds = new Set(
    report.evidence.flatMap(evidence =>
      evidence.kind === 'tool-result' && evidence.toolCallId !== undefined ? [evidence.toolCallId] : [],
    ),
  );
  if (citedIds.size === 0) return [];
  const { receipts, overflow } = collectToolReceipts(value);
  if (overflow) return [];
  return receipts.filter(receipt => citedIds.has(receipt.toolCallId));
}

/**
 * Validate semantic tool evidence against framework-observed execution receipts.
 * The model may describe evidence, but it cannot mint its identity or status:
 * every `tool-result` item must bind to the exact tool call and observed result.
 * A completed tool-using run must cite at least one successful tool receipt.
 */
export function verifyHarnessSubagentOutcomeReport(value: unknown): HarnessSubagentOutcomeReport | undefined {
  const report = parseHarnessSubagentOutcomeReport(value);
  if (report === undefined) return undefined;

  const { receipts, overflow } = collectToolReceipts(value);
  if (overflow) return undefined;
  const byCallId = new Map<string, HarnessSubagentToolReceipt[]>();
  for (const receipt of receipts) {
    const existing = byCallId.get(receipt.toolCallId) ?? [];
    existing.push(receipt);
    byCallId.set(receipt.toolCallId, existing);
  }

  const toolEvidence = report.evidence.filter(
    (
      evidence,
    ): evidence is typeof evidence & {
      toolName: string;
      toolCallId: string;
      status: 'success' | 'error';
    } =>
      evidence.kind === 'tool-result' &&
      evidence.toolName !== undefined &&
      evidence.toolCallId !== undefined &&
      evidence.status !== undefined,
  );
  for (const evidence of toolEvidence) {
    const candidates = byCallId.get(evidence.toolCallId) ?? [];
    if (
      candidates.length === 0 ||
      candidates.some(candidate => candidate.toolName !== evidence.toolName || candidate.status !== evidence.status)
    ) {
      return undefined;
    }
  }

  if (
    report.outcome === 'completed' &&
    receipts.length > 0 &&
    !toolEvidence.some(evidence => evidence.status === 'success')
  ) {
    return undefined;
  }
  return report;
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
  const reported = verifyHarnessSubagentOutcomeReport(value);
  if (reported !== undefined) return reported;

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

  const projected = harnessSubagentOutcomeReportSchema.safeParse({
    kind: HARNESS_SUBAGENT_OUTCOME_REPORT_KIND,
    outcome: 'completed',
    summary,
    evidence,
  });
  return projected.success ? projected.data : undefined;
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
