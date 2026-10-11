import { Buffer } from 'node:buffer';
import { createHash } from 'node:crypto';

import type { TerminalToolResult } from '../../tools/types';
import { materializeWorkflowTerminalCanonicalJson } from '../../workflows/terminal-recovery/canonical-json';
import type { WorkflowTerminalCanonicalJsonValue } from '../../workflows/terminal-recovery/types';
import { stableStringify } from './cache/stable-stringify';

export const MAX_TERMINAL_TOOL_RESULT_ENVELOPE_BYTES = 64 * 1024;
export const MAX_TERMINAL_TOOL_RESULT_MODEL_TEXT_BYTES = 128 * 1024;

/** Build a stable data-part id without embedding an unbounded caller run id. */
export function createTerminalToolResultPartId(runId: string, stepCount: number): string {
  if (runId.length === 0 || !Number.isSafeInteger(stepCount) || stepCount < 0) {
    throw new TypeError('Terminal tool-result identity is invalid');
  }
  const runDigest = createHash('sha256').update(runId, 'utf8').digest('hex');
  return `terminal-tool-result:${runDigest}:${stepCount}`;
}

/**
 * Canonicalize and validate a terminal result received at a persistence,
 * transport, or replay trust boundary.
 */
export function materializeTerminalToolResult(value: unknown): TerminalToolResult<WorkflowTerminalCanonicalJsonValue> {
  const canonical = materializeWorkflowTerminalCanonicalJson(value, 'terminal tool result');
  if (!canonical || typeof canonical !== 'object' || Array.isArray(canonical)) {
    throw new TypeError('Terminal tool result must be an object');
  }
  const record = canonical as Record<string, unknown>;
  if (record.status !== 'success' || !Array.isArray(record.items) || record.items.length === 0) {
    throw new TypeError('Terminal tool result must contain one or more successful items');
  }

  const items: TerminalToolResult<WorkflowTerminalCanonicalJsonValue>['items'] = record.items.map((item, index) => {
    if (!item || typeof item !== 'object' || Array.isArray(item)) {
      throw new TypeError(`Terminal tool result item ${index} must be an object`);
    }
    const candidate = item as Record<string, unknown>;
    if (
      typeof candidate.toolName !== 'string' ||
      candidate.toolName.length === 0 ||
      typeof candidate.toolCallId !== 'string' ||
      candidate.toolCallId.length === 0 ||
      candidate.status !== 'success' ||
      !Object.hasOwn(candidate, 'value')
    ) {
      throw new TypeError(`Terminal tool result item ${index} is malformed`);
    }
    return {
      toolName: candidate.toolName,
      toolCallId: candidate.toolCallId,
      status: 'success',
      value: candidate.value as WorkflowTerminalCanonicalJsonValue,
    };
  });

  const result: TerminalToolResult<WorkflowTerminalCanonicalJsonValue> = { status: 'success', items };
  if (Buffer.byteLength(JSON.stringify(result), 'utf8') > MAX_TERMINAL_TOOL_RESULT_ENVELOPE_BYTES) {
    throw new TypeError('Terminal tool result exceeds the terminal envelope limit');
  }
  return result;
}

function formatProjectedValue(value: unknown): string {
  if (typeof value === 'string' && value.trim().length > 0) return value;

  if (value && typeof value === 'object' && !Array.isArray(value)) {
    const record = value as Record<string, unknown>;
    if (Object.hasOwn(record, 'text') && typeof record.text === 'string' && record.text.trim().length > 0) {
      const metadata = Object.fromEntries(Object.entries(record).filter(([key]) => key !== 'text'));
      return Object.keys(metadata).length > 0
        ? `${record.text}\n\n[Terminal result metadata: ${JSON.stringify(metadata)}]`
        : record.text;
    }
  }

  return JSON.stringify(value);
}

/**
 * Create the provider/Observer-visible projection for a persisted terminal
 * result. The structured data part remains unchanged for UI replay; only model
 * conversion uses this bounded text. A single top-level `text` field is emitted
 * exactly once, while other approved projection fields remain compact metadata.
 */
export function formatTerminalToolResultForModel(value: unknown): string {
  const result = materializeTerminalToolResult(value);
  const text =
    result.items.length === 1
      ? formatProjectedValue(result.items[0]!.value)
      : result.items
          .map(item => `[Terminal result from ${JSON.stringify(item.toolName)}]\n${formatProjectedValue(item.value)}`)
          .join('\n\n');

  if (!text || Buffer.byteLength(text, 'utf8') > MAX_TERMINAL_TOOL_RESULT_MODEL_TEXT_BYTES) {
    throw new TypeError('Terminal tool result model projection is empty or exceeds its byte limit');
  }
  return text;
}

/** Fail closed when replayed/untrusted terminal data is malformed. */
export function tryFormatTerminalToolResultForModel(value: unknown): string | undefined {
  try {
    return formatTerminalToolResultForModel(value);
  } catch {
    return undefined;
  }
}

type TerminalToolResultPart = { type: 'data-terminal-tool-result'; id: string; data: unknown };
type ResponseMessagesSource = {
  get: { response: { db(): Array<{ content?: unknown }> } };
};

function responseTerminalToolResultParts(messageList: ResponseMessagesSource): TerminalToolResultPart[] {
  return messageList.get.response.db().flatMap(message => {
    const parts = (message.content as { parts?: unknown } | undefined)?.parts;
    return Array.isArray(parts)
      ? (parts.filter(
          part =>
            part !== null &&
            typeof part === 'object' &&
            (part as { type?: unknown }).type === 'data-terminal-tool-result',
        ) as TerminalToolResultPart[])
      : [];
  });
}

function terminalPartFingerprint(part: TerminalToolResultPart): string {
  let data: string;
  try {
    data = stableStringify(part.data);
  } catch {
    data = '\u0000unserializable';
  }
  return `${String(part.id)}\u0000${data}`;
}

/**
 * Snapshot the terminal tool-result parts in the response by identity and
 * content. Take it before final output processors run, so the retention check
 * can tell an earlier run's terminal part (recalled history a signal boundary
 * promoted into the response) from one a processor introduced or rewrote.
 */
export function countResponseTerminalToolResultParts(messageList: ResponseMessagesSource): Map<string, number> {
  const counts = new Map<string, number>();
  for (const part of responseTerminalToolResultParts(messageList)) {
    const fingerprint = terminalPartFingerprint(part);
    counts.set(fingerprint, (counts.get(fingerprint) ?? 0) + 1);
  }
  return counts;
}

/**
 * Find this run's terminal part after the final output processors ran. The
 * part is matched by its framework id, not by the id of the message carrying
 * it, because a processor may re-identify that message (for example a memory
 * processor that stamps a deterministic storage id). Exactly one part with
 * `terminalPartId` must remain, and every other terminal part must already
 * have been in the response, unchanged, before the processors ran: none may
 * appear, multiply, or have its data rewritten.
 */
export function findRetainedTerminalToolResultPart(
  messageList: ResponseMessagesSource,
  terminalPartId: string,
  beforeProcessors: ReadonlyMap<string, number>,
): TerminalToolResultPart | undefined {
  const parts = responseTerminalToolResultParts(messageList);
  const retained = parts.filter(part => part.id === terminalPartId);
  if (retained.length !== 1) return undefined;
  const others = new Map<string, number>();
  for (const part of parts) {
    if (part.id === terminalPartId) continue;
    const fingerprint = terminalPartFingerprint(part);
    others.set(fingerprint, (others.get(fingerprint) ?? 0) + 1);
  }
  for (const [fingerprint, count] of others) {
    if (count > (beforeProcessors.get(fingerprint) ?? 0)) return undefined;
  }
  return retained[0];
}
