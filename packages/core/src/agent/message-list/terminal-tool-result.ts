import { Buffer } from 'node:buffer';
import { createHash } from 'node:crypto';

import type { TerminalToolResult } from '../../tools/types';
import { materializeWorkflowTerminalCanonicalJson } from '../../workflows/terminal-recovery/canonical-json';
import type { WorkflowTerminalCanonicalJsonValue } from '../../workflows/terminal-recovery/types';

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
