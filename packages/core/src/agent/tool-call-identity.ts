import { createHash } from 'node:crypto';
import { stableStringify } from './message-list/cache/stable-stringify';

export function createToolCallIdentityDigest({
  toolCallId,
  toolName,
  args,
}: {
  toolCallId: string;
  toolName: string;
  args: unknown;
}): string {
  return createHash('sha256').update(stableStringify({ toolCallId, toolName, args }), 'utf8').digest('hex');
}

export function parseToolApprovalDecision(value: unknown): { approved: boolean; reason?: string } | undefined {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return undefined;
  const record = value as Record<string, unknown>;
  if (!Object.hasOwn(record, 'approved') || typeof record.approved !== 'boolean') return undefined;
  if (Object.keys(record).some(key => key !== 'approved' && key !== 'reason')) return undefined;
  if (Object.hasOwn(record, 'reason') && typeof record.reason !== 'string') return undefined;
  return {
    approved: record.approved,
    ...(typeof record.reason === 'string' ? { reason: record.reason } : {}),
  };
}
