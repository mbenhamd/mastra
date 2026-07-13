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
  if (Object.keys(record).some(key => !['approved', 'reason', 'toolName', 'toolCallId'].includes(key)))
    return undefined;
  const hasReason = Object.hasOwn(record, 'reason');
  if (hasReason && typeof record.reason !== 'string') return undefined;
  return {
    approved: record.approved,
    ...(hasReason && typeof record.reason === 'string' ? { reason: record.reason } : {}),
  };
}

export type ToolApprovalGrant = { id: string; approved: true; reason?: string };

export function parseToolApprovalGrant(value: unknown, expectedToolCallId: string): ToolApprovalGrant | undefined {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return undefined;
  const record = value as Record<string, unknown>;
  if (!Object.hasOwn(record, 'id') || !Object.hasOwn(record, 'approved')) return undefined;
  if (Object.keys(record).some(key => key !== 'id' && key !== 'approved' && key !== 'reason')) return undefined;
  if (record.id !== expectedToolCallId || record.approved !== true) return undefined;
  const hasReason = Object.hasOwn(record, 'reason');
  if (hasReason && typeof record.reason !== 'string') return undefined;
  return {
    id: expectedToolCallId,
    approved: true,
    ...(hasReason && typeof record.reason === 'string' ? { reason: record.reason } : {}),
  };
}
