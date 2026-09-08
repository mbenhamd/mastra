import { createHash } from 'node:crypto';
import { z } from 'zod/v4';
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

export const toolApprovalEditedArgsSchema = z.record(z.string(), z.json());
export type ToolApprovalDecision = {
  approved: boolean;
  reason?: string;
  /** Shallow JSON patch for a regular agent's tool-gate approval. */
  editedArgs?: z.infer<typeof toolApprovalEditedArgsSchema>;
};

export function parseToolApprovalDecision(value: unknown): ToolApprovalDecision | undefined {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return undefined;
  const record = value as Record<string, unknown>;
  if (!Object.hasOwn(record, 'approved') || typeof record.approved !== 'boolean') return undefined;
  if (Object.keys(record).some(key => !['approved', 'reason', 'toolName', 'toolCallId', 'editedArgs'].includes(key)))
    return undefined;
  const hasReason = Object.hasOwn(record, 'reason');
  if (hasReason && typeof record.reason !== 'string') return undefined;
  let editedArgs: ToolApprovalDecision['editedArgs'];
  if (Object.hasOwn(record, 'editedArgs')) {
    if (!record.approved) return undefined;
    if (
      record.editedArgs &&
      typeof record.editedArgs === 'object' &&
      ['resumeData', 'suspendData', 'suspendedToolCallId', 'suspendedToolRunId', '__proto__'].some(key =>
        Object.hasOwn(record.editedArgs!, key),
      )
    )
      return undefined;
    try {
      const parsed = toolApprovalEditedArgsSchema.safeParse(record.editedArgs);
      if (!parsed.success) return undefined;
      editedArgs = parsed.data;
    } catch {
      return undefined;
    }
  }
  return {
    approved: record.approved,
    ...(editedArgs !== undefined ? { editedArgs } : {}),
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
