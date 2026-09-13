import { describe, it, expect } from 'vitest';
import type { MastraDBMessage } from '../state/types';
import { AIV4Adapter } from './AIV4Adapter';

describe('AIV4Adapter.toUIMessage — v5-shaped file parts (mediaType)', () => {
  const userMessage = (parts: MastraDBMessage['content']['parts']): MastraDBMessage => ({
    id: 'm1',
    role: 'user',
    createdAt: new Date('2024-01-01'),
    threadId: 't1',
    resourceId: 'r1',
    content: { format: 2, parts },
  });

  // v5 shape: `mediaType`/`data`. The stored union only describes v4, so cast at the boundary.
  const v5FilePart = (mediaType: string, data: string) =>
    ({ type: 'file', mediaType, data }) as unknown as MastraDBMessage['content']['parts'][number];

  it('carries the media type into experimental_attachments.contentType for a v5 file part', () => {
    const ui = AIV4Adapter.toUIMessage(userMessage([v5FilePart('application/pdf', 'JVBERi0xLjQ=')]));
    const attachment = ui.experimental_attachments?.[0];
    expect(attachment).toBeDefined();
    expect(attachment!.contentType).toBe('application/pdf');
    expect(attachment!.url).toBe('data:application/pdf;base64,JVBERi0xLjQ=');
  });

  it('still works for a persisted v4 file part (mimeType/data)', () => {
    const v4FilePart = {
      type: 'file',
      mimeType: 'application/pdf',
      data: 'JVBERi0xLjQ=',
    } as MastraDBMessage['content']['parts'][number];
    const ui = AIV4Adapter.toUIMessage(userMessage([v4FilePart]));
    const attachment = ui.experimental_attachments?.[0];
    expect(attachment!.contentType).toBe('application/pdf');
    expect(attachment!.url).toBe('data:application/pdf;base64,JVBERi0xLjQ=');
  });

  it('strips private tool state from metadata and extended parts without mutating the source', () => {
    const suspendedTool = {
      toolCallId: 'call-private',
      toolName: 'requestApproval',
      args: { documentId: 'PUBLIC_ARGS' },
      approvedArgs: { documentId: 'PRIVATE_APPROVED_ARGS' },
      approvalInputIdentityDigest: 'PRIVATE_APPROVAL_DIGEST',
      suspendPayload: { reason: 'PUBLIC_SUSPENSION_PAYLOAD' },
    };
    const message: MastraDBMessage = {
      id: 'm-private-tool-state',
      role: 'assistant',
      createdAt: new Date('2024-01-01'),
      content: {
        format: 2,
        parts: [{ type: 'data-tool-call-suspended', data: suspendedTool } as any],
        metadata: {
          suspendedTools: {
            'call-private': structuredClone(suspendedTool),
          },
        },
      },
    };
    const sourceBefore = structuredClone(message);

    const uiMessage = AIV4Adapter.toUIMessage(message);
    const suspendedPart = uiMessage.parts.find((part: any) => part.type === 'data-tool-call-suspended') as any;
    const suspendedMetadata = (uiMessage.metadata as any)?.suspendedTools?.['call-private'];

    expect(suspendedPart.data).toMatchObject({
      toolCallId: 'call-private',
      args: { documentId: 'PUBLIC_ARGS' },
      suspendPayload: { reason: 'PUBLIC_SUSPENSION_PAYLOAD' },
    });
    expect(suspendedPart.data).not.toHaveProperty('approvedArgs');
    expect(suspendedPart.data).not.toHaveProperty('approvalInputIdentityDigest');
    expect(suspendedMetadata).toMatchObject({
      toolCallId: 'call-private',
      args: { documentId: 'PUBLIC_ARGS' },
      suspendPayload: { reason: 'PUBLIC_SUSPENSION_PAYLOAD' },
    });
    expect(suspendedMetadata).not.toHaveProperty('approvedArgs');
    expect(suspendedMetadata).not.toHaveProperty('approvalInputIdentityDigest');
    expect(message).toEqual(sourceBefore);
  });
});
