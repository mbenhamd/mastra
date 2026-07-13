import { describe, expect, it } from 'vitest';
import { createToolCallIdentityDigest, parseToolApprovalDecision } from './tool-call-identity';

describe('createToolCallIdentityDigest', () => {
  it('is stable across object key order', () => {
    expect(
      createToolCallIdentityDigest({
        toolCallId: 'call-1',
        toolName: 'lookup',
        args: { nested: { a: 1, b: 2 } },
      }),
    ).toBe(
      createToolCallIdentityDigest({
        toolCallId: 'call-1',
        toolName: 'lookup',
        args: { nested: { b: 2, a: 1 } },
      }),
    );
  });

  it('binds own reserved-key arguments without prototype collisions', () => {
    const first = JSON.parse('{"__proto__":{"decision":"allow"}}');
    const second = JSON.parse('{"__proto__":{"decision":"deny"}}');

    expect(createToolCallIdentityDigest({ toolCallId: 'call-1', toolName: 'lookup', args: first })).not.toBe(
      createToolCallIdentityDigest({ toolCallId: 'call-1', toolName: 'lookup', args: second }),
    );
  });
});

describe('parseToolApprovalDecision', () => {
  it('accepts only own approved and optional reason fields', () => {
    expect(parseToolApprovalDecision({ approved: true, reason: 'reviewed' })).toEqual({
      approved: true,
      reason: 'reviewed',
    });
    expect(parseToolApprovalDecision({ approved: true, injected: 'payload' })).toBeUndefined();
    expect(parseToolApprovalDecision(Object.create({ approved: true }))).toBeUndefined();
  });
});
