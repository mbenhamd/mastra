import { describe, expect, it } from 'vitest';
import { createToolCallIdentityDigest, parseToolApprovalDecision, parseToolApprovalGrant } from './tool-call-identity';

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
  it('requires an own approved field while ignoring unrelated resume context', () => {
    expect(parseToolApprovalDecision({ approved: true, reason: 'reviewed' })).toEqual({
      approved: true,
      reason: 'reviewed',
    });
    expect(parseToolApprovalDecision({ approved: true, toolName: 'lookup', toolCallId: 'call-1' })).toEqual({
      approved: true,
    });
    expect(parseToolApprovalDecision({ approved: true, injected: 'payload' })).toBeUndefined();
    expect(
      parseToolApprovalDecision(Object.assign(Object.create({ reason: 'inherited' }), { approved: true })),
    ).toEqual({
      approved: true,
    });
    expect(parseToolApprovalDecision({ approved: true, reason: 42 })).toBeUndefined();
    expect(parseToolApprovalDecision(Object.create({ approved: true }))).toBeUndefined();
  });
});

describe('parseToolApprovalGrant', () => {
  it('accepts only an exact approved grant for the expected call', () => {
    expect(parseToolApprovalGrant(Object.create({ id: 'call-1', approved: true }), 'call-1')).toBeUndefined();
    expect(parseToolApprovalGrant({ id: 'call-1', approved: true }, 'call-1')).toEqual({
      id: 'call-1',
      approved: true,
    });
    expect(parseToolApprovalGrant({ id: 'call-other', approved: true }, 'call-1')).toBeUndefined();
    expect(parseToolApprovalGrant({ id: 'call-1', approved: false }, 'call-1')).toBeUndefined();
    expect(parseToolApprovalGrant({ id: 'call-1', approved: true, injected: true }, 'call-1')).toBeUndefined();
    expect(
      parseToolApprovalGrant(
        Object.assign(Object.create({ reason: 'inherited' }), { id: 'call-1', approved: true }),
        'call-1',
      ),
    ).toEqual({ id: 'call-1', approved: true });
  });
});
