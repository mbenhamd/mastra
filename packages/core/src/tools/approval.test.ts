import { describe, expect, it, vi } from 'vitest';
import { resolveToolApprovalRequirement, resolveToolRequiresApproval } from './approval';

describe('resolveToolRequiresApproval', () => {
  it('lets needsApprovalFn override a static requireApproval seed (#17337 precedence)', async () => {
    await expect(
      resolveToolRequiresApproval({
        tool: {
          requireApproval: true,
          needsApprovalFn: vi.fn().mockReturnValue(false),
        },
        args: { path: '/tmp/file.txt' },
      }),
    ).resolves.toBe(false);
  });

  it('lets needsApprovalFn override the global requireToolApproval seed (#17337 precedence)', async () => {
    await expect(
      resolveToolRequiresApproval({
        tool: {
          requireApproval: false,
          needsApprovalFn: vi.fn().mockReturnValue(false),
        },
        requireToolApproval: true,
        args: { path: '/tmp/file.txt' },
      }),
    ).resolves.toBe(false);
  });

  it('lets dynamic-only approval skip safe calls', async () => {
    await expect(
      resolveToolRequiresApproval({
        tool: {
          requireApproval: false,
          needsApprovalFn: vi.fn().mockReturnValue(false),
        },
        args: { path: '/tmp/file.txt' },
      }),
    ).resolves.toBe(false);
  });

  it('supports raw tools with function-valued requireApproval', async () => {
    await expect(
      resolveToolRequiresApproval({
        tool: {
          requireApproval: vi.fn().mockReturnValue(true),
        },
        args: { path: '/protected/file.txt' },
      }),
    ).resolves.toBe(true);
  });

  it('supports raw AI SDK needsApproval callbacks', async () => {
    await expect(
      resolveToolRequiresApproval({
        tool: {
          needsApproval: vi.fn().mockReturnValue(true),
        },
        args: { path: '/protected/file.txt' },
      }),
    ).resolves.toBe(true);
  });

  it('passes request context and workspace to dynamic approval functions', async () => {
    const workspace = { id: 'workspace' };
    const needsApprovalFn = vi.fn().mockReturnValue(true);

    await resolveToolRequiresApproval({
      tool: { needsApprovalFn },
      args: { path: '/tmp/file.txt' },
      requestContext: new Map([['userId', 'user-1']]),
      workspace,
    });

    expect(needsApprovalFn).toHaveBeenCalledWith(
      { path: '/tmp/file.txt' },
      {
        requestContext: { userId: 'user-1' },
        workspace,
      },
    );
  });

  it('requires approval when the dynamic approval function throws', async () => {
    const logger = { error: vi.fn() };

    await expect(
      resolveToolRequiresApproval({
        tool: {
          needsApprovalFn: vi.fn().mockImplementation(() => {
            throw new Error('boom');
          }),
        },
        logger,
        toolName: 'dangerous-tool',
      }),
    ).resolves.toBe(true);
    expect(logger.error).toHaveBeenCalledWith(expect.stringContaining('dangerous-tool'), expect.any(Error));
  });
});

describe('resolveToolApprovalRequirement', () => {
  it('captures a reason from an object-returning predicate', async () => {
    await expect(
      resolveToolApprovalRequirement({
        tool: { needsApprovalFn: vi.fn().mockReturnValue({ required: true, reason: 'writes outside workspace' }) },
        args: { path: '/etc/passwd' },
      }),
    ).resolves.toEqual({ required: true, reasons: ['writes outside workspace'] });
  });

  it('returns no reasons for a boolean-returning predicate', async () => {
    await expect(
      resolveToolApprovalRequirement({ tool: { needsApprovalFn: vi.fn().mockReturnValue(true) } }),
    ).resolves.toEqual({ required: true, reasons: [] });
  });

  it('drops the reason when the predicate does not require approval', async () => {
    await expect(
      resolveToolApprovalRequirement({
        tool: { needsApprovalFn: vi.fn().mockReturnValue({ required: false, reason: 'safe path' }) },
      }),
    ).resolves.toEqual({ required: false, reasons: [] });
  });

  it('a predicate returning { required: false } overrides the static seed (#17337 precedence)', async () => {
    await expect(
      resolveToolApprovalRequirement({
        tool: { requireApproval: true, needsApprovalFn: vi.fn().mockReturnValue({ required: false, reason: 'x' }) },
      }),
    ).resolves.toEqual({ required: false, reasons: [] });
  });

  it('supports a function-valued requireApproval returning an object', async () => {
    await expect(
      resolveToolApprovalRequirement({
        tool: { requireApproval: vi.fn().mockReturnValue({ required: true, reason: 'protected' }) },
        args: { path: '/protected' },
      }),
    ).resolves.toEqual({ required: true, reasons: ['protected'] });
  });

  it('ignores an empty reason string', async () => {
    await expect(
      resolveToolApprovalRequirement({
        tool: { needsApprovalFn: vi.fn().mockReturnValue({ required: true, reason: '' }) },
      }),
    ).resolves.toEqual({ required: true, reasons: [] });
  });

  it('fails safe with no reason when the predicate throws', async () => {
    const logger = { error: vi.fn() };
    await expect(
      resolveToolApprovalRequirement({
        tool: {
          needsApprovalFn: vi.fn().mockImplementation(() => {
            throw new Error('boom');
          }),
        },
        logger,
        toolName: 'dangerous-tool',
      }),
    ).resolves.toEqual({ required: true, reasons: [] });
  });

  it('keeps the boolean resolver in sync with the richer resolver', async () => {
    const tool = { needsApprovalFn: vi.fn().mockReturnValue({ required: true, reason: 'because' }) };
    await expect(resolveToolRequiresApproval({ tool })).resolves.toBe(true);
  });
});
