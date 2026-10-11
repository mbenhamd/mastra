import { describe, expect, it } from 'vitest';
import { z } from 'zod/v4';
import type { CoreTool } from '../../../tools';
import { resolveTerminalToolResult } from '../../shared/terminal-tool-result';

function tool(terminalResult?: Partial<NonNullable<CoreTool['terminalResult']>>, backgroundConfig?: unknown): CoreTool {
  return {
    parameters: z.object({}),
    execute: async () => undefined,
    ...(terminalResult
      ? {
          terminalResult: {
            project: output => output,
            outputSchema: z.any(),
            ...terminalResult,
          } as NonNullable<CoreTool['terminalResult']>,
        }
      : {}),
    ...(backgroundConfig ? { backgroundConfig } : {}),
  } as CoreTool;
}

function incompleteTerminalTool(terminalResult: Record<string, unknown>): CoreTool {
  return {
    parameters: z.object({}),
    execute: async () => undefined,
    terminalResult,
  } as unknown as CoreTool;
}

describe('resolveTerminalToolResult', () => {
  it('projects an ordered fully-terminal batch', async () => {
    const seenContexts: unknown[] = [];
    const tools = {
      first: tool({
        isSuccess: (output, context) => {
          seenContexts.push(context);
          return (output as { ok: boolean }).ok;
        },
        outputSchema: z.object({ answer: z.string() }),
        project: output => ({ answer: (output as { value: string }).value }),
      }),
      second: tool({
        isSuccess: (_output, context) => {
          seenContexts.push(context);
          return true;
        },
        project: output => output,
      }),
    };

    await expect(
      resolveTerminalToolResult({
        calls: [
          { toolName: 'first', toolCallId: 'call-1', args: {}, result: { ok: true, value: 'A' } },
          { toolName: 'second', toolCallId: 'call-2', args: {}, result: { value: 'B' } },
        ],
        tools: tools as any,
        runId: 'run-1',
      }),
    ).resolves.toEqual({
      status: 'success',
      items: [
        { toolName: 'first', toolCallId: 'call-1', status: 'success', value: { answer: 'A' } },
        { toolName: 'second', toolCallId: 'call-2', status: 'success', value: { value: 'B' } },
      ],
    });
    expect(seenContexts).toEqual([
      {
        toolName: 'first',
        toolCallId: 'call-1',
        args: {},
        batchSize: 2,
        batchIndex: 0,
        runId: 'run-1',
        abortSignal: expect.any(AbortSignal),
      },
      {
        toolName: 'second',
        toolCallId: 'call-2',
        args: {},
        batchSize: 2,
        batchIndex: 1,
        runId: 'run-1',
        abortSignal: expect.any(AbortSignal),
      },
    ]);
  });

  it.each([
    ['predicate rejected', { result: { ok: false } }, tool({ isSuccess: output => (output as any).ok })],
    ['ordinary tool', { result: { ok: true } }, tool()],
    [
      'missing projection',
      { result: { ok: true } },
      incompleteTerminalTool({ isSuccess: () => true, outputSchema: z.any() }),
    ],
    [
      'missing projection schema',
      { result: { ok: true } },
      incompleteTerminalTool({ isSuccess: () => true, project: (output: unknown) => output }),
    ],
    ['provider tool', { result: { ok: true }, providerExecuted: true }, tool({ isSuccess: () => true })],
    ['tool error', { error: new Error('nope') }, tool({ isSuccess: () => true })],
    [
      'validation result',
      { result: { error: true, message: 'bad', validationErrors: { errors: ['bad'], fields: {} } } },
      tool({ isSuccess: () => true }),
    ],
    ['permission denial', { result: 'denied', disposition: 'denied' }, tool({ isSuccess: () => true })],
    ['approved HITL call', { result: 'done', approval: { approved: true } }, tool({ isSuccess: () => true })],
    ['denied approval', { result: 'denied', approval: { approved: false } }, tool({ isSuccess: () => true })],
    ['resumed suspension', { result: 'done', resumedFromSuspension: true }, tool({ isSuccess: () => true })],
    ['background tool', { result: 'started' }, tool({ isSuccess: () => true }, { enabled: true })],
    [
      'tool output transform',
      { result: { secret: 'raw' } },
      Object.assign(tool({ isSuccess: () => true }), {
        transform: { display: { output: () => ({ redacted: true }) } },
      }),
    ],
  ])('fails closed for %s', async (_label, call, configuredTool) => {
    await expect(
      resolveTerminalToolResult({
        calls: [{ toolName: 'candidate', toolCallId: 'call-1', args: {}, ...call }],
        tools: { candidate: configuredTool } as any,
      }),
    ).resolves.toBeUndefined();
  });

  it('lets an afterApproval tool end the run after an approved or resumed call', async () => {
    const seen: unknown[] = [];
    const applyTool = tool({
      afterApproval: true,
      isSuccess: (_output, context) => {
        seen.push({ approvalGranted: context.approvalGranted, resumed: context.resumedFromSuspension });
        return true;
      },
    });
    for (const call of [
      { result: { applied: true }, approval: { approved: true } },
      { result: { applied: true }, resumedFromSuspension: true },
      { result: { applied: true } },
    ]) {
      await expect(
        resolveTerminalToolResult({
          calls: [{ toolName: 'apply', toolCallId: 'call-1', args: {}, ...call }],
          tools: { apply: applyTool } as any,
        }),
      ).resolves.toEqual({
        status: 'success',
        items: [{ toolName: 'apply', toolCallId: 'call-1', status: 'success', value: { applied: true } }],
      });
    }
    expect(seen).toEqual([
      { approvalGranted: true, resumed: undefined },
      { approvalGranted: undefined, resumed: true },
      { approvalGranted: undefined, resumed: undefined },
    ]);
  });

  it.each([
    ['declined approval', { result: 'declined', approval: { approved: false } }],
    ['approval without a decision', { result: 'done', approval: {} }],
    ['permission denial', { result: 'denied', disposition: 'denied' as const, approval: { approved: true } }],
    ['failed apply', { result: 'done', error: new Error('conflict'), approval: { approved: true } }],
  ])('keeps an afterApproval tool non-terminal for a %s', async (_label, call) => {
    await expect(
      resolveTerminalToolResult({
        calls: [{ toolName: 'apply', toolCallId: 'call-1', args: {}, ...call }],
        tools: { apply: tool({ afterApproval: true, isSuccess: () => true }) } as any,
      }),
    ).resolves.toBeUndefined();
  });

  it('keeps a batch that mixes an approved call with an ordinary call non-terminal', async () => {
    const optIn = tool({ afterApproval: true, isSuccess: () => true });
    await expect(
      resolveTerminalToolResult({
        calls: [
          { toolName: 'apply', toolCallId: 'call-1', args: {}, result: 'done', approval: { approved: true } },
          { toolName: 'apply', toolCallId: 'call-2', args: {}, result: 'done' },
        ],
        tools: { apply: optIn } as any,
      }),
    ).resolves.toBeUndefined();
    await expect(
      resolveTerminalToolResult({
        calls: [
          { toolName: 'apply', toolCallId: 'call-1', args: {}, result: 'done', resumedFromSuspension: true },
          { toolName: 'apply', toolCallId: 'call-2', args: {}, result: 'done', approval: { approved: true } },
        ],
        tools: { apply: optIn } as any,
      }),
    ).resolves.toMatchObject({ status: 'success' });
  });

  it('keeps a mixed batch non-terminal when only one approved tool opted in', async () => {
    await expect(
      resolveTerminalToolResult({
        calls: [
          { toolName: 'apply', toolCallId: 'call-1', args: {}, result: 'done', approval: { approved: true } },
          { toolName: 'other', toolCallId: 'call-2', args: {}, result: 'done', approval: { approved: true } },
        ],
        tools: {
          apply: tool({ afterApproval: true, isSuccess: () => true }),
          other: tool({ isSuccess: () => true }),
        } as any,
      }),
    ).resolves.toBeUndefined();
  });

  it('does not confuse a domain result containing error: true with framework validation failure', async () => {
    await expect(
      resolveTerminalToolResult({
        calls: [
          {
            toolName: 'candidate',
            toolCallId: 'call-domain-error',
            result: { error: true, answer: 'This is a domain value, not a validation envelope.' },
          },
        ],
        tools: { candidate: tool({ isSuccess: () => true }) } as any,
      }),
    ).resolves.toEqual({
      status: 'success',
      items: [
        {
          toolName: 'candidate',
          toolCallId: 'call-domain-error',
          status: 'success',
          value: { error: true, answer: 'This is a domain value, not a validation envelope.' },
        },
      ],
    });
  });

  it('refuses terminal optimization without exposing the raw result when projection is invalid or oversized', async () => {
    const rawSentinel = 'RAW-SECRET-TOOL-RESULT';
    const configured = tool({
      isSuccess: () => true,
      project: () => ({ answer: rawSentinel.repeat(100) }),
      outputSchema: z.object({ answer: z.string() }),
      maxBytes: 32,
    });

    const failures: Error[] = [];
    await expect(
      resolveTerminalToolResult({
        calls: [{ toolName: 'candidate', toolCallId: 'call-1', result: { rawSentinel } }],
        tools: { candidate: configured } as any,
        onPolicyFailure: error => failures.push(error),
      }),
    ).resolves.toBeUndefined();

    expect(failures).toHaveLength(1);
    expect(failures[0]?.message).toContain('exceeds maxBytes');
    expect(failures[0]?.message).not.toContain(rawSentinel);
  });

  it('bounds the combined parallel terminal envelope, not only each item', async () => {
    const largeValue = 'x'.repeat(34 * 1024);
    const configuredTool = tool({ isSuccess: () => true, maxBytes: 64 * 1024 });

    const failures: Error[] = [];
    await expect(
      resolveTerminalToolResult({
        calls: [
          { toolName: 'first', toolCallId: 'call-1', result: { answer: largeValue } },
          { toolName: 'second', toolCallId: 'call-2', result: { answer: largeValue } },
        ],
        tools: { first: configuredTool, second: configuredTool } as any,
        onPolicyFailure: error => failures.push(error),
      }),
    ).resolves.toBeUndefined();
    expect(failures[0]?.message).toContain('combined projected output exceeds the terminal envelope limit');
  });

  it.each([
    [
      'predicate exception',
      tool({
        isSuccess: () => {
          throw new Error('raw predicate detail');
        },
      }),
      'isSuccess predicate threw',
    ],
    [
      'projection exception',
      tool({
        isSuccess: () => true,
        project: () => {
          throw new Error('raw projection detail');
        },
      }),
      'projection threw',
    ],
    [
      'schema exception',
      tool({
        isSuccess: () => true,
        outputSchema: z.any().transform(() => {
          throw new Error('raw schema detail');
        }),
      }),
      'output schema validation threw',
    ],
    [
      'schema rejection',
      tool({ isSuccess: () => true, outputSchema: z.object({ answer: z.string() }) }),
      'projected output did not match outputSchema',
    ],
    [
      'non-canonical projection',
      tool({
        isSuccess: () => true,
        project: () => {
          const cyclic: Record<string, unknown> = {};
          cyclic.self = cyclic;
          return cyclic;
        },
      }),
      'projection must be bounded, data-only canonical JSON',
    ],
  ])('falls back safely on %s', async (_label, configuredTool, expectedDiagnostic) => {
    const failures: Error[] = [];
    await expect(
      resolveTerminalToolResult({
        calls: [{ toolName: 'candidate', toolCallId: 'call-1', result: { ok: true } }],
        tools: { candidate: configuredTool } as any,
        onPolicyFailure: error => failures.push(error),
      }),
    ).resolves.toBeUndefined();
    expect(failures).toHaveLength(1);
    expect(failures[0]?.message).toContain(expectedDiagnostic);
    expect(failures[0]?.message).not.toContain('raw ');
  });

  it('shares a bounded abortable budget across terminal policy evaluation', async () => {
    let policySignal: AbortSignal | undefined;
    const startedAt = Date.now();
    const failures: Error[] = [];
    const configuredTool = tool({
      evaluationTimeoutMs: 20,
      isSuccess: async (_output, context) => {
        policySignal = context.abortSignal;
        await new Promise<void>(resolve =>
          context.abortSignal.addEventListener('abort', () => resolve(), { once: true }),
        );
        return true;
      },
    });

    await expect(
      resolveTerminalToolResult({
        calls: [{ toolName: 'candidate', toolCallId: 'call-timeout', result: { ok: true } }],
        tools: { candidate: configuredTool } as any,
        onPolicyFailure: error => failures.push(error),
      }),
    ).resolves.toBeUndefined();

    expect(policySignal?.aborted).toBe(true);
    expect(Date.now() - startedAt).toBeLessThan(500);
    // Timeout is an expected refusal rather than a policy exception.
    expect(failures).toHaveLength(0);
  });

  it('bounds a large batch by one timeout window and aborts every sibling evaluator', async () => {
    const callCount = 50;
    const policySignals: AbortSignal[] = [];
    const configuredTool = tool({
      evaluationTimeoutMs: 25,
      isSuccess: async (_output, context) => {
        policySignals.push(context.abortSignal);
        await new Promise<void>(resolve =>
          context.abortSignal.addEventListener('abort', () => resolve(), { once: true }),
        );
        return true;
      },
    });
    const startedAt = Date.now();

    await expect(
      resolveTerminalToolResult({
        calls: Array.from({ length: callCount }, (_, index) => ({
          toolName: 'candidate',
          toolCallId: `call-timeout-${index}`,
          result: { ok: true },
        })),
        tools: { candidate: configuredTool } as any,
      }),
    ).resolves.toBeUndefined();

    expect(policySignals).toHaveLength(callCount);
    expect(policySignals.every(signal => signal.aborted)).toBe(true);
    // The previous sequential implementation took roughly 1.25s here.
    expect(Date.now() - startedAt).toBeLessThan(500);
  });
});
