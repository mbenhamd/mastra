import { describe, expect, it, vi } from 'vitest';

import {
  getTransformedToolPayload,
  normalizeToolPayloadTransformPolicy,
  toolPayloadTransformAllowsTerminalToolResult,
  transformToolPayloadForTargets,
  withToolPayloadTransformMetadata,
} from './payload-transform';

describe('tool payload transform', () => {
  it('allows terminal delivery only for an explicitly opted-in display-only policy', () => {
    expect(toolPayloadTransformAllowsTerminalToolResult(undefined)).toBe(true);
    expect(toolPayloadTransformAllowsTerminalToolResult(undefined, true)).toBe(false);
    expect(
      toolPayloadTransformAllowsTerminalToolResult({
        targets: ['display'],
        terminalToolResultPolicy: 'pass-through',
        transformToolPayload: () => ({ redacted: true }),
      }),
    ).toBe(true);
    expect(
      toolPayloadTransformAllowsTerminalToolResult({
        targets: ['display', 'transcript'],
        terminalToolResultPolicy: 'pass-through',
        transformToolPayload: () => ({ redacted: true }),
      }),
    ).toBe(false);
    expect(
      toolPayloadTransformAllowsTerminalToolResult({
        targets: ['display'],
        transformToolPayload: () => ({ redacted: true }),
      }),
    ).toBe(false);
  });

  it('keeps payloads untransformed when no policy is configured', async () => {
    const transform = await transformToolPayloadForTargets(
      {
        phase: 'output-available',
        toolName: 'lookupCustomer',
        toolCallId: 'call-1',
        input: { customerId: 'cus_123', secret: 'raw-input' },
        output: { displayName: 'Acme', apiKey: 'secret-output' },
      },
      undefined,
    );

    expect(transform).toBeUndefined();
  });

  it('stores target and phase specific transforms on chunk metadata', async () => {
    const transform = await transformToolPayloadForTargets(
      {
        phase: 'input-available',
        toolName: 'lookupCustomer',
        toolCallId: 'call-1',
        input: { customerId: 'cus_123', secret: 'raw-input' },
      },
      {
        policy: {
          targets: ['display'],
          transformToolPayload: ({ target, input }) =>
            target === 'display' ? { customerId: (input as { customerId: string }).customerId } : undefined,
        },
      },
    );

    const chunk = withToolPayloadTransformMetadata({ metadata: {} }, transform);

    expect(getTransformedToolPayload(chunk.metadata, 'display', 'input-available')).toEqual({
      transformed: { customerId: 'cus_123' },
    });
    expect(getTransformedToolPayload(chunk.metadata, 'transcript', 'input-available')).toBeUndefined();
  });

  it('reads legacy projected metadata as transformed metadata', () => {
    expect(
      getTransformedToolPayload(
        {
          mastra: {
            toolPayloadProjection: {
              display: {
                'input-available': { projected: { customerId: 'cus_123' } },
              },
            },
          },
        },
        'display',
        'input-available',
      ),
    ).toEqual({
      transformed: { customerId: 'cus_123' },
    });
  });

  it('normalizes legacy projection policies to transform policies', async () => {
    const policy = normalizeToolPayloadTransformPolicy({
      targets: ['display'],
      projectToolPayload: ({ input }) => ({ customerId: (input as { customerId: string }).customerId }),
    });

    const transform = await transformToolPayloadForTargets(
      {
        phase: 'input-available',
        toolName: 'lookupCustomer',
        toolCallId: 'call-1',
        input: { customerId: 'cus_123', secret: 'raw-input' },
      },
      { policy },
    );

    expect(transform?.display?.['input-available']).toEqual({
      transformed: { customerId: 'cus_123' },
    });
  });

  it('fails closed per target when a scoped central policy returns undefined', async () => {
    const transform = await transformToolPayloadForTargets(
      {
        phase: 'input-available',
        toolName: 'lookupCustomer',
        toolCallId: 'call-1',
        input: { customerId: 'cus_123', secret: 'raw-input' },
      },
      {
        policy: {
          targets: ['display'],
          transformToolPayload: () => undefined,
        },
      },
    );

    expect(transform?.display?.['input-available']).toEqual({
      transformed: { message: 'Tool input-available payload unavailable' },
    });
    expect(transform?.transcript).toBeUndefined();
  });

  it('suppresses input deltas when transform is configured without a delta projector', async () => {
    const transform = await transformToolPayloadForTargets(
      {
        phase: 'input-delta',
        toolName: 'lookupCustomer',
        toolCallId: 'call-1',
        inputTextDelta: '{"apiKey":"secret',
      },
      {
        toolTransform: {
          display: {
            input: ({ input }) => input,
          },
        },
      },
    );

    expect(transform?.display?.['input-delta']).toEqual({ suppress: true });
  });

  it('fails closed when a configured projector throws', async () => {
    const logger = { warn: vi.fn() };
    const transform = await transformToolPayloadForTargets(
      {
        phase: 'output-available',
        toolName: 'lookupCustomer',
        toolCallId: 'call-1',
        output: { apiKey: 'secret-output' },
      },
      {
        policy: {
          transformToolPayload: () => {
            throw new Error('transform failed');
          },
        },
      },
      logger as any,
    );

    expect(transform?.display?.['output-available']).toEqual({
      transformed: { message: 'Tool output-available payload unavailable' },
      failed: true,
    });
    expect(logger.warn).toHaveBeenCalled();
  });
  it('never transforms a tool the policy declares pass-through, for any target or phase', async () => {
    const transformToolPayload = vi.fn(() => ({ redacted: true }));
    const source = {
      policy: {
        targets: ['display', 'transcript'] as const,
        passThroughToolNames: ['report_subagent_outcome'],
        transformToolPayload,
      },
    };
    for (const phase of ['input-delta', 'input-available', 'output-available'] as const) {
      const passThrough = await transformToolPayloadForTargets(
        { phase, toolName: 'report_subagent_outcome', toolCallId: 'r1', input: { summary: 's' } } as never,
        source as never,
      );
      expect(passThrough).toBeUndefined();
      const other = await transformToolPayloadForTargets(
        { phase, toolName: 'search_trials', toolCallId: 's1', input: { q: 'x' } } as never,
        source as never,
      );
      expect(other?.display?.[phase]).toEqual({ transformed: { redacted: true } });
    }
    expect(transformToolPayload).toHaveBeenCalledTimes(6);
    expect(
      transformToolPayload.mock.calls.every(
        ([context]) => (context as { toolName: string }).toolName === 'search_trials',
      ),
    ).toBe(true);
  });
});
