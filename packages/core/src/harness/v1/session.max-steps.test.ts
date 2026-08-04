/**
 * Harness v1 — every agent-run entry point must carry the
 * HARNESS_SESSION_MAX_STEPS ceiling.
 *
 * Without an explicit `maxSteps`, `agent.stream()` / `agent.resumeStream()`
 * fall back to the agent's small default (5) and a tool-heavy turn terminates
 * CLEANLY at the step boundary with no synthesis text. Live-diagnosed as a
 * silent empty chat turn: five tool-only steps, `finish-step`×5, `finish`,
 * zero text deltas — models that batch small parallel tool calls burn the
 * budget fastest. The agent-controller lane documents the identical trap and
 * pins CONTROLLER_MAX_STEPS; these tests pin the session lane.
 */

import { describe, expect, it, vi } from 'vitest';

import { mergeAgentExecutionOptions } from '../../agent/merge-execution-options';

import { setupHarness } from './__test-utils__';

describe('Session step ceiling', () => {
  it('message() carries an escape-proof maxSteps into the agent run options', async () => {
    const { harness, agent } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    await session.message({ content: 'hi' });

    const call = agent.streamCalls.at(-1);
    expect(call).toBeDefined();
    // The exact value is an internal constant; the invariant is that it exists
    // and dwarfs the 5-step agent default a missing option would fall back to.
    expect(call?.options?.maxSteps).toBeGreaterThanOrEqual(1000);
  });

  it('every recorded run of a multi-turn session carries the ceiling', async () => {
    const { harness, agent } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });

    await session.message({ content: 'first' });
    await session.message({ content: 'second' });

    const runCalls = agent.streamCalls.filter(call => call.type === 'stream');
    expect(runCalls.length).toBeGreaterThanOrEqual(2);
    for (const call of runCalls) {
      expect(call.options?.maxSteps).toBeGreaterThanOrEqual(1000);
    }
  });
});

describe('Session empty-final-synthesis nudge', () => {
  async function capturedSynthesisOptions(defaultOptions: Record<string, unknown> = {}) {
    const { harness, agent } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    await session.message({ content: 'do something' });
    const call = agent.streamCalls.at(-1);
    const merged = mergeAgentExecutionOptions(defaultOptions, call?.options ?? {});
    const nudge = merged.onIterationComplete as
      | ((context: {
          text: string;
          toolResults: Array<{ id: string; name: string; result: unknown }>;
          isFinal: boolean;
          finishReason: string;
        }) => Promise<{ continue?: boolean; feedback?: string } | undefined>)
      | undefined;
    const prepareStep = merged.prepareStep as
      ((args?: unknown) => Promise<Record<string, unknown> | undefined>) | undefined;
    expect(typeof nudge).toBe('function');
    expect(typeof prepareStep).toBe('function');
    return { nudge: nudge!, prepareStep: prepareStep!, harness };
  }

  const toolResult = { id: 'tc1', name: 'create_task', result: { ok: true } };

  it('forces one response-only continuation when tools completed and the final iteration has no text', async () => {
    const { nudge, prepareStep } = await capturedSynthesisOptions();
    await expect(prepareStep()).resolves.toBeUndefined();
    expect(
      await nudge({
        text: 'I will create it now.',
        toolResults: [toolResult],
        isFinal: false,
        finishReason: 'tool-calls',
      }),
    ).toBeUndefined();
    expect(await nudge({ text: '', toolResults: [], isFinal: true, finishReason: 'stop' })).toMatchObject({
      continue: true,
      feedback: expect.stringContaining('no reply'),
    });
    await expect(prepareStep()).resolves.toEqual({ activeTools: [], toolChoice: 'none' });
    // Never loops: the single response-only iteration is forced to stop.
    expect(await nudge({ text: '', toolResults: [], isFinal: true, finishReason: 'stop' })).toMatchObject({
      continue: false,
    });
  });

  it('stays silent when the final iteration produced text or the turn ran no tools', async () => {
    const { nudge: spoke } = await capturedSynthesisOptions();
    expect(
      await spoke({ text: '', toolResults: [toolResult], isFinal: false, finishReason: 'tool-calls' }),
    ).toBeUndefined();
    expect(await spoke({ text: 'Done.', toolResults: [], isFinal: true, finishReason: 'stop' })).toBeUndefined();

    const { nudge: toolless } = await capturedSynthesisOptions();
    expect(await toolless({ text: '', toolResults: [], isFinal: true, finishReason: 'stop' })).toBeUndefined();
  });

  it('asks for a result-grounded report when a tool was denied instead of implying it ran', async () => {
    const { nudge } = await capturedSynthesisOptions();
    const result = await nudge({
      text: '',
      toolResults: [{ ...toolResult, result: { approved: false, denied: true } }],
      isFinal: true,
      finishReason: 'stop',
    });
    expect(result?.feedback).toContain('succeeded, failed, or was denied');
    expect(result?.feedback).toContain('Do not claim an action ran');
    expect(result?.feedback).not.toContain('completed tool actions');
  });

  it('keeps the one-shot synthesis available after more than ten ordinary tool generations', async () => {
    const { nudge } = await capturedSynthesisOptions();
    for (let index = 0; index < 12; index += 1) {
      expect(
        await nudge({
          text: `Working on tool step ${index + 1}.`,
          toolResults: [{ ...toolResult, id: `tc-${index}` }],
          isFinal: false,
          finishReason: 'tool-calls',
        }),
      ).toBeUndefined();
    }
    expect(await nudge({ text: '', toolResults: [], isFinal: true, finishReason: 'stop' })).toMatchObject({
      continue: true,
    });
  });

  it('leaves failed, truncated, and safety-blocked finishes alone and is a fresh closure per turn', async () => {
    const { nudge, prepareStep } = await capturedSynthesisOptions();
    expect(await nudge({ text: '', toolResults: [toolResult], isFinal: true, finishReason: 'error' })).toBeUndefined();
    expect(await nudge({ text: '', toolResults: [toolResult], isFinal: true, finishReason: 'abort' })).toBeUndefined();
    expect(
      await nudge({ text: '', toolResults: [toolResult], isFinal: true, finishReason: 'tripwire' }),
    ).toBeUndefined();
    expect(
      await nudge({ text: '', toolResults: [toolResult], isFinal: true, finishReason: 'content-filter' }),
    ).toBeUndefined();
    expect(await nudge({ text: '', toolResults: [toolResult], isFinal: true, finishReason: 'length' })).toBeUndefined();
    await expect(prepareStep()).resolves.toBeUndefined();

    const { harness, agent } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    await session.message({ content: 'one' });
    await session.message({ content: 'two' });
    const runCalls = agent.streamCalls.filter(call => call.type === 'stream');
    const firstOptions = mergeAgentExecutionOptions({}, runCalls.at(0)?.options ?? {});
    const secondOptions = mergeAgentExecutionOptions({}, runCalls.at(1)?.options ?? {});
    const first = firstOptions.onIterationComplete;
    const second = secondOptions.onIterationComplete;
    const firstPrepare = firstOptions.prepareStep;
    const secondPrepare = secondOptions.prepareStep;
    expect(typeof first).toBe('function');
    expect(typeof second).toBe('function');
    expect(typeof firstPrepare).toBe('function');
    expect(typeof secondPrepare).toBe('function');
    expect(first).not.toBe(second);
    expect(firstPrepare).not.toBe(secondPrepare);
  });

  it('preserves configured hooks while forcing the recovery step tool-free', async () => {
    const configuredIteration = vi.fn(async () => ({ feedback: 'Configured feedback.' }));
    const configuredPrepare = vi.fn(async () => ({ activeTools: ['safe_tool'], workspace: 'configured-workspace' }));
    const { nudge, prepareStep } = await capturedSynthesisOptions({
      onIterationComplete: configuredIteration,
      prepareStep: configuredPrepare,
    });

    await expect(prepareStep()).resolves.toEqual({
      activeTools: ['safe_tool'],
      workspace: 'configured-workspace',
    });
    await nudge({ text: 'Working.', toolResults: [toolResult], isFinal: false, finishReason: 'tool-calls' });
    expect(await nudge({ text: '', toolResults: [], isFinal: true, finishReason: 'stop' })).toMatchObject({
      continue: true,
      feedback: expect.stringContaining('Configured feedback.'),
    });
    await expect(prepareStep()).resolves.toEqual({
      activeTools: [],
      toolChoice: 'none',
      workspace: 'configured-workspace',
    });
    await expect(nudge({ text: '', toolResults: [], isFinal: false, finishReason: 'tool-calls' })).resolves.toEqual({
      continue: false,
    });
    expect(configuredIteration).toHaveBeenCalledTimes(3);
    expect(configuredPrepare).toHaveBeenCalledTimes(2);

    const configuredStop = vi.fn(async () => ({ continue: false, feedback: 'Stop here.' }));
    const stopped = await capturedSynthesisOptions({ onIterationComplete: configuredStop });
    await expect(
      stopped.nudge({ text: '', toolResults: [toolResult], isFinal: true, finishReason: 'stop' }),
    ).resolves.toEqual({ continue: false, feedback: 'Stop here.' });
    await expect(stopped.prepareStep()).resolves.toBeUndefined();
  });

  it('remembers completed tools when a configured iteration hook rejects', async () => {
    const configuredIteration = vi
      .fn()
      .mockRejectedValueOnce(new Error('configured hook failed'))
      .mockResolvedValue(undefined);
    const { nudge, prepareStep } = await capturedSynthesisOptions({
      onIterationComplete: configuredIteration,
    });

    await expect(
      nudge({ text: 'Working.', toolResults: [toolResult], isFinal: false, finishReason: 'tool-calls' }),
    ).rejects.toThrow('configured hook failed');
    await expect(nudge({ text: '', toolResults: [], isFinal: true, finishReason: 'stop' })).resolves.toMatchObject({
      continue: true,
    });
    await expect(prepareStep()).resolves.toEqual({ activeTools: [], toolChoice: 'none' });
    expect(configuredIteration).toHaveBeenCalledTimes(2);
  });

  it('forces recovery and logs when the configured hook rejects through the recovery turn', async () => {
    const hookError = new Error('configured hook failed');
    const configuredIteration = vi.fn(async () => {
      throw hookError;
    });
    const { nudge, prepareStep, harness } = await capturedSynthesisOptions({
      onIterationComplete: configuredIteration,
    });
    const loggerError = vi.spyOn(harness.mastra.getLogger(), 'error');

    await expect(
      nudge({ text: 'Working.', toolResults: [toolResult], isFinal: false, finishReason: 'tool-calls' }),
    ).rejects.toThrow('configured hook failed');
    await expect(nudge({ text: '', toolResults: [], isFinal: true, finishReason: 'stop' })).resolves.toMatchObject({
      continue: true,
      feedback: expect.stringContaining('no reply'),
    });
    await expect(prepareStep()).resolves.toEqual({ activeTools: [], toolChoice: 'none' });
    await expect(nudge({ text: 'Recovered.', toolResults: [], isFinal: true, finishReason: 'stop' })).resolves.toEqual({
      continue: false,
    });
    expect(configuredIteration).toHaveBeenCalledTimes(3);
    expect(loggerError).toHaveBeenCalledTimes(2);
    expect(loggerError).toHaveBeenNthCalledWith(1, 'Error in onIterationComplete hook:', hookError);
    expect(loggerError).toHaveBeenNthCalledWith(2, 'Error in onIterationComplete hook:', hookError);
    loggerError.mockRestore();
  });
});
