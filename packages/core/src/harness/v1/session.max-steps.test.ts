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

import { describe, expect, it } from 'vitest';

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
  async function capturedNudge() {
    const { harness, agent } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    await session.message({ content: 'do something' });
    const call = agent.streamCalls.at(-1);
    const nudge = call?.options?.onIterationComplete as
      | ((context: {
          text: string;
          toolResults: Array<{ id: string; name: string; result: unknown }>;
          isFinal: boolean;
          finishReason: string;
        }) => { continue: boolean } | undefined)
      | undefined;
    expect(typeof nudge).toBe('function');
    return nudge!;
  }

  const toolResult = { id: 'tc1', name: 'create_task', result: { ok: true } };

  it('forces one continuation when tools completed and the final iteration has no text', async () => {
    const nudge = await capturedNudge();
    expect(
      nudge({ text: 'I will create it now.', toolResults: [toolResult], isFinal: false, finishReason: 'tool-calls' }),
    ).toBeUndefined();
    expect(nudge({ text: '', toolResults: [], isFinal: true, finishReason: 'stop' })).toMatchObject({
      continue: true,
      feedback: expect.stringContaining('no reply'),
    });
    // Never loops: a second silent final is allowed to finish.
    expect(nudge({ text: '', toolResults: [], isFinal: true, finishReason: 'stop' })).toBeUndefined();
  });

  it('stays silent when the final iteration produced text or the turn ran no tools', async () => {
    const spoke = await capturedNudge();
    expect(spoke({ text: '', toolResults: [toolResult], isFinal: false, finishReason: 'tool-calls' })).toBeUndefined();
    expect(spoke({ text: 'Done.', toolResults: [], isFinal: true, finishReason: 'stop' })).toBeUndefined();

    const toolless = await capturedNudge();
    expect(toolless({ text: '', toolResults: [], isFinal: true, finishReason: 'stop' })).toBeUndefined();
  });

  it('keeps the one-shot synthesis available after more than ten ordinary tool generations', async () => {
    const nudge = await capturedNudge();
    for (let index = 0; index < 12; index += 1) {
      expect(
        nudge({
          text: `Working on tool step ${index + 1}.`,
          toolResults: [{ ...toolResult, id: `tc-${index}` }],
          isFinal: false,
          finishReason: 'tool-calls',
        }),
      ).toBeUndefined();
    }
    expect(nudge({ text: '', toolResults: [], isFinal: true, finishReason: 'stop' })).toMatchObject({
      continue: true,
    });
  });

  it('leaves failed and safety-blocked finishes alone and is a fresh closure per turn', async () => {
    const nudge = await capturedNudge();
    expect(nudge({ text: '', toolResults: [toolResult], isFinal: true, finishReason: 'error' })).toBeUndefined();
    expect(nudge({ text: '', toolResults: [toolResult], isFinal: true, finishReason: 'abort' })).toBeUndefined();
    expect(nudge({ text: '', toolResults: [toolResult], isFinal: true, finishReason: 'tripwire' })).toBeUndefined();
    expect(
      nudge({ text: '', toolResults: [toolResult], isFinal: true, finishReason: 'content-filter' }),
    ).toBeUndefined();

    const { harness, agent } = setupHarness();
    const session = await harness.session({ resourceId: 'u1', threadId: { fresh: true } });
    await session.message({ content: 'one' });
    await session.message({ content: 'two' });
    const runCalls = agent.streamCalls.filter(call => call.type === 'stream');
    const first = runCalls.at(0)?.options?.onIterationComplete;
    const second = runCalls.at(1)?.options?.onIterationComplete;
    expect(typeof first).toBe('function');
    expect(typeof second).toBe('function');
    expect(first).not.toBe(second);
  });
});
