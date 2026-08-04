import { it, expect } from 'vitest';
import { z } from 'zod/v4';
import { createTool } from '../../../../tools';
import { runLoopScenario, useLoopScenarioAimock, describeForAllEngines } from '../aimock-scenario';
import type { IterationCompleteContext } from '../../../../agent';

/**
 * Regression class: onIterationComplete hook — supervisor iteration tracking.
 *
 * The `onIterationComplete` hook fires after each iteration of the agent loop,
 * providing visibility into what happened (text, tool calls) and the ability
 * to control whether to continue. This scenario proves:
 *
 * 1. The hook receives the correct context (iteration number, tool calls, tool results).
 * 2. The hook can stop iteration early by returning `continue: false`.
 * 3. The hook can inject feedback that the model sees on the next iteration.
 */
describeForAllEngines('AIMock loop scenario: onIterationComplete hook', engine => {
  const getMock = useLoopScenarioAimock();

  it('onIterationComplete receives iteration context with tool calls and results', async () => {
    const iterations: IterationCompleteContext[] = [];

    const addTool = createTool({
      id: 'add',
      description: 'Add two numbers',
      inputSchema: z.object({ a: z.number(), b: z.number() }),
      outputSchema: z.object({ sum: z.number() }),
      execute: async ({ a, b }) => ({ sum: a + b }),
    });

    const { chunks } = await runLoopScenario({
      engine,
      llm: getMock(),
      prompt: 'Add 2 and 3',
      tools: { add: addTool },
      stopWhen: ({ steps }: { steps: number }) => steps >= 2,
      collectChunks: true,
      onIterationComplete: async (context: IterationCompleteContext) => {
        iterations.push(context);
      },
      fixtures: llm => {
        // First iteration: call the add tool
        llm.on(
          { endpoint: 'chat', sequenceIndex: 0 },
          {
            toolCalls: [{ id: 'call_add_1', name: 'add', arguments: { a: 2, b: 3 } }],
          },
        );
        // Second iteration: summarize the result (match on toolCallId)
        llm.on(
          { endpoint: 'chat', toolCallId: 'call_add_1', hasToolResult: true },
          { content: 'The sum of 2 and 3 is 5.' },
        );
      },
    });

    // Should have 2 iterations
    expect(iterations).toHaveLength(2);

    // First iteration: tool call
    expect(iterations[0].iteration).toBe(1);
    expect(iterations[0].toolCalls).toHaveLength(1);
    expect(iterations[0].toolCalls[0].name).toBe('add');
    expect(iterations[0].toolCalls[0].args).toEqual({ a: 2, b: 3 });
    expect(iterations[0].toolResults).toHaveLength(1);
    expect(iterations[0].toolResults[0]).toMatchObject({
      id: 'call_add_1',
      name: 'add',
      result: { sum: 5 },
    });
    expect(iterations[0].isFinal).toBe(false);

    // Second iteration: final response
    expect(iterations[1].iteration).toBe(2);
    expect(iterations[1].toolResults).toEqual([]);
    expect(iterations[1].isFinal).toBe(true);

    // Verify the final output contains the expected text
    const textDeltas = chunks?.filter(c => c.type === 'text-delta') || [];
    const text = textDeltas.map((c: any) => c.payload?.text || '').join('');
    expect(text).toContain('sum of 2 and 3 is 5');
  });

  it('onIterationComplete can stop iteration early with continue: false', async () => {
    const iterations: IterationCompleteContext[] = [];

    const searchTool = createTool({
      id: 'search',
      description: 'Search for information',
      inputSchema: z.object({ query: z.string() }),
      outputSchema: z.object({ result: z.string() }),
      execute: async ({ query }) => ({ result: `Found: ${query}` }),
    });

    const { chunks } = await runLoopScenario({
      engine,
      llm: getMock(),
      prompt: 'Search for test',
      tools: { search: searchTool },
      stopWhen: ({ steps }: { steps: number }) => steps >= 5, // Would normally run 5 steps
      collectChunks: true,
      onIterationComplete: async (context: IterationCompleteContext) => {
        iterations.push(context);
        // Stop after 2 iterations even though stopWhen allows 5
        if (context.iteration >= 2) {
          return { continue: false };
        }
      },
      fixtures: llm => {
        // First iteration: call search
        llm.on(
          { endpoint: 'chat', sequenceIndex: 0 },
          {
            toolCalls: [{ id: 'call_search_0', name: 'search', arguments: { query: 'query 0' } }],
          },
        );
        // Second iteration: return text (should stop here)
        llm.on({ endpoint: 'chat', toolCallId: 'call_search_0', hasToolResult: true }, { content: 'Response 1' });
        // Third+ iterations: should not be reached
        llm.on({ endpoint: 'chat', sequenceIndex: 2, hasToolResult: true }, { content: 'Response 2' });
      },
    });

    // Should have stopped at 2 iterations, not 5
    expect(iterations).toHaveLength(2);
    expect(iterations[1].iteration).toBe(2);

    // Final output should be from iteration 2
    const textDeltas = chunks?.filter(c => c.type === 'text-delta') || [];
    const text = textDeltas.map((c: any) => c.payload?.text || '').join('');
    expect(text).toBe('Response 1');
  });

  it('onIterationComplete can inject feedback (basic verification)', async () => {
    const iterations: IterationCompleteContext[] = [];
    let feedbackInjected = false;

    const searchTool = createTool({
      id: 'search',
      description: 'Search for information',
      inputSchema: z.object({ query: z.string() }),
      outputSchema: z.object({ result: z.string() }),
      execute: async ({ query }) => ({ result: `Found: ${query}` }),
    });

    await runLoopScenario({
      engine,
      llm: getMock(),
      prompt: 'Search for test',
      tools: { search: searchTool },
      stopWhen: ({ steps }: { steps: number }) => steps >= 2,
      collectChunks: true,
      onIterationComplete: async (context: IterationCompleteContext) => {
        iterations.push(context);
        // On first iteration with tool call, inject feedback
        if (context.iteration === 1 && context.toolCalls.length > 0) {
          feedbackInjected = true;
          return {
            feedback: 'Additional context provided.',
            continue: true,
          };
        }
      },
      fixtures: llm => {
        // First iteration: call search
        llm.on(
          { endpoint: 'chat', sequenceIndex: 0 },
          {
            toolCalls: [{ id: 'call_search_1', name: 'search', arguments: { query: 'test query' } }],
          },
        );
        // Second iteration: return final text
        llm.on(
          { endpoint: 'chat', toolCallId: 'call_search_1', hasToolResult: true },
          { content: 'Search completed with additional context.' },
        );
      },
    });

    // Verify the hook was called and feedback was injected
    expect(feedbackInjected).toBe(true);
    expect(iterations).toHaveLength(2);
    expect(iterations[0].toolCalls[0].name).toBe('search');
    expect(iterations[1].isFinal).toBe(true);
  });
});

describeForAllEngines(
  'AIMock loop scenario: forced continuation from a FINAL iteration (silent-turn nudge)',
  engine => {
    const getMock = useLoopScenarioAimock();

    it('re-enters generation when the hook returns continue:true on the final iteration', async () => {
      // Live-diagnosed shape: the model runs a tool, then STOPS with empty
      // text — the user gets a mutation and silence. The harness nudge
      // returns {continue:true} exactly once from the final iteration; the
      // loop must run one more model call whose text becomes the answer.
      const iterations: IterationCompleteContext[] = [];
      let nudged = false;

      const createThing = createTool({
        id: 'create_thing',
        description: 'Create a thing',
        inputSchema: z.object({ name: z.string() }),
        outputSchema: z.object({ ok: z.boolean() }),
        execute: async () => ({ ok: true }),
      });

      const { chunks } = await runLoopScenario({
        engine,
        llm: getMock(),
        prompt: 'Create a thing named probe',
        tools: { create_thing: createThing },
        stopWhen: ({ steps }: { steps: number }) => steps >= 5,
        collectChunks: true,
        onIterationComplete: async (context: IterationCompleteContext) => {
          iterations.push(context);
          const segmentHasToolResults = iterations.some(entry => entry.toolResults.length > 0);
          if (context.isFinal && !nudged && segmentHasToolResults && context.text.trim() === '') {
            nudged = true;
            return { continue: true };
          }
          return undefined;
        },
        fixtures: llm => {
          llm.on(
            { endpoint: 'chat', sequenceIndex: 0 },
            {
              toolCalls: [{ id: 'call_create_1', name: 'create_thing', arguments: { name: 'probe' } }],
            },
          );
          // Second iteration: the model goes silent after the tool result.
          // Matched by sequence index — a toolCallId matcher would also
          // capture the post-nudge third call (the tool result stays in
          // history) and keep answering with silence.
          llm.on({ endpoint: 'chat', sequenceIndex: 1 }, { content: '' });
          // Third iteration exists ONLY if the forced continuation re-enters.
          llm.on({ endpoint: 'chat', sequenceIndex: 2 }, { content: 'Created the thing "probe".' });
        },
      });

      expect(nudged).toBe(true);
      expect(iterations).toHaveLength(3);
      const textDeltas = chunks?.filter(c => c.type === 'text-delta') || [];
      const text = textDeltas.map((c: any) => c.payload?.text || '').join('');
      expect(text).toContain('Created the thing "probe".');
    });

    it('continues once after earlier tool-step text and does not replay the completed tool', async () => {
      const iterations: IterationCompleteContext[] = [];
      let nudged = false;
      let toolExecutions = 0;
      let segmentHasToolResults = false;

      const updateThing = createTool({
        id: 'update_thing',
        description: 'Update a thing',
        inputSchema: z.object({ name: z.string() }),
        outputSchema: z.object({ ok: z.boolean() }),
        execute: async () => {
          toolExecutions += 1;
          return { ok: true };
        },
      });

      const { chunks, requests } = await runLoopScenario({
        engine,
        llm: getMock(),
        prompt: 'Update the probe and report the outcome.',
        tools: { update_thing: updateThing },
        maxSteps: 20,
        collectChunks: true,
        onIterationComplete: async (context: IterationCompleteContext) => {
          iterations.push(context);
          if (context.toolResults.length > 0) segmentHasToolResults = true;
          if (context.isFinal && !nudged && segmentHasToolResults && context.text.trim() === '') {
            nudged = true;
            return {
              continue: true,
              feedback: 'Reply with the outcome only. Do not call tools.',
            };
          }
          return undefined;
        },
        fixtures: llm => {
          llm.on(
            { endpoint: 'chat', sequenceIndex: 0 },
            {
              content: 'I will update the probe now.',
              toolCalls: [{ id: 'call_update_1', name: 'update_thing', arguments: { name: 'probe' } }],
            },
          );
          llm.on({ endpoint: 'chat', sequenceIndex: 1 }, { content: '' });
          llm.on({ endpoint: 'chat', sequenceIndex: 2 }, { content: 'Updated the probe.' });
        },
      });

      expect(toolExecutions).toBe(1);
      expect(requests).toHaveLength(3);
      expect(JSON.stringify(requests[2]?.body?.messages ?? [])).toContain(
        'Reply with the outcome only. Do not call tools.',
      );
      expect(nudged).toBe(true);
      expect(iterations[0]).toMatchObject({
        text: 'I will update the probe now.',
        isFinal: false,
      });
      expect(iterations[1]).toMatchObject({
        text: '',
        isFinal: true,
      });
      const text = (chunks ?? [])
        .filter(chunk => chunk.type === 'text-delta')
        .map((chunk: any) => chunk.payload?.text ?? '')
        .join('');
      expect(text).toContain('Updated the probe.');
    });
  },
);
