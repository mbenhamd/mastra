import { it, expect } from 'vitest';
import { z } from 'zod/v4';
import type { OutputProcessor, Processor } from '../../../../processors';
import { createTool } from '../../../../tools';
import { runLoopScenario, useLoopScenarioAimock, describeForAllEngines } from '../aimock-scenario';
import type { IterationCompleteContext } from '../../../../agent';
import {
  AGENT_RESPONSE_RECOVERY_CONTINUATION,
  AGENT_RESPONSE_RECOVERY_STEP,
} from '../../../../agent/merge-execution-options';

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

  it('reports only the current step rather than cumulative narration', async () => {
    const iterations: IterationCompleteContext[] = [];
    const inspectTool = createTool({
      id: 'inspect_current_step',
      description: 'Inspect a value before reporting it.',
      inputSchema: z.object({ value: z.string() }),
      outputSchema: z.object({ value: z.string() }),
      execute: async ({ value }) => ({ value }),
    });

    await runLoopScenario({
      engine,
      llm: getMock(),
      prompt: 'Inspect the value and report it.',
      tools: { inspect_current_step: inspectTool },
      onIterationComplete: (context: IterationCompleteContext) => {
        iterations.push(context);
      },
      fixtures: llm => {
        llm.on(
          { endpoint: 'chat', sequenceIndex: 0 },
          {
            content: 'Inspecting now.',
            toolCalls: [
              {
                id: 'call_inspect_current_step',
                name: 'inspect_current_step',
                arguments: { value: 'ready' },
              },
            ],
          },
        );
        llm.on({ endpoint: 'chat', sequenceIndex: 1 }, { content: 'Inspection complete.' });
      },
    });

    expect(iterations.map(iteration => iteration.text)).toEqual(['Inspecting now.', 'Inspection complete.']);
  });

  it('reports text rewritten by a successful output-step processor', async () => {
    const rawText = 'RAW_PRIVATE_TEXT';
    const visibleText = 'Caller-visible text.';
    const iterations: IterationCompleteContext[] = [];
    const rewriteProcessor: OutputProcessor = {
      id: 'rewrite-current-step-text',
      processOutputStep({ messages }) {
        return messages.map(message => ({
          ...message,
          ...(message.content.parts.some(part => part.type === 'text' && part.text === rawText)
            ? { id: `${message.id}-processed` }
            : {}),
          content: {
            ...message.content,
            parts: message.content.parts.map(part =>
              part.type === 'text' && part.text === rawText ? { ...part, text: visibleText } : part,
            ),
          },
        }));
      },
    };

    await runLoopScenario({
      engine,
      llm: getMock(),
      prompt: 'Return a safe summary.',
      outputProcessors: [rewriteProcessor],
      onIterationComplete: (context: IterationCompleteContext) => {
        iterations.push(context);
      },
      fixtures: llm => {
        llm.on({ endpoint: 'chat' }, { content: rawText });
      },
    });

    expect(iterations).toHaveLength(1);
    expect(iterations[0]?.text).toBe(visibleText);
  });

  it('does not fall back to raw text when an output-step processor removes the response', async () => {
    const privateText = 'PRIVATE_REMOVED_TEXT';
    const iterations: IterationCompleteContext[] = [];
    const removeResponseProcessor: OutputProcessor = {
      id: 'remove-current-step-response',
      processOutputStep({ messages }) {
        return messages.filter(
          message => !message.content.parts.some(part => part.type === 'text' && part.text.includes(privateText)),
        );
      },
    };

    await runLoopScenario({
      engine,
      llm: getMock(),
      prompt: 'Return the private marker.',
      outputProcessors: [removeResponseProcessor],
      onIterationComplete: (context: IterationCompleteContext) => {
        iterations.push(context);
      },
      fixtures: llm => {
        llm.on({ endpoint: 'chat' }, { content: privateText });
      },
    });

    expect(iterations).toHaveLength(1);
    expect(iterations[0]?.text).toBe('');
  });

  it('attributes inserted prefix text to the processed step that introduced it', async () => {
    const firstStepText = 'First step narration.';
    const insertedPrefix = 'Inserted prefix before prior narration.';
    const iterations: IterationCompleteContext[] = [];
    const inspectTool = createTool({
      id: 'insert_prefix_attribution_probe',
      description: 'Return a value before the final silent step.',
      inputSchema: z.object({ value: z.string() }),
      outputSchema: z.object({ value: z.string() }),
      execute: async ({ value }) => ({ value }),
    });
    const insertPrefixBeforePriorNarration: OutputProcessor = {
      id: 'insert-prefix-before-prior-narration',
      processOutputStep({ messages, stepNumber }) {
        if (stepNumber === 0) return messages;
        return messages.map(message => {
          const containsPriorNarration = message.content.parts.some(
            part => part.type === 'text' && part.text === firstStepText,
          );
          return containsPriorNarration
            ? {
                ...message,
                content: {
                  ...message.content,
                  parts: [{ type: 'text' as const, text: insertedPrefix }, ...message.content.parts],
                },
              }
            : message;
        });
      },
    };

    await runLoopScenario({
      engine,
      llm: getMock(),
      prompt: 'Inspect the value, then finish silently.',
      tools: { insert_prefix_attribution_probe: inspectTool },
      outputProcessors: [insertPrefixBeforePriorNarration],
      onIterationComplete: (context: IterationCompleteContext) => {
        iterations.push(context);
      },
      fixtures: llm => {
        llm.on(
          { endpoint: 'chat', sequenceIndex: 0 },
          {
            content: firstStepText,
            toolCalls: [
              {
                id: 'call_insert_prefix_attribution_probe',
                name: 'insert_prefix_attribution_probe',
                arguments: { value: 'ready' },
              },
            ],
          },
        );
        llm.on({ endpoint: 'chat', sequenceIndex: 1 }, { content: '' });
      },
    });

    expect(iterations.map(iteration => iteration.text)).toEqual([firstStepText, '']);
    expect(iterations[1]?.text).not.toContain(insertedPrefix);
    expect(iterations[1]?.text).not.toContain(firstStepText);
  });

  it('keeps the current step visible when a processor removes earlier narration', async () => {
    const earlierText = 'Private setup narration.';
    const finalText = 'Caller-visible final answer.';
    const iterations: IterationCompleteContext[] = [];
    const inspectTool = createTool({
      id: 'remove_prior_narration_probe',
      description: 'Return a value before the final answer.',
      inputSchema: z.object({ value: z.string() }),
      outputSchema: z.object({ value: z.string() }),
      execute: async ({ value }) => ({ value }),
    });
    const removeEarlierNarration: OutputProcessor = {
      id: 'remove-earlier-narration',
      processOutputStep({ messages, stepNumber }) {
        if (stepNumber === 0) return messages;
        return messages.map(message => ({
          ...message,
          content: {
            ...message.content,
            parts: message.content.parts.filter(part => !(part.type === 'text' && part.text === earlierText)),
          },
        }));
      },
    };

    await runLoopScenario({
      engine,
      llm: getMock(),
      prompt: 'Inspect the value, remove setup narration, and report the final answer.',
      tools: { remove_prior_narration_probe: inspectTool },
      outputProcessors: [removeEarlierNarration],
      onIterationComplete: (context: IterationCompleteContext) => {
        iterations.push(context);
      },
      fixtures: llm => {
        llm.on(
          { endpoint: 'chat', sequenceIndex: 0 },
          {
            content: earlierText,
            toolCalls: [
              {
                id: 'call_remove_prior_narration_probe',
                name: 'remove_prior_narration_probe',
                arguments: { value: 'ready' },
              },
            ],
          },
        );
        llm.on({ endpoint: 'chat', sequenceIndex: 1 }, { content: finalText });
      },
    });

    expect(iterations.map(iteration => iteration.text)).toEqual([earlierText, finalText]);
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
      let responseOnly = false;
      const outputProcessorToolParts: Array<{ type: string; toolCallId?: string }> = [];

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

      const { chunks, requests, output } = await runLoopScenario({
        engine,
        llm: getMock(),
        prompt: 'Update the probe and report the outcome.',
        tools: { update_thing: updateThing },
        maxSteps: 20,
        collectChunks: true,
        outputProcessors: [
          {
            id: 'observe-recovery-tool-chunks',
            async processOutputStream({ part }: { part: any }) {
              if (part?.type?.startsWith?.('tool-')) {
                outputProcessorToolParts.push({ type: part.type, toolCallId: part.payload?.toolCallId });
              }
              return part;
            },
          },
        ],
        prepareStep: () => (responseOnly ? { activeTools: [], toolChoice: 'none' } : undefined),
        onIterationComplete: async (context: IterationCompleteContext) => {
          iterations.push(context);
          if (context.toolResults.length > 0) segmentHasToolResults = true;
          if (responseOnly) return { continue: false };
          if (context.isFinal && !nudged && segmentHasToolResults && context.text.trim() === '') {
            nudged = true;
            responseOnly = true;
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
          // Deliberately violate the feedback. The response-only prepareStep
          // must keep the completed action unavailable, so this call is never
          // executed and the model gets one more chance to return plain text.
          llm.on(
            { endpoint: 'chat', sequenceIndex: 2 },
            {
              toolCalls: [{ id: 'call_update_2', name: 'update_thing', arguments: { name: 'probe' } }],
            },
          );
          llm.on({ endpoint: 'chat', sequenceIndex: 3 }, { content: 'UNEXPECTED_EXTRA_RECOVERY_STEP' });
        },
      });

      expect(toolExecutions).toBe(1);
      expect(requests).toHaveLength(3);
      expect(JSON.stringify(requests[2]?.body?.messages ?? [])).toContain(
        'Reply with the outcome only. Do not call tools.',
      );
      expect(requests[2]?.body?.tools ?? []).toHaveLength(0);
      expect(outputProcessorToolParts).toContainEqual({ type: 'tool-call', toolCallId: 'call_update_1' });
      expect(
        outputProcessorToolParts.some(part => part.toolCallId === 'call_update_1' && part.type !== 'tool-call'),
      ).toBe(true);
      expect(outputProcessorToolParts.some(part => part.toolCallId === 'call_update_2')).toBe(false);
      expect(nudged).toBe(true);
      expect(iterations[0]).toMatchObject({
        text: 'I will update the probe now.',
        isFinal: false,
      });
      expect(iterations[1]).toMatchObject({
        text: '',
        isFinal: true,
      });
      expect(
        (chunks ?? []).some(
          chunk => chunk.type.startsWith('tool-') && (chunk as any).payload?.toolCallId === 'call_update_2',
        ),
      ).toBe(false);
      const text = (chunks ?? [])
        .filter(chunk => chunk.type === 'text-delta')
        .map((chunk: any) => chunk.payload?.text ?? '')
        .join('');
      expect(text).not.toContain('UNEXPECTED_EXTRA_RECOVERY_STEP');
      expect(JSON.stringify((await output.getFullOutput()).steps)).not.toContain('call_update_2');
    });

    it('uses a hook-forced continuation as the only call beyond maxSteps', async () => {
      let responseOnly = false;
      let toolExecutions = 0;
      const cappedTool = createTool({
        id: 'capped_tool',
        description: 'Run the capped action',
        inputSchema: z.object({}),
        outputSchema: z.object({ ok: z.boolean() }),
        execute: async () => {
          toolExecutions += 1;
          return { ok: true };
        },
      });

      const { requests, chunks } = await runLoopScenario({
        engine,
        llm: getMock(),
        prompt: 'Run the capped action and report it.',
        tools: { capped_tool: cappedTool },
        maxSteps: 1,
        recoveryMaxSteps: 1,
        collectChunks: true,
        prepareStep: () =>
          responseOnly ? { activeTools: [], toolChoice: 'none', [AGENT_RESPONSE_RECOVERY_STEP]: true } : undefined,
        onIterationComplete: (context: IterationCompleteContext) => {
          if (!responseOnly && context.isFinal && context.text.trim() === '' && context.toolResults.length > 0) {
            responseOnly = true;
            return {
              continue: true,
              feedback: 'Report the completed result without tools.',
              [AGENT_RESPONSE_RECOVERY_CONTINUATION]: true,
            };
          }
          if (responseOnly) return { continue: false };
          return undefined;
        },
        fixtures: llm => {
          llm.on(
            { endpoint: 'chat', sequenceIndex: 0 },
            { toolCalls: [{ id: 'capped-call', name: 'capped_tool', arguments: {} }] },
          );
          llm.on({ endpoint: 'chat', sequenceIndex: 1 }, { content: 'The capped action succeeded.' });
          llm.on({ endpoint: 'chat', sequenceIndex: 2 }, { content: 'UNEXPECTED_THIRD_CALL' });
        },
      });

      expect(toolExecutions).toBe(1);
      expect(requests).toHaveLength(2);
      expect(requests[1]?.body?.tools ?? []).toHaveLength(0);
      const text = (chunks ?? [])
        .filter(chunk => chunk.type === 'text-delta')
        .map((chunk: any) => chunk.payload?.text ?? '')
        .join('');
      expect(text).toContain('The capped action succeeded.');
      expect(text).not.toContain('UNEXPECTED_THIRD_CALL');
    });

    it('does not continue when the recovery hook aborts the run', async () => {
      const abortController = new AbortController();
      let responseOnly = false;
      const recoveryTool = createTool({
        id: 'abort_recovery_tool',
        description: 'Run work before recovery',
        inputSchema: z.object({}),
        outputSchema: z.object({ ok: z.boolean() }),
        execute: async () => ({ ok: true }),
      });

      const { requests } = await runLoopScenario({
        engine,
        llm: getMock(),
        prompt: 'Run work, then recover.',
        tools: { abort_recovery_tool: recoveryTool },
        maxSteps: 1,
        recoveryMaxSteps: 1,
        abortSignal: abortController.signal,
        prepareStep: () => (responseOnly ? { activeTools: [], toolChoice: 'none' } : undefined),
        onIterationComplete: (context: IterationCompleteContext) => {
          if (context.isFinal && context.toolResults.length > 0) {
            responseOnly = true;
            abortController.abort();
            return {
              continue: true,
              [AGENT_RESPONSE_RECOVERY_CONTINUATION]: true,
            };
          }
          return undefined;
        },
        fixtures: llm => {
          llm.on(
            { endpoint: 'chat', sequenceIndex: 0 },
            { toolCalls: [{ id: 'abort-recovery-call', name: 'abort_recovery_tool', arguments: {} }] },
          );
          llm.on({ endpoint: 'chat', sequenceIndex: 1 }, { content: 'UNEXPECTED_ABORTED_RECOVERY' });
        },
      });

      expect(requests).toHaveLength(1);
    });

    it('does not spend recoveryMaxSteps on an ordinary continued tool iteration', async () => {
      let toolExecutions = 0;
      const ordinaryTool = createTool({
        id: 'ordinary_tool',
        description: 'Run ordinary work',
        inputSchema: z.object({}),
        outputSchema: z.object({ ok: z.boolean() }),
        execute: async () => {
          toolExecutions += 1;
          return { ok: true };
        },
      });

      const { requests } = await runLoopScenario({
        engine,
        llm: getMock(),
        prompt: 'Run ordinary work.',
        tools: { ordinary_tool: ordinaryTool },
        maxSteps: 1,
        recoveryMaxSteps: 1,
        fixtures: llm => {
          llm.on(
            { endpoint: 'chat', sequenceIndex: 0 },
            { toolCalls: [{ id: 'ordinary-call-1', name: 'ordinary_tool', arguments: {} }] },
          );
          llm.on(
            { endpoint: 'chat', sequenceIndex: 1 },
            { toolCalls: [{ id: 'ordinary-call-2', name: 'ordinary_tool', arguments: {} }] },
          );
        },
      });

      expect(requests).toHaveLength(1);
      expect(toolExecutions).toBe(1);
    });

    it('does not reopen a tripwire stop after earlier tool work', async () => {
      const iterations: IterationCompleteContext[] = [];
      let toolExecutions = 0;
      let segmentHasToolResults = false;

      const inspectThing = createTool({
        id: 'inspect_thing',
        description: 'Inspect a thing',
        inputSchema: z.object({ name: z.string() }),
        outputSchema: z.object({ ok: z.boolean() }),
        execute: async () => {
          toolExecutions += 1;
          return { ok: true };
        },
      });
      const blockFinalResponse = {
        id: 'block-final-response',
        processOutputStep: async ({
          text,
          abort,
          messages,
        }: Parameters<NonNullable<Processor['processOutputStep']>>[0]) => {
          if (text === '') abort('Blocked after tool inspection.');
          return messages;
        },
      } satisfies Processor;

      const { output, requests } = await runLoopScenario({
        engine,
        llm: getMock(),
        prompt: 'Inspect the probe and report the outcome.',
        tools: { inspect_thing: inspectThing },
        outputProcessors: [blockFinalResponse],
        maxSteps: 20,
        onIterationComplete: async (context: IterationCompleteContext) => {
          iterations.push(context);
          if (context.toolResults.length > 0) segmentHasToolResults = true;
          const safetyBlocked = context.finishReason === 'tripwire' || context.finishReason === 'content-filter';
          if (context.isFinal && segmentHasToolResults && context.text.trim() === '' && !safetyBlocked) {
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
              content: 'I will inspect the probe now.',
              toolCalls: [{ id: 'call_inspect_1', name: 'inspect_thing', arguments: { name: 'probe' } }],
            },
          );
          llm.on({ endpoint: 'chat', sequenceIndex: 1 }, { content: '' });
          llm.on({ endpoint: 'chat', sequenceIndex: 2 }, { content: 'UNEXPECTED_REOPENED_GENERATION' });
        },
      });

      expect(toolExecutions).toBe(1);
      expect(requests).toHaveLength(2);
      if (engine === 'durable') {
        // Durable terminalizes the tripwire before a second iteration callback;
        // there is therefore no callback opportunity to reopen the run.
        expect(iterations).toHaveLength(1);
      } else {
        expect(iterations).toHaveLength(2);
        expect(iterations[1]).toMatchObject({
          text: '',
          isFinal: true,
          finishReason: 'tripwire',
        });
      }
      expect(await output.finishReason).toBe(engine === 'durable' ? 'other' : 'tripwire');
      expect(await output.text).not.toContain('UNEXPECTED_REOPENED_GENERATION');
    });
  },
);
