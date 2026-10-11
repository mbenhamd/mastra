import { it, expect } from 'vitest';
import { z } from 'zod/v4';
import { ToolResultSupersedeProcessor } from '../../../../processors';
import { createTool } from '../../../../tools';
import { runLoopScenario, useLoopScenarioAimock, describeForAllEngines } from '../aimock-scenario';

/**
 * PF-5349: a re-read of the same resource replaces the earlier read in model
 * context. The earlier tool call stays (with a fixed pointer as its result), the
 * newest read is sent verbatim, and the stored history is untouched.
 */
describeForAllEngines('AIMock loop scenario: keyed supersede compaction', engine => {
  const getMock = useLoopScenarioAimock();

  it('sends a re-read file once: the earlier result becomes a pointer in the next request', async () => {
    let reads = 0;
    const readFile = createTool({
      id: 'read_file',
      description: 'Read a file',
      inputSchema: z.object({ path: z.string() }),
      outputSchema: z.object({ path: z.string(), text: z.string() }),
      execute: async ({ path }) => {
        reads += 1;
        return { path, text: `BODY-${reads}-${'x'.repeat(200)}` };
      },
    });

    const { requests, output } = await runLoopScenario({
      engine,
      llm: getMock(),
      prompt: 'Read main.tex twice, then answer.',
      tools: { read_file: readFile },
      maxSteps: 5,
      inputProcessors: [
        new ToolResultSupersedeProcessor({
          tools: { read_file: { key: ({ input }) => (input as { path?: string }).path } },
        }),
      ],
      fixtures: llm => {
        llm.on(
          { endpoint: 'chat', sequenceIndex: 0 },
          { toolCalls: [{ id: 'read-1', name: 'read_file', arguments: { path: 'main.tex' } }] },
        );
        llm.on(
          { endpoint: 'chat', sequenceIndex: 1 },
          { toolCalls: [{ id: 'read-2', name: 'read_file', arguments: { path: 'main.tex' } }] },
        );
        llm.on({ endpoint: 'chat', sequenceIndex: 2 }, { content: 'Done.' });
      },
    });

    expect(requests).toHaveLength(3);
    const second = JSON.stringify(requests[1]?.body?.messages ?? []);
    const third = JSON.stringify(requests[2]?.body?.messages ?? []);
    expect(second).toContain('BODY-1-');
    expect(third).not.toContain('BODY-1-');
    expect(third).toContain('BODY-2-');
    expect(third).toContain('[superseded] A later read_file result for the same resource replaced this one');
    // Both calls stay paired in the prompt.
    expect(third).toContain('read-1');
    expect(third).toContain('read-2');
    // The stored run keeps the full first result.
    expect(JSON.stringify((await output.getFullOutput()).steps)).toContain('BODY-1-');
  });

  it('keeps both results of one step that read the same file, then supersedes them from a later step', async () => {
    let reads = 0;
    const readFile = createTool({
      id: 'read_file',
      description: 'Read a file',
      inputSchema: z.object({ path: z.string() }),
      outputSchema: z.object({ path: z.string(), text: z.string() }),
      execute: async ({ path }) => {
        reads += 1;
        return { path, text: `BODY-${reads}-` };
      },
    });

    const { requests } = await runLoopScenario({
      engine,
      llm: getMock(),
      prompt: 'Read main.tex, then answer.',
      tools: { read_file: readFile },
      maxSteps: 5,
      inputProcessors: [
        new ToolResultSupersedeProcessor({
          tools: { read_file: { key: ({ input }) => (input as { path?: string }).path } },
        }),
      ],
      fixtures: llm => {
        llm.on(
          { endpoint: 'chat', sequenceIndex: 0 },
          {
            content: 'Reading.',
            toolCalls: [
              { id: 'read-a', name: 'read_file', arguments: { path: 'main.tex' } },
              { id: 'read-b', name: 'read_file', arguments: { path: 'main.tex' } },
            ],
          },
        );
        llm.on(
          { endpoint: 'chat', sequenceIndex: 1 },
          { toolCalls: [{ id: 'read-c', name: 'read_file', arguments: { path: 'main.tex' } }] },
        );
        llm.on({ endpoint: 'chat', sequenceIndex: 2 }, { content: 'Done.' });
      },
    });

    expect(requests).toHaveLength(3);
    const second = JSON.stringify(requests[1]?.body?.messages ?? []);
    expect(second).not.toContain('[superseded]');
    const third = JSON.stringify(requests[2]?.body?.messages ?? []);
    expect(third.match(/\[superseded\]/g)).toHaveLength(2);
    expect(third).toContain('BODY-3-');
  });
});
