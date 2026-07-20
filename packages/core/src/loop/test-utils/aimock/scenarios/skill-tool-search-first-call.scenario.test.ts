import * as fs from 'node:fs/promises';
import * as os from 'node:os';
import * as path from 'node:path';

import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { z } from 'zod/v4';

import { SkillSearchProcessor, ToolSearchProcessor } from '../../../../processors';
import { createTool } from '../../../../tools';
import { LocalFilesystem } from '../../../../workspace/filesystem';
import { Workspace } from '../../../../workspace/workspace';
import { runLoopScenario, useLoopScenarioAimock } from '../aimock-scenario';

const SKILL_MD = `---
name: bounded-latex-repair
description: Diagnose and repair a bounded multi-file LaTeX source change.
---

Read the root file and referenced file before editing. Make one bounded replacement,
re-read the changed file, and do not edit source for compiler-service failures.
`;

describe('AIMock loop scenario: first-call skill and tool discovery budget', () => {
  const getMock = useLoopScenarioAimock();
  let tempDir: string;
  let workspace: Workspace;

  beforeEach(async () => {
    tempDir = await fs.mkdtemp(path.join(os.tmpdir(), 'aimock-skill-tool-search-'));
    const skillDir = path.join(tempDir, 'skills', 'bounded-latex-repair');
    await fs.mkdir(skillDir, { recursive: true });
    await fs.writeFile(path.join(skillDir, 'SKILL.md'), SKILL_MD);
    workspace = new Workspace({
      filesystem: new LocalFilesystem({ basePath: tempDir }),
      skills: ['skills'],
      bm25: true,
    });
  });

  afterEach(async () => {
    await fs.rm(tempDir, { recursive: true, force: true }).catch(() => {});
  });

  function createReadTool(onRead: () => void) {
    return createTool({
      id: 'latex_read_file',
      description: 'Read one exact Compose LaTeX source file.',
      inputSchema: z.object({ path: z.string() }),
      outputSchema: z.object({ path: z.string(), content: z.string() }),
      execute: async ({ path: filePath }) => {
        onRead();
        return {
          path: filePath,
          content: filePath === 'main.tex' ? '\\input{sections/body.tex}' : 'The bounded marker is BEFORE.',
        };
      },
    });
  }

  function createProcessors(readTool: ReturnType<typeof createReadTool>, autoLoadSkills: boolean) {
    return [
      new ToolSearchProcessor({
        tools: { latex_read_file: readTool },
        storage: 'context',
        search: { autoLoad: true, topK: 1 },
      }),
      new SkillSearchProcessor({
        workspace,
        storage: 'context',
        search: { autoLoad: autoLoadSkills, topK: 1 },
      }),
    ];
  }

  function requestToolNames(request: { body?: { tools?: Array<{ function?: { name?: string } }> } } | undefined) {
    return (request?.body?.tools ?? []).flatMap(tool =>
      typeof tool.function?.name === 'string' ? [tool.function.name] : [],
    );
  }

  it('reproduces duplicate manual skill loads exhausting the default five-call cap before synthesis', async () => {
    let readCount = 0;
    const finishReasons: string[] = [];
    const readTool = createReadTool(() => {
      readCount += 1;
    });

    const { output, requests } = await runLoopScenario({
      llm: getMock(),
      prompt: 'Read main.tex and sections/body.tex, report the exact marker, and do not edit.',
      workspace,
      inputProcessors: createProcessors(readTool, false),
      onStepFinish: (step: { finishReason?: string }) => {
        if (step.finishReason) finishReasons.push(step.finishReason);
      },
      // Deliberately omit maxSteps/stopWhen: this exercises ModelLoop's
      // framework default stepCountAtLeast(5) cap.
      fixtures: llm => {
        llm.on(
          { endpoint: 'chat', hasToolResult: false },
          {
            toolCalls: [
              {
                id: 'call_load_skill_1a',
                name: 'load_skill',
                arguments: { skillName: 'bounded-latex-repair' },
              },
              {
                id: 'call_load_skill_1b',
                name: 'load_skill',
                arguments: { skillName: 'bounded-latex-repair' },
              },
            ],
          },
        );
        llm.onTurn(2, /.*/, {
          toolCalls: [
            {
              id: 'call_load_skill_2',
              name: 'load_skill',
              arguments: { skillName: 'bounded-latex-repair' },
            },
            {
              id: 'call_search_skills_2',
              name: 'search_skills',
              arguments: { query: 'bounded latex repair' },
            },
          ],
        });
        llm.onTurn(3, /.*/, {
          toolCalls: [
            {
              id: 'call_load_skill_3a',
              name: 'load_skill',
              arguments: { skillName: 'bounded-latex-repair' },
            },
            {
              id: 'call_load_skill_3b',
              name: 'load_skill',
              arguments: { skillName: 'bounded-latex-repair' },
            },
          ],
        });
        llm.onTurn(4, /.*/, {
          toolCalls: [
            {
              id: 'call_load_skill_4',
              name: 'load_skill',
              arguments: { skillName: 'bounded-latex-repair' },
            },
            {
              id: 'call_search_tools_4',
              name: 'search_tools',
              arguments: { query: 'latex read file' },
            },
          ],
        });
        llm.onTurn(5, /.*/, {
          toolCalls: [
            {
              id: 'call_read_main_5',
              name: 'latex_read_file',
              arguments: { path: 'main.tex' },
            },
            {
              id: 'call_read_body_5',
              name: 'latex_read_file',
              arguments: { path: 'sections/body.tex' },
            },
          ],
        });
        llm.onTurn(6, /.*/, { content: 'The exact marker is BEFORE.' });
      },
    });

    expect(requests).toHaveLength(5);
    expect(readCount).toBe(2);
    expect(await output.text).toBe('');
    expect(await output.finishReason).toBe('tool-calls');
    expect(finishReasons.at(-1)).toBe('tool-calls');
    expect(finishReasons).not.toEqual(expect.arrayContaining(['length', 'error', 'content-filter']));
  });

  it('completes auto-loaded skill and tool discovery, parallel reads, and synthesis within four calls', async () => {
    let readCount = 0;
    const finishReasons: string[] = [];
    const readTool = createReadTool(() => {
      readCount += 1;
    });

    const { output, requests } = await runLoopScenario({
      llm: getMock(),
      prompt: 'Read main.tex and sections/body.tex, report the exact marker, and do not edit.',
      workspace,
      inputProcessors: createProcessors(readTool, true),
      onStepFinish: (step: { finishReason?: string }) => {
        if (step.finishReason) finishReasons.push(step.finishReason);
      },
      // No expanded step budget: the optimized path must fit under the
      // framework's default five-call cap.
      fixtures: llm => {
        llm.on(
          { endpoint: 'chat', hasToolResult: false },
          {
            toolCalls: [
              {
                id: 'call_search_skills_1',
                name: 'search_skills',
                arguments: { query: 'bounded latex repair' },
              },
            ],
          },
        );
        llm.onTurn(2, /.*/, {
          toolCalls: [
            {
              id: 'call_search_tools_2',
              name: 'search_tools',
              arguments: { query: 'latex read file' },
            },
          ],
        });
        llm.onTurn(3, /.*/, {
          toolCalls: [
            {
              id: 'call_read_main_3',
              name: 'latex_read_file',
              arguments: { path: 'main.tex' },
            },
            {
              id: 'call_read_body_3',
              name: 'latex_read_file',
              arguments: { path: 'sections/body.tex' },
            },
          ],
        });
        llm.onTurn(4, /.*/, { content: 'The exact marker is BEFORE.' });
      },
    });

    expect(requests).toHaveLength(4);
    expect(readCount).toBe(2);
    expect(await output.text).toBe('The exact marker is BEFORE.');
    expect(await output.finishReason).toBe('stop');
    expect(finishReasons).toEqual(['tool-calls', 'tool-calls', 'tool-calls', 'stop']);

    const firstTurnTools = requestToolNames(requests[0]);
    expect(firstTurnTools).toContain('search_skills');
    expect(firstTurnTools).toContain('search_tools');
    expect(firstTurnTools).not.toContain('load_skill');
    expect(firstTurnTools).not.toContain('latex_read_file');

    expect(JSON.stringify(requests[1]?.body?.messages ?? [])).toContain('[Skill: bounded-latex-repair]');
    expect(requestToolNames(requests[2])).toContain('latex_read_file');
  });
});
