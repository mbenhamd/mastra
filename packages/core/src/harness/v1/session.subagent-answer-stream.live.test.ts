/**
 * PF-5620 — LIVE streamed subagent answers against a real provider.
 *
 * Opt-in. It runs only with `PF5620_LIVE=1` plus `AZURE_LUNA_RESOURCE_NAME` and
 * `AZURE_LUNA_API_KEY` (the Azure deployment `gpt-6-luna` served on the v1
 * Responses route), so fork CI stays offline. Example:
 *
 *   PF5620_LIVE=1 PF5620_LIVE_OUT=/tmp/pf5620.jsonl \
 *     <load the two AZURE_LUNA_* names> npx vitest run \
 *     src/harness/v1/session.subagent-answer-stream.live.test.ts
 *
 * A real Harness runs a real child Agent that calls research tools over
 * several steps and then answers through `report_subagent_outcome`. The
 * parent agent is only a turn holder for `task_delegate`. Each scenario
 * records its timings as one JSON line in `PF5620_LIVE_OUT`.
 */
import { appendFileSync } from 'node:fs';

import { describe, expect, it, vi } from 'vitest';
import { z } from 'zod';

import { Agent } from '../../agent';
import { AzureOpenAIGateway, ModelRouterLanguageModel } from '../../llm';
import { Mastra } from '../../mastra';
import { InMemoryHarness } from '../../storage/domains/harness/inmemory';
import { InMemoryDB } from '../../storage/domains/inmemory-db';
import { createTool } from '../../tools';

import { MockAgent } from './__test-utils__';
import type { HarnessEvent } from './events';
import { Harness } from './harness';
import { createPlanTaskTools, TASK_ADD_TOOL_ID, TASK_DELEGATE_TOOL_ID } from './plan-task-tool';
import { Session } from './session';
import { createSpawnSubagentTool, SPAWN_SUBAGENT_TOOL_ID } from './spawn-subagent-tool';
import { HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID } from './terminal-subagent-result';

/**
 * How many upcoming `report_subagent_outcome` calls are rejected. The built-in
 * report tool is wrapped so a test can force its first report to fail with a
 * tool error, after which the model reports again under a new call id.
 */
const forcedReportRejections = { remaining: 0 };

vi.mock('./subagent-outcome-tool', async importOriginal => {
  const original = await importOriginal<typeof import('./subagent-outcome-tool')>();
  return {
    ...original,
    createSubagentOutcomeReportTool: () => {
      const tool = original.createSubagentOutcomeReportTool() as any;
      const execute = tool.execute;
      tool.execute = async (...args: unknown[]) => {
        if (forcedReportRejections.remaining > 0) {
          forcedReportRejections.remaining -= 1;
          throw new Error(
            'Report rejected by the test protocol: call report_subagent_outcome again with the full report.',
          );
        }
        return execute(...args);
      };
      return tool;
    },
  };
});

/** The live scenarios call a paid provider, so they run only on an explicit opt-in. */
function liveEnabled(env: NodeJS.ProcessEnv): boolean {
  return env.PF5620_LIVE === '1' && Boolean(env.AZURE_LUNA_RESOURCE_NAME) && Boolean(env.AZURE_LUNA_API_KEY);
}

const LIVE = liveEnabled(process.env);
const MODEL_ID = 'azure-openai-luna/gpt-6-luna';
const LIVE_TIMEOUT_MS = 600_000;

const TRIALS: Record<string, { title: string; finding: string }> = {
  'NCT-0001': {
    title: 'SGLT2 "EMPA-HF" trial — Zürich cohort',
    finding: 'All-cause mortality fell from 14.2% to 11.8% (HR 0.82; 95% CI 0.71–0.95).',
  },
  'NCT-0002': {
    title: 'Dapagliflozin in HFpEF — São Paulo registry',
    finding: 'No significant change in mortality; hospitalisations dropped by 19%.',
  },
  'NCT-0003': {
    title: "Empagliflozin in elderly patients ≥ 80 — 'real world' data",
    finding: 'Mortality benefit persisted (HR 0.86) with more volume depletion events.',
  },
};

function researchTools() {
  return {
    search_trials: createTool({
      id: 'search_trials',
      description: 'Search the trial registry. Returns trial ids for a query.',
      inputSchema: z.object({ query: z.string() }),
      execute: async () => ({ trialIds: Object.keys(TRIALS) }),
    }),
    read_trial: createTool({
      id: 'read_trial',
      description: 'Read one trial record by id. Call it once per trial id.',
      inputSchema: z.object({ trialId: z.string() }),
      execute: async ({ trialId }) => TRIALS[trialId] ?? { error: `unknown trial ${trialId}` },
    }),
  };
}

const MORTALITY_TASK =
  'Summarise the mortality evidence for SGLT2 inhibitors in heart failure. Format the summary as at least three ' +
  'paragraphs separated by a blank line, put every trial title in straight double quotes exactly as recorded, ' +
  'and include the emoji 🫀.';

const RESEARCH_INSTRUCTIONS = [
  'You are a clinical evidence researcher.',
  'Always call search_trials first, then read_trial for EVERY returned trial id, one call per id.',
  'Then answer through report_subagent_outcome. Its summary is the full answer shown to the user.',
  'Write the summary as 3 to 5 short paragraphs; separate paragraphs with an empty line (two newline characters).',
  'Quote each trial title exactly as the record gives it, wrapped in straight double quotes (the " character),',
  'keeping every accent, symbol, inner quote and apostrophe.',
  'Include at least one emoji (for example 🫀) and the exact numbers from the records.',
  'Use ONLY the records the tools return; never add outside knowledge or other trials.',
  "Report evidence as exactly one item: { kind: 'analysis', description: <one sentence naming the trial ids you read> }.",
  'Never set a field to null.',
].join(' ');

function buildLiveHarness(extraInstructions = '') {
  const gateway = new AzureOpenAIGateway({
    gatewayId: 'azure-openai-luna',
    resourceName: process.env.AZURE_LUNA_RESOURCE_NAME!,
    apiKey: process.env.AZURE_LUNA_API_KEY!,
    useResponsesAPI: true,
    deployments: ['gpt-6-luna'],
  });
  const child = new Agent({
    id: 'researcher',
    name: 'researcher',
    instructions: `${RESEARCH_INSTRUCTIONS} ${extraInstructions}`.trim(),
    // Bind the gateway to the model itself, so every run (including a
    // subagent child's) resolves the same Azure deployment.
    model: new ModelRouterLanguageModel(MODEL_ID as never, [gateway]),
    tools: researchTools(),
  });
  const mastra = new Mastra({ agents: { researcher: child }, gateways: { 'azure-openai-luna': gateway } });
  const parentAgent = new MockAgent({ id: 'parent-agent' });
  const harness = new Harness({
    agents: { 'parent-agent': parentAgent, researcher: mastra.getAgent('researcher') } as any,
    modes: [
      { id: 'default', agentId: 'parent-agent' },
      // The child gets only its research tools and the report tool.
      { id: 'research-mode', agentId: 'researcher', harnessBuiltins: 'exclude' },
    ],
    defaultModeId: 'default',
    sessions: { storage: new InMemoryHarness({ db: new InMemoryDB() }) },
    subagents: {
      maxDepth: 2,
      types: {
        researcher: {
          agentId: 'researcher',
          modeId: 'research-mode',
          description: 'Clinical evidence researcher',
          workspace: 'inherit',
        },
      },
    },
  });
  return { harness, parentAgent };
}

function execCtx(toolCallId: string, abortSignal = new AbortController().signal) {
  return {
    abortSignal,
    agent: { toolCallId, runId: `run-${toolCallId}` },
    runId: `run-${toolCallId}`,
    tracingContext: {} as any,
    requestContext: { get: () => undefined },
    mastra: undefined,
  } as any;
}

function terminalCtx(toolCallId: string) {
  return {
    toolName: SPAWN_SUBAGENT_TOOL_ID,
    toolCallId,
    args: { delivery: 'final' },
    batchSize: 1,
    batchIndex: 0,
    runId: `run-${toolCallId}`,
    abortSignal: new AbortController().signal,
  };
}

type Timed = { at: number; event: HarnessEvent };

/** Record every parent event with its arrival time. */
function recordEvents(session: Session) {
  const events: Timed[] = [];
  const unsubscribe = session.subscribe(event => events.push({ at: performance.now(), event }));
  return { events, unsubscribe };
}

/** First raw report-input chunk per child report call, seen by the child session. */
function recordReportInputStarts() {
  const starts = new Map<string, number>();
  const original = (Session.prototype as any)._streamSubagentAnswer;
  const spy = vi.spyOn(Session.prototype as any, '_streamSubagentAnswer').mockImplementation(function (
    this: Session,
    ...args: any[]
  ) {
    const [toolCallId, toolName] = args as [string, string];
    if (toolName === HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID && !starts.has(toolCallId)) {
      starts.set(toolCallId, performance.now());
    }
    return original.apply(this, args);
  });
  return { starts, restore: () => spy.mockRestore() };
}

function answerDeltas(events: Timed[], toolCallId: string) {
  return events.filter(
    (entry): entry is Timed & { event: Extract<HarnessEvent, { type: 'subagent_answer_delta' }> } =>
      entry.event.type === 'subagent_answer_delta' && entry.event.toolCallId === toolCallId,
  );
}

function endOf(events: Timed[], toolCallId: string) {
  return events.find(entry => entry.event.type === 'subagent_end' && entry.event.toolCallId === toolCallId);
}

/** The projected direct answer, or undefined when the child's report did not verify. */
function directAnswer(tool: ReturnType<typeof createSpawnSubagentTool>, result: unknown, toolCallId: string) {
  try {
    return (tool!.terminalResult!.project!(result as any, terminalCtx(toolCallId)) as { text: string }).text;
  } catch {
    return undefined;
  }
}

/**
 * The summary of the child's accepted (non-error) report call for a spawn. The
 * Harness may still refuse it as a direct answer when its evidence does not
 * bind to the tool receipts; streaming fidelity is checked against it either way.
 */
function acceptedReportSummary(events: Timed[], subagentSessionId: string | undefined) {
  const end = events.find(
    entry =>
      entry.event.type === 'subagent_tool_end' &&
      entry.event.toolName === HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID &&
      !entry.event.isError &&
      entry.event.subagentSessionId === subagentSessionId,
  )?.event as { innerToolCallId: string; output: { summary?: string } } | undefined;
  return end === undefined ? undefined : { innerToolCallId: end.innerToolCallId, summary: end.output.summary };
}

/** The deltas of the last report call, which is the one a direct answer comes from. */
function lastReportText(deltas: ReturnType<typeof answerDeltas>) {
  const last = deltas.at(-1)?.event.innerToolCallId;
  return deltas
    .filter(entry => entry.event.innerToolCallId === last)
    .map(entry => entry.event.delta)
    .join('');
}

/** A bounded, secret-free view of a spawn result for the evidence log. */
function resultShape(result: unknown) {
  const summary = (result as { result?: Record<string, any> } | undefined)?.result;
  return {
    isError: (result as { isError?: boolean } | undefined)?.isError === true,
    status: summary?.status,
    outcome: summary?.outcome,
    finishReason: summary?.finishReason,
    error: summary?.error?.code,
    textChars: typeof summary?.text === 'string' ? summary.text.length : undefined,
  };
}

/** Teardown is not under test: record a shutdown failure instead of masking the result. */
async function shutdown(harness: Harness) {
  forcedReportRejections.remaining = 0;
  try {
    await harness.shutdown();
  } catch (error) {
    record('shutdown-error', { message: error instanceof Error ? error.message : String(error) });
  }
}

function record(scenario: string, data: Record<string, unknown>) {
  const out = process.env.PF5620_LIVE_OUT;
  if (out) appendFileSync(out, `${JSON.stringify({ scenario, at: new Date().toISOString(), ...data })}\n`);
}

describe('PF-5620 live suite opt-in', () => {
  it('stays off unless PF5620_LIVE=1 and both Luna credentials are set', () => {
    const credentials = { AZURE_LUNA_RESOURCE_NAME: 'resource', AZURE_LUNA_API_KEY: 'key' };
    expect(liveEnabled({})).toBe(false);
    expect(liveEnabled(credentials)).toBe(false);
    expect(liveEnabled({ ...credentials, PF5620_LIVE: 'true' })).toBe(false);
    expect(liveEnabled({ PF5620_LIVE: '1', AZURE_LUNA_RESOURCE_NAME: 'resource' })).toBe(false);
    expect(liveEnabled({ PF5620_LIVE: '1', AZURE_LUNA_API_KEY: 'key' })).toBe(false);
    expect(liveEnabled({ ...credentials, PF5620_LIVE: '1' })).toBe(true);
  });
});

describe.skipIf(!LIVE)('PF-5620 live streamed subagent answers (Azure gpt-6-luna)', () => {
  it(
    'streams a long multi-step spawn answer early, in many deltas that equal the direct answer',
    async () => {
      const { harness } = buildLiveHarness();
      const reportStarts = recordReportInputStarts();
      try {
        const parent = await harness.session({ resourceId: 'u-live', threadId: { fresh: true } });
        const { events } = recordEvents(parent);
        const tool = createSpawnSubagentTool(parent)!;
        const startedAt = performance.now();

        const result = await tool.execute!({ agentType: 'researcher', task: MORTALITY_TASK }, execCtx('tc-live-spawn'));
        record('spawn-result', {
          ...resultShape(result),
          deltas: answerDeltas(events, 'tc-live-spawn').length,
          tools: events.flatMap(entry => (entry.event.type === 'subagent_tool_start' ? [entry.event.toolName] : [])),
          textDeltas: events.filter(entry => entry.event.type === 'subagent_text_delta').length,
          reportEvidence: events.flatMap(entry =>
            entry.event.type === 'subagent_tool_start' &&
            entry.event.toolName === HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID
              ? [(entry.event.input as { evidence?: unknown } | undefined)?.evidence]
              : [],
          ),
        });
        const deltas = answerDeltas(events, 'tc-live-spawn');
        const answerText = directAnswer(tool, result, 'tc-live-spawn');
        const accepted = acceptedReportSummary(events, deltas[0]?.event.subagentSessionId);
        const answer = { text: answerText ?? accepted?.summary ?? '' };
        const end = endOf(events, 'tc-live-spawn');
        const firstDelta = deltas[0];
        const reportToolStart = events.find(
          entry =>
            entry.event.type === 'subagent_tool_start' &&
            entry.event.toolName === HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID,
        );
        const toolSteps = events.filter(
          entry =>
            entry.event.type === 'subagent_tool_start' &&
            entry.event.toolName !== HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID,
        ).length;
        const innerId = deltas.at(-1)?.event.innerToolCallId ?? '';
        const reportInputStart = reportStarts.starts.get(innerId);
        record('spawn', {
          deltas: deltas.length,
          answerChars: answer.text.length,
          toolSteps,
          hasNewline: answer.text.includes('\n'),
          hasQuote: answer.text.includes('"'),
          hasNonAscii: /[^\u0000-\u007f]/.test(answer.text),
          totalMs: end === undefined ? null : Math.round(end.at - startedAt),
          firstDeltaAfterReportInputMs:
            reportInputStart === undefined || firstDelta === undefined
              ? null
              : Math.round(firstDelta.at - reportInputStart),
          firstDeltaBeforeEndMs:
            end === undefined || firstDelta === undefined ? null : Math.round(end.at - firstDelta.at),
          firstDeltaBeforeReportToolStartMs:
            reportToolStart === undefined || firstDelta === undefined
              ? null
              : Math.round(reportToolStart.at - firstDelta.at),
          reportCalls: new Set(deltas.map(entry => entry.event.innerToolCallId)).size,
          directAnswerVerified: answerText !== undefined,
        });

        // The record above is written first, so a run without deltas or an end
        // event still leaves its timing evidence before these assertions fail.
        expect(end).toBeDefined();
        expect(toolSteps).toBeGreaterThanOrEqual(2);
        expect(deltas.length).toBeGreaterThanOrEqual(5);
        expect(deltas.every(entry => entry.at < end!.at)).toBe(true);
        expect(accepted).toBeDefined();
        expect(lastReportText(deltas)).toBe(accepted!.summary);
        if (answerText !== undefined) expect(lastReportText(deltas)).toBe(answerText);
        // The answer streams while the report input is still arriving, well
        // before the completed tool call.
        expect(reportToolStart === undefined || deltas[0]!.at < reportToolStart.at).toBe(true);
      } finally {
        reportStarts.restore();
        await shutdown(harness);
      }
    },
    LIVE_TIMEOUT_MS,
  );

  it(
    'keeps two parallel spawns in separate per-call streams',
    async () => {
      const { harness } = buildLiveHarness();
      try {
        const parent = await harness.session({ resourceId: 'u-live', threadId: { fresh: true } });
        const { events } = recordEvents(parent);
        const tool = createSpawnSubagentTool(parent)!;
        const startedAt = performance.now();

        const [first, second] = await Promise.all([
          tool.execute!({ agentType: 'researcher', task: MORTALITY_TASK }, execCtx('tc-live-a')),
          tool.execute!(
            {
              agentType: 'researcher',
              task: 'Summarise the hospitalisation and safety evidence for SGLT2 inhibitors, for a cardiologist.',
            },
            execCtx('tc-live-b'),
          ),
        ]);
        const a = answerDeltas(events, 'tc-live-a');
        const b = answerDeltas(events, 'tc-live-b');
        const answers = [directAnswer(tool, first, 'tc-live-a'), directAnswer(tool, second, 'tc-live-b')];
        const acceptedA = acceptedReportSummary(events, a[0]?.event.subagentSessionId);
        const acceptedB = acceptedReportSummary(events, b[0]?.event.subagentSessionId);
        const all = events.filter(entry => entry.event.type === 'subagent_answer_delta');
        let switches = 0;
        for (let i = 1; i < all.length; i += 1) {
          if ((all[i]!.event as any).toolCallId !== (all[i - 1]!.event as any).toolCallId) switches += 1;
        }
        record('parallel', {
          deltasA: a.length,
          deltasB: b.length,
          interleavingSwitches: switches,
          totalMs: Math.round(performance.now() - startedAt),
          directAnswersVerified: answers.filter(text => text !== undefined).length,
        });

        expect(a.length).toBeGreaterThanOrEqual(5);
        expect(b.length).toBeGreaterThanOrEqual(5);
        expect(lastReportText(a)).toBe(acceptedA?.summary);
        expect(lastReportText(b)).toBe(acceptedB?.summary);
        if (answers[0] !== undefined) expect(lastReportText(a)).toBe(answers[0]);
        if (answers[1] !== undefined) expect(lastReportText(b)).toBe(answers[1]);
        expect(new Set(a.map(entry => entry.event.subagentSessionId)).size).toBe(1);
        expect(a[0]!.event.subagentSessionId).not.toBe(b[0]!.event.subagentSessionId);
      } finally {
        await shutdown(harness);
      }
    },
    LIVE_TIMEOUT_MS,
  );

  it(
    'stops the answer stream when the spawn is aborted mid-answer',
    async () => {
      const { harness } = buildLiveHarness();
      try {
        const parent = await harness.session({ resourceId: 'u-live', threadId: { fresh: true } });
        const controller = new AbortController();
        let seen = 0;
        let atAbort = 0;
        parent.subscribe(event => {
          if (event.type !== 'subagent_answer_delta') return;
          seen += 1;
          if (seen === 3) {
            atAbort = seen;
            controller.abort();
          }
        });
        const tool = createSpawnSubagentTool(parent)!;

        const result = (await tool.execute!(
          { agentType: 'researcher', task: MORTALITY_TASK },
          execCtx('tc-live-abort', controller.signal),
        )) as any;
        // Give a still-running provider stream time to deliver anything late.
        await new Promise(resolve => setTimeout(resolve, 3_000));
        record('abort', { deltasAtAbort: atAbort, deltasTotal: seen, isError: result.isError === true });

        expect(atAbort).toBe(3);
        expect(seen).toBe(3);
        expect(result.isError).toBe(true);
      } finally {
        await shutdown(harness);
      }
    },
    LIVE_TIMEOUT_MS,
  );

  it(
    'restarts the stream on the new report call after the first report is rejected',
    async () => {
      // The first report is rejected with a tool error, so the model reports
      // again under a new call id.
      const { harness } = buildLiveHarness();
      forcedReportRejections.remaining = 1;
      try {
        const parent = await harness.session({ resourceId: 'u-live', threadId: { fresh: true } });
        const { events } = recordEvents(parent);
        const tool = createSpawnSubagentTool(parent)!;

        const result = await tool.execute!({ agentType: 'researcher', task: MORTALITY_TASK }, execCtx('tc-live-retry'));
        record('retry-result', {
          ...resultShape(result),
          tools: events.flatMap(entry =>
            entry.event.type === 'subagent_tool_end'
              ? [
                  {
                    tool: entry.event.toolName,
                    isError: entry.event.isError,
                    output:
                      entry.event.toolName === HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID ? entry.event.output : undefined,
                  },
                ]
              : [],
          ),
        });
        const answerText = directAnswer(tool, result, 'tc-live-retry');
        const deltas = answerDeltas(events, 'tc-live-retry');
        const reportCalls = [...new Set(deltas.map(entry => entry.event.innerToolCallId))];
        record('retry', {
          reportCalls: reportCalls.length,
          deltas: deltas.length,
          directAnswerVerified: answerText !== undefined,
        });

        const acceptedReport = events.find(
          entry =>
            entry.event.type === 'subagent_tool_end' &&
            entry.event.toolName === HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID &&
            !entry.event.isError,
        )?.event as { innerToolCallId: string; output: { summary: string } } | undefined;
        expect(reportCalls.length).toBeGreaterThanOrEqual(2);
        expect(acceptedReport).toBeDefined();
        // The stream restarted on the accepted call, whose deltas equal its summary.
        expect(reportCalls.at(-1)).toBe(acceptedReport!.innerToolCallId);
        expect(lastReportText(deltas)).toBe(acceptedReport!.output.summary);
        if (answerText !== undefined) expect(lastReportText(deltas)).toBe(answerText);
        // Every earlier call's deltas precede the final call's first delta.
        const finalFirst = deltas.findIndex(entry => entry.event.innerToolCallId === reportCalls.at(-1));
        expect(deltas.slice(finalFirst).every(entry => entry.event.innerToolCallId === reportCalls.at(-1))).toBe(true);
      } finally {
        await shutdown(harness);
      }
    },
    LIVE_TIMEOUT_MS,
  );

  it(
    'streams a delegated task answer that equals the delegation result',
    async () => {
      const { harness, parentAgent } = buildLiveHarness();
      try {
        const parent = await harness.session({ resourceId: 'u-live', threadId: { fresh: true } });
        const { events } = recordEvents(parent);
        // Plan-task tools are turn-gated: hold a parent turn open.
        let release!: () => void;
        parentAgent.enqueueRun({ holdUntil: new Promise<void>(r => (release = r)), finishReason: 'stop', text: 'ok' });
        const turn = parent.message({ content: 'go' });
        await vi.waitFor(() => expect(parentAgent.streamCalls.length).toBeGreaterThan(0));
        const tools = createPlanTaskTools(parent);
        const toolCtx = {
          abortSignal: new AbortController().signal,
          agent: { toolCallId: 'tc-live-deleg', runId: 'parent-run' },
          requestContext: { get: () => undefined },
        } as any;
        const startedAt = performance.now();
        const task = (await tools[TASK_ADD_TOOL_ID]!.execute!({ content: MORTALITY_TASK } as any, toolCtx)) as any;
        await tools[TASK_DELEGATE_TOOL_ID]!.execute!({ taskId: task.taskId, agentType: 'researcher' } as any, toolCtx);

        await vi.waitFor(() => expect(endOf(events, 'tc-live-deleg')).toBeDefined(), {
          timeout: LIVE_TIMEOUT_MS - 30_000,
          interval: 250,
        });
        const page = await parent._internalStorage.listPlanTasks({ sessionId: parent.id, limit: 100 });
        const row = page.tasks.find(entry => entry.taskId === task.taskId) as any;
        const endOutput = (endOf(events, 'tc-live-deleg')!.event as any).output as {
          text: string;
          textTruncated: boolean;
        };
        const deltas = answerDeltas(events, 'tc-live-deleg');
        const end = endOf(events, 'tc-live-deleg')!;
        record('delegate', {
          deltas: deltas.length,
          status: row?.status,
          directAnswerVerified: row?.status === 'completed',
          totalMs: Math.round(end.at - startedAt),
          firstDeltaBeforeEndMs: deltas.length > 0 ? Math.round(end.at - deltas[0]!.at) : null,
        });

        const accepted = acceptedReportSummary(events, deltas[0]?.event.subagentSessionId);
        expect(deltas.length).toBeGreaterThanOrEqual(5);
        expect(deltas.every(entry => entry.at < end.at)).toBe(true);
        expect(lastReportText(deltas)).toBe(accepted?.summary);
        if (row?.status === 'completed') {
          expect(endOutput.textTruncated).toBe(false);
          expect(lastReportText(deltas)).toBe(endOutput.text);
        }
        release();
        await turn.catch(() => {});
      } finally {
        await shutdown(harness);
      }
    },
    LIVE_TIMEOUT_MS,
  );
});
