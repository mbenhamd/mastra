import { randomUUID } from 'node:crypto';
import { z } from 'zod';

import { Agent } from '../../agent';
import { ModelRouterLanguageModel } from '../../llm/model/router';
import { PrefillErrorHandler, ProviderHistoryCompat, StreamErrorRetryProcessor } from '../../processors';
import { RequestContext } from '../../request-context';
import type {
  GoalJudgeDecision,
  GoalState,
  QueueAdmissionReceipt,
  QueuedItem,
  SessionRecord,
} from '../../storage/domains/harness';
import type { ObservationalMemorySourceWriteGuard } from '../../storage/types';
import type { FullOutput } from '../../stream/base/output';
import { HarnessValidationError } from './errors';
import type { EmitInput, HarnessEvent } from './events';
import type { Harness } from './harness';
import type { QueueBackpressureDrop } from './session';
import type { GoalOptions, HarnessMessage } from './types';

/**
 * System prompt for the goal judge. Lifted verbatim from
 * mastracode/src/tui/goal-manager.ts so the harness-native judge produces
 * the same verdicts as the TUI implementation. The wording matters — the
 * "don't wait for yourself" rule and the asked-question-vs-checkpoint
 * distinction prevent the loop from flip-flopping.
 */
export const JUDGE_SYSTEM_PROMPT = `You are the goal judge. Your decision directly controls whether the assistant continues working toward the goal.

Given a goal and the assistant's latest response, reason about whether the goal's requirements have been satisfied. Compare what the goal asks for against what the assistant has actually produced. Focus on substance, not phrasing.

Use "done" when the goal is fully achieved.
Use "waiting" only when the goal explicitly requires a user checkpoint, user feedback, human verification, human confirmation, or another external event outside the goal-judge loop before the assistant should continue, and the assistant has correctly stopped at that checkpoint. Do not use "waiting" merely because the assistant asked a question or could benefit from user input.
Use "continue" when the goal is not done and the assistant should keep working autonomously, including when it asked for input that the goal did not explicitly require.
If your previous decision was "waiting" for an explicit user checkpoint, keep choosing "waiting" when the user's latest response asks a question, requests clarification, or otherwise does not satisfy the checkpoint. Do not continue until the required user feedback/confirmation/verification has actually been provided.
If the goal says to wait for the goal judge, judge, evaluator, or you to respond, approve, verify, validate, tell the assistant to continue, or otherwise provide the next signal, treat your own decision as that judge response. Verification can be performed by you unless the goal explicitly says it needs human/user verification. Choose "continue" when the assistant should proceed to the next step. Do not choose "waiting" for judge-controlled checkpoints, because that would mean waiting for yourself.

Your "reason" field is sent back to the assistant as guidance when the goal is not yet done — be specific about what still needs to be accomplished. When choosing "continue", write the reason as an instruction for what the assistant should do next. When choosing "waiting", explain what specific user checkpoint is still outstanding.`;

/** Structured-output schema used by the goal judge call (§4.7). */
export const GoalJudgeSchema = z.object({
  decision: z
    .enum(['done', 'continue', 'waiting'])
    .describe(
      'Whether the goal is done, should continue autonomously, or is at an explicit user checkpoint required by the goal',
    ),
  reason: z.string().describe('Brief explanation of what was accomplished or what remains to be done'),
});

/** Per-message cap on judge-context strings to keep judge latency bounded. */
const JUDGE_TRUNCATE_LIMIT = 4000;

export function truncateForJudge(value: string): string {
  return value.length > JUDGE_TRUNCATE_LIMIT ? value.slice(0, JUDGE_TRUNCATE_LIMIT) + '\n...[truncated]' : value;
}

function escapeGoalXml(value: string): string {
  return value.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;');
}

/**
 * Continuation prompts. The wording matters — these are lifted verbatim
 * from `mastracode/src/tui/goal-manager.ts` + `commands/goal.ts` so the
 * harness-native goal API produces byte-identical kickoff/resume/judge-
 * continue messages.
 */

/** Kickoff sent by `setGoal` (parity with TUI's `createGoalReminderXml`). */
export function buildKickoffContinuation(objective: string): string {
  return `<system-reminder type="goal">${escapeGoalXml(objective)}</system-reminder>`;
}

/** Continuation sent by `resumeGoal` (parity with TUI's `/goal resume`). */
export function buildResumeContinuation(objective: string): string {
  return `Continue working toward the goal: ${objective}`;
}

/**
 * Continuation sent after a judge `continue` verdict (parity with TUI's
 * `GoalManager.buildContinuationPrompt`).
 */
export function buildJudgeContinuation(opts: {
  turn: number;
  max: number;
  objective: string;
  judgeReason: string;
}): string {
  const message = `[Goal attempt ${opts.turn}/${opts.max}] The goal is not yet complete. Judge feedback: ${opts.judgeReason}\n\nContinue working toward the goal: ${opts.objective}`;
  return `<system-reminder type="goal-judge">${escapeGoalXml(message)}</system-reminder>`;
}

/**
 * The slice of `Session` the goal-judge operations (§4.7) need. `Session` builds
 * it in `_goalJudgePort()`; `record`, `currentTurnSourceWriteGuard` and
 * `testJudge` are getters so the operations always see the current value.
 */
export interface GoalJudgeSessionPort {
  readonly parentSessionId?: string;
  readonly record: SessionRecord;
  readonly harness: Harness;
  readonly currentTurnSourceWriteGuard: ObservationalMemorySourceWriteGuard | undefined;
  /** Test-injection hook `Session.__testJudge`. */
  readonly testJudge: ((goal: GoalState) => Promise<Omit<GoalJudgeDecision, 'judgedAt'>>) | undefined;
  assertLive(method: string): void;
  flushUpdate(update: (prev: SessionRecord) => SessionRecord): Promise<void>;
  emit(event: EmitInput): HarnessEvent;
  currentCancelRequest(): SessionRecord['cancelRequest'];
  applyQueueBackpressureForAppend(
    prev: SessionRecord,
    opts: {
      item: QueuedItem;
      receipt?: QueueAdmissionReceipt;
      maxQueueDepth: number;
      source: 'queue' | 'goal';
      goalId?: string;
      droppedItems: QueueBackpressureDrop[];
      onRejected?: () => void;
      onAdmitted?: () => void;
    },
  ): SessionRecord;
  failBackpressureDroppedItems(droppedItems: QueueBackpressureDrop[]): void;
  maybeDrainQueue(): Promise<void>;
  listMessages(): Promise<HarnessMessage[]>;
}

export async function setSessionGoal(port: GoalJudgeSessionPort, opts: GoalOptions): Promise<GoalState> {
  port.assertLive('setGoal()');
  if (port.parentSessionId !== undefined || port.record.origin === 'subagent-tool') {
    throw new HarnessValidationError('setGoal', 'goals cannot be set on subagent sessions (parent owns the loop)');
  }
  if (typeof opts.objective !== 'string' || opts.objective.length === 0) {
    throw new HarnessValidationError('setGoal.objective', 'must be a non-empty string');
  }
  if (opts.maxTurns !== undefined && (!Number.isInteger(opts.maxTurns) || opts.maxTurns < 1)) {
    throw new HarnessValidationError('setGoal.maxTurns', 'must be a positive integer');
  }

  const defaults = port.harness._internalGoalDefaults;
  const judgeModelId = opts.judgeModel ?? defaults.defaultJudgeModel;
  if (typeof judgeModelId !== 'string' || judgeModelId.length === 0) {
    throw new HarnessValidationError(
      'setGoal.judgeModel',
      'no judge model provided and `goals.defaultJudgeModel` is not configured',
    );
  }

  const priorId = port.record.goal?.id;
  const goal: GoalState = {
    id: `goal-${randomUUID()}`,
    objective: opts.objective,
    status: 'active',
    turnsUsed: 0,
    maxTurns: opts.maxTurns ?? defaults.defaultMaxTurns,
    judgeModelId,
    createdAt: Date.now(),
  };

  await port.flushUpdate(prev => ({ ...prev, goal }));
  if (priorId !== undefined) {
    port.emit({ type: 'goal_cleared', goalId: priorId });
  }
  port.emit({ type: 'goal_set', goal });

  if (opts.kickoff !== false) {
    await enqueueGoalContinuation(port, goal, buildKickoffContinuation(opts.objective));
  }

  return goal;
}

export function getSessionGoal(port: GoalJudgeSessionPort): GoalState | undefined {
  port.assertLive('getGoal()');
  return port.record.goal;
}

export async function pauseSessionGoal(port: GoalJudgeSessionPort): Promise<GoalState | undefined> {
  port.assertLive('pauseGoal()');
  const goal = port.record.goal;
  if (!goal || goal.status === 'paused') return goal;
  const updated: GoalState = { ...goal, status: 'paused' };
  await port.flushUpdate(prev => ({ ...prev, goal: updated }));
  port.emit({ type: 'goal_paused', goalId: goal.id, reason: 'requested' });
  return updated;
}

export async function resumeSessionGoal(port: GoalJudgeSessionPort): Promise<GoalState | undefined> {
  port.assertLive('resumeGoal()');
  const goal = port.record.goal;
  if (!goal) return undefined;
  if (goal.status === 'active') return goal;
  const updated: GoalState = { ...goal, status: 'active' };
  await port.flushUpdate(prev => ({ ...prev, goal: updated }));
  port.emit({ type: 'goal_resumed', goalId: goal.id });
  await enqueueGoalContinuation(port, updated, buildResumeContinuation(updated.objective));
  return updated;
}

export async function clearSessionGoal(port: GoalJudgeSessionPort): Promise<void> {
  port.assertLive('clearGoal()');
  const goal = port.record.goal;
  if (!goal) return;
  await port.flushUpdate(prev => {
    const next = { ...prev };
    delete next.goal;
    return next;
  });
  port.emit({ type: 'goal_cleared', goalId: goal.id });
}

export async function updateGoalJudgeDefaults(
  port: GoalJudgeSessionPort,
  opts: { judgeModelId?: string; maxTurns?: number },
): Promise<GoalState | undefined> {
  port.assertLive('updateJudgeDefaults()');
  const goal = port.record.goal;
  if (!goal) return undefined;
  if (opts.judgeModelId === undefined && opts.maxTurns === undefined) return goal;
  if (opts.maxTurns !== undefined && (!Number.isFinite(opts.maxTurns) || opts.maxTurns <= 0)) {
    throw new HarnessValidationError('maxTurns', 'maxTurns must be a positive number');
  }
  const updated: GoalState = {
    ...goal,
    ...(opts.judgeModelId !== undefined ? { judgeModelId: opts.judgeModelId } : {}),
    ...(opts.maxTurns !== undefined ? { maxTurns: opts.maxTurns } : {}),
  };
  await port.flushUpdate(prev => ({ ...prev, goal: updated }));
  return updated;
}

/**
 * @internal — enqueue a goal-driven continuation turn. Caller is responsible
 * for building the final prompt content (kickoff / resume / judge-continue
 * each use a distinct template — see `buildKickoffContinuation` /
 * `buildResumeContinuation` / `buildJudgeContinuation`). Marked with
 * `source: 'goal'` so the judge loop knows to skip re-judging on the
 * resulting turn (otherwise the loop would never terminate).
 */
async function enqueueGoalContinuation(port: GoalJudgeSessionPort, goal: GoalState, content: string): Promise<void> {
  if (port.currentCancelRequest() !== undefined) return;
  const cap = port.harness._internalMaxQueueDepth;
  const item: QueuedItem = {
    id: `q-${randomUUID()}`,
    enqueuedAt: Date.now(),
    content,
    attachments: [],
    mode: port.record.modeId,
    source: 'goal',
    goalId: goal.id,
    ...(port.currentTurnSourceWriteGuard
      ? { observationalMemorySourceWriteGuard: port.currentTurnSourceWriteGuard }
      : {}),
  };
  const droppedItems: QueueBackpressureDrop[] = [];
  let admitted = false;
  await port.flushUpdate(prev => {
    if (prev.cancelRequest !== undefined) return prev;
    return port.applyQueueBackpressureForAppend(prev, {
      item,
      receipt: undefined,
      maxQueueDepth: cap,
      source: 'goal',
      goalId: goal.id,
      droppedItems,
      onRejected: () => {
        admitted = false;
      },
      onAdmitted: () => {
        admitted = true;
      },
    });
  });
  port.failBackpressureDroppedItems(droppedItems);
  if (!admitted) {
    // §10.2: queue-full/backpressure is not a public event — the goal item is
    // simply not admitted (rejected at the caller for direct queue()).
    return;
  }
  void port.maybeDrainQueue();
}

export async function runGoalJudge(
  port: GoalJudgeSessionPort,
  turn: FullOutput<unknown>,
  wasGoalDriven: boolean,
): Promise<void> {
  // Skip re-judging on goal-driven continuation turns to avoid a tight
  // loop where every continuation triggers another judge call. The
  // judge only runs after user-driven turns; continuations are auto-
  // generated from the prior judge call.
  if (wasGoalDriven) return;

  const goal = port.record.goal;
  if (!goal || goal.status !== 'active') return;

  const evaluatedGoalId = goal.id;

  // Suspended turns don't count toward the judge loop — wait for resume.
  if (turn.finishReason === 'suspended') return;

  // Gate 1 — re-read goal after the async context fetch. If it's been
  // cleared / paused / replaced, drop this judge cycle silently.
  const context = await getGoalJudgeContext(port, turn);
  if (port.record.goal?.id !== evaluatedGoalId || port.record.goal.status !== 'active') return;

  // No-assistant-message fallback: parity with TUI's evaluateAfterTurn.
  // The judge has nothing to score, but the agent still made some attempt
  // (typically a tool call without a closing assistant message). Push a
  // gentle nudge unless we've hit the budget.
  if (!context.lastAssistantContent) {
    if (goal.turnsUsed >= goal.maxTurns) {
      await port.flushUpdate(prev =>
        prev.goal && prev.goal.id === evaluatedGoalId
          ? { ...prev, goal: { ...prev.goal, status: 'paused' as const } }
          : prev,
      );
      port.emit({ type: 'goal_paused', goalId: evaluatedGoalId, reason: 'budget_exhausted' });
      return;
    }
    await enqueueGoalContinuation(
      port,
      goal,
      buildJudgeContinuation({
        turn: goal.turnsUsed,
        max: goal.maxTurns,
        objective: goal.objective,
        judgeReason: 'No response yet, keep working.',
      }),
    );
    return;
  }

  let decision: GoalJudgeDecision;
  try {
    decision = await callGoalJudge(port, goal, turn);
  } catch {
    // Gate 2a — goal might have changed during the judge call.
    if (port.record.goal?.id !== evaluatedGoalId) return;
    await port.flushUpdate(prev =>
      prev.goal && prev.goal.id === evaluatedGoalId
        ? { ...prev, goal: { ...prev.goal, status: 'paused' as const } }
        : prev,
    );
    port.emit({ type: 'goal_paused', goalId: evaluatedGoalId, reason: 'judge_failed' });
    return;
  }

  // Gate 2b — goal might have changed during the judge call.
  if (port.record.goal?.id !== evaluatedGoalId) return;

  const turnsUsed = decision.decision === 'waiting' ? goal.turnsUsed : goal.turnsUsed + 1;
  const updated: GoalState = { ...goal, turnsUsed, lastDecision: decision };

  await port.flushUpdate(prev => (prev.goal && prev.goal.id === evaluatedGoalId ? { ...prev, goal: updated } : prev));

  port.emit({
    type: 'goal_judged',
    goalId: evaluatedGoalId,
    decision,
    turnsUsed,
    maxTurns: updated.maxTurns,
  });

  if (decision.decision === 'done') {
    await port.flushUpdate(prev =>
      prev.goal && prev.goal.id === evaluatedGoalId
        ? { ...prev, goal: { ...prev.goal, status: 'done' as const } }
        : prev,
    );
    port.emit({ type: 'goal_done', goalId: evaluatedGoalId, reason: decision.reason, turnsUsed });
    return;
  }

  if (decision.decision === 'waiting') {
    // §10.2: the judge parked the goal at an external checkpoint. Surface it
    // as `goal_waiting` (turnsUsed is unchanged — see the computation above)
    // so subscribers can distinguish a checkpoint pause from a budget/judge
    // failure (`goal_paused`) or completion (`goal_done`).
    port.emit({ type: 'goal_waiting', goalId: evaluatedGoalId, reason: decision.reason, turnsUsed });
    return;
  }

  // decision.decision === 'continue'
  if (turnsUsed >= updated.maxTurns) {
    await port.flushUpdate(prev =>
      prev.goal && prev.goal.id === evaluatedGoalId
        ? { ...prev, goal: { ...prev.goal, status: 'paused' as const } }
        : prev,
    );
    port.emit({ type: 'goal_paused', goalId: evaluatedGoalId, reason: 'budget_exhausted' });
    return;
  }

  // Gate 3 — final stale check before enqueueing.
  if (port.record.goal?.id !== evaluatedGoalId || port.record.goal.status !== 'active') return;
  await enqueueGoalContinuation(
    port,
    updated,
    buildJudgeContinuation({
      turn: turnsUsed,
      max: updated.maxTurns,
      objective: updated.objective,
      judgeReason: decision.reason,
    }),
  );
}

/**
 * @internal — execute the judge model. Returns a `GoalJudgeDecision`.
 *
 * Test-injection hook: when `__testJudge` is set on this session, it
 * runs in place of the real judge call so unit tests can drive verdicts
 * deterministically without standing up a live model.
 */
async function callGoalJudge(
  port: GoalJudgeSessionPort,
  goal: GoalState,
  turn: FullOutput<unknown>,
): Promise<GoalJudgeDecision> {
  const hook = port.testJudge;
  if (hook) {
    const verdict = await hook(goal);
    return { ...verdict, judgedAt: Date.now() };
  }
  // Real judge path. Mirrors the TUI's GoalManager.callJudge:
  //   - dedicated `goal-judge` Agent with JUDGE_SYSTEM_PROMPT baked in
  //   - input processor: ProviderHistoryCompat (history-shape parity
  //     across providers, esp. Anthropic)
  //   - error processors: StreamErrorRetryProcessor, PrefillErrorHandler,
  //     ProviderHistoryCompat (retry flaky judge streams cleanly)
  //   - dedicated memory thread `${sessionId}-${goalId}` so the judge
  //     sees continuity across iterations (its own prior verdicts)
  //   - structured output via the judge schema
  //   - context: goal + last user content + assistantStepsSinceLastUser
  //     + truncated last assistant content (4000-char cap)
  const context = await getGoalJudgeContext(port, turn);
  const judgeAgent = createGoalJudgeAgent(port, goal);
  const memory = await judgeAgent.getMemory({ requestContext: new RequestContext() });
  const judgeThreadId = `${port.record.id}-${goal.id}`;

  if (memory) {
    const existing = await memory.getThreadById({ threadId: judgeThreadId });
    if (!existing) {
      await memory.createThread({
        threadId: judgeThreadId,
        resourceId: port.record.resourceId,
        title: `Goal judge: ${goal.objective.slice(0, 80)}`,
        metadata: {
          goalJudge: true,
          parentSessionId: port.record.id,
          goalId: goal.id,
        },
      });
    }
  }

  const truncatedAssistant = truncateForJudge(context.lastAssistantContent ?? 'No response yet, keep working.');
  const recentUser = context.lastUserContent
    ? `\n\nLatest user message:\n${truncateForJudge(context.lastUserContent)}\n\nAssistant steps since that user message: ${context.assistantStepsSinceLastUser}`
    : '';
  const prompt = `Goal: ${goal.objective}${recentUser}\n\nLatest assistant message:\n${truncatedAssistant}`;

  const stream = await judgeAgent.stream(prompt, {
    ...(memory ? { memory: { thread: judgeThreadId, resource: port.record.resourceId } } : {}),
    structuredOutput: { schema: GoalJudgeSchema },
  } as never);

  await (stream as { consumeStream: () => Promise<void> }).consumeStream();
  const full = (await (stream as { getFullOutput: () => Promise<unknown> }).getFullOutput()) as {
    object?: unknown;
  };
  const obj = full.object as { decision: 'done' | 'continue' | 'waiting'; reason: string } | undefined;
  if (!obj || typeof obj !== 'object') {
    throw new Error('judge returned no structured output');
  }
  return { decision: obj.decision, reason: obj.reason, judgedAt: Date.now() };
}

export async function getGoalJudgeContext(
  port: GoalJudgeSessionPort,
  turn?: FullOutput<unknown>,
): Promise<{
  lastUserContent: string | null;
  assistantStepsSinceLastUser: number;
  lastAssistantContent: string | null;
}> {
  let messages: HarnessMessage[] = [];
  try {
    messages = await port.listMessages();
  } catch {
    // listMessages can fail in ad-hoc-thread setups; we'll fall back to
    // the in-memory turn text below.
  }

  let lastUserIndex = -1;
  let lastAssistantContent: string | null = null;

  for (let i = messages.length - 1; i >= 0; i--) {
    const msg = messages[i] as { role?: string; content?: unknown } | undefined;
    if (!msg) continue;
    if (!lastAssistantContent && msg.role === 'assistant') {
      lastAssistantContent = extractJudgeTextContent(port, msg.content);
    }
    if (msg.role === 'user') {
      lastUserIndex = i;
      break;
    }
  }

  // Storage may not have the assistant turn yet (depends on the agent
  // wiring). Fall back to the in-memory `turn.text` we just produced.
  if (!lastAssistantContent && turn) {
    const text = (turn as { text?: string }).text;
    if (typeof text === 'string' && text.length > 0) {
      lastAssistantContent = text;
    }
  }

  const lastUserContent =
    lastUserIndex >= 0
      ? extractJudgeTextContent(port, (messages[lastUserIndex] as { content?: unknown }).content)
      : null;
  const assistantStepsSinceLastUser =
    lastUserIndex >= 0
      ? messages.slice(lastUserIndex + 1).filter(m => (m as { role?: string }).role === 'assistant').length
      : 0;

  return {
    lastUserContent,
    assistantStepsSinceLastUser,
    lastAssistantContent,
  };
}

function extractJudgeTextContent(port: GoalJudgeSessionPort, content: unknown): string {
  if (typeof content === 'string') return content;
  if (Array.isArray(content)) {
    return content
      .filter(part => (part as { type?: string })?.type === 'text')
      .map(part => (part as { text?: string }).text ?? '')
      .join('\n');
  }
  return String(content ?? '');
}

export function createGoalJudgeAgent(port: GoalJudgeSessionPort, goal: GoalState): Agent {
  const model = new ModelRouterLanguageModel(goal.judgeModelId as never);
  return new Agent({
    id: 'goal-judge',
    name: 'Goal Judge',
    instructions: JUDGE_SYSTEM_PROMPT,
    model,
    mastra: port.harness.mastra,
    inputProcessors: [new ProviderHistoryCompat()],
    errorProcessors: [new StreamErrorRetryProcessor(), new PrefillErrorHandler(), new ProviderHistoryCompat()],
  });
}
