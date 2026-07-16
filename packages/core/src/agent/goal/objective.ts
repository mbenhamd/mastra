import type { MastraUnion } from '../../action';
import type { RequestContext } from '../../request-context';
import type { GoalObjectiveRecord, ThreadStateMutation } from '../../storage/domains/thread-state/base';

// =============================================================================
// Goal objective: durable thread state + state-signal projection
// =============================================================================
//
// A goal objective is held in the thread-scoped `threadState` storage domain
// under `type: 'goal'`. It drives the in-loop goal scorer (`goal-step.ts`): the
// agent keeps working until the objective is judged complete or the run budget
// is exhausted. The `GoalStateProcessor` projects the active objective onto the
// agent state-signal lane so the model always knows what it is working toward.
//
// Like the task tools, a within-turn write surfaces the objective on the shared
// `RequestContext` under `GOAL_REQUEST_CONTEXT_KEY`, so the state processor can
// build a snapshot that reflects a mid-turn `setObjective` in the same step.

/** RequestContext key under which the current objective is surfaced within a turn. */
export const GOAL_REQUEST_CONTEXT_KEY = 'mastra:goal';

/** State-signal lane id used for the current objective. */
export const GOAL_STATE_ID = 'goal';

/** `threadState` storage `type` namespace under which the objective is stored. */
export const GOAL_STATE_TYPE = 'goal';

/** Default max goal evaluations before the goal stops. */
export const DEFAULT_GOAL_MAX_RUNS = 50;

/**
 * Score the default goal scorer emits to signal an explicit "waiting for the
 * user" checkpoint (tri-state decision `waiting`). It is deliberately neither 1
 * (complete) nor 0 (continue): the generic completion reducer treats it as "not
 * passed" (so the loop does not declare the goal done), while the goal step
 * detects this exact value and stops the auto-loop (`isContinued = false`) so
 * the user gets a chance to provide input. The record stays `active` — the next
 * agent turn is still judged. Shared between `scorer.ts` and `goal-step.ts`.
 */
export const GOAL_SCORE_WAITING = 0.5;

/**
 * Stable id of the built-in goal scorer (see `createGoalScorer`). The goal step
 * uses this to attribute the `GOAL_SCORE_WAITING` sentinel to the default
 * scorer only — a custom `goal.scorer` that happens to return `0.5` must not be
 * misread as an explicit "waiting" checkpoint.
 */
export const GOAL_SCORER_ID = 'goal-scorer';

/**
 * Default goal-judge system prompt. Ported from MastraCode's `JUDGE_SYSTEM_PROMPT`
 * so the native goal scorer behaves like the original `/goal` judge. A
 * user-supplied `goal.prompt` (or per-objective `prompt`) overrides this.
 */
export const DEFAULT_GOAL_JUDGE_PROMPT = `You are the goal judge. Your decision directly controls whether the assistant continues working toward the goal.

Given a goal and the assistant's latest response, reason about whether the goal's requirements have been satisfied. Compare what the goal asks for against what the assistant has actually produced. Focus on substance, not phrasing.

Use "done" when the goal is fully achieved.
Use "waiting" when the goal explicitly requires a user checkpoint, user feedback, human verification, human confirmation, or another external event outside the goal-judge loop before the assistant should continue, and the assistant has correctly stopped at that checkpoint.
Use "waiting" when the latest user message asks a question or requests clarification and the latest assistant message answers it; let the user acknowledge the answer, ask a follow-up, or otherwise return control before continuing goal work. Use common sense and do not wait if the user explicitly asked the assistant to continue autonomously after answering.
Use "continue" when the goal is not done and the assistant should keep working autonomously, including when it asked for input that the goal did not explicitly require.
If your previous decision was "waiting" for an explicit user checkpoint, keep choosing "waiting" when the user's latest response asks a question, requests clarification, or otherwise does not satisfy the checkpoint. Do not continue until the required user feedback/confirmation/verification has actually been provided.
If the goal says to wait for the goal judge, judge, evaluator, or you to respond, approve, verify, validate, tell the assistant to continue, or otherwise provide the next signal, treat your own decision as that judge response. Verification can be performed by you unless the goal explicitly says it needs human/user verification. Choose "continue" when the assistant should proceed to the next step. Do not choose "waiting" for judge-controlled checkpoints, because that would mean waiting for yourself.

Your "reason" field is sent back to the assistant as guidance when the goal is not yet done — be specific about what still needs to be accomplished. When choosing "continue", write the reason as an instruction for what the assistant should do next. When choosing "waiting", explain what specific user checkpoint is still outstanding.`;

/**
 * Effective goal settings resolved per evaluation. `judgeModelId` is `undefined`
 * when neither the objective record nor the agent config supplies a judge model;
 * the goal step treats that as "do nothing".
 */
export interface EffectiveGoalSettings {
  judgeModelId: string | undefined;
  maxRuns: number;
  prompt: string;
  maxSteps: number | undefined;
}

/** The agent-level goal config the loop step resolves defaults from. */
export interface AgentGoalConfigDefaults {
  judgeModelId?: string;
  maxRuns?: number;
  prompt?: string;
  maxSteps?: number;
}

/**
 * Apply the precedence rule: ThreadState record value if present, else the
 * agent's `goal` config default, else a built-in default. A record only persists
 * the fields a caller explicitly provided, so unset fields fall back here.
 */
export function resolveEffectiveGoalSettings(
  record: GoalObjectiveRecord | undefined,
  agentDefaults: AgentGoalConfigDefaults | undefined,
): EffectiveGoalSettings {
  return {
    judgeModelId: record?.judgeModelId ?? agentDefaults?.judgeModelId,
    maxRuns: record?.maxRuns ?? agentDefaults?.maxRuns ?? DEFAULT_GOAL_MAX_RUNS,
    prompt: record?.prompt ?? agentDefaults?.prompt ?? DEFAULT_GOAL_JUDGE_PROMPT,
    maxSteps: agentDefaults?.maxSteps,
  };
}

// -----------------------------------------------------------------------------
// Thread-state store resolution
// -----------------------------------------------------------------------------

/**
 * Typed in terms of the storage domain's `GoalObjectiveRecord` (the storage
 * contract). The `threadState` domain defines the record so the storage layer
 * does not depend on this tools package; typing the store methods here means any
 * drift between shapes breaks the build at the read/write call sites rather than
 * silently passing the duck-typed `isThreadStateStore` guard.
 */
export type ResolvedGoalStore = {
  getState<T = unknown>(args: { resourceId: string; threadId: string; type: string }): Promise<T | undefined>;
  setState(args: { resourceId: string; threadId: string; type: string; value: GoalObjectiveRecord }): Promise<void>;
  deleteState(args: { resourceId: string; threadId: string; type: string }): Promise<void>;
  mutateState<T = unknown, TResult = void>(args: {
    resourceId: string;
    threadId: string;
    type: string;
    mutate: (current: T | undefined) => ThreadStateMutation<T, TResult>;
  }): Promise<TResult>;
};

function isThreadStateStore(value: unknown): value is ResolvedGoalStore {
  return (
    !!value &&
    typeof (value as ResolvedGoalStore).getState === 'function' &&
    typeof (value as ResolvedGoalStore).setState === 'function' &&
    typeof (value as ResolvedGoalStore).deleteState === 'function' &&
    typeof (value as ResolvedGoalStore).mutateState === 'function'
  );
}

/** Resolve the thread-scoped state store from a Mastra instance, if available. */
export async function resolveGoalStore(mastra: MastraUnion | undefined): Promise<ResolvedGoalStore | undefined> {
  const store = await mastra?.getStorage?.()?.getStore('threadState');
  return isThreadStateStore(store) ? store : undefined;
}

// -----------------------------------------------------------------------------
// Objective accessors
// -----------------------------------------------------------------------------

/** Read the current objective record for a thread from the store. */
export async function readObjective(
  store: ResolvedGoalStore | undefined,
  resourceId: string | undefined,
  threadId: string | undefined,
): Promise<GoalObjectiveRecord | undefined> {
  if (!store || !resourceId || !threadId) return undefined;
  return store.getState<GoalObjectiveRecord>({ resourceId, threadId, type: GOAL_STATE_TYPE });
}

/**
 * Persist an objective record for a thread, surfacing it on the RequestContext
 * so the state processor reflects the write in the same step.
 */
export async function writeObjective(
  store: ResolvedGoalStore | undefined,
  resourceId: string | undefined,
  threadId: string | undefined,
  record: GoalObjectiveRecord,
  requestContext?: RequestContext,
): Promise<void> {
  if (!store || !resourceId || !threadId) return;
  await store.setState({ resourceId, threadId, type: GOAL_STATE_TYPE, value: record });
  requestContext?.set(GOAL_REQUEST_CONTEXT_KEY, record);
}

/** Drop the objective for a thread. */
export async function clearObjective(
  store: ResolvedGoalStore | undefined,
  resourceId: string | undefined,
  threadId: string | undefined,
  requestContext?: RequestContext,
): Promise<void> {
  if (!store || !resourceId || !threadId) return;
  await store.deleteState({ resourceId, threadId, type: GOAL_STATE_TYPE });
  requestContext?.set(GOAL_REQUEST_CONTEXT_KEY, undefined);
}

// -----------------------------------------------------------------------------
// Atomic objective mutation
// -----------------------------------------------------------------------------
//
// `readObjective` + `writeObjective` is a read-modify-write. It is safe only
// when nothing else can touch the slot in between. Every caller that derives its
// next record from the current one (the goal steps, which read, then `await` a
// judge model call, then write; `updateObjectiveOptions`, which reads, merges,
// then writes) must instead decide inside `mutateState`, where the adapter
// serializes the read and the write against other writers.

/** What an objective mutator decided to do with the slot it was handed. */
export type ObjectiveMutation<TResult> =
  | { operation: 'set'; value: GoalObjectiveRecord; result: TResult }
  | { operation: 'keep'; result: TResult };

/**
 * Atomically read-modify-write the objective slot.
 *
 * `mutate` receives the record as it stands *inside* the store's lock, so it
 * must derive its new record from that argument and never from an earlier read.
 * It is synchronous (and may be re-invoked by adapters that retry on conflict),
 * so it must stay deterministic and side-effect free — no model calls, no I/O.
 */
export async function mutateObjective<TResult extends { record: GoalObjectiveRecord | undefined }>(
  store: ResolvedGoalStore | undefined,
  resourceId: string | undefined,
  threadId: string | undefined,
  mutate: (current: GoalObjectiveRecord | undefined) => ObjectiveMutation<TResult>,
  requestContext?: RequestContext,
): Promise<TResult | undefined> {
  if (!store || !resourceId || !threadId) return undefined;
  const result = await store.mutateState<GoalObjectiveRecord, TResult>({
    resourceId,
    threadId,
    type: GOAL_STATE_TYPE,
    mutate,
  });
  requestContext?.set(GOAL_REQUEST_CONTEXT_KEY, result.record);
  return result;
}

/** The judge's verdict for one goal evaluation, as the loop steps compute it. */
export interface GoalJudgeDecision {
  judgeFailed: boolean;
  judgeFailureReason?: string;
  complete: boolean;
  waiting: boolean;
}

/** Outcome of committing a judge verdict to the durable objective. */
export interface GoalEvaluationCommit {
  /**
   * `false` when the verdict was discarded because a concurrent writer owned the
   * slot. The caller must not treat the evaluation as having happened: stop the
   * auto-loop and leave the objective to whoever changed it.
   */
  applied: boolean;
  /** The record as it now stands, whether this commit wrote it or not. */
  record: GoalObjectiveRecord | undefined;
  runsUsed: number;
  maxRuns: number;
  maxRunsReached: boolean;
  status: GoalObjectiveRecord['status'];
  pausedReason: string | undefined;
}

/**
 * Identity of the objective a verdict belongs to. A `setObjective` mid-judge
 * replaces the record (new `id`) while leaving it `active`, so the status gate
 * alone would let the previous objective's verdict land on the new one.
 */
function isSameObjective(a: GoalObjectiveRecord, b: GoalObjectiveRecord): boolean {
  if (a.id !== undefined && b.id !== undefined) return a.id === b.id;
  return a.objective === b.objective && a.startedAt === b.startedAt;
}

/**
 * Commit one judge verdict to the durable objective, atomically.
 *
 * The budget increment and the status transition are derived from the record as
 * it stands at write time, not from the pre-judge read the verdict was computed
 * against — the judge call between them is an `await`, during which a user can
 * pause or clear the objective (TUI `/goal`), or a second worker resuming the
 * same run can land its own evaluation. Those writers are strictly fresher, so
 * they win: the verdict is dropped (`applied: false`) rather than clobbering
 * them back to a stale `active`/`runsUsed`.
 */
export async function commitGoalEvaluation(args: {
  store: ResolvedGoalStore | undefined;
  resourceId: string | undefined;
  threadId: string | undefined;
  requestContext?: RequestContext;
  /** The record the judge actually evaluated (this step's pre-judge read). */
  evaluated: GoalObjectiveRecord;
  agentDefaults: AgentGoalConfigDefaults | undefined;
  decision: GoalJudgeDecision;
}): Promise<GoalEvaluationCommit | undefined> {
  const { store, resourceId, threadId, requestContext, evaluated, agentDefaults, decision } = args;
  // Resolved out here: `mutate` must be deterministic across retries.
  const now = Date.now();

  return mutateObjective<GoalEvaluationCommit>(
    store,
    resourceId,
    threadId,
    current => {
      if (!current || current.status !== 'active' || !isSameObjective(current, evaluated)) {
        return {
          operation: 'keep',
          result: {
            applied: false,
            record: current,
            runsUsed: current?.runsUsed ?? evaluated.runsUsed,
            maxRuns: resolveEffectiveGoalSettings(current ?? evaluated, agentDefaults).maxRuns,
            maxRunsReached: false,
            status: current?.status ?? evaluated.status,
            pausedReason: current?.pausedReason,
          },
        };
      }

      // Budget is resolved from the live record too: a concurrent
      // `updateObjectiveOptions` may have raised `maxRuns` to keep the goal
      // going, and parking it on the stale budget would undo exactly that.
      const effective = resolveEffectiveGoalSettings(current, agentDefaults);
      const runsUsed = current.runsUsed + 1;
      const maxRunsReached = runsUsed >= effective.maxRuns;

      // Precedence: judge failure → paused; complete → done; budget exhausted →
      // paused. A "waiting" decision does NOT change the persisted status — the
      // record stays `active` so the next agent turn is still judged; only
      // `isContinued` is set to false (by the caller) to stop the auto-loop and
      // give the user a chance to provide input.
      let status: GoalObjectiveRecord['status'] = current.status;
      let pausedReason: string | undefined;
      if (decision.judgeFailed) {
        status = 'paused';
        pausedReason = decision.judgeFailureReason ?? 'The goal judge failed to evaluate the objective.';
      } else if (decision.complete) {
        status = 'done';
      } else if (maxRunsReached && !decision.waiting) {
        // Budget exhausted without reaching the goal: park it (visibly) instead
        // of leaving it `active` but stuck. Raising maxRuns + setting status
        // back to `active` (updateObjectiveOptions) resumes evaluation.
        status = 'paused';
        pausedReason = `Ran out of evaluation budget (${effective.maxRuns} runs) before reaching the goal — raise maxRuns to resume.`;
      }

      const value: GoalObjectiveRecord = {
        ...current,
        runsUsed,
        status,
        // Only persist a pause reason while parked; clear it otherwise so a
        // resumed/continuing objective does not carry a stale reason.
        pausedReason: status === 'paused' ? pausedReason : undefined,
        updatedAt: now,
      };

      return {
        operation: 'set',
        value,
        result: {
          applied: true,
          record: value,
          runsUsed,
          maxRuns: effective.maxRuns,
          maxRunsReached,
          status,
          pausedReason,
        },
      };
    },
    requestContext,
  );
}

function isGoalObjectiveRecord(value: unknown): value is GoalObjectiveRecord {
  return (
    !!value &&
    typeof (value as GoalObjectiveRecord).objective === 'string' &&
    typeof (value as GoalObjectiveRecord).status === 'string'
  );
}

/**
 * Read the within-turn objective a `setObjective` surfaced on the shared
 * RequestContext this step, if any. Returns `null` when the objective was
 * explicitly cleared this step, `undefined` when nothing was carried (so the
 * caller can fall back to the durable store).
 */
export function getObjectiveFromRequestContext(
  requestContext: RequestContext | undefined,
): GoalObjectiveRecord | null | undefined {
  if (!requestContext?.has?.(GOAL_REQUEST_CONTEXT_KEY)) return undefined;
  const carried = requestContext.get(GOAL_REQUEST_CONTEXT_KEY);
  if (carried === undefined) return null;
  return isGoalObjectiveRecord(carried) ? carried : undefined;
}
