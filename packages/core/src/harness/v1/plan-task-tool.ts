/**
 * Built-in plan-task tools (HARNESS_V1_SPEC.md §6.4 / §5.1k — TM-3).
 *
 * This is the ONLY model-facing mutation path for the durable `HarnessPlanTask`
 * tree. Every tool closes over the live parent `Session` and routes its write
 * through the session's plan-task methods, which fence on the session owner +
 * version (§5.8), run the TM-4 hierarchy semantics (rollup / cycle / single
 * in_progress), and emit `papersflow.plan_task.updated`. The tools never touch
 * storage directly and never act on any session but their own (the §6.4
 * calling-session ownership rule: a subagent's tools mutate the subagent's tree).
 *
 * Following the `spawn_subagent` convention, validation/rejection results travel
 * as `isError` tool payloads rather than thrown errors, so a rejected mutation
 * (e.g. a cycle or a second in_progress) lets the agent recover and continue
 * without aborting the whole turn.
 */

import { z } from 'zod';

import { createTool } from '../../tools/tool';
import { sha256CanonicalJson } from './canonical-json';
import {
  HarnessError,
  HarnessSubagentConcurrencyLimitError,
  HarnessSubagentDepthExceededError,
  HarnessValidationError,
} from './errors';
import { projectHarnessPublicError } from './events';
import {
  PLAN_TASK_ACTIVE_FORM_MAX_BYTES,
  PLAN_TASK_CHECK_MAX_LIMIT,
  PLAN_TASK_CONTENT_MAX_BYTES,
  PLAN_TASK_DELEGATED_BODY_MAX_BYTES,
  PLAN_TASK_MAX_NODES,
} from './plan-task-session';
import type { Session } from './session';
import { harnessSubagentResultSummarySchema } from './terminal-subagent-result';

export const TASK_ADD_TOOL_ID = 'task_add';
export const TASK_DECOMPOSE_TOOL_ID = 'task_decompose';
export const TASK_REPARENT_TOOL_ID = 'task_reparent';
export const TASK_UPDATE_TOOL_ID = 'task_update';
export const TASK_COMPLETE_TOOL_ID = 'task_complete';
export const TASK_CHECK_TOOL_ID = 'plan_task_check';
/** TM-6 — durable subtask→subagent delegation (§5.1k / §5.6). Registered only
 * when the harness has subagent types configured. */
export const TASK_DELEGATE_TOOL_ID = 'task_delegate';

const statusEnum = z.enum(['pending', 'in_progress', 'blocked', 'completed', 'cancelled', 'failed']);

const planTaskViewShape = {
  taskId: z.string(),
  parentTaskId: z.string().optional(),
  order: z.number(),
  status: statusEnum,
  statusSource: z.enum(['explicit', 'derived']),
  content: z.string(),
  activeForm: z.string().optional(),
  priority: z.number().optional(),
  blockedBy: z.array(z.string()).optional(),
  /** TM-6: the subagent session this task was durably delegated to, if any. */
  delegatedSubagentSessionId: z.string().optional(),
  /** Echoed by task_decompose to map a call-local child key to its generated taskId. */
  decompositionLocalKey: z.string().optional(),
  delegation: z
    .object({
      subagentSessionId: z.string(),
      attemptId: z.string(),
      parentToolCallId: z.string(),
      parentRunId: z.string().optional(),
      status: z.enum(['running', 'completed', 'blocked', 'failed']),
      startedAt: z.number(),
      deadlineAt: z.number(),
      result: harnessSubagentResultSummarySchema.optional(),
      settledAt: z.number().optional(),
    })
    .optional(),
};

/** Shared error fields appended to every tool's output schema. */
const errorShape = {
  isError: z.boolean().optional(),
  errorName: z.string().optional(),
  code: z.string().optional(),
  field: z.string().optional(),
  reason: z.string().optional(),
  message: z.string().optional(),
};

interface ToolError {
  isError: true;
  errorName: string;
  code?: string;
  field?: string;
  reason?: string;
  message: string;
}

/** Project a thrown harness error into the `isError` payload shape. */
function toToolError(err: unknown): ToolError {
  if (err instanceof HarnessValidationError) {
    return {
      isError: true,
      errorName: err.name,
      field: err.field,
      reason: err.reason,
      message: err.message,
    };
  }
  // These are constructed framework control-flow errors intended to help the
  // model choose a smaller/shallower delegation strategy. Preserve their safe
  // names and messages; arbitrary Error subclasses still redact below.
  if (err instanceof HarnessSubagentConcurrencyLimitError || err instanceof HarnessSubagentDepthExceededError) {
    return {
      isError: true,
      errorName: err.name,
      message: err.message,
    };
  }
  const projected = projectHarnessPublicError(err);
  const errorName =
    err instanceof HarnessError || (projected.code !== 'harness.internal' && err instanceof Error)
      ? err.name
      : 'HarnessError';
  return {
    isError: true,
    errorName,
    code: projected.code,
    message: projected.message,
  };
}

/**
 * Build the plan-task tools scoped to one session. Always returns the full set —
 * unlike `spawn_subagent`, plan tasks are universally available to a v1 agent.
 */
export function createPlanTaskTools(session: Session): Record<string, ReturnType<typeof createTool>> {
  const taskAdd = createTool({
    id: TASK_ADD_TOOL_ID,
    description:
      'Add one task to the plan tree. Optionally nest it under `parentTaskId`, order it among siblings, ' +
      'set a priority, an `activeForm` (present-continuous label), an initial status, or `blockedBy` ' +
      'dependencies on other task ids. Returns the created task.',
    inputSchema: z.object({
      content: z.string().min(1).max(PLAN_TASK_CONTENT_MAX_BYTES).describe('Concise imperative task title.'),
      parentTaskId: z.string().optional().describe('Nest under this existing task.'),
      order: z.number().optional().describe('Sibling order; defaults to append.'),
      priority: z.number().optional(),
      activeForm: z
        .string()
        .max(PLAN_TASK_ACTIVE_FORM_MAX_BYTES)
        .optional()
        .describe('Present-continuous label shown while in progress.'),
      status: statusEnum.optional().describe('Initial status; defaults to pending.'),
      blockedBy: z.array(z.string()).max(PLAN_TASK_MAX_NODES).optional().describe('Task ids this task depends on.'),
    }),
    outputSchema: z
      .object({ ...planTaskViewShape, ...errorShape })
      .partial({ taskId: true, order: true, status: true, statusSource: true, content: true }),
    execute: async (input, context) => {
      try {
        const agentContext = context?.agent;
        const idempotencyKey =
          agentContext?.toolCallId === undefined ||
          typeof agentContext.agentId !== 'string' ||
          !Array.isArray(agentContext.messages)
            ? undefined
            : `plan-task-add:${sha256CanonicalJson({
                runId: agentContext.runId ?? 'missing',
                toolCallId: agentContext.toolCallId,
              }).slice(0, 40)}`;
        return await session._planTaskAdd(
          { ...input, ...(idempotencyKey === undefined ? {} : { idempotencyKey }) },
          context?.abortSignal,
        );
      } catch (err) {
        return toToolError(err) as any;
      }
    },
  });

  const taskDecompose = createTool({
    id: TASK_DECOMPOSE_TOOL_ID,
    description:
      'Decompose a parent task into multiple child tasks in one atomic step. The parent becomes a ' +
      'derived-status node whose status rolls up from its children. Returns the created children.',
    inputSchema: z.object({
      parentTaskId: z.string().describe('Existing task to decompose.'),
      children: z
        .array(
          z.object({
            content: z.string().min(1).max(PLAN_TASK_CONTENT_MAX_BYTES),
            order: z.number().optional(),
            priority: z.number().optional(),
            activeForm: z.string().max(PLAN_TASK_ACTIVE_FORM_MAX_BYTES).optional(),
            localKey: z
              .string()
              .min(1)
              .max(64)
              .optional()
              .describe('Call-local child label used by blockedByLocalKeys; must be unique in this decompose.'),
            blockedBy: z.array(z.string()).max(PLAN_TASK_MAX_NODES).optional(),
            blockedByLocalKeys: z
              .array(z.string().min(1).max(64))
              .max(PLAN_TASK_MAX_NODES)
              .optional()
              .describe('Sibling localKey values this child depends on in this same atomic decompose.'),
          }),
        )
        .min(1)
        .max(PLAN_TASK_MAX_NODES)
        .describe('Child tasks to create under the parent.'),
    }),
    outputSchema: z.object({
      children: z.array(z.object(planTaskViewShape)).optional(),
      ...errorShape,
    }),
    execute: async (input, context) => {
      try {
        const children = await session._planTaskDecompose(input.parentTaskId, input.children, context?.abortSignal);
        return { children } as any;
      } catch (err) {
        return toToolError(err) as any;
      }
    },
  });

  const taskReparent = createTool({
    id: TASK_REPARENT_TOOL_ID,
    description:
      'Move a task (and its whole subtree) under a new parent, or to the root by passing ' +
      '`newParentTaskId: null`. Rejects moves that would create a cycle (a task cannot become its own ' +
      'ancestor).',
    inputSchema: z.object({
      taskId: z.string(),
      newParentTaskId: z.string().nullable().describe('New parent task id, or null to move to root.'),
      order: z.number().optional(),
    }),
    outputSchema: z.object({ ok: z.boolean().optional(), ...errorShape }),
    execute: async (input, context) => {
      try {
        await session._planTaskReparent(input.taskId, input.newParentTaskId, input.order, context?.abortSignal);
        return { ok: true } as any;
      } catch (err) {
        return toToolError(err) as any;
      }
    },
  });

  const taskUpdate = createTool({
    id: TASK_UPDATE_TOOL_ID,
    description:
      "Update a task's status, content, priority, activeForm, or blockedBy dependencies. Setting " +
      'status to in_progress is rejected if another task in the same root is already in progress. ' +
      'Setting blockedBy is rejected if it would create a dependency cycle.',
    inputSchema: z.object({
      taskId: z.string(),
      status: statusEnum.optional(),
      content: z.string().min(1).max(PLAN_TASK_CONTENT_MAX_BYTES).optional(),
      priority: z.number().optional(),
      activeForm: z.string().max(PLAN_TASK_ACTIVE_FORM_MAX_BYTES).optional(),
      blockedBy: z.array(z.string()).max(PLAN_TASK_MAX_NODES).optional(),
    }),
    outputSchema: z
      .object({ ...planTaskViewShape, ...errorShape })
      .partial({ taskId: true, order: true, status: true, statusSource: true, content: true }),
    execute: async (input, context) => {
      const { taskId, ...patch } = input;
      try {
        return await session._planTaskUpdate(taskId, patch, context?.abortSignal);
      } catch (err) {
        return toToolError(err) as any;
      }
    },
  });

  const taskComplete = createTool({
    id: TASK_COMPLETE_TOOL_ID,
    description:
      'Mark a task completed. Triggers status rollup on its ancestors (a parent whose children are all ' +
      'completed becomes completed).',
    inputSchema: z.object({ taskId: z.string() }),
    outputSchema: z
      .object({ ...planTaskViewShape, ...errorShape })
      .partial({ taskId: true, order: true, status: true, statusSource: true, content: true }),
    execute: async (input, context) => {
      try {
        return await session._planTaskComplete(input.taskId, context?.abortSignal);
      } catch (err) {
        return toToolError(err) as any;
      }
    },
  });

  const taskCheck = createTool({
    id: TASK_CHECK_TOOL_ID,
    description:
      'Read a BOUNDED slice of the plan tree to re-orient: the next-N tasks under `rootTaskId` (or the ' +
      'roots when omitted), limited by `depth` and optionally filtered by `status`. Never returns the ' +
      'whole tree by default — call repeatedly to walk deeper. Completed delegated tasks carry their ' +
      'bounded durable delegation result; read and synthesize it before answering.',
    inputSchema: z.object({
      rootTaskId: z.string().optional().describe('Walk from this task; omit for the plan roots.'),
      depth: z.number().optional().describe('Max depth from the root (0 = just the root level).'),
      status: statusEnum.optional().describe('Only return tasks with this status.'),
      limit: z
        .number()
        .int()
        .positive()
        .max(PLAN_TASK_CHECK_MAX_LIMIT)
        .optional()
        .describe(`Max tasks to return (default 25, hard cap ${PLAN_TASK_CHECK_MAX_LIMIT}).`),
      cursor: z
        .string()
        .min(1)
        .max(512)
        .optional()
        .describe('Opaque nextCursor from the previous check; keep all other filters unchanged.'),
    }),
    outputSchema: z.object({
      tasks: z.array(z.object(planTaskViewShape)).optional(),
      truncated: z.boolean().optional(),
      nextCursor: z.string().optional(),
      ...errorShape,
    }),
    execute: async input => {
      try {
        return (await session._planTaskCheck(input)) as any;
      } catch (err) {
        return toToolError(err) as any;
      }
    },
  });

  // TM-6 — `task_delegate`: durably hand a plan task (and optionally its subtree
  // subset) to a subagent SESSION. Distinct from `spawn_subagent` (a synchronous
  // in-turn child): the parent turn does NOT block; the plan task carries the
  // durable `delegatedSubagentSessionId` link and rolls up (completed/failed)
  // when the subagent session terminalizes, across turns/restarts. Registered
  // only when the harness has subagent types configured (mirrors spawn_subagent).
  const harness = (
    session as unknown as {
      _harness?: {
        _listSubagentTypeIds(options: { invocation: 'inline' | 'delegated' }): string[];
      };
    }
  )._harness;
  const subagentTypeIds = harness?._listSubagentTypeIds({ invocation: 'delegated' }) ?? [];
  const taskDelegate =
    subagentTypeIds.length === 0
      ? undefined
      : createTool({
          id: TASK_DELEGATE_TOOL_ID,
          description:
            'Durably DELEGATE an existing plan task to a specialized subagent session. Unlike ' +
            'spawn_subagent (which runs inline and blocks this turn), delegation hands the task off ' +
            'to a subagent session whose completion this plan task tracks across turns: the task ' +
            'stays in_progress until the subagent finishes, then rolls up completed or failed. Pass ' +
            '`includeSubtree: true` to delegate the task and its descendants as one unit. Available ' +
            'agent types: ' +
            subagentTypeIds.join(', ') +
            '.',
          inputSchema: z.object({
            taskId: z.string().describe('Existing plan task to delegate.'),
            agentType: z.enum(subagentTypeIds as [string, ...string[]]).describe('Registered subagent type to run.'),
            task: z
              .string()
              .min(1)
              .max(PLAN_TASK_DELEGATED_BODY_MAX_BYTES)
              .optional()
              .describe('Self-contained task description for the subagent. Defaults to the plan task title.'),
            includeSubtree: z
              .boolean()
              .optional()
              .describe('Delegate the task AND its descendants as one unit (surfaced to the subagent).'),
            modelOverride: z.string().optional().describe('Model id override; falls back to the subagent default.'),
          }),
          outputSchema: z
            .object({ ...planTaskViewShape, subagentSessionId: z.string().optional(), ...errorShape })
            .partial({ taskId: true, order: true, status: true, statusSource: true, content: true }),
          execute: async (input, context) => {
            try {
              return (await session._planTaskDelegate(
                input,
                context?.abortSignal,
                context?.agent?.toolCallId,
                context?.agent?.runId,
                context?.requestContext,
              )) as any;
            } catch (err) {
              return toToolError(err) as any;
            }
          },
        });

  return {
    [TASK_ADD_TOOL_ID]: taskAdd,
    [TASK_DECOMPOSE_TOOL_ID]: taskDecompose,
    [TASK_REPARENT_TOOL_ID]: taskReparent,
    [TASK_UPDATE_TOOL_ID]: taskUpdate,
    [TASK_COMPLETE_TOOL_ID]: taskComplete,
    [TASK_CHECK_TOOL_ID]: taskCheck,
    ...(taskDelegate ? { [TASK_DELEGATE_TOOL_ID]: taskDelegate } : {}),
  };
}
