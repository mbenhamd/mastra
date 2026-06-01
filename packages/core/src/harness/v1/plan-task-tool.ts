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
import { HarnessValidationError } from './errors';
import { PLAN_TASK_CHECK_MAX_LIMIT } from './plan-task-session';
import type { PlanTaskView } from './plan-task-session';
import type { Session } from './session';

export const TASK_ADD_TOOL_ID = 'task_add';
export const TASK_DECOMPOSE_TOOL_ID = 'task_decompose';
export const TASK_REPARENT_TOOL_ID = 'task_reparent';
export const TASK_UPDATE_TOOL_ID = 'task_update';
export const TASK_COMPLETE_TOOL_ID = 'task_complete';
export const TASK_CHECK_TOOL_ID = 'plan_task_check';
/** TM-6 — durable subtask→subagent delegation (§5.1k / §5.6). Registered only
 * when the harness has subagent types configured. */
export const TASK_DELEGATE_TOOL_ID = 'task_delegate';
/** Back-compat alias: maps to add (no taskId) / update (taskId) semantics. */
export const TASK_WRITE_TOOL_ID = 'task_write';

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
};

/** Shared error fields appended to every tool's output schema. */
const errorShape = {
  isError: z.boolean().optional(),
  errorName: z.string().optional(),
  field: z.string().optional(),
  reason: z.string().optional(),
  message: z.string().optional(),
};

interface ToolError {
  isError: true;
  errorName: string;
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
  const e = err as { name?: string; message?: string };
  return {
    isError: true,
    errorName: e?.name ?? 'Error',
    message: e?.message ?? String(err),
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
      content: z.string().describe('Imperative task title.'),
      parentTaskId: z.string().optional().describe('Nest under this existing task.'),
      order: z.number().optional().describe('Sibling order; defaults to append.'),
      priority: z.number().optional(),
      activeForm: z.string().optional().describe('Present-continuous label shown while in progress.'),
      status: statusEnum.optional().describe('Initial status; defaults to pending.'),
      blockedBy: z.array(z.string()).optional().describe('Task ids this task depends on.'),
    }),
    outputSchema: z.object({ ...planTaskViewShape, ...errorShape }).partial({ taskId: true, order: true, status: true, statusSource: true, content: true }),
    execute: async input => {
      try {
        return await session._planTaskAdd(input);
      } catch (err) {
        return toToolError(err) as any;
      }
    },
  });

  const taskDecompose = createTool({
    id: TASK_DECOMPOSE_TOOL_ID,
    description:
      'Decompose a parent task into multiple child tasks in one atomic step. The parent becomes a ' +
      "derived-status node whose status rolls up from its children. Returns the created children.",
    inputSchema: z.object({
      parentTaskId: z.string().describe('Existing task to decompose.'),
      children: z
        .array(
          z.object({
            content: z.string(),
            order: z.number().optional(),
            priority: z.number().optional(),
            activeForm: z.string().optional(),
            blockedBy: z.array(z.string()).optional(),
          }),
        )
        .min(1)
        .describe('Child tasks to create under the parent.'),
    }),
    outputSchema: z.object({
      children: z.array(z.object(planTaskViewShape)).optional(),
      ...errorShape,
    }),
    execute: async input => {
      try {
        const children = await session._planTaskDecompose(input.parentTaskId, input.children);
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
    execute: async input => {
      try {
        await session._planTaskReparent(input.taskId, input.newParentTaskId, input.order);
        return { ok: true } as any;
      } catch (err) {
        return toToolError(err) as any;
      }
    },
  });

  const taskUpdate = createTool({
    id: TASK_UPDATE_TOOL_ID,
    description:
      'Update a task\'s status, content, priority, activeForm, or blockedBy dependencies. Setting ' +
      'status to in_progress is rejected if another task in the same root is already in progress. ' +
      'Setting blockedBy is rejected if it would create a dependency cycle.',
    inputSchema: z.object({
      taskId: z.string(),
      status: statusEnum.optional(),
      content: z.string().optional(),
      priority: z.number().optional(),
      activeForm: z.string().optional(),
      blockedBy: z.array(z.string()).optional(),
    }),
    outputSchema: z.object({ ...planTaskViewShape, ...errorShape }).partial({ taskId: true, order: true, status: true, statusSource: true, content: true }),
    execute: async input => {
      const { taskId, ...patch } = input;
      try {
        return await session._planTaskUpdate(taskId, patch);
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
    outputSchema: z.object({ ...planTaskViewShape, ...errorShape }).partial({ taskId: true, order: true, status: true, statusSource: true, content: true }),
    execute: async input => {
      try {
        return await session._planTaskComplete(input.taskId);
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
      'whole tree by default — call repeatedly to walk deeper.',
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
    }),
    outputSchema: z.object({
      tasks: z.array(z.object(planTaskViewShape)).optional(),
      truncated: z.boolean().optional(),
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
  const harness = (session as unknown as { _harness?: { _listSubagentTypeIds(): string[] } })._harness;
  const subagentTypeIds = harness?._listSubagentTypeIds() ?? [];
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
          execute: async input => {
            try {
              return (await session._planTaskDelegate(input)) as any;
            } catch (err) {
              return toToolError(err) as any;
            }
          },
        });

  // Back-compat `task_write`. Two accepted shapes:
  //   1. LEGACY mastracode todo-list shape `{ tasks: [{content,status,activeForm}] }` —
  //      a FULL list pass that replaces the prior list (the original tool's contract:
  //      "Pass the FULL task list each time"). Reconciled against the existing root
  //      tasks by `content`: each incoming task updates the matching root's status /
  //      activeForm, or is added when no root matches. Tasks the model dropped are
  //      left intact (no destructive delete) — the tree is the durable record.
  //   2. SINGLE-task shape `{ content?, taskId?, ... }` — with a `taskId` it updates
  //      that task; without one it adds a new task.
  // Both map onto the same add/update semantics, so an agent trained on either the
  // legacy list tool OR the single-task name keeps working against the tree.
  const legacyTaskItem = z.object({
    content: z.string(),
    status: statusEnum.optional(),
    activeForm: z.string().optional(),
  });
  const taskWrite = createTool({
    id: TASK_WRITE_TOOL_ID,
    description:
      'Track plan tasks. Either pass the FULL task list as `tasks: [{content, status, activeForm}]` ' +
      '(legacy todo-list form — reconciles each task against the plan by content), or write a single ' +
      'task: provide `taskId` to update an existing one, omit it to add a new one. Prefer ' +
      '`task_add` / `task_update` for clarity; this exists for back-compat.',
    inputSchema: z.object({
      tasks: z.array(legacyTaskItem).optional().describe('Legacy full task list (replaces the prior list).'),
      taskId: z.string().optional(),
      content: z.string().optional(),
      parentTaskId: z.string().optional(),
      order: z.number().optional(),
      priority: z.number().optional(),
      activeForm: z.string().optional(),
      status: statusEnum.optional(),
      blockedBy: z.array(z.string()).optional(),
    }),
    outputSchema: z
      .object({ ...planTaskViewShape, tasks: z.array(z.object(planTaskViewShape)).optional(), ...errorShape })
      .partial({ taskId: true, order: true, status: true, statusSource: true, content: true }),
    execute: async input => {
      try {
        // Legacy full-list shape: reconcile each incoming task by content.
        if (input.tasks !== undefined) {
          // Existing roots (bounded read; the legacy list is a flat root-level set).
          const existing = await session._planTaskCheck({ depth: 0, limit: PLAN_TASK_CHECK_MAX_LIMIT });
          const byContent = new Map<string, string>();
          for (const t of existing.tasks) {
            if (!byContent.has(t.content)) byContent.set(t.content, t.taskId);
          }
          const out: PlanTaskView[] = [];
          for (const item of input.tasks) {
            const existingId = byContent.get(item.content);
            if (existingId !== undefined) {
              out.push(
                await session._planTaskUpdate(existingId, { status: item.status, activeForm: item.activeForm }),
              );
            } else {
              const added = await session._planTaskAdd({
                content: item.content,
                status: item.status,
                activeForm: item.activeForm,
              });
              byContent.set(item.content, added.taskId);
              out.push(added);
            }
          }
          return { tasks: out } as any;
        }
        if (input.taskId !== undefined) {
          const { taskId, content, status, priority, activeForm, blockedBy } = input;
          return await session._planTaskUpdate(taskId, { content, status, priority, activeForm, blockedBy });
        }
        if (input.content === undefined) {
          throw new HarnessValidationError(
            'content',
            'task_write requires either `tasks` (legacy list), a `taskId` to update, or `content` to add a task',
          );
        }
        return await session._planTaskAdd({
          content: input.content,
          parentTaskId: input.parentTaskId,
          order: input.order,
          priority: input.priority,
          activeForm: input.activeForm,
          status: input.status,
          blockedBy: input.blockedBy,
        });
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
    [TASK_WRITE_TOOL_ID]: taskWrite,
    ...(taskDelegate ? { [TASK_DELEGATE_TOOL_ID]: taskDelegate } : {}),
  };
}
