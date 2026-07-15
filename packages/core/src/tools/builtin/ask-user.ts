import { z } from 'zod/v4';

import { createTool } from '../tool';
import { ASK_USER_TOOL_ID, askUserOptionSchema, askUserSelectionModeSchema } from './shared';

/**
 * A structured choice rendered by the host for an `ask_user` prompt.
 *
 * The label is the value returned to the model when the option is selected. The
 * optional description gives the host more context without changing the answer value.
 */
export interface AskUserOption {
  label: string;
  description?: string;
}

/**
 * Controls whether an `ask_user` prompt accepts one choice or multiple choices.
 *
 * `single_select` is the default for prompts that provide options, preserving the
 * original one-answer behavior. `multi_select` tells the host that the user may choose
 * more than one option and resume with those selections as an array.
 */
export type AskUserSelectionMode = 'single_select' | 'multi_select';

/**
 * Answer shape used to resume a suspended `ask_user` call.
 *
 * Free-text and single-select prompts resume with a string. Multi-select prompts
 * resume with a string array containing each selected option label.
 */
export type AskUserAnswer = string | string[];

/**
 * Payload carried by the native `tool-call-suspended` event when `ask_user` pauses.
 * Hosts read this to render the question, choices, and selection mode.
 */
export interface AskUserSuspendPayload {
  question: string;
  options?: AskUserOption[];
  selectionMode?: AskUserSelectionMode;
}

const optionSchema = z.object({
  label: z.string().describe('Short display text for this option (1-5 words)'),
  description: z.string().optional().describe('Explanation of what this option means'),
});

/**
 * Shape of the Harness slot exposed on the request context. When present, the
 * built-in question tools pre-register their pending interaction durably
 * ([PF-348]) so a crash between `suspend()` and event emission still recovers
 * `pendingResume` from storage.
 */
type HarnessQuestionSlot = {
  registerQuestion?: (params: {
    questionId: string;
    question: string;
    options?: Array<{ label: string; description?: string }>;
    selectionMode?: AskUserSelectionMode;
    runId?: string;
    toolCallId?: string;
  }) => Promise<void>;
};

/**
 * Converts the resume answer into the text returned to the model after `ask_user`
 * resumes. Free-text and single-select prompts already produce a single string,
 * while multi-select prompts resume with an array of selected labels that must be
 * flattened before the tool result is added back into the generation context.
 *
 * The formatter keeps the model-facing output compact by joining multi-select
 * answers with commas, mirroring the single-answer behavior while still preserving
 * every selected option in a readable form.
 */
export function formatQuestionAnswer(answer: AskUserAnswer): string {
  return Array.isArray(answer) ? answer.join(', ') : answer;
}

/**
 * Built-in, agent-agnostic tool: ask the user a question and wait for their response.
 *
 * The tool supports three prompt shapes. Omitting `options` asks an open-ended
 * free-text question. Providing `options` without `selectionMode` asks the host to
 * render a single-select prompt for backwards compatibility. Providing
 * `selectionMode: 'multi_select'` lets the host resume with multiple selected option
 * labels as a string array.
 *
 * Pausing uses the agent-native tool suspension primitive: the tool calls
 * `suspend({ question, options, selectionMode })`, which makes the agent emit a
 * `tool-call-suspended` event and persist run state. The host renders the question,
 * collects the user's answer, and continues the run via `agent.resumeStream(answer)`;
 * the tool re-runs with `resumeData` set to the answer and returns it to the model.
 *
 * When executed without an agent `suspend` (e.g. direct invocation outside an agent
 * run), the tool returns a readable fallback prompt so the question and choices are
 * still surfaced.
 */
export const askUserTool = createTool({
  id: ASK_USER_TOOL_ID,
  description:
    'Ask the user a question and wait for their response. Use this when you need clarification, want to validate assumptions, or need the user to make a decision between options. Provide options for structured choices (2-4 options), or omit them for open-ended questions. Use selectionMode to choose whether the user can pick one option or multiple options.',
  inputSchema: z.object({
    question: z.string().min(1).describe('The question to ask the user. Should be clear and specific.'),
    options: z
      .array(optionSchema)
      .optional()
      .describe('Optional choices. If provided, shows a selection list. If omitted, shows a free-text input.'),
    selectionMode: z
      .enum(['single_select', 'multi_select'])
      .optional()
      .describe(
        'Controls how many provided options the user can select. Defaults to single_select when options are provided. Requires options.',
      ),
  }),
  suspendSchema: z.object({
    question: z.string(),
    options: z.array(optionSchema).optional(),
    selectionMode: z.enum(['single_select', 'multi_select']).optional(),
  }),
  resumeSchema: z.union([z.string(), z.array(z.string())]),
  execute: async ({ question, options, selectionMode }, context) => {
    try {
      if (selectionMode && !options?.length) {
        return {
          content: 'Failed to ask user: selectionMode requires options.',
          isError: true,
        };
      }

      const resolvedSelectionMode = options?.length ? (selectionMode ?? 'single_select') : undefined;

      const resumeData = context?.agent?.resumeData as AskUserAnswer | undefined;
      if (resumeData !== undefined) {
        return { content: `User answered: ${formatQuestionAnswer(resumeData)}`, isError: false };
      }

      const suspend = context?.agent?.suspend;
      if (suspend) {
        // [PF-348] Durable Harness registration (additive to native suspension):
        // pre-register the pending question so it survives a crash between
        // suspend() and event emission.
        const harness = context?.requestContext?.get('harness') as HarnessQuestionSlot | undefined;
        if (harness?.registerQuestion && context?.agent?.runId) {
          await harness.registerQuestion({
            questionId: context.agent.toolCallId,
            question,
            ...(options ? { options } : {}),
            ...(resolvedSelectionMode ? { selectionMode: resolvedSelectionMode } : {}),
            runId: context.agent.runId,
            toolCallId: context.agent.toolCallId,
          });
        }
        await suspend({ question, options, selectionMode: resolvedSelectionMode });
        return;
      }

      // No agent context available: surface the question as readable text so non-agent
      // execution paths still expose the question and available choices to the model.
      return {
        content: `[Question for user]: ${question}${
          options?.length ? '\nOptions: ' + options.map(o => o.label).join(', ') : ''
        }${resolvedSelectionMode ? '\nSelection mode: ' + resolvedSelectionMode : ''}`,
        isError: false,
      };
    } catch (error) {
      const msg = error instanceof Error ? error.message : 'Unknown error';
      return { content: `Failed to ask user: ${msg}`, isError: true };
    }
  },
});

const askUserInputSchema = z.object({
  question: z.string().describe('The question to ask the user. Should be clear and specific.'),
  options: z
    .array(askUserOptionSchema)
    .optional()
    .describe('Optional choices. If provided, shows a selection list. If omitted, shows a free-text input.'),
  selectionMode: askUserSelectionModeSchema
    .optional()
    .describe('Controls how many options the user can select. Defaults to single_select when options are provided.'),
});

const askUserOutputSchema = z.object({
  answer: z
    .unknown()
    .describe("The user's response. Free-text string for open questions, label(s) for option selections."),
});

const askUserResumeSchema = askUserOutputSchema;

/**
 * `askUser` — pause execution and ask the user a question ([PF-348] Harness lane).
 *
 * Calls `ctx.agent.suspend()` on first invocation, halting the agent loop.
 * The caller resumes with `{ answer }` (matching `resumeSchema`). Outside a
 * Harness this is `agent.resumeStream({ answer })`; inside a Harness it is
 * `Session.respondToQuestion({ answer })`.
 *
 * Unlike `askUserTool` (which carries the question in the suspend payload and
 * formats the resumed answer for the model), this variant durably registers the
 * question with the Harness before suspending and passes the `{ answer }`
 * resume payload through unchanged.
 *
 * Harness routing keys off the tool ID (`ASK_USER_TOOL_ID`).
 */
export const askUser = createTool({
  id: ASK_USER_TOOL_ID,
  description: 'Ask the user a question and wait for their response. Use for clarification, validation, or decisions.',
  inputSchema: askUserInputSchema,
  outputSchema: askUserOutputSchema,
  suspendSchema: z.object({}),
  resumeSchema: askUserResumeSchema,
  execute: async (_input, ctx) => {
    const input = _input as z.infer<typeof askUserInputSchema>;
    const resumeData = ctx.agent?.resumeData as z.infer<typeof askUserResumeSchema> | undefined;
    if (resumeData !== undefined) return resumeData;

    if (!ctx.agent?.suspend) {
      throw new Error(`${ASK_USER_TOOL_ID} requires an agent execution context with suspend support.`);
    }

    const harness = ctx.requestContext?.get('harness') as HarnessQuestionSlot | undefined;
    if (harness?.registerQuestion && ctx.agent.runId) {
      await harness.registerQuestion({
        questionId: ctx.agent.toolCallId,
        question: input.question,
        ...(input.options ? { options: input.options } : {}),
        ...(input.selectionMode ? { selectionMode: input.selectionMode } : {}),
        runId: ctx.agent.runId,
        toolCallId: ctx.agent.toolCallId,
      });
    }

    await ctx.agent.suspend({});
    // suspend() never returns normally — the next invocation receives resumeData.
    return { answer: undefined };
  },
});
