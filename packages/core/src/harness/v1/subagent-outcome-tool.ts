import { z } from 'zod';

import { createTool } from '../../tools/tool';
import {
  HARNESS_SUBAGENT_OUTCOME_REPORT_KIND,
  HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID,
  MAX_HARNESS_SUBAGENT_DIRECT_ANSWER_BYTES,
  harnessSubagentOutcomeEvidenceSchema,
  harnessSubagentOutcomeIssueSchema,
  harnessSubagentOutcomeReportSchema,
  harnessSubagentOutcomeSchema,
} from './terminal-subagent-result';

const harnessSubagentOutcomeInputSchema = z
  .object({
    outcome: harnessSubagentOutcomeSchema.describe(
      'completed only when the assigned task is actually done; blocked for an external/dependency failure; failed for an unrecoverable task failure',
    ),
    summary: z
      .string()
      .min(1)
      .max(48 * 1024)
      .describe('Bounded result the parent can directly synthesize.'),
    evidence: z
      .array(harnessSubagentOutcomeEvidenceSchema)
      .min(1)
      .max(24)
      .describe('Concrete tool, artifact, source, or analysis evidence supporting the outcome.'),
    issue: harnessSubagentOutcomeIssueSchema.optional().describe('Required for blocked/failed; omitted for completed.'),
  })
  .strict()
  .superRefine((report, ctx) => {
    if (report.outcome === 'completed' && report.issue !== undefined) {
      ctx.addIssue({ code: z.ZodIssueCode.custom, message: 'completed reports cannot include issue' });
    }
    if (report.outcome !== 'completed' && report.issue === undefined) {
      ctx.addIssue({ code: z.ZodIssueCode.custom, message: 'blocked/failed reports require issue' });
    }
    if (
      new TextEncoder().encode(JSON.stringify({ kind: HARNESS_SUBAGENT_OUTCOME_REPORT_KIND, ...report })).byteLength >
      MAX_HARNESS_SUBAGENT_DIRECT_ANSWER_BYTES
    ) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: `subagent outcome report exceeds ${MAX_HARNESS_SUBAGENT_DIRECT_ANSWER_BYTES} UTF-8 bytes`,
      });
    }
  });

/**
 * Terminal subagent-only tool. Its structured value becomes the child run's
 * terminal tool result without another model call, giving live and recovered
 * delegation drivers one durable semantic settlement contract.
 */
export function createSubagentOutcomeReportTool() {
  return createTool({
    id: HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID,
    description:
      'Finish this subagent assignment. Call this exactly once, by itself, as the final action. ' +
      'Use completed only when the requested work is actually complete and supported by evidence. ' +
      'Use blocked for external service/dependency/input failures that prevent completion, and failed for unrecoverable work failure. ' +
      'A normal text answer or provider stop does not complete a delegated task.',
    inputSchema: harnessSubagentOutcomeInputSchema,
    outputSchema: harnessSubagentOutcomeReportSchema,
    execute: async input => ({ kind: HARNESS_SUBAGENT_OUTCOME_REPORT_KIND, ...input }),
    terminalResult: {
      isSuccess: output => harnessSubagentOutcomeReportSchema.safeParse(output).success,
      outputSchema: harnessSubagentOutcomeReportSchema,
      project: output => output,
      maxBytes: MAX_HARNESS_SUBAGENT_DIRECT_ANSWER_BYTES,
    },
  });
}
