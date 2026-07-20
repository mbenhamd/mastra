/**
 * Built-in `spawn_subagent` tool (HARNESS_V1_SPEC.md §9).
 *
 * Registered on every session by `Session._buildToolsets()` when
 * `HarnessConfig.subagents.types` is non-empty. The factory closes over
 * the parent `Session` so the tool can:
 *
 *   - resolve the `agentType` against the parent harness's registry,
 *   - create a fresh subagent-tool child session via `harness.session(...)`,
 *   - subscribe to the child's turn events and re-emit them as `subagent_*`
 *     on the parent via `parent._emitSubagentEvent(...)`,
 *   - close the child after the child's turn settles (cascade rule §5.6
 *     does the same on shutdown).
 *
 * Errors travel as tool-error payloads, never thrown, so the parent agent
 * can recover and continue without aborting the whole turn.
 */

import { z } from 'zod';

import { createTool } from '../../tools/tool';
import { HarnessSubagentDepthExceededError, HarnessValidationError } from './errors';
import { projectHarnessPublicError } from './events';
import type { Session } from './session';
import {
  HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID,
  harnessSubagentResultSummarySchema,
  harnessSubagentDirectAnswerSchema,
  MAX_HARNESS_SUBAGENT_DIRECT_ANSWER_BYTES,
  parseHarnessSubagentOutcomeReport,
  parseHarnessTerminalToolResultText,
  projectHarnessSpawnSubagentResult,
  summarizeHarnessSubagentEventResult,
  summarizeHarnessSubagentResult,
  verifyHarnessSubagentTerminalCompletion,
} from './terminal-subagent-result';
import type { SubagentDefinition } from './types';

export const SPAWN_SUBAGENT_TOOL_ID = 'spawn_subagent';

/**
 * Build a `spawn_subagent` tool scoped to a single parent session + turn.
 * Returns `undefined` when the harness has no subagent types registered
 * so the tool registry stays clean.
 */
export function createSpawnSubagentTool(parent: Session) {
  const harness = (parent as any)._harness as {
    _listSubagentTypeIds(options: { invocation: 'inline' | 'delegated' }): string[];
    _getSubagentType(id: string): SubagentDefinition | undefined;
    _getSubagentMaxDepth(): number;
    session(opts: unknown): Promise<Session>;
    _internalCloseSessionIfUnclaimed(session: Session): Promise<void> | undefined;
  };
  const typeIds = harness._listSubagentTypeIds({ invocation: 'inline' });
  if (typeIds.length === 0) return undefined;

  const description =
    'Delegate a focused task to a specialized subagent. The subagent runs ' +
    'independently with a constrained toolset, then returns a verified structured outcome. ' +
    'Available agent types:\n' +
    typeIds
      .map(id => {
        const def = harness._getSubagentType(id);
        return def ? `- **${id}**: ${def.description}` : `- **${id}**`;
      })
      .join('\n');

  const inputSchema = z.object({
    agentType: z.enum(typeIds as [string, ...string[]]).describe('Which registered subagent type to spawn.'),
    task: z
      .string()
      .describe(
        'Self-contained task description. The subagent does not see the parent conversation, so include every piece of context it needs.',
      ),
    modelOverride: z
      .string()
      .optional()
      .describe('Optional model id override for this invocation. Falls back to the subagent type default.'),
    delivery: z
      .enum(['continue', 'final'])
      .optional()
      .describe(
        'Use "final" only when this single subagent result should be returned directly to the caller with no parent aggregation, tool call, or narration. Omit or use "continue" for intermediate delegation.',
      ),
  });

  const outputSchema = z.object({
    subagentSessionId: z.string(),
    result: harnessSubagentResultSummarySchema.optional(),
    isError: z.boolean().optional(),
    errorName: z.string().optional(),
    field: z.string().optional(),
    reason: z.string().optional(),
    message: z.string().optional(),
    depth: z.number().optional(),
    /** Attempted child depth for a `HarnessSubagentDepthExceededError` result (§4.5b). */
    attemptedDepth: z.number().optional(),
    maxDepth: z.number().optional(),
    /** Per-parent concurrent-spawn cap for a `HarnessSubagentConcurrencyLimitError` result (§SA3). */
    maxConcurrent: z.number().optional(),
  });

  return createTool({
    id: SPAWN_SUBAGENT_TOOL_ID,
    description,
    inputSchema,
    outputSchema,
    terminalResult: {
      isSuccess: (output, context) =>
        context.batchSize === 1 &&
        (context.args as { delivery?: unknown } | undefined)?.delivery === 'final' &&
        projectHarnessSpawnSubagentResult(output) !== undefined,
      project: (output, context) => {
        if (context.batchSize !== 1 || (context.args as { delivery?: unknown } | undefined)?.delivery !== 'final') {
          throw new Error('spawn_subagent direct delivery was not explicitly selected');
        }
        const answer = projectHarnessSpawnSubagentResult(output);
        if (!answer) throw new Error('spawn_subagent output is not a complete direct answer');
        return answer;
      },
      outputSchema: harnessSubagentDirectAnswerSchema,
      maxBytes: MAX_HARNESS_SUBAGENT_DIRECT_ANSWER_BYTES,
    },
    execute: async (input, ctx) => {
      const { agentType, task, modelOverride } = input;
      const toolCallId = ctx.agent?.toolCallId ?? 'unknown';

      const def = harness._getSubagentType(agentType);
      if (!def || def.allowInline === false) {
        return {
          isError: true,
          errorName: 'HarnessValidationError',
          field: 'agentType',
          reason:
            def?.allowInline === false
              ? `subagent type "${agentType}" requires durable task_delegate execution`
              : `unknown subagent type "${agentType}"`,
          subagentSessionId: '',
          result: undefined,
        };
      }

      const parentDepth = (parent as any).subagentDepth as number;
      const childDepth = parentDepth + 1;
      const maxDepth = harness._getSubagentMaxDepth();
      if (childDepth > maxDepth) {
        const err = new HarnessSubagentDepthExceededError(maxDepth, childDepth);
        return {
          isError: true,
          errorName: err.name,
          message: err.message,
          attemptedDepth: childDepth,
          maxDepth,
          subagentSessionId: '',
          result: undefined,
        };
      }

      if (modelOverride !== undefined && typeof modelOverride !== 'string') {
        const err = new HarnessValidationError('modelOverride', 'must be a string when provided');
        return {
          isError: true,
          errorName: err.name,
          field: err.field,
          reason: err.reason,
          subagentSessionId: '',
          result: undefined,
        };
      }

      let admissionEpoch: number;
      try {
        admissionEpoch = parent._internalCaptureSubagentAdmission();
        parent._internalAssertSubagentAdmission(admissionEpoch, ctx.abortSignal);
      } catch (error) {
        const publicError = projectHarnessPublicError(error);
        return {
          isError: true,
          errorName: error instanceof Error ? error.name : 'Error',
          reason: publicError.code,
          message: publicError.message,
          subagentSessionId: '',
          result: undefined,
        };
      }

      // §SA3 backpressure — inline spawn and durable task delegation share one
      // parent-local reservation. Reserve BEFORE the create await so parallel
      // tool calls cannot both claim the final slot.
      const reservation = parent._internalTryReserveSubagentExecution();
      if (!reservation.reserved) {
        return {
          isError: true,
          errorName: 'HarnessSubagentConcurrencyLimitError',
          reason: `subagent concurrency limit reached (maxConcurrent ${reservation.maxConcurrent}, in flight ${reservation.inFlight})`,
          maxConcurrent: reservation.maxConcurrent,
          subagentSessionId: '',
          result: undefined,
        };
      }
      let allocatedChild: Session | undefined;
      try {
        // Create a fresh thread + session for the subagent. The session is
        // `origin: 'subagent-tool'` and `parentSessionId` is wired so cascade
        // rules + the depth field on the record are populated correctly.
        const child = await harness.session({
          resourceId: parent.resourceId,
          threadId: { fresh: true },
          parentSessionId: parent.id,
          origin: 'subagent-tool',
          // §9 — an unset `def.modeId` inherits the PARENT's current mode (as
          // documented), not the harness default mode.
          modeId: def.modeId ?? parent.getCurrentModeId(),
          modelId: modelOverride ?? def.defaultModelId,
          subagentDepth: childDepth,
          // M4 — persist the type so the child's per-subagent overrides (tools /
          // workspace / toolAllowlist) survive a direct-by-id hydrate.
          subagentTypeId: agentType,
        });
        allocatedChild = child;
        // An edit/retry reset or Stop may have won while child allocation was
        // waiting on storage. The child has done no provider work yet, so fail
        // the stale tool call closed and clean up the unstarted descendant.
        parent._internalAssertSubagentAdmission(admissionEpoch, ctx.abortSignal);

        // Workspace inheritance (§2.7 / §8). `'inherit'` (default) makes the
        // child share the parent's workspace via a refcount on the same entry.
        // `'fresh'` provisions a new per-session workspace; only valid under
        // `kind: 'per-session'` (validated at harness construction).
        const subagentWorkspaceMode = def.workspace ?? 'inherit';
        child._subagentInheritWorkspace = subagentWorkspaceMode === 'inherit';
        if (def.tools) child._subagentToolsOverride = def.tools;
        if (def.toolAllowlist) child._subagentToolAllowlist = def.toolAllowlist;
        child._subagentParentToolCallId = toolCallId;

        // Bridge the child's per-turn events into the parent's subscriber
        // stream as `subagent_*`. `_emitSubagentEvent` stamps `parentId` and
        // `queuedItemId` automatically. Track inner tool names by call id so
        // `subagent_tool_end` can carry the same `toolName` as its start.
        const innerToolNames = new Map<string, string>();
        const resolvedModelId = modelOverride ?? def.defaultModelId ?? '';
        const unsub = child.subscribe(event => {
          if (!event.type) return;
          // §SA2 — fold the child's live progress into the parent's display
          // projection (keyed by this spawn's toolCallId).
          parent._internalUpdateSubagentProgress(toolCallId, event);
          switch (event.type) {
            case 'agent_start':
              parent._emitSubagentEvent({
                type: 'subagent_start',
                toolCallId,
                subagentSessionId: child.id,
                agentType,
                task,
                modelId: resolvedModelId,
                depth: childDepth,
              });
              break;
            case 'text_delta':
              if (typeof event.delta === 'string' && event.delta.length > 0) {
                parent._emitSubagentEvent({
                  type: 'subagent_text_delta',
                  toolCallId,
                  subagentSessionId: child.id,
                  agentType,
                  delta: event.delta,
                  depth: childDepth,
                });
              }
              break;
            case 'reasoning_delta':
              if (typeof event.delta === 'string' && event.delta.length > 0) {
                parent._emitSubagentEvent({
                  type: 'subagent_reasoning_delta',
                  toolCallId,
                  subagentSessionId: child.id,
                  agentType,
                  delta: event.delta,
                  depth: childDepth,
                });
              }
              break;
            case 'tool_start':
              innerToolNames.set(event.toolCallId, event.toolName);
              parent._emitSubagentEvent({
                type: 'subagent_tool_start',
                toolCallId,
                subagentSessionId: child.id,
                agentType,
                innerToolCallId: event.toolCallId,
                toolName: event.toolName,
                // `event.input` is already the JSON-safe projection the child's
                // own `_emitForChunk` produced via projectToolEventPayloadForJson
                // + `_internalMaxEventPayloadBytes`, so forward it as-is.
                input: event.input,
                depth: childDepth,
              });
              break;
            case 'tool_end': {
              const toolName = innerToolNames.get(event.toolCallId) ?? 'unknown';
              innerToolNames.delete(event.toolCallId);
              parent._emitSubagentEvent({
                type: 'subagent_tool_end',
                toolCallId,
                subagentSessionId: child.id,
                agentType,
                innerToolCallId: event.toolCallId,
                toolName,
                output: event.output,
                isError: event.isError ?? false,
                ...(event.durationMs !== undefined ? { durationMs: event.durationMs } : {}),
                depth: childDepth,
              });
              break;
            }
          }
        });

        // Track the active subagent so `getDisplayState()` renders it.

        const activeMap = (parent as any)._activeSubagents as Map<
          string,
          {
            subagentSessionId: string;
            agentType: string;
            task: string;
            parentToolCallId: string;
            startedAt: number;
          }
        >;
        activeMap.set(toolCallId, {
          subagentSessionId: child.id,
          agentType,
          task,
          parentToolCallId: toolCallId,
          startedAt: Date.now(),
        });

        const startTime = Date.now();
        let rawResult: unknown;
        let isError = false;
        let publicError: { code: string; message: string } | undefined;
        try {
          // Registering the active-map entry happens before this final fence.
          // If invalidation begins after the check, reset/Stop discovery sees
          // and cancels the child; if it began earlier, this check prevents the
          // first provider call. There is no await between the check and
          // `child.message()` invocation.
          parent._internalAssertSubagentAdmission(admissionEpoch, ctx.abortSignal);
          rawResult = await child.message({ content: task, abortSignal: ctx.abortSignal });
          const finishReason = (rawResult as { finishReason?: string } | undefined)?.finishReason;
          const declaredReport = parseHarnessSubagentOutcomeReport(rawResult);
          const declaredTerminalText = parseHarnessTerminalToolResultText(
            (rawResult as { terminalToolResult?: unknown } | undefined)?.terminalToolResult,
          );
          const report = verifyHarnessSubagentTerminalCompletion(rawResult);
          // §S3.3 — an inline spawn_subagent has NO independent driver to resume a
          // human-in-the-loop suspension, and the child is auto-closed below. A
          // default `message()` RESOLVES (not rejects) with finishReason
          // 'suspended', so without this it would be reported as a successful
          // `subagent_end{isError:false}` and the child closed mid-suspension.
          // Treat it as an error result so the parent model gets an error-shaped
          // tool output instead of a misleading completion.
          if (finishReason === 'suspended') {
            isError = true;
            publicError = {
              code: 'harness.subagent_suspended',
              message:
                `Subagent "${agentType}" suspended for human input, but an inline spawn_subagent cannot be ` +
                `resumed. Re-invoke it with self-contained context that does not require approval/input.`,
            };
            // This inline child has no independent resume driver and will be
            // auto-closed below. Persist cancellation now so pendingResume no
            // longer keeps close() busy until its 30s drain deadline.
            try {
              await child.cancel({
                reason: 'inline_subagent_suspended_unresumable',
                requestedBy: parent.id,
              });
            } catch {
              // Best-effort cleanup; close() remains the final lifecycle fence.
            }
          } else if (ctx.abortSignal?.aborted || child.getRecord().cancelRequest !== undefined) {
            isError = true;
            publicError = {
              code: 'harness.subagent_cancelled',
              message: `Subagent "${agentType}" was cancelled before it reported a terminal outcome`,
            };
          } else if (report === undefined) {
            // Provider finish reasons describe the transport/model turn, not
            // whether the assigned task was achieved. A bare stop is no more
            // authoritative than length/content-filter/tool-loop exhaustion.
            isError = true;
            publicError = {
              code:
                declaredReport !== undefined || declaredTerminalText !== undefined
                  ? 'harness.subagent_evidence_unverified'
                  : finishReason === 'stop'
                    ? 'harness.subagent_outcome_missing'
                    : 'harness.subagent_incomplete',
              message:
                declaredReport !== undefined || declaredTerminalText !== undefined
                  ? `Subagent "${agentType}" terminal completion did not match framework execution receipts`
                  : finishReason === 'stop'
                    ? `Subagent "${agentType}" ended without ${HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID}`
                    : `Subagent "${agentType}" ended with finish reason "${finishReason ?? 'missing'}" and no ${HARNESS_SUBAGENT_OUTCOME_REPORT_TOOL_ID}`,
            };
          } else if (report.outcome !== 'completed') {
            isError = true;
            publicError = {
              code: report.issue!.code,
              message: report.issue!.message,
            };
          }
        } catch (err) {
          isError = true;
          publicError = projectHarnessPublicError(err);
        } finally {
          unsub();
          activeMap.delete(toolCallId);
          // Auto-close the subagent-tool child per §5.6, but never join a
          // close already owned by an ancestor subtree. An ancestor close
          // drains this parent turn; the parent turn cannot finish until this
          // tool returns, so awaiting that same close promise would form a
          // cycle and consume the full close deadline. The Harness performs
          // the ownership check + independent-close claim atomically.
          const independentClose = harness._internalCloseSessionIfUnclaimed(child);
          if (independentClose) {
            try {
              await independentClose;
            } catch {
              // Best-effort cleanup: lifecycle close errors must not mask the
              // bounded child result already produced for the parent model.
            }
          }
        }

        const result = summarizeHarnessSubagentResult(rawResult, { isError, error: publicError });
        // A child can resolve normally with finishReason=error/aborted. Do not
        // mislabel that as a successful tool result merely because it did not
        // reject the in-process promise.
        isError = result.status === 'error';
        const durationMs = Date.now() - startTime;
        parent._emitSubagentEvent({
          type: 'subagent_end',
          toolCallId,
          subagentSessionId: child.id,
          agentType,
          output: summarizeHarnessSubagentEventResult(result),
          isError,
          durationMs,
          depth: childDepth,
        });

        return {
          subagentSessionId: child.id,
          result,
          // §S3.3 — surface isError on the tool OUTPUT (not only on subagent_end)
          // so the parent model receives an error-shaped result for a thrown OR
          // suspended subagent, instead of a value that looks like success.
          ...(isError ? { isError: true } : {}),
        };
      } catch (error) {
        if (allocatedChild) {
          const independentClose = harness._internalCloseSessionIfUnclaimed(allocatedChild);
          if (independentClose) {
            try {
              await independentClose;
            } catch {
              // Preserve the bounded public execution error below.
            }
          }
        }
        const publicError = projectHarnessPublicError(error);
        return {
          isError: true,
          errorName: error instanceof Error ? error.name : 'Error',
          reason: publicError.code,
          message: publicError.message,
          subagentSessionId: allocatedChild?.id ?? '',
          result: undefined,
        };
      } finally {
        // Release the SA3 reservation on every exit path (create failure, run
        // error, or normal completion).
        parent._internalReleaseSubagentExecution();
      }
    },
  });
}
