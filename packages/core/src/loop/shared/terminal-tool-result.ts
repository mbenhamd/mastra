import { Buffer } from 'node:buffer';
import type { ToolSet } from '@internal/ai-sdk-v5';
import {
  createTerminalToolResultPartId,
  MAX_TERMINAL_TOOL_RESULT_ENVELOPE_BYTES,
  materializeTerminalToolResult,
} from '../../agent/message-list/terminal-tool-result';
import { toStandardSchema } from '../../schema';
import type { CoreTool, TerminalToolResult, ToolTerminalResultConfig } from '../../tools';
import { isValidationError } from '../../tools';
import { findProviderToolByName } from '../../tools/provider-tool-utils';
import { materializeWorkflowTerminalCanonicalJson } from '../../workflows/terminal-recovery';
import type { WorkflowTerminalCanonicalJsonValue } from '../../workflows/terminal-recovery/types';

export const DEFAULT_TERMINAL_TOOL_RESULT_MAX_BYTES = 16 * 1024;
export const MAX_TERMINAL_TOOL_RESULT_MAX_BYTES = 64 * 1024;
export const DEFAULT_TERMINAL_TOOL_RESULT_EVALUATION_TIMEOUT_MS = 1_000;
export const MAX_TERMINAL_TOOL_RESULT_EVALUATION_TIMEOUT_MS = 10_000;
export { createTerminalToolResultPartId, MAX_TERMINAL_TOOL_RESULT_ENVELOPE_BYTES, materializeTerminalToolResult };

/**
 * Direct delivery must not bypass an output policy. Persistence/indexing
 * processors opt in explicitly; every other processor keeps the ordinary
 * model continuation path.
 */
export function outputProcessorsAllowTerminalToolResult(processors: readonly unknown[] | undefined): boolean {
  if (!processors?.length) return true;

  const persistenceOwners: number[] = [];
  for (const [index, processor] of processors.entries()) {
    if (
      processor === null ||
      typeof processor !== 'object' ||
      (processor as { terminalToolResultPolicy?: unknown }).terminalToolResultPolicy !== 'pass-through'
    ) {
      return false;
    }
    if ((processor as { terminalToolResultPersistence?: unknown }).terminalToolResultPersistence === 'owner') {
      persistenceOwners.push(index);
    }
  }

  // A persistence side effect must be unique and last. Otherwise a later
  // fallible processor could fail after the terminal answer was already saved.
  return (
    persistenceOwners.length === 0 || (persistenceOwners.length === 1 && persistenceOwners[0] === processors.length - 1)
  );
}

/** True only for the unique, final persistence owner accepted by the policy check above. */
export function outputProcessorsOwnTerminalPersistence(processors: readonly unknown[] | undefined): boolean {
  return Boolean(
    processors?.length &&
    outputProcessorsAllowTerminalToolResult(processors) &&
    (processors[processors.length - 1] as { terminalToolResultPersistence?: unknown })
      ?.terminalToolResultPersistence === 'owner',
  );
}

type SettledToolCall = {
  toolCallId: string;
  toolName: string;
  args?: unknown;
  result?: unknown;
  error?: unknown;
  providerExecuted?: boolean;
  approval?: { approved?: boolean };
  /** True when this result was produced by resuming an in-tool suspension. */
  resumedFromSuspension?: boolean;
  disposition?: 'denied';
};

function terminalPolicyError(toolName: string, reason: string): Error {
  const error = new Error(`Terminal result policy failed for tool "${toolName}": ${reason}`);
  error.name = 'MastraTerminalToolResultError';
  return error;
}

function resolveTool(tools: ToolSet | undefined, toolName: string): CoreTool | undefined {
  const registry = tools as unknown as Record<string, CoreTool | undefined> | undefined;
  return (
    registry?.[toolName] ||
    (findProviderToolByName(tools as any, toolName) as CoreTool | undefined) ||
    Object.values(registry || {}).find(tool => tool && typeof tool === 'object' && tool.id === toolName)
  );
}

function resolveMaxBytes(toolName: string, policy: ToolTerminalResultConfig): number {
  const maxBytes = policy.maxBytes ?? DEFAULT_TERMINAL_TOOL_RESULT_MAX_BYTES;
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 1 || maxBytes > MAX_TERMINAL_TOOL_RESULT_MAX_BYTES) {
    throw terminalPolicyError(
      toolName,
      `maxBytes must be an integer between 1 and ${MAX_TERMINAL_TOOL_RESULT_MAX_BYTES}`,
    );
  }
  return maxBytes;
}

function resolveEvaluationTimeoutMs(toolName: string, policy: ToolTerminalResultConfig): number {
  const timeoutMs = policy.evaluationTimeoutMs ?? DEFAULT_TERMINAL_TOOL_RESULT_EVALUATION_TIMEOUT_MS;
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > MAX_TERMINAL_TOOL_RESULT_EVALUATION_TIMEOUT_MS) {
    throw terminalPolicyError(
      toolName,
      `evaluationTimeoutMs must be an integer between 1 and ${MAX_TERMINAL_TOOL_RESULT_EVALUATION_TIMEOUT_MS}`,
    );
  }
  return timeoutMs;
}

const TERMINAL_POLICY_ABORTED = Symbol('terminal-policy-aborted');

async function evaluateTerminalPolicy<T>(options: {
  parentSignal?: AbortSignal;
  timeoutMs: number;
  evaluate: (signal: AbortSignal) => Promise<T>;
}): Promise<T | typeof TERMINAL_POLICY_ABORTED> {
  const controller = new AbortController();
  const abort = () => controller.abort(options.parentSignal?.reason);
  if (options.parentSignal?.aborted) return TERMINAL_POLICY_ABORTED;
  options.parentSignal?.addEventListener('abort', abort, { once: true });
  const timeout = setTimeout(
    () => controller.abort(new Error('Terminal policy evaluation timed out')),
    options.timeoutMs,
  );
  timeout.unref?.();
  try {
    const result = await Promise.race([
      options.evaluate(controller.signal),
      new Promise<typeof TERMINAL_POLICY_ABORTED>(resolve => {
        controller.signal.addEventListener('abort', () => resolve(TERMINAL_POLICY_ABORTED), { once: true });
      }),
    ]);
    // A cooperative callback may settle in the same abort dispatch that settles
    // the sentinel promise. Abort always wins that race, independent of listener order.
    return controller.signal.aborted ? TERMINAL_POLICY_ABORTED : result;
  } finally {
    clearTimeout(timeout);
    options.parentSignal?.removeEventListener('abort', abort);
  }
}

async function validateProjection(
  toolName: string,
  policy: ToolTerminalResultConfig,
  value: unknown,
): Promise<unknown> {
  let result: { value?: unknown; issues?: ReadonlyArray<unknown> };
  try {
    result = await toStandardSchema(policy.outputSchema)['~standard'].validate(value);
  } catch {
    throw terminalPolicyError(toolName, 'output schema validation threw');
  }
  if ('issues' in result && result.issues) {
    throw terminalPolicyError(toolName, 'projected output did not match outputSchema');
  }
  return result.value;
}

function hasToolOutputTransform(tool: CoreTool): boolean {
  const transform = tool.transform;
  return Boolean(transform?.display?.output || transform?.transcript?.output);
}

/**
 * Resolve a terminal result only when the complete provider-selected batch is
 * eligible. Mixed ordinary/provider/background/failed batches fail closed and
 * retain the normal model repair/continuation path.
 */
export async function resolveTerminalToolResult(options: {
  calls: SettledToolCall[];
  tools: ToolSet | undefined;
  fallbackTools?: ToolSet;
  runId?: string;
  abortSignal?: AbortSignal;
  /** Receives a sanitized diagnostic when terminal optimization is refused by its policy. */
  onPolicyFailure?: (error: Error) => void;
}): Promise<TerminalToolResult<WorkflowTerminalCanonicalJsonValue> | undefined> {
  try {
    if (options.calls.length === 0) return undefined;

    const candidates: Array<{
      call: SettledToolCall;
      batchIndex: number;
      policy: ToolTerminalResultConfig;
      maxBytes: number;
      timeoutMs: number;
    }> = [];
    for (const [batchIndex, call] of options.calls.entries()) {
      if (
        call.providerExecuted ||
        call.error !== undefined ||
        call.result === undefined ||
        // Human intervention is an explicit semantic boundary. Even an
        // approved call must return to the model once so approval/suspension
        // context cannot be bypassed by the terminal optimization.
        call.approval !== undefined ||
        call.resumedFromSuspension === true ||
        call.disposition === 'denied' ||
        isValidationError(call.result)
      ) {
        return undefined;
      }

      const tool = resolveTool(options.tools, call.toolName) ?? resolveTool(options.fallbackTools, call.toolName);
      const policy = tool?.terminalResult;
      // A background-capable converted tool can return a dispatch placeholder.
      // V1 fails closed even when a particular call happened to run inline.
      if (
        !tool ||
        !policy ||
        (tool as CoreTool & { backgroundConfig?: unknown }).backgroundConfig ||
        hasToolOutputTransform(tool)
      ) {
        return undefined;
      }

      if (
        typeof policy.isSuccess !== 'function' ||
        typeof policy.project !== 'function' ||
        policy.outputSchema === undefined
      ) {
        return undefined;
      }
      candidates.push({
        call,
        batchIndex,
        policy,
        maxBytes: resolveMaxBytes(call.toolName, policy),
        timeoutMs: resolveEvaluationTimeoutMs(call.toolName, policy),
      });
    }

    // A provider may select many tools in one step. Evaluate independent,
    // side-effect-free terminal policies concurrently so the latency is bounded
    // by one policy timeout rather than batchSize × timeout. One refusal or
    // failure makes the entire batch non-terminal and aborts every sibling.
    const batchController = new AbortController();
    const abortBatch = () => batchController.abort(options.abortSignal?.reason);
    if (options.abortSignal?.aborted) return undefined;
    options.abortSignal?.addEventListener('abort', abortBatch, { once: true });

    let evaluatedItems: Array<TerminalToolResult<WorkflowTerminalCanonicalJsonValue>['items'][number] | undefined>;
    try {
      evaluatedItems = await Promise.all(
        candidates.map(async ({ call, batchIndex, policy, maxBytes, timeoutMs }) => {
          try {
            const evaluated = await evaluateTerminalPolicy({
              parentSignal: batchController.signal,
              timeoutMs,
              evaluate: async abortSignal => {
                const context = {
                  toolName: call.toolName,
                  toolCallId: call.toolCallId,
                  args: call.args,
                  batchSize: options.calls.length,
                  batchIndex,
                  ...(options.runId ? { runId: options.runId } : {}),
                  abortSignal,
                };

                let accepted: boolean;
                try {
                  accepted = await policy.isSuccess(call.result, context);
                } catch {
                  throw terminalPolicyError(call.toolName, 'isSuccess predicate threw');
                }
                if (!accepted) return undefined;

                let projected: unknown;
                try {
                  projected = await policy.project(call.result, context);
                } catch {
                  throw terminalPolicyError(call.toolName, 'projection threw');
                }
                return validateProjection(call.toolName, policy, projected);
              },
            });
            if (evaluated === TERMINAL_POLICY_ABORTED || evaluated === undefined) {
              batchController.abort();
              return undefined;
            }

            let canonical: WorkflowTerminalCanonicalJsonValue;
            try {
              canonical = materializeWorkflowTerminalCanonicalJson(evaluated, `terminal result for ${call.toolName}`);
            } catch {
              throw terminalPolicyError(call.toolName, 'projection must be bounded, data-only canonical JSON');
            }
            if (Buffer.byteLength(JSON.stringify(canonical), 'utf8') > maxBytes) {
              throw terminalPolicyError(call.toolName, 'projected output exceeds maxBytes');
            }

            return {
              toolName: call.toolName,
              toolCallId: call.toolCallId,
              status: 'success' as const,
              value: canonical,
            };
          } catch (error) {
            batchController.abort(error);
            throw error;
          }
        }),
      );
    } finally {
      options.abortSignal?.removeEventListener('abort', abortBatch);
    }
    if (evaluatedItems.some(item => item === undefined)) return undefined;
    const items = evaluatedItems as TerminalToolResult<WorkflowTerminalCanonicalJsonValue>['items'];

    const envelope: TerminalToolResult<WorkflowTerminalCanonicalJsonValue> = { status: 'success', items };
    if (Buffer.byteLength(JSON.stringify(envelope), 'utf8') > MAX_TERMINAL_TOOL_RESULT_ENVELOPE_BYTES) {
      throw terminalPolicyError('batch', 'combined projected output exceeds the terminal envelope limit');
    }
    return envelope;
  } catch (error) {
    // Terminal delivery is an optimization over the ordinary model continuation.
    // A broken, slow, or over-sized policy must never turn an otherwise successful
    // tool call into a failed agent run. Report only the bounded framework message;
    // never include the raw tool output or thrown callback value.
    const diagnostic =
      error instanceof Error && error.name === 'MastraTerminalToolResultError'
        ? error
        : terminalPolicyError('batch', 'unexpected policy evaluation failure');
    try {
      options.onPolicyFailure?.(diagnostic);
    } catch {
      // Diagnostics are observational and cannot change delivery semantics.
    }
    return undefined;
  }
}
