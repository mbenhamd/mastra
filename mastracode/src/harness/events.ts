import type { HarnessEvent as LegacyHarnessEvent, HarnessMessage, HarnessThread } from '@mastra/core/harness';
import type { HarnessEvent as HarnessV1Event, SessionDisplayState } from '@mastra/core/harness/v1';

type EmitLegacyEvent = (event: LegacyHarnessEvent) => void;

function textMessage(messageId: string, text: string): HarnessMessage {
  return {
    id: messageId,
    role: 'assistant',
    content: [{ type: 'text', text }],
    createdAt: new Date(),
  };
}

/**
 * Projects current harness-v1 events (`@mastra/core/harness/v1`) into the legacy
 * `@mastra/core/harness` event vocabulary the TUI dispatcher consumes.
 *
 * The v1 event union no longer carries the message-boundary, thread-lifecycle,
 * or `suspension_required` events this projector once switched on (§10.2: the
 * only streaming-text event is `text_delta`; suspensions are four discrete
 * events; thread CRUD emits no stream event — see
 * `createHarnessOperatorThreadController`). The mappings below reconcile to the
 * current vocabulary:
 *   - `text_delta`                  -> legacy `message_start` / `message_update`
 *   - `agent_end` / `run_completed` -> legacy `message_end`
 *   - `tool_start.input`            -> legacy `tool_start.args`
 *   - `subagent_tool_start.input`   -> legacy subagent tool args
 *   - `question_pending`            -> legacy `ask_question` / `sandbox_access_request`
 *   - `plan_approval_required`      -> legacy `plan_approval_required`
 *   - `tool_approval_required`      -> legacy `tool_approval_required`
 *   - `tool_suspension_required`    -> legacy `tool_suspended`
 *
 * Thread-lifecycle legacy events (`thread_created` / `thread_cloned` /
 * `thread_renamed`) are emitted directly at the runtime's thread-CRUD call
 * sites (they have no v1 stream event), not here.
 */
export class MastraCodeHarnessEventProjector {
  /** Accumulated assistant text keyed by the run that is streaming it. */
  private readonly assistantTextByRun = new Map<string, string>();
  /** Whether a legacy `message_start` was already emitted for a run. */
  private readonly startedRuns = new Set<string>();
  private readonly toolNames = new Map<string, string>();
  /** Keep terminal projection ordered when resolving the durable full message. */
  private projectionChain: Promise<void> = Promise.resolve();

  constructor(
    private readonly emitLegacy: EmitLegacyEvent,
    private readonly getDisplayState: () => SessionDisplayState | Record<string, unknown>,
    // Retained for parity with the runtime wiring; thread snapshots are resolved
    // by the runtime's thread-CRUD call sites that emit legacy thread events.
    private readonly getThread: (threadId: string, resourceId: string) => Promise<HarnessThread | undefined>,
    private readonly listMessages: () => Promise<HarnessMessage[]> = async () => [],
  ) {
    void this.getThread;
  }

  async project(event: HarnessV1Event): Promise<void> {
    const projection = this.projectionChain.then(() => this.projectOne(event));
    this.projectionChain = projection.catch(() => {});
    return projection;
  }

  private async projectOne(event: HarnessV1Event): Promise<void> {
    const projected = await this.toLegacyEvents(event);
    for (const legacy of projected) {
      this.emitLegacy(legacy);
      this.emitLegacy({
        type: 'display_state_changed',
        displayState: this.getDisplayState(),
      } as unknown as LegacyHarnessEvent);
    }
  }

  private async toLegacyEvents(event: HarnessV1Event): Promise<LegacyHarnessEvent[]> {
    switch (event.type) {
      case 'text_delta': {
        const runId = event.runId;
        const next = `${this.assistantTextByRun.get(runId) ?? ''}${event.delta}`;
        this.assistantTextByRun.set(runId, next);
        const legacy: LegacyHarnessEvent[] = [];
        if (!this.startedRuns.has(runId)) {
          this.startedRuns.add(runId);
          legacy.push({ type: 'message_start', message: textMessage(runId, '') } as unknown as LegacyHarnessEvent);
        }
        legacy.push({ type: 'message_update', message: textMessage(runId, next) } as unknown as LegacyHarnessEvent);
        return legacy;
      }
      case 'agent_end': {
        const reason =
          event.finishReason === 'aborted' ||
          event.finishReason === 'error' ||
          event.finishReason === 'suspended' ||
          event.finishReason === 'complete'
            ? event.finishReason
            : 'complete';
        return await this.flushMessageEnd(event.runId, { ...event, reason } as unknown as LegacyHarnessEvent);
      }
      case 'run_completed':
        // `run_completed` is the canonical terminal; `agent_end` already flushed
        // the streamed text in the common case, but flush here too in case a run
        // completed without a preceding `agent_end` reaching this projector.
        return await this.flushMessageEnd(event.runId, event as unknown as LegacyHarnessEvent);
      case 'subagent_start':
        return [{ ...event, forked: (event as { forked?: boolean }).forked ?? false } as unknown as LegacyHarnessEvent];
      case 'subagent_text_delta':
        return [{ ...event, textDelta: event.delta } as unknown as LegacyHarnessEvent];
      case 'tool_start':
        this.toolNames.set(event.toolCallId, event.toolName);
        return [{ ...event, args: event.input } as unknown as LegacyHarnessEvent];
      case 'tool_end':
        this.toolNames.delete(event.toolCallId);
        return [{ ...event, result: event.output } as unknown as LegacyHarnessEvent];
      case 'subagent_tool_start':
        return [
          {
            ...event,
            subToolCallId: event.innerToolCallId,
            subToolName: event.toolName,
            subToolArgs: event.input,
          } as unknown as LegacyHarnessEvent,
        ];
      case 'subagent_tool_end':
        return [
          {
            ...event,
            subToolCallId: event.innerToolCallId,
            subToolName: event.toolName,
            subToolResult: event.output,
            result: event.output,
          } as unknown as LegacyHarnessEvent,
        ];
      case 'subagent_end':
        return [{ ...event, result: stringifySubagentOutput(event.output) } as unknown as LegacyHarnessEvent];
      case 'question_pending':
        return this.projectQuestionPending(event);
      case 'plan_approval_required':
        return [
          {
            ...event,
            type: 'plan_approval_required',
            planId: event.itemId,
            title: event.title,
            plan: event.plan,
          } as unknown as LegacyHarnessEvent,
        ];
      case 'tool_approval_required':
        return [
          {
            ...event,
            type: 'tool_approval_required',
            toolCallId: event.toolCallId,
            toolName: event.toolName,
            args: event.input,
            category: event.toolCategory,
          } as unknown as LegacyHarnessEvent,
        ];
      case 'tool_suspension_required':
        return [
          {
            ...event,
            type: 'tool_suspended',
            toolCallId: event.toolCallId,
            toolName: event.toolName,
            args: this.suspendInput(event.suspendData),
            suspendPayload: event.suspendData,
          } as unknown as LegacyHarnessEvent,
        ];
      default:
        return [event as unknown as LegacyHarnessEvent];
    }
  }

  /** Flush the authoritative assistant message for `runId` as a legacy `message_end`. */
  private async flushMessageEnd(
    runId: string | undefined,
    fallback: LegacyHarnessEvent,
  ): Promise<LegacyHarnessEvent[]> {
    if (runId === undefined) {
      // No streamed text was projected for this run — pass the terminal through
      // so downstream lifecycle handling (e.g. legacy `agent_end`) still runs.
      return [fallback];
    }
    const text = this.assistantTextByRun.get(runId) ?? '';
    const fullMessage = await this.resolveAssistantMessage(runId);
    if (!this.startedRuns.has(runId) && fullMessage === undefined) return [fallback];

    this.assistantTextByRun.delete(runId);
    this.startedRuns.delete(runId);
    const message = fullMessage ?? textMessage(runId, text);
    return [
      // Harness v1 intentionally keeps assistant drafts text-only. Re-read the
      // completed message so structured parts (for example system reminders)
      // survive this one-way legacy projection as the final update and end.
      { type: 'message_update', message } as unknown as LegacyHarnessEvent,
      { type: 'message_end', message } as unknown as LegacyHarnessEvent,
      fallback,
    ];
  }

  private async resolveAssistantMessage(runId: string): Promise<HarnessMessage | undefined> {
    const displayState = this.getDisplayState() as SessionDisplayState;
    const draft = displayState.assistantDrafts?.[runId];
    const messages = await this.listMessages();
    let assistantIndex =
      draft?.messageId === undefined
        ? -1
        : messages.findIndex(message => message.id === draft.messageId && message.role === 'assistant');

    // The AI SDK stream-part id recorded on a draft is not necessarily the
    // memory row id. At a terminal boundary the newest assistant row is the
    // authoritative message for the just-completed run.
    if (assistantIndex < 0) {
      assistantIndex = messages.findLastIndex(message => message.role === 'assistant');
    }
    const assistant = messages[assistantIndex];
    if (assistant === undefined || assistant.role !== 'assistant') return undefined;

    // Reactive system reminders are durable user-role rows immediately before
    // the assistant response. The legacy stream represented those parts inside
    // its in-flight assistant message, so fold only that contiguous suffix back
    // into the one-way projection.
    const reminders: HarnessMessage['content'] = [];
    for (let index = assistantIndex - 1; index >= 0; index--) {
      const candidate = messages[index];
      if (
        candidate?.role !== 'user' ||
        candidate.content.length === 0 ||
        candidate.content.some(part => part.type !== 'system_reminder')
      ) {
        break;
      }
      reminders.unshift(...candidate.content);
    }

    return reminders.length > 0 ? { ...assistant, content: [...reminders, ...assistant.content] } : assistant;
  }

  private projectQuestionPending(event: Extract<HarnessV1Event, { type: 'question_pending' }>): LegacyHarnessEvent[] {
    const itemId = event.itemId;
    if (isSandboxAccessQuestion(itemId, event.question)) {
      const sandboxRequest = parseSandboxAccessQuestion(event.question);
      if (sandboxRequest) {
        return [
          {
            ...event,
            type: 'sandbox_access_request',
            questionId: itemId,
            path: sandboxRequest.path,
            reason: sandboxRequest.reason,
          } as unknown as LegacyHarnessEvent,
        ];
      }
    }
    return [
      {
        ...event,
        type: 'ask_question',
        questionId: itemId,
        question: event.question ?? 'The agent needs your input.',
        options: event.options,
        selectionMode: event.selectionMode,
      } as unknown as LegacyHarnessEvent,
    ];
  }

  private suspendInput(suspendData: unknown): unknown {
    if (suspendData && typeof suspendData === 'object' && !Array.isArray(suspendData)) {
      const record = suspendData as Record<string, unknown>;
      if ('input' in record) return record.input;
      if ('args' in record) return record.args;
    }
    return suspendData;
  }
}

function isSandboxAccessQuestion(itemId: string, question: unknown): boolean {
  return itemId.startsWith('sandbox_') && typeof question === 'string';
}

function parseSandboxAccessQuestion(question: unknown): { path: string; reason: string } | undefined {
  if (typeof question !== 'string') return undefined;
  const prefix = 'Allow Mastra Code to access ';
  const separator = '?\n\n';
  if (!question.startsWith(prefix)) return undefined;
  const separatorIndex = question.lastIndexOf(separator);
  if (separatorIndex <= prefix.length) return undefined;
  return {
    path: question.slice(prefix.length, separatorIndex),
    reason: question.slice(separatorIndex + separator.length),
  };
}

function stringifySubagentOutput(output: unknown): string {
  if (typeof output === 'string') return output;
  if (output instanceof Error) return output.message;
  try {
    return JSON.stringify(output);
  } catch {
    return String(output);
  }
}
