import deepEqual from 'fast-deep-equal';
import { z } from 'zod/v4';
import { MastraError, ErrorDomain, ErrorCategory } from '../../../error';
import type { SystemMessage } from '../../../llm';
import type { MastraMemory } from '../../../memory/memory';
import { MemoryRunState } from '../../../memory/run-state';
import type { MemoryConfigInternal, StorageThreadType } from '../../../memory/types';
import { resolveObservabilityContext } from '../../../observability';
import type { ProcessorState } from '../../../processors/runner';
import type { RequestContext } from '../../../request-context';
import {
  assertObservationalMemorySourceWriteGuardSupported,
  ObservationalMemorySourceWriteConflictError,
} from '../../../storage';
import type { ObservationalMemorySourceWriteGuard } from '../../../storage';
import { createStep } from '../../../workflows/workflow';
import type { InnerAgentExecutionOptions } from '../../agent.types';
import { assertThreadOwnedByResource } from '../../memory-thread-ownership';
import { MessageList } from '../../message-list';
import { mastraDBMessageToSignal } from '../../signals';
import type { AgentMethodType } from '../../types';
import type { PrepareStreamRunScope } from './run-scope';
import {
  INITIAL_SIGNAL_ECHOES_KEY,
  MEMORY_RUN_STATE_KEY,
  MESSAGE_LIST_KEY,
  PROCESSOR_STATES_KEY,
} from './run-scope-keys';
import type { AgentCapabilities } from './schema';
import { prepareMemoryStepOutputSchema } from './schema';

/**
 * Helper function to add system message(s) to a MessageList
 * Handles string, CoreSystemMessage, SystemModelMessage, and arrays of these message formats
 * Used for both agent instructions and user-provided system messages
 */
function addSystemMessage(messageList: MessageList, content: SystemMessage | undefined, tag?: string): void {
  if (!content) return;

  if (Array.isArray(content)) {
    // Handle array of system messages
    for (const msg of content) {
      messageList.addSystem(msg, tag);
    }
  } else {
    // Handle string, CoreSystemMessage, or SystemModelMessage
    messageList.addSystem(content, tag);
  }
}

function getInitialSignalEchoes(messageList: MessageList) {
  const inputMessageIds = messageList.makeMessageSourceChecker().input;
  return messageList.get.all
    .db()
    .filter(message => message.role === 'signal' && inputMessageIds.has(message.id))
    .map(mastraDBMessageToSignal);
}

interface PrepareMemoryStepOptions<OUTPUT = undefined> {
  capabilities: AgentCapabilities;
  options: InnerAgentExecutionOptions<OUTPUT>;
  threadFromArgs?: (Partial<StorageThreadType> & { id: string }) | undefined;
  resourceId?: string;
  runId: string;
  requestContext: RequestContext;
  methodType: AgentMethodType;
  instructions: SystemMessage;
  /** MCP server guidance to include as a separate system message. */
  mcpServerGuidance?: string;
  memoryConfig?: MemoryConfigInternal;
  memory?: MastraMemory;
  resumeContext?: {
    snapshot: any;
  };
  isResume?: boolean;
  runScope: PrepareStreamRunScope<OUTPUT>;
}

function readResumeSourceWriteGuard(
  resumeContext: PrepareMemoryStepOptions['resumeContext'],
): ObservationalMemorySourceWriteGuard | undefined {
  const guards: ObservationalMemorySourceWriteGuard[] = [];
  const addGuard = (messageListState: unknown) => {
    if (!messageListState || typeof messageListState !== 'object' || Array.isArray(messageListState)) return;
    const memoryInfo = (messageListState as { memoryInfo?: unknown }).memoryInfo;
    if (!memoryInfo || typeof memoryInfo !== 'object' || Array.isArray(memoryInfo)) return;
    const value = (memoryInfo as { observationalMemorySourceWriteGuard?: unknown }).observationalMemorySourceWriteGuard;
    if (!value || typeof value !== 'object' || Array.isArray(value)) return;
    const guard = value as Partial<ObservationalMemorySourceWriteGuard>;
    if (
      typeof guard.recordId !== 'string' ||
      guard.recordId.length === 0 ||
      typeof guard.resourceId !== 'string' ||
      guard.resourceId.length === 0 ||
      !Object.prototype.hasOwnProperty.call(guard, 'threadId') ||
      (guard.threadId !== null && typeof guard.threadId !== 'string')
    ) {
      return;
    }
    const normalized = {
      recordId: guard.recordId,
      threadId: guard.threadId ?? null,
      resourceId: guard.resourceId,
    } satisfies ObservationalMemorySourceWriteGuard;
    if (
      !guards.some(
        existing =>
          existing.recordId === normalized.recordId &&
          existing.threadId === normalized.threadId &&
          existing.resourceId === normalized.resourceId,
      )
    ) {
      guards.push(normalized);
    }
  };

  for (const step of Object.values(resumeContext?.snapshot?.context ?? {})) {
    if (!step || typeof step !== 'object' || (step as { status?: string }).status !== 'suspended') continue;
    const payload = (step as { suspendPayload?: any }).suspendPayload;
    if (!payload || typeof payload !== 'object') continue;
    addGuard(payload.__streamState?.messageList);
    const foreachOutput = payload.__workflow_meta?.foreachOutput;
    const iterations = Array.isArray(foreachOutput)
      ? foreachOutput
      : foreachOutput && typeof foreachOutput === 'object'
        ? Object.values(foreachOutput)
        : [];
    for (const iteration of iterations) {
      if (iteration?.status === 'suspended') addGuard(iteration.suspendPayload?.__streamState?.messageList);
    }
  }

  return guards.length === 1 ? guards[0] : undefined;
}

export function createPrepareMemoryStep<OUTPUT = undefined>({
  capabilities,
  options,
  threadFromArgs,
  resourceId,
  runId: _runId,
  requestContext,
  instructions,
  mcpServerGuidance,
  memoryConfig,
  memory,
  resumeContext,
  isResume,
  runScope,
}: PrepareMemoryStepOptions<OUTPUT>) {
  return createStep({
    id: 'prepare-memory-step',
    inputSchema: z.object({}),
    outputSchema: prepareMemoryStepOutputSchema,
    execute: async ({ ...rest }) => {
      const observabilityContext = resolveObservabilityContext(rest);
      const thread = threadFromArgs;
      let observationalMemorySourceWriteGuard = options.observationalMemorySourceWriteGuard;
      const observationalMemoryConfig = memory?.getMergedThreadConfig(memoryConfig).observationalMemory;
      if (isResume) {
        observationalMemorySourceWriteGuard =
          readResumeSourceWriteGuard(resumeContext) ?? observationalMemorySourceWriteGuard;
        if (observationalMemorySourceWriteGuard) {
          options.observationalMemorySourceWriteGuard = observationalMemorySourceWriteGuard;
        }
        if (
          observationalMemoryConfig &&
          typeof observationalMemoryConfig === 'object' &&
          observationalMemoryConfig.enabled !== false &&
          observationalMemoryConfig.sourceWriteFencing === 'required' &&
          !observationalMemorySourceWriteGuard
        ) {
          throw new ObservationalMemorySourceWriteConflictError(
            'Observational memory source write fencing is required but the resumed execution has no captured record guard.',
          );
        }
      }
      // A resumed execution must reuse the guard serialized in its existing
      // MessageList state. Only a fresh execution may prepare a token here.
      if (!isResume && !observationalMemorySourceWriteGuard && memory && thread?.id) {
        observationalMemorySourceWriteGuard = await memory.prepareObservationalMemorySourceWriteGuard(
          thread.id,
          resourceId,
        );
        if (observationalMemorySourceWriteGuard) {
          options.observationalMemorySourceWriteGuard = observationalMemorySourceWriteGuard;
        }
      }
      if (observationalMemorySourceWriteGuard && memory) {
        assertObservationalMemorySourceWriteGuardSupported(await memory.storage.getStore('memory'));
      }
      const messageList = new MessageList({
        threadId: thread?.id,
        resourceId,
        observationalMemorySourceWriteGuard,
        generateMessageId: capabilities.generateMessageId,
        logger: capabilities.logger,
        filterIncompleteToolCalls: memoryConfig?.filterIncompleteToolCalls,
        logicalMessageIdentity: options.logicalMessageIdentity,
        // @ts-expect-error Flag for agent network messages
        _agentNetworkAppend: capabilities._agentNetworkAppend,
      });

      // Create processorStates map - persists across loop iterations within this agent turn
      // Shared by all processor methods (input and output) for state sharing
      const processorStates = new Map<string, ProcessorState>();

      // Add instructions as system message(s)
      addSystemMessage(messageList, instructions);

      // Add MCP server guidance as a separate system message so the base
      // instructions remain a stable prefix for prompt caching.
      addSystemMessage(messageList, mcpServerGuidance, 'mcp-guidance');

      messageList.add(options.context || [], 'context');

      // Add user-provided system message if present
      addSystemMessage(messageList, options.system, 'user-provided');

      if (!memory || (!thread?.id && !resourceId)) {
        messageList.add(options.messages, 'input');
        const initialSignalEchoes = getInitialSignalEchoes(messageList);

        // Skip input processors during resume — the messageList has no user messages
        // (resumeStream passes messages: []) and the real conversation state lives in the
        // workflow snapshot. Running processors on an empty messageList would cause
        // processors like TokenLimiterProcessor to throw a TripWire.
        let tripwire;
        if (!isResume) {
          ({ tripwire } = await capabilities.runInputProcessors({
            requestContext,
            ...observabilityContext,
            messageList,
            inputProcessorOverrides: options.inputProcessors,
            processorStates,
          }));
        }

        // Class instances (MessageList) and Maps (processorStates) live on the
        // factory closure's runScope instead of step outputs, because the evented
        // engine serializes step outputs via JSON and would strip them. CreatedAgentSignal
        // carries `toDataPart`/`toLLMMessage`/`toDBMessage` methods that would not survive.
        runScope.set(MESSAGE_LIST_KEY, messageList);
        runScope.set(PROCESSOR_STATES_KEY, processorStates);
        runScope.set(INITIAL_SIGNAL_ECHOES_KEY, initialSignalEchoes);
        return {
          threadExists: false,
          thread: thread as StorageThreadType | undefined,
          tripwire,
        };
      }

      if (!thread?.id || !resourceId) {
        const mastraError = new MastraError({
          id: 'AGENT_MEMORY_MISSING_RESOURCE_ID',
          domain: ErrorDomain.AGENT,
          category: ErrorCategory.USER,
          details: {
            agentName: capabilities.agentName,
            threadId: thread?.id || '',
            resourceId: resourceId || '',
          },
          text: `A resourceId and a threadId must be provided when using Memory. Saw threadId "${thread?.id}" and resourceId "${resourceId}"`,
        });
        capabilities.logger.trackException(mastraError);
        throw mastraError;
      }

      let threadObject: StorageThreadType | undefined = undefined;
      const existingThread = await memory.getThreadById({ threadId: thread?.id });

      if (existingThread) {
        assertThreadOwnedByResource({
          thread: existingThread,
          resourceId,
          agentName: capabilities.agentName,
        });

        if (
          (!existingThread.metadata && thread.metadata) ||
          (thread.metadata && !deepEqual(existingThread.metadata, thread.metadata))
        ) {
          threadObject = await memory.saveThread({
            thread: { ...existingThread, metadata: { ...(existingThread.metadata ?? {}), ...thread.metadata } },
            memoryConfig,
            observationalMemorySourceWriteGuard,
          });
        } else {
          threadObject = existingThread;
        }
      } else {
        // saveThread: true ensures the thread is persisted to the database immediately.
        // This is required because output processors (like MessageHistory) may call
        // saveMessages() before executeOnFinish(), and some storage backends (like PostgresStore)
        // validate that the thread exists before saving messages.
        threadObject = await memory.createThread({
          threadId: thread?.id,
          metadata: thread.metadata,
          title: thread.title,
          memoryConfig,
          resourceId,
          saveThread: true,
          observationalMemorySourceWriteGuard,
        });
      }

      const memoryRunState = new MemoryRunState({
        memory,
        threadId: thread.id,
        resourceId,
        thread: threadObject ?? null,
        ownershipValidated: true,
      });
      runScope.set(MEMORY_RUN_STATE_KEY, memoryRunState);

      // Set memory context in RequestContext for processors to access
      requestContext.set('MastraMemory', {
        thread: threadObject,
        resourceId,
        memoryConfig,
        ...(observationalMemorySourceWriteGuard ? { observationalMemorySourceWriteGuard } : {}),
        runState: () => runScope.get(MEMORY_RUN_STATE_KEY),
      });

      // Add user messages - memory processors will handle history/semantic recall/working memory
      messageList.add(options.messages, 'input');
      const initialSignalEchoes = getInitialSignalEchoes(messageList);

      // Skip input processors during resume — the messageList has no user messages
      // (resumeStream passes messages: []) and the real conversation state lives in the
      // workflow snapshot. Running processors on an empty messageList would cause
      // processors like TokenLimiterProcessor to throw a TripWire.
      let tripwire;
      if (!isResume) {
        ({ tripwire } = await capabilities.runInputProcessors({
          requestContext,
          ...observabilityContext,
          messageList,
          inputProcessorOverrides: options.inputProcessors,
          processorStates,
        }));
      }

      runScope.set(MESSAGE_LIST_KEY, messageList);
      runScope.set(PROCESSOR_STATES_KEY, processorStates);
      runScope.set(INITIAL_SIGNAL_ECHOES_KEY, initialSignalEchoes);
      return {
        thread: threadObject,
        tripwire,
        threadExists: !!existingThread,
      };
    },
  });
}
