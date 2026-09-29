import { z } from 'zod';
import type { PubSub } from '../../../../events/pubsub';
import { drainSignalsToTranscript } from '../../../../loop/shared/steps/signal-drain-core';
import type { Mastra } from '../../../../mastra';
import { PUBSUB_SYMBOL } from '../../../../workflows/constants';
import { createStep } from '../../../../workflows/workflow';
import { ensureRemoteAbortListener } from '../../abort-transport';
import { DurableStepIds } from '../../constants';
import { getBoundRunRegistryEntry } from '../../run-registry';
import { emitChunkEvent } from '../../stream-adapter';
import { createRunMessageList } from '../../utils/run-message-list';

const SIGNAL_DRAIN_STEP_ID = `${DurableStepIds.AGENTIC_EXECUTION}-signal-drain`;

/**
 * Create a standalone durable signal drain step for integrations that compose
 * their own iteration workflow (e.g. `@mastra/inngest`) instead of using the
 * core `DurableLoopBuilder`, whose drain step is not reusable outside it.
 *
 * Sits between the background-task check and the iteration-state mapping:
 * - Drains any signals queued while tool execution was running
 * - Adds drained signals to the messageList transcript
 * - Emits signal chunks via pubsub for the stream adapter
 * - Clears a terminal candidate and sets isContinued=true so the LLM
 *   processes the signals on the next turn
 * - Best-effort: signals remain queued if the drain itself fails
 *
 * The drain sequence is the shared `drainSignalsToTranscript` core used by the
 * builder's drain sites. The runtime binding is checked before the remote
 * abort listener is installed and re-checked afterwards, so a reused caller
 * runId never drains another execution's signals.
 */
export function createDurableSignalDrainStep() {
  return createStep({
    id: SIGNAL_DRAIN_STEP_ID,
    inputSchema: z.any(),
    outputSchema: z.any(),
    execute: async params => {
      const { inputData, getInitData } = params;
      const execOutput = inputData as Record<string, any>;
      const initData = getInitData<{ runId: string; runtimeBindingId?: string }>();
      const runId = initData.runId;
      getBoundRunRegistryEntry(runId, initData.runtimeBindingId);
      const pubsub = (params as any)[PUBSUB_SYMBOL] as PubSub | undefined;
      if (pubsub) {
        try {
          await ensureRemoteAbortListener(pubsub, runId, initData.runtimeBindingId);
        } catch (error) {
          params.mastra?.getLogger?.()?.warn?.('Failed to subscribe to cross-process abort requests', {
            runId,
            error: error instanceof Error ? error.message : String(error),
          });
        }
      }
      const registryEntry = getBoundRunRegistryEntry(runId, initData.runtimeBindingId);
      if (!registryEntry?.drainPendingSignals) return execOutput;

      const mastra = params.mastra as Mastra | undefined;
      try {
        let drainList: ReturnType<typeof createRunMessageList> | undefined;
        const list = () => (drainList ??= createRunMessageList({ mastra }).deserialize(execOutput.messageListState));
        const outcome = await drainSignalsToTranscript({
          drainPendingSignals: registryEntry.drainPendingSignals,
          rotateResponseMessageId: sealMessageId => list().rotateResponseMessageId(sealMessageId),
          addSignal: signal => list().addSignal(signal),
          emitChunk: async chunk => {
            if (pubsub) await emitChunkEvent(pubsub, runId, chunk as any);
          },
          sealMessageId: execOutput.messageId,
          errorPolicy: 'best-effort',
          logger: mastra?.getLogger?.(),
        });
        if (!outcome.drained || !drainList) return execOutput;

        return {
          ...execOutput,
          terminalToolResult: undefined,
          messageListState: drainList.serialize(),
          messageId: outcome.nextMessageId,
          stepResult: {
            ...execOutput.stepResult,
            messageId: outcome.nextMessageId,
            isContinued: true,
          },
        };
      } catch {
        // Transcript mutations are local to this step's drainList, so
        // returning execOutput drops them cleanly.
        return execOutput;
      }
    },
  });
}
