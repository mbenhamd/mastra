/**
 * Session message admission, reservation and result evidence.
 *
 * A message is admitted under a durable identity and hash; a duplicate of an
 * admitted message is answered from its live run, its durable result evidence
 * or its committed terminal failure instead of running again. These functions
 * own that duplicate resolution, the result-evidence writes, and the
 * active-work admission epoch that an abort invalidates.
 *
 * A method that carries out a session lifecycle operation across several
 * concerns stays with `Session`, the class that owns the ports:
 * `abortActiveWork` (the abort: invalidating admission is one step among
 * discarding pending interactions, downgrading plan-task wake notices and
 * cancelling subagents) and `_pendingAdmissionBlockingClose` (the close check:
 * orphan recovery, pending interactions and delegated completions). Using
 * shared services of the session, such as events, the lease check, waiters or
 * persistence, does not make a method a coordinator. They call the functions
 * here through `Session`.
 *
 * It reaches the session only through `AdmissionReservationSessionPort`,
 * which `Session` builds in `_admissionReservationPort()`.
 */

import type { Agent } from '../../agent';
import type { AgentEditorConfig, AgentThreadSubscription, ToolsInput } from '../../agent/types';
import {
  HarnessStorageAdmissionConflictError,
  HarnessStorageSessionNotFoundError,
  HarnessTerminalHandoffCancelledError,
  HarnessTerminalHandoffFencedError,
  harnessTerminalIntentId,
} from '../../storage/domains/harness';
import type {
  AgentSignalResultEvidence,
  AgentSignalResultStatus,
  HarnessStorage,
  HarnessTerminalCommitReceipt,
  HarnessTerminalExecutionGrant,
  HarnessTerminalFinalizer,
  HarnessTerminalHandoffError,
  HarnessTerminalIdentity,
  InboxResponseReceipt,
  OperationAdmissionTombstone,
  PersistedRequestContextInput,
  SessionRecord,
} from '../../storage/domains/harness';
import type { FullOutput } from '../../stream/base/output';
import { sha256CanonicalJsonChecked as sha256CanonicalJson } from './canonical-json';
import {
  HarnessAbortedError,
  HarnessAdmissionConflictError,
  HarnessBusyError,
  HarnessMessageResultErasedError,
  HarnessValidationError,
} from './errors';
import type { HarnessPendingInteractionDiscardedError } from './errors';
import { projectHarnessPublicError } from './events';
import type { Harness } from './harness';
import type { MessageAdmissionHashes, MessageAdmissionIdentity, MessageAdmissionStart, TokenUsage } from './session';
import {
  delay,
  MESSAGE_ADMISSION_DURABLE_WAIT_EXTENDED_INTERVAL_MS,
  MESSAGE_ADMISSION_DURABLE_WAIT_INTERVAL_MS,
  MESSAGE_ADMISSION_DURABLE_WAIT_MAX_INTERVAL_MS,
  MESSAGE_ADMISSION_DURABLE_WAIT_TIMEOUT_MS,
  MESSAGE_RESULT_EVIDENCE_BACKGROUND_OBSERVE_TIMEOUT_MS,
  throwIfAborted,
} from './session-durable-wait';
import { publicErrorProjectionToError } from './session-errors';
import { pendingInteractionGenerationKey } from './session-pending-interactions';
import type { RunCompletionWaiters } from './session-run-completion-waiters';
import type { RunOutcomeCache } from './session-run-outcome-cache';
import { RejectedTerminalRun } from './session-terminal-finalizers';
import {
  cloneAttachmentMetadata,
  clonePersistedRequestContext,
  normalizeObservationalMemorySourceWriteGuard,
} from './session-turn-options';
import type { AgentResult, AgentStream, MessageOptions } from './types';

export interface AdmissionReservationSessionPort {
  activeWorkAdmissionEpoch: number;
  activeWorkInvalidationsInFlight: number;
  awaitDurableMessageResult: (evidence: AgentSignalResultEvidence, opts: MessageOptions) => Promise<AgentResult>;
  awaitRunCompletion: (runId: string) => Promise<FullOutput<unknown>>;
  captureMessageSuspendWithTokenUsage: (
    full: FullOutput<unknown>,
    queuedItemId: string | undefined,
    modeId: string,
    modelId: string,
    activeTurnWaiter?: Promise<never> | undefined,
    opts?: { restoreFence?: { expectedKey?: string | undefined } | undefined },
  ) => Promise<void>;
  cleanupOperationEvidenceIfDeleted: (status: { signalId: string }) => Promise<void>;
  commitCachedDuplicateTerminal: (
    full: FullOutput<unknown> | RejectedTerminalRun,
    evidence: AgentSignalResultEvidence,
    opts: MessageOptions,
    activeDeleted?: Promise<never> | undefined,
  ) => Promise<boolean>;
  commitTerminalHandoff: (
    identity: HarnessTerminalIdentity,
    full: FullOutput<unknown> | RejectedTerminalRun,
    options: {
      modeId?: string | undefined;
      modelId?: string | undefined;
      onReceipt?: ((receipt: HarnessTerminalCommitReceipt) => void) | undefined;
      onFailure?: ((error: HarnessTerminalHandoffError) => void) | undefined;
    },
  ) => Promise<HarnessTerminalCommitReceipt | undefined>;
  discardedErrorForReceipt: (receipt: InboxResponseReceipt) => HarnessPendingInteractionDiscardedError;
  ensureThreadSubscription: (
    agent: Agent<string, ToolsInput, undefined, any, AgentEditorConfig | undefined>,
  ) => Promise<AgentThreadSubscription<any, false>>;
  readonly harness: Harness;
  readonly id: string;
  readonly messageAdmissionStarts: Map<string, MessageAdmissionStart>;
  readonly operationEvidenceSignalIds: Set<string>;
  persistTokenUsageOrLatch: () => Promise<void>;
  prepareTerminalIdentity: (
    admissionIdentity: MessageAdmissionIdentity | undefined,
    admissionId: string | undefined,
    admissionHash: string | undefined,
    executionGrant: HarnessTerminalExecutionGrant | undefined,
  ) => HarnessTerminalIdentity;
  raceActiveTurnWaiter: <T>(promise: Promise<T>, activeTurnWaiter?: Promise<never> | undefined) => Promise<T>;
  readonly record: SessionRecord;
  recordMessageTurnCompletion: (
    full: FullOutput<unknown>,
    opts?: { persist?: boolean | undefined },
  ) => { tokenUsageDelta?: TokenUsage | undefined; tokenUsageAccounted: boolean };
  readonly resourceId: string;
  readonly resultEvidenceWrites: Set<Promise<unknown>>;
  retainTerminalObservers: (
    runId: string,
    options: {
      onReceipt?: ((receipt: HarnessTerminalCommitReceipt) => void) | undefined;
      onFailure?: ((error: HarnessTerminalHandoffError) => void) | undefined;
    },
  ) => void;
  readonly runCompletionWaiters: RunCompletionWaiters;
  readonly runOutcomes: RunOutcomeCache;
  sessionDeletedError: (err: unknown) => unknown;
  stageCompletedTerminalOutput: (
    runId: string,
    full: FullOutput<unknown>,
    opts?: { tolerateUnknownRun?: boolean | undefined; provisional?: boolean | undefined },
  ) => Promise<void>;
  readonly storage: HarnessStorage;
  terminalFailure: (
    err: unknown,
    onFailure?: ((error: HarnessTerminalHandoffError) => void) | undefined,
    runId?: string | undefined,
  ) => HarnessTerminalHandoffError;
  readonly terminalFinalizer: HarnessTerminalFinalizer | undefined;
  readonly threadId: string;
  withActiveDeletedWaiter: <T>(fn: (activeTurnWaiter: Promise<never>) => Promise<T>) => Promise<T>;
}

export function captureActiveWorkAdmission(port: AdmissionReservationSessionPort, operation: string): number {
  if (port.activeWorkInvalidationsInFlight > 0) {
    void operation;
    throw new HarnessBusyError(port.id, 'in_flight');
  }
  return port.activeWorkAdmissionEpoch;
}

export function activeWorkAdmissionIsCurrent(port: AdmissionReservationSessionPort, epoch: number): boolean {
  return port.activeWorkInvalidationsInFlight === 0 && port.activeWorkAdmissionEpoch === epoch;
}

export function assertActiveWorkAdmission(
  port: AdmissionReservationSessionPort,
  epoch: number,
  operation: string,
  signal?: AbortSignal,
): void {
  if (signal?.aborted) {
    throw signal.reason instanceof Error ? signal.reason : new HarnessAbortedError(port.id, 'agent_aborted');
  }
  if (!activeWorkAdmissionIsCurrent(port, epoch)) {
    void operation;
    throw new HarnessBusyError(port.id, 'in_flight');
  }
}

export function beginActiveWorkInvalidation(port: AdmissionReservationSessionPort): () => void {
  port.activeWorkAdmissionEpoch += 1;
  port.activeWorkInvalidationsInFlight += 1;
  let released = false;
  return () => {
    if (released) return;
    released = true;
    port.activeWorkInvalidationsInFlight = Math.max(0, port.activeWorkInvalidationsInFlight - 1);
  };
}

export async function resolveMessageAdmissionDuplicate(
  port: AdmissionReservationSessionPort,
  {
    admissionId,
    admissionHash,
    compatibleAdmissionHashes,
  }: {
    admissionId: string;
    admissionHash: string;
    compatibleAdmissionHashes?: readonly string[];
  },
): Promise<AgentSignalResultEvidence | OperationAdmissionTombstone | undefined> {
  const resolved = await port.storage.resolveOperationAdmissionEvidence({
    harnessName: port.record.harnessName,
    sessionId: port.id,
    resourceId: port.resourceId,
    threadId: port.threadId,
    kind: 'signal',
    admissionId,
    attemptedAdmissionHash: admissionHash,
  });
  if (resolved.status === 'none') return undefined;
  if (resolved.status === 'conflict') {
    if (
      resolved.storedAdmissionHash !== undefined &&
      compatibleAdmissionHashes?.includes(resolved.storedAdmissionHash)
    ) {
      return resolved.evidence as AgentSignalResultEvidence | OperationAdmissionTombstone | undefined;
    }
    throw new HarnessAdmissionConflictError(port.id, admissionId, resolved.storedAdmissionHash ?? '', admissionHash);
  }
  return resolved.evidence as AgentSignalResultEvidence | OperationAdmissionTombstone | undefined;
}

export async function returnDuplicateMessageResult(
  port: AdmissionReservationSessionPort,
  evidence: AgentSignalResultEvidence | OperationAdmissionTombstone,
  opts: MessageOptions,
): Promise<AgentResult | AgentStream | unknown> {
  return port.withActiveDeletedWaiter(async activeDeleted => {
    if ('status' in evidence) {
      // A cancelled terminal admission settles its turn with this code; a
      // same-admission retry keeps reporting the cancellation.
      if (
        evidence.status === 'failed' &&
        evidence.error.code === 'harness.terminal_cancelled' &&
        opts.executionAuthorityGrant !== undefined
      ) {
        const terminalError = new HarnessTerminalHandoffCancelledError(opts.executionAuthorityGrant.key);
        try {
          opts.onTerminalCommitError?.(terminalError);
        } catch {
          // The terminal outcome below is authoritative.
        }
        throw terminalError;
      }
      if (opts.stream === true) {
        if (evidence.status === 'pending') {
          const duplicateModeId = messageDuplicateModeId(port, evidence, opts);
          const duplicateModelId = messageDuplicateModelId(port, evidence, opts);
          const agent = port.harness.getAgentForMode(duplicateModeId);
          await port.raceActiveTurnWaiter(port.ensureThreadSubscription(agent), activeDeleted);
          const runId = await pendingMessageRunId(port, evidence);
          if (runId && port.runOutcomes.has(runId)) {
            const cached = port.runOutcomes.get(runId);
            // A run whose own output rejected in this process: commit its
            // durable failed terminal instead of leaving it pending.
            if (cached !== undefined && !cached.ok && cached.runFailed === true) {
              await port.commitCachedDuplicateTerminal(
                new RejectedTerminalRun(cached.err),
                evidence,
                opts,
                activeDeleted,
              );
            }
            if (cached?.ok && evidence.admissionId !== undefined && evidence.admissionHash !== undefined) {
              await prepareCachedDuplicateMessageCompletion(port, cached.full, evidence, opts, activeDeleted);
              const terminalCommitted = await port.commitCachedDuplicateTerminal(
                cached.full,
                evidence,
                opts,
                activeDeleted,
              );
              if (!terminalCommitted) {
                await writeMessageResultEvidenceBestEffort(port, {
                  status: 'completed',
                  signalId: evidence.signalId,
                  runId,
                  modeId: duplicateModeId,
                  modelId: duplicateModelId,
                  operationKind: 'message',
                  result: cached.full,
                  admissionId: evidence.admissionId,
                  admissionHash: evidence.admissionHash,
                });
              }
            }
            throw new HarnessValidationError('message().admissionId', 'duplicate stream is no longer live');
          }
          let output = runId ? (agent.getRunOutput(runId) as AgentStream | undefined) : undefined;
          let retainedCompletedOutput = false;
          if (
            output &&
            (output as { status?: string }).status !== undefined &&
            (output as { status?: string }).status !== 'running'
          ) {
            retainedCompletedOutput = true;
            output = undefined;
          }
          if (runId && !output && !retainedCompletedOutput) {
            const waitAbortController = new AbortController();
            const completion = port.runCompletionWaiters.promiseFor(runId)?.then(
              () => undefined,
              () => undefined,
            );
            try {
              output = (await Promise.race([
                port.raceActiveTurnWaiter(
                  (
                    agent.waitForRunOutput(runId, { abortSignal: waitAbortController.signal }) as Promise<AgentStream>
                  ).catch(() => undefined),
                  activeDeleted,
                ),
                ...(completion ? [completion] : []),
                delay(MESSAGE_ADMISSION_DURABLE_WAIT_TIMEOUT_MS, waitAbortController.signal).then(
                  () => undefined,
                  () => undefined,
                ),
              ])) as AgentStream | undefined;
            } finally {
              waitAbortController.abort(
                new HarnessValidationError('message().admissionId', 'duplicate stream wait ended'),
              );
            }
          }
          if (output) {
            // The returned stream may have been adopted after the original
            // caller died between dispatch and bookkeeping — retain this
            // retry's terminal observers AND take settlement ownership: a
            // detached task runs the durable completion bookkeeping the
            // original caller would have run (a suspended outcome must still
            // park its pendingResume or the approval can never be answered)
            // and then drives the terminal commit when the run ends. The
            // per-runId in-flight dedup makes the commit a no-op when the
            // original caller is still alive and commits first.
            const adoptedTerminal =
              port.terminalFinalizer !== undefined &&
              opts.executionAuthorityGrant !== undefined &&
              runId !== undefined &&
              evidence.admissionId !== undefined &&
              evidence.admissionHash !== undefined;
            if (adoptedTerminal) {
              port.retainTerminalObservers(runId!, {
                onReceipt: opts.onTerminalCommit,
                onFailure: opts.onTerminalCommitError,
              });
            }
            // `_prepareTerminalIdentity` can throw synchronously — drain the
            // observers retained above before propagating or they strand.
            let identity: HarnessTerminalIdentity | undefined;
            try {
              identity = adoptedTerminal
                ? port.prepareTerminalIdentity(
                    messageAdmissionIdentity(port, evidence.admissionId!),
                    evidence.admissionId!,
                    evidence.admissionHash!,
                    opts.executionAuthorityGrant!,
                  )
                : undefined;
            } catch (err) {
              throw port.terminalFailure(err, undefined, runId);
            }
            const duplicateMode = messageDuplicateModeId(port, evidence, opts);
            const duplicateModel = messageDuplicateModelId(port, evidence, opts);
            // Settle through the canonical run-completion barrier, not the raw
            // stream output — `getFullOutput()` alone misses the output-drain
            // barrier, tool receipts, and terminal-text materialization, so a
            // terminal-tool response would commit empty evidence instead of
            // the authoritative answer.
            void port
              .awaitRunCompletion(runId!)
              .then(async full => {
                const fullOutput = full as FullOutput<unknown>;
                try {
                  await prepareCachedDuplicateMessageCompletion(port, fullOutput, evidence, opts, activeDeleted);
                  if (identity !== undefined) {
                    await port.commitTerminalHandoff(identity, fullOutput, {
                      modeId: duplicateMode,
                      modelId: duplicateModel,
                    });
                  }
                } catch (err) {
                  // `_commitTerminalHandoff` drains retained observers on its
                  // own failures, but pre-commit failures (suspension park,
                  // usage persist, run-materialization) never reach that
                  // drain — surface an indeterminate outcome here instead of
                  // stranding the observers retained above. The drain is
                  // idempotent against the helper's own.
                  port.terminalFailure(err, undefined, runId);
                }
              })
              .catch(async err => {
                // A run whose own output rejected commits its durable failed
                // terminal (the commit drains the retained observers).
                const cached = port.runOutcomes.get(runId!);
                if (
                  identity !== undefined &&
                  cached !== undefined &&
                  !cached.ok &&
                  cached.runFailed === true &&
                  cached.err === err
                ) {
                  await port
                    .commitTerminalHandoff(identity, new RejectedTerminalRun(err), {
                      modeId: duplicateMode,
                      modelId: duplicateModel,
                    })
                    .catch(() => {});
                  return;
                }
                // Otherwise `_awaitRunCompletion` itself failed (collector or
                // output-drain) — the `.then` never ran, so no settlement
                // helper saw the failure. Drain the retained observers with
                // the indeterminate outcome; the admission stays pending for
                // recovery either way.
                port.terminalFailure(err, undefined, runId);
              });
            return output;
          }
        }
        // A committed terminal admission must replay its durable receipt to
        // this retry's observers before reporting the stream as unavailable —
        // the throw below is not a substitute for the terminal outcome.
        if (evidence.status === 'completed' && evidence.erased === true) {
          await rejectErasedDuplicate(port, evidence, opts, activeDeleted);
        }
        if (
          evidence.status === 'completed' &&
          port.terminalFinalizer !== undefined &&
          opts.executionAuthorityGrant !== undefined
        ) {
          await port.commitCachedDuplicateTerminal(
            completedEvidenceResult(port, evidence) as FullOutput<unknown>,
            evidence,
            opts,
            activeDeleted,
          );
        }
        if (evidence.status === 'failed') await replayCommittedTerminalFailure(port, evidence, opts, activeDeleted);
        throw new HarnessValidationError('message().admissionId', 'duplicate stream is no longer live');
      }
      if (evidence.status === 'completed') {
        // A terminal caller still needs its durable receipt replayed — the
        // committed fast-path converges on the stored winner without
        // recomputing it.
        if (evidence.erased === true) await rejectErasedDuplicate(port, evidence, opts, activeDeleted);
        const completedResult = completedEvidenceResult(port, evidence);
        // A completed (terminal, immutable) suspended result whose
        // suspension was discarded replays the discard, not `suspended`.
        const discardedSuspension = discardReceiptForSuspendedOutput(port, completedResult);
        if (discardedSuspension !== undefined) throw port.discardedErrorForReceipt(discardedSuspension);
        if (port.terminalFinalizer !== undefined && opts.executionAuthorityGrant !== undefined) {
          await port.commitCachedDuplicateTerminal(
            completedResult as FullOutput<unknown>,
            evidence,
            opts,
            activeDeleted,
          );
        }
        return completedResult;
      }
      if (evidence.status === 'failed') {
        await replayCommittedTerminalFailure(port, evidence, opts, activeDeleted);
        throw publicErrorProjectionToError(evidence.error);
      }
      const runId = await pendingMessageRunId(port, evidence);
      if (runId) {
        const duplicateModeId = messageDuplicateModeId(port, evidence, opts);
        const duplicateModelId = messageDuplicateModelId(port, evidence, opts);
        const agent = port.harness.getAgentForMode(duplicateModeId);
        await port.raceActiveTurnWaiter(port.ensureThreadSubscription(agent), activeDeleted);
        const cached = port.runOutcomes.get(runId);
        if (cached) {
          if (!cached.ok) {
            // A run whose own output rejected in this process: commit its
            // durable failed terminal (a no-op replay once committed), then
            // report the run's failure as before.
            if (cached.runFailed === true) {
              await port.commitCachedDuplicateTerminal(
                new RejectedTerminalRun(cached.err),
                evidence,
                opts,
                activeDeleted,
              );
            }
            throw cached.err;
          }
          if (evidence.admissionId !== undefined && evidence.admissionHash !== undefined) {
            await prepareCachedDuplicateMessageCompletion(port, cached.full, evidence, opts, activeDeleted);
            const terminalCommitted = await port.commitCachedDuplicateTerminal(
              cached.full,
              evidence,
              opts,
              activeDeleted,
            );
            if (!terminalCommitted) {
              await writeMessageResultEvidenceBestEffort(port, {
                status: 'completed',
                signalId: evidence.signalId,
                runId,
                modeId: duplicateModeId,
                modelId: duplicateModelId,
                operationKind: 'message',
                result: cached.full,
                admissionId: evidence.admissionId,
                admissionHash: evidence.admissionHash,
              });
            }
          }
          return cached.full;
        }
        if (!hasLiveMessageRun(port, agent, runId)) {
          let durable: AgentResult;
          try {
            durable = await port.raceActiveTurnWaiter(port.awaitDurableMessageResult(evidence, opts), activeDeleted);
          } catch (err) {
            // The admission may have committed a failed or interrupted
            // terminal meanwhile: replay its durable receipt to this retry.
            // A completed turn whose result a session delete erased mid-wait
            // replays its completed receipt too (PF-5309).
            await replayCommittedTerminalFailure(
              port,
              evidence,
              opts,
              activeDeleted,
              err instanceof HarnessMessageResultErasedError ? { includeCompleted: true } : {},
            );
            throw err;
          }
          // The durable wait resolved because the admission committed — the
          // committed fast-path replays the stored receipt so this retry's
          // terminal observers see the same outcome as the winner's caller.
          await port.commitCachedDuplicateTerminal(durable as FullOutput<unknown>, evidence, opts, activeDeleted);
          return durable;
        }
        let live: FullOutput<unknown>;
        try {
          live = await port.raceActiveTurnWaiter(port.awaitRunCompletion(runId), activeDeleted);
        } catch (err) {
          // A live run whose own output rejected: commit (or replay) its
          // durable failed terminal for this retry, then report the failure.
          const rejected = port.runOutcomes.get(runId);
          if (rejected !== undefined && !rejected.ok && rejected.runFailed === true && rejected.err === err) {
            await port.commitCachedDuplicateTerminal(new RejectedTerminalRun(err), evidence, opts, activeDeleted);
          }
          throw err;
        }
        if (evidence.admissionId !== undefined && evidence.admissionHash !== undefined) {
          await prepareCachedDuplicateMessageCompletion(port, live, evidence, opts, activeDeleted);
          const terminalCommitted = await port.commitCachedDuplicateTerminal(live, evidence, opts, activeDeleted);
          if (!terminalCommitted) {
            await writeMessageResultEvidenceBestEffort(port, {
              status: 'completed',
              signalId: evidence.signalId,
              runId,
              modeId: duplicateModeId,
              modelId: duplicateModelId,
              operationKind: 'message',
              result: live,
              admissionId: evidence.admissionId,
              admissionHash: evidence.admissionHash,
            });
          }
        }
        return live;
      }
    }
    throw new HarnessValidationError('message().admissionId', 'duplicate message result evidence has expired');
  });
}

/**
 * A same-admission retry whose turn already committed a non-completed
 * terminal — a rejected run's `failed` result, or an interrupted dispatch's
 * `aborted` one — gets that durable receipt replayed to its
 * `onTerminalCommit`, as a retry of a completed turn does, before it reports
 * the failure. An admission that is not committed is left alone, and a
 * failed lookup only skips the replay: the durable intent is authoritative.
 */
export async function replayCommittedTerminalFailure(
  port: AdmissionReservationSessionPort,
  evidence: AgentSignalResultEvidence,
  opts: MessageOptions,
  activeDeleted?: Promise<never>,
  replay: { includeCompleted?: true } = {},
): Promise<void> {
  const grant = opts.executionAuthorityGrant;
  if (
    opts.onTerminalCommit === undefined ||
    grant === undefined ||
    evidence.admissionId === undefined ||
    !port.storage.supportsTerminalHandoff
  ) {
    return;
  }
  let receipt: HarnessTerminalCommitReceipt;
  try {
    const admission = await port.raceActiveTurnWaiter(
      port.storage.loadTerminalAdmission({
        harnessName: port.record.harnessName,
        sessionId: port.id,
        admissionId: evidence.admissionId,
        executionGrant: grant,
      }),
      activeDeleted,
    );
    if (
      admission?.status !== 'committed' ||
      (replay.includeCompleted !== true && admission.terminalResult?.status === 'completed')
    ) {
      return;
    }
    const intent = await port.raceActiveTurnWaiter(
      port.storage.loadTerminalIntent({
        harnessName: admission.harnessName,
        intentId: harnessTerminalIntentId(admission.id),
      }),
      activeDeleted,
    );
    receipt = { status: 'duplicate', admission, ...(intent !== null ? { intent } : {}) };
  } catch {
    return;
  }
  try {
    opts.onTerminalCommit(receipt);
  } catch {
    // Receipt observers are diagnostics only and cannot change the durable winner.
  }
}

export async function prepareCachedDuplicateMessageCompletion(
  port: AdmissionReservationSessionPort,
  full: FullOutput<unknown>,
  evidence: AgentSignalResultEvidence,
  opts: MessageOptions,
  activeDeleted?: Promise<never>,
): Promise<void> {
  const runId = full.runId ?? evidence.runId;
  if (runId !== undefined) {
    try {
      // The cached output exists only in this process, so a native run
      // here is known and still fails closed on a lookup failure.
      await port.raceActiveTurnWaiter(
        port.stageCompletedTerminalOutput(runId, full, { tolerateUnknownRun: true }),
        activeDeleted,
      );
    } catch (error) {
      throw port.terminalFailure(error, opts.onTerminalCommitError, runId);
    }
  }
  if (full.finishReason === 'suspended') {
    // A cached suspension is a replay of an earlier generation — a resumed
    // run may already have parked a *newer* pendingResume (different tool
    // call, or a different run entirely). The durable park is authoritative;
    // restoring the stale cached suspension would revert it.
    const parked = port.record.pendingResume;
    const payload = full.suspendPayload as { toolCallId?: string } | undefined;
    if (parked !== undefined && (parked.runId !== full.runId || parked.toolCallId !== payload?.toolCallId)) {
      return;
    }
    // `discardPendingInteraction` resolved this suspended call. Re-parking
    // the cached suspension (with a fresh `requestedAt`) would make the
    // discarded run answerable again; settle the still-pending message
    // admission as discarded instead and report that outcome.
    const discardedCallRunId = full.runId ?? evidence.runId;
    const discard = parked === undefined ? discardReceiptForSuspendedOutput(port, full, evidence.runId) : undefined;
    if (discard !== undefined) {
      const discardError = port.discardedErrorForReceipt(discard);
      if (evidence.status === 'pending') {
        // Same admitted failure write a failed message turn makes, so later
        // retries replay a terminal outcome. Best effort: the discard
        // receipt keeps refusing the re-park if this write fails.
        await port.raceActiveTurnWaiter(
          writeMessageResultEvidence(port, {
            status: 'failed',
            signalId: evidence.signalId,
            runId: discardedCallRunId,
            ...(evidence.modeId !== undefined ? { modeId: evidence.modeId } : {}),
            ...(evidence.modelId !== undefined ? { modelId: evidence.modelId } : {}),
            operationKind: 'message',
            error: projectHarnessPublicError(discardError),
            ...(evidence.admissionId !== undefined && evidence.admissionHash !== undefined
              ? { admissionId: evidence.admissionId, admissionHash: evidence.admissionHash }
              : {}),
          }).catch(() => undefined),
          activeDeleted,
        );
      }
      throw discardError;
    }
    // The deferred admission bound to this run may already be terminal
    // (aborted/cancelled/fenced) or sealed. Re-parking the cached suspension
    // then resurrects a pending interaction whose grant can never settle —
    // a later respond would drive the provider against a revoked or finished
    // grant. Only a still-pending admission may be re-parked here; the
    // commit helper below reports the terminal outcome to this caller. The
    // probe keys off durable state, not local finalizer registration — a
    // reopened session without a finalizer must still see the dead grant.
    const admissionRunId = full.runId ?? evidence.runId;
    const sessionIncarnation = port.record.sessionIncarnation;
    if (
      port.storage.supportsTerminalHandoff &&
      sessionIncarnation !== undefined &&
      evidence.admissionId !== undefined &&
      admissionRunId !== undefined
    ) {
      const admission = await port.raceActiveTurnWaiter(
        port.storage.loadTerminalAdmissionByRun({
          harnessName: port.record.harnessName,
          sessionId: port.id,
          runId: admissionRunId,
          sessionIncarnation,
        }),
        activeDeleted,
      );
      if (admission !== null && admission.status !== 'pending') return;
    }
    await port.captureMessageSuspendWithTokenUsage(
      full,
      undefined,
      messageDuplicateModeId(port, evidence, opts),
      messageDuplicateModelId(port, evidence, opts),
      activeDeleted,
      {
        restoreFence: {
          expectedKey: parked === undefined ? undefined : pendingInteractionGenerationKey(parked),
        },
      },
    );
    return;
  }

  const { tokenUsageAccounted } = port.recordMessageTurnCompletion(full, { persist: false });
  if (tokenUsageAccounted) {
    await port.raceActiveTurnWaiter(port.persistTokenUsageOrLatch(), activeDeleted);
  }
}

export async function pendingMessageRunId(
  port: AdmissionReservationSessionPort,
  evidence: AgentSignalResultEvidence,
): Promise<string | undefined> {
  if (evidence.status !== 'pending') return evidence.runId;
  const starting = evidence.admissionId ? port.messageAdmissionStarts.get(evidence.admissionId) : undefined;
  if (!starting) return evidence.runId;
  try {
    const startingEvidence = await starting.promise;
    return startingEvidence.runId ?? evidence.runId;
  } catch {
    return evidence.runId;
  }
}

export function messageDuplicateModeId(
  port: AdmissionReservationSessionPort,
  evidence: AgentSignalResultEvidence,
  opts: MessageOptions,
): string {
  const starting = evidence.admissionId ? port.messageAdmissionStarts.get(evidence.admissionId) : undefined;
  return starting?.modeId ?? evidence.modeId ?? opts.mode ?? port.record.modeId;
}

export function messageDuplicateModelId(
  port: AdmissionReservationSessionPort,
  evidence: AgentSignalResultEvidence,
  opts: MessageOptions,
): string {
  const starting = evidence.admissionId ? port.messageAdmissionStarts.get(evidence.admissionId) : undefined;
  return starting?.modelId ?? evidence.modelId ?? opts.model ?? port.record.modelId;
}

export function hasLiveMessageRun(port: AdmissionReservationSessionPort, agent: Agent, runId: string): boolean {
  return Boolean(agent.getRunOutput(runId) || port.runCompletionWaiters.has(runId) || port.runOutcomes.has(runId));
}

export async function awaitDurableMessageResult(
  port: AdmissionReservationSessionPort,
  evidence: AgentSignalResultEvidence,
  opts: MessageOptions,
): Promise<AgentResult> {
  const deadline = Date.now() + MESSAGE_ADMISSION_DURABLE_WAIT_TIMEOUT_MS;
  let extendedWaitMs = MESSAGE_ADMISSION_DURABLE_WAIT_EXTENDED_INTERVAL_MS;
  while (true) {
    throwIfAborted(opts.abortSignal, 'message().admissionId');
    const latest = await port.storage.loadMessageResultEvidence({
      harnessName: port.record.harnessName,
      sessionId: port.id,
      resourceId: port.resourceId,
      threadId: port.threadId,
      signalId: evidence.signalId,
    });
    if (!latest) {
      throw new HarnessValidationError('message().admissionId', 'duplicate message result evidence has expired');
    }
    if ('status' in latest) {
      if (latest.status === 'completed') return completedEvidenceResult(port, latest);
      if (latest.status === 'failed') throw publicErrorProjectionToError(latest.error);
      // While canonical evidence stays pending, consult the durable terminal
      // admission each pass — a cancelled/fenced grant can never produce the
      // completed evidence this loop waits on, and surfacing the durable
      // outcome beats expiring into an unrelated liveness error (or waiting
      // forever on a parked pendingResume).
      const sessionIncarnation = port.record.sessionIncarnation;
      if (port.storage.supportsTerminalHandoff && sessionIncarnation !== undefined && latest.runId !== undefined) {
        const boundAdmission = await port.storage.loadTerminalAdmissionByRun({
          harnessName: port.record.harnessName,
          sessionId: port.id,
          runId: latest.runId,
          sessionIncarnation,
        });
        if (boundAdmission?.status === 'cancelled' || boundAdmission?.status === 'fenced') {
          const terminalError =
            boundAdmission.status === 'cancelled'
              ? new HarnessTerminalHandoffCancelledError(boundAdmission.executionGrant.key)
              : new HarnessTerminalHandoffFencedError(port.id);
          try {
            opts.onTerminalCommitError?.(terminalError);
          } catch {
            // The terminal outcome below is authoritative.
          }
          throw terminalError;
        }
      }
    } else {
      throw new HarnessValidationError('message().admissionId', 'duplicate message result evidence has expired');
    }
    let pastDeadline = false;
    if (Date.now() >= deadline) {
      // A terminal-mode message whose run is parked in `pendingResume`
      // legitimately stays pending until the approval-gated resume settles
      // the deferred admission — waiting past the probe deadline is correct.
      // Key off the durable parked state, not local finalizer registration —
      // a reopened session that has not re-registered a finalizer must still
      // extend the wait for a run durably parked in pendingResume.
      const suspendedOnThisRun = evidence.runId !== undefined && port.record.pendingResume?.runId === evidence.runId;
      if (!suspendedOnThisRun) {
        throw new HarnessValidationError('message().admissionId', 'pending message admission is not live');
      }
      pastDeadline = true;
    }
    await delay(pastDeadline ? extendedWaitMs : MESSAGE_ADMISSION_DURABLE_WAIT_INTERVAL_MS, opts.abortSignal);
    if (pastDeadline) {
      extendedWaitMs = Math.min(extendedWaitMs * 2, MESSAGE_ADMISSION_DURABLE_WAIT_MAX_INTERVAL_MS);
    }
  }
}

export function messageAdmissionIdentity(
  port: AdmissionReservationSessionPort,
  admissionId: string,
): MessageAdmissionIdentity {
  const digest = sha256CanonicalJson({
    kind: 'message-admission',
    harnessName: port.record.harnessName,
    sessionId: port.id,
    resourceId: port.resourceId,
    threadId: port.threadId,
    admissionId,
  });
  return {
    signalId: `harness-message-${digest.slice(0, 32)}`,
    runId: `harness-message-${digest.slice(32, 64)}`,
  };
}

export async function writeMessageResultEvidence(
  port: AdmissionReservationSessionPort,
  status: AgentSignalResultStatus & { admissionId?: string; admissionHash?: string },
  options?: { compatibleAdmissionHashes?: readonly string[]; leaseOwner?: { ownerId: string } },
): Promise<{
  created: boolean;
  applied: boolean;
  evidence?: AgentSignalResultEvidence | OperationAdmissionTombstone;
}> {
  if (status.admissionId !== undefined && status.admissionHash !== undefined && status.operationKind === undefined) {
    throw new Error(
      `Invalid Harness admission evidence for session "${port.id}" signal "${status.signalId}": operationKind is required`,
    );
  }
  const now = Date.now();
  port.operationEvidenceSignalIds.add(status.signalId);
  try {
    const evidence: AgentSignalResultEvidence = {
      ...status,
      harnessName: port.record.harnessName,
      sessionId: port.id,
      resourceId: port.resourceId,
      threadId: port.threadId,
      createdAt: now,
      updatedAt: now,
    };
    const write = port.storage.writeMessageResultEvidence(
      evidence,
      options?.leaseOwner !== undefined ? { leaseOwner: options.leaseOwner } : undefined,
    );
    // A turn's result write can outlive the turn (an aborted turn does not
    // await it); close waits for these before judging what is pending.
    port.resultEvidenceWrites.add(write);
    void write
      .finally(() => {
        port.resultEvidenceWrites.delete(write);
      })
      .catch(() => {});
    const result = await write;
    await port.cleanupOperationEvidenceIfDeleted(status);
    return result.created ? { created: true, applied: result.applied, evidence } : result;
  } catch (err) {
    if (err instanceof HarnessStorageSessionNotFoundError) throw port.sessionDeletedError(err);
    if (err instanceof HarnessStorageAdmissionConflictError && status.admissionId && status.admissionHash) {
      const duplicate = await resolveMessageAdmissionDuplicate(port, {
        admissionId: status.admissionId,
        admissionHash: status.admissionHash,
        compatibleAdmissionHashes: options?.compatibleAdmissionHashes,
      });
      if (duplicate) return { created: false, applied: false, evidence: duplicate };
      throw new HarnessAdmissionConflictError(port.id, status.admissionId, '', status.admissionHash);
    }
    throw err;
  }
}

export async function writeMessageResultEvidenceBestEffort(
  port: AdmissionReservationSessionPort,
  status: AgentSignalResultStatus & { admissionId?: string; admissionHash?: string },
  options?: { compatibleAdmissionHashes?: readonly string[] },
): Promise<
  | {
      created: boolean;
      applied: boolean;
      evidence?: AgentSignalResultEvidence | OperationAdmissionTombstone;
    }
  | undefined
> {
  try {
    return await writeMessageResultEvidence(port, status, options);
  } catch (err) {
    if (status.admissionId !== undefined) throw err;
    // The initial pre-dispatch admission reservation is the durable barrier.
    // Non-idempotent callers have no durable replay contract, so storage
    // evidence is only best-effort for them.
  }
}

export function writeMessageResultEvidenceBestEffortInBackground(
  port: AdmissionReservationSessionPort,
  status: AgentSignalResultStatus & { admissionId?: string; admissionHash?: string },
  options?: { compatibleAdmissionHashes?: readonly string[] },
): void {
  const write = writeMessageResultEvidenceBestEffort(port, status, options);
  let timer: ReturnType<typeof setTimeout> | undefined;
  const timeout = new Promise<void>(resolve => {
    timer = setTimeout(resolve, MESSAGE_RESULT_EVIDENCE_BACKGROUND_OBSERVE_TIMEOUT_MS);
    timer.unref?.();
  });
  void Promise.race([write, timeout])
    .catch(() => {})
    .finally(() => {
      if (timer !== undefined) clearTimeout(timer);
    });
}

/**
 * A duplicate of a turn whose session was deleted (PF-5309): its result is
 * erased, but its committed terminal receipt is still durable. Replay that
 * receipt to the caller's terminal observers, then fail with the typed
 * erased error, so no observer is left waiting and no caller sees an empty
 * success.
 */
export async function rejectErasedDuplicate(
  port: AdmissionReservationSessionPort,
  evidence: AgentSignalResultEvidence & { status: 'completed' },
  opts: MessageOptions,
  activeDeleted?: Promise<never>,
): Promise<never> {
  await replayCommittedTerminalFailure(port, evidence, opts, activeDeleted, { includeCompleted: true });
  throw new HarnessMessageResultErasedError(port.id, evidence.signalId);
}

export function completedEvidenceResult(
  port: AdmissionReservationSessionPort,
  evidence: { signalId: string; result: unknown; erased?: true },
): AgentResult {
  if (evidence.erased === true) throw new HarnessMessageResultErasedError(port.id, evidence.signalId);
  return evidence.result as AgentResult;
}

export function computeMessageAdmissionHashes(
  port: AdmissionReservationSessionPort,
  opts: MessageOptions,
  stable: { modeId: string; modelId: string },
  persistedRequestContext?: PersistedRequestContextInput,
): MessageAdmissionHashes {
  const primary = sha256CanonicalJson(
    messageAdmissionHashInput(port, opts, undefined, { hashVersion: 2 }, persistedRequestContext),
  );
  // Pre-v2 evidence hashed the effective mode/model. Keep compatibility
  // candidates for the current effective tuple only; old evidence does not
  // persist enough metadata to safely infer previous defaults after drift.
  const legacyCompatible = [
    sha256CanonicalJson(messageAdmissionHashInput(port, opts, stable, undefined, persistedRequestContext)),
    sha256CanonicalJson(
      messageAdmissionHashInput(port, opts, stable, { includeAttachmentMetadata: false }, persistedRequestContext),
    ),
  ];
  return {
    primary,
    legacyCompatible: [...new Set(legacyCompatible)].filter(hash => hash !== primary),
  };
}

export function messageAdmissionHashInput(
  port: AdmissionReservationSessionPort,
  opts: MessageOptions,
  stable?: { modeId: string; modelId: string },
  options?: { hashVersion?: number; includeAttachmentMetadata?: boolean },
  persistedRequestContext?: PersistedRequestContextInput,
) {
  return {
    // Operation-kind discriminator in the admission hash (signal vs queue);
    // §5.1d token is `'signal'`. Distinct from the queue path's `kind: 'queue'`.
    kind: 'signal',
    ...(options?.hashVersion !== undefined ? { hashVersion: options.hashVersion } : {}),
    content: opts.content,
    ...(stable !== undefined
      ? { mode: stable.modeId, model: stable.modelId }
      : {
          ...(opts.mode !== undefined ? { mode: opts.mode } : {}),
          ...(opts.model !== undefined ? { model: opts.model } : {}),
        }),
    ...(opts.modelSettings !== undefined ? { modelSettings: opts.modelSettings } : {}),
    ...(opts.logicalMessageIdentity !== undefined
      ? { logicalMessageIdentity: { ...opts.logicalMessageIdentity } }
      : {}),
    ...(opts.executionAuthorityGrant !== undefined
      ? { executionAuthorityGrant: { ...opts.executionAuthorityGrant } }
      : {}),
    ...(opts.terminalAdmissionSeed !== undefined
      ? { terminalAdmissionSeed: structuredClone(opts.terminalAdmissionSeed) }
      : {}),
    attachments: (opts.attachments ?? []).map(attachment => ({
      attachmentId: attachment.attachmentId,
      resourceId: attachment.resourceId,
      ...(attachment.ownerSessionId !== undefined ? { ownerSessionId: attachment.ownerSessionId } : {}),
      ...(attachment.bytes !== undefined ? { bytes: attachment.bytes } : {}),
      ...(attachment.sha256 !== undefined ? { sha256: attachment.sha256 } : {}),
      ...(attachment.source !== undefined ? { source: attachment.source } : {}),
      ...(options?.includeAttachmentMetadata !== false
        ? {
            ...(attachment.kind !== undefined ? { kind: attachment.kind } : {}),
            ...(attachment.name !== undefined ? { name: attachment.name } : {}),
            ...(attachment.mimeType !== undefined ? { mimeType: attachment.mimeType } : {}),
            ...(attachment.primitiveType !== undefined ? { primitiveType: attachment.primitiveType } : {}),
            ...(attachment.elementType !== undefined ? { elementType: attachment.elementType } : {}),
            ...(attachment.renderer !== undefined ? { renderer: attachment.renderer } : {}),
            ...(attachment.schemaId !== undefined ? { schemaId: attachment.schemaId } : {}),
            ...(attachment.metadata !== undefined ? { metadata: cloneAttachmentMetadata(attachment.metadata) } : {}),
            ...(attachment.object !== undefined ? { object: attachment.object } : {}),
          }
        : {}),
    })),
    // §4.4c / §5.1: caller request context is part of admission identity when
    // present. Absent => omitted => the hash is byte-identical to pre-feature
    // evidence (backward-compatible). Mirrors the queue path's requestContext.
    ...(persistedRequestContext ? { requestContext: clonePersistedRequestContext(persistedRequestContext) } : {}),
    ...(opts.observationalMemorySourceWriteGuard
      ? {
          observationalMemorySourceWriteGuard: normalizeObservationalMemorySourceWriteGuard(
            opts.observationalMemorySourceWriteGuard,
            'message().observationalMemorySourceWriteGuard',
          ),
        }
      : {}),
  };
}

/** The discard receipt for the suspended call `output` parked on, if discarded. */
export function discardReceiptForSuspendedOutput(
  port: AdmissionReservationSessionPort,
  output: unknown,
  fallbackRunId?: string,
): InboxResponseReceipt | undefined {
  const full = output as { finishReason?: unknown; runId?: unknown; suspendPayload?: unknown } | undefined;
  if (full?.finishReason !== 'suspended') return undefined;
  const runId = typeof full.runId === 'string' ? full.runId : fallbackRunId;
  const toolCallId = (full.suspendPayload as { toolCallId?: unknown } | undefined)?.toolCallId;
  if (runId === undefined || typeof toolCallId !== 'string') return undefined;
  return Object.values(port.record.inboxResponseReceipts ?? {}).find(
    receipt => receipt.discardedAt !== undefined && receipt.runId === runId && receipt.toolCallId === toolCallId,
  );
}
