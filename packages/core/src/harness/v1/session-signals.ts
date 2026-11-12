/**
 * Session signal admission, dispatch and settlement.
 *
 * A signal is admitted under a durable identity and hash, claimed for
 * dispatch (a compare-and-swap kept alive by a claim heartbeat), accepted by
 * the native run, and settled once into durable result evidence that
 * duplicates and later callers read back. These functions own that lifecycle,
 * wake-terminal turn admission and system-reminder injection. The public
 * `signal()` entry point, the queue and message admission stay with `Session`.
 *
 * It reaches the session only through `SignalsSessionPort`, which `Session`
 * builds in `_signalsPort()`.
 */

import { randomUUID } from 'node:crypto';
import type { Agent } from '../../agent';
import type { AgentExecutionOptionsBase } from '../../agent/agent.types';
import type {
  AGENT_EXECUTION_OPTION_COMPOSERS,
  AgentExecutionOptionComposers,
} from '../../agent/merge-execution-options';
import { createSignal } from '../../agent/signals';
import type { AgentSignalContents, CreatedAgentSignal } from '../../agent/signals';
import type { AgentEditorConfig, AgentThreadSubscription, ToolsInput } from '../../agent/types';
import type { TracingContext } from '../../observability';
import type { RequestContext } from '../../request-context';
import {
  HarnessTerminalFinalizationPendingError,
  HarnessTerminalHandoffCancelledError,
  HarnessTerminalHandoffFencedError,
  HarnessTerminalHandoffIdentityConflictError,
  harnessWakeExecutionGrant,
  validateHarnessTerminalExecutionGrant,
} from '../../storage/domains/harness';
import type {
  AgentSignalDispatchState,
  AgentSignalResultEvidence,
  AgentSignalResultStatus,
  HarnessStorage,
  HarnessTerminalCommitReceipt,
  HarnessTerminalFinalizer,
  HarnessTerminalHandoffError,
  HarnessTerminalIdentity,
  OperationAdmissionTombstone,
  PersistedAttachment,
  PersistedRequestContextInput,
  SessionRecord,
} from '../../storage/domains/harness';
import type { ObservationalMemorySourceWriteGuard } from '../../storage/types';
import type { FullOutput } from '../../stream/base/output';
import { sha256CanonicalJsonChecked as sha256CanonicalJson } from './canonical-json';
import {
  HarnessAttachmentUnavailableError,
  HarnessBusyError,
  HarnessConfigError,
  HarnessOverrideConflictError,
  HarnessSessionDeletedError,
  HarnessValidationError,
  redactPublicBoundaryRejection,
} from './errors';
import { projectHarnessPublicError } from './events';
import type { EmitInput, HarnessEvent } from './events';
import type { Harness } from './harness';
import type { PendingDelegationSettlementNotice } from './plan-task-session';
import type {
  ActiveTurnWaiter,
  HarnessWakeTerminalSeed,
  JsonValue,
  SessionLifecycleState,
  SignalAcceptedDispatchState,
  SignalAdmission,
  SignalAdmissionIdentity,
  SignalDispatchingState,
  SignalSettlementAdmission,
  TokenUsage,
} from './session';
import {
  delay,
  MESSAGE_ADMISSION_DURABLE_WAIT_EXTENDED_INTERVAL_MS,
  MESSAGE_ADMISSION_DURABLE_WAIT_INTERVAL_MS,
  MESSAGE_ADMISSION_DURABLE_WAIT_MAX_INTERVAL_MS,
  MESSAGE_ADMISSION_DURABLE_WAIT_TIMEOUT_MS,
  throwIfAborted,
} from './session-durable-wait';
import { publicErrorProjectionToError } from './session-errors';
import type { RunCompletionWaiters } from './session-run-completion-waiters';
import type { RunOutcomeCache } from './session-run-outcome-cache';
import {
  DelegationNoticeWakeNotSentError,
  WAKE_TERMINAL_COMMIT_MAX_ATTEMPTS,
  WAKE_TERMINAL_COMMIT_RETRY_BASE_MS,
  WAKE_TERMINAL_COMMIT_RETRY_MAX_MS,
} from './session-settlement-notices';
import type { WakeReminderRowOrigin } from './session-settlement-notices';
import {
  NativeSignalAcceptanceTimeoutError,
  SIGNAL_DISPATCH_CLAIM_TTL_MS,
  SIGNAL_NATIVE_ACCEPT_TIMEOUT_MS,
} from './session-signal-acceptance';
import { RejectedTerminalRun } from './session-terminal-finalizers';
import {
  cloneAttachmentMetadata,
  clonePersistedRequestContext,
  HARNESS_SESSION_MAX_STEPS,
  normalizeObservationalMemorySourceWriteGuard,
  sameObservationalMemorySourceWriteGuard,
} from './session-turn-options';
import type { WakeAdmissions } from './session-wake-admissions';
import type {
  AgentResult,
  HarnessMode,
  SessionInjectSystemReminderOptions,
  SessionInjectSystemReminderResult,
  SessionSignalOptions,
  SessionSignalResult,
} from './types';

export interface SignalsSessionPort {
  activeWorkAdmissionIsCurrent: (epoch: number) => boolean;
  admitTerminalHandoff: (
    identity: HarnessTerminalIdentity,
    seed: JsonValue,
    onFailure?: ((error: HarnessTerminalHandoffError) => void) | undefined,
    finalizer?: HarnessTerminalFinalizer | undefined,
  ) => Promise<void>;
  agentEndReasonForFullOutput: (full: FullOutput<unknown>) => 'complete' | 'aborted' | 'error' | 'suspended';
  assertLive: (_method: string) => void;
  assertOpenForTurn: (_method: string) => void;
  assertSourceWriteGuardScope: (guard: ObservationalMemorySourceWriteGuard, field: string) => void;
  awaitRunCompletion: (runId: string) => Promise<FullOutput<unknown>>;
  beginTurn: (
    callerSignal: AbortSignal | undefined,
    runIdentity?: { modeId?: string | undefined; modelId?: string | undefined } | undefined,
    sourceWriteGuard?: ObservationalMemorySourceWriteGuard | undefined,
  ) => AbortController;
  buildToolSurface: (
    mode: HarnessMode,
    callAdditional?: ToolsInput | undefined,
  ) => Pick<AgentExecutionOptionsBase<unknown>, 'toolsets' | 'toolsetsMode'>;
  buildTurnContextAndInstructions: (
    turn: {
      modeId: string;
      modelId: string;
      abortSignal: AbortSignal;
      persistedRequestContext?: PersistedRequestContextInput | undefined;
      resolveWorkspace?: boolean | undefined;
      yolo?: boolean | undefined;
    },
    mode: Pick<HarnessMode, 'instructions' | 'harnessBuiltins'>,
  ) => Promise<[RequestContext<unknown>, string | undefined]>;
  cancelWakeTerminalAdmission: (identity: HarnessTerminalIdentity) => void;
  cleanupOperationEvidenceIfDeleted: (status: { signalId: string }) => Promise<void>;
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
  completedEvidenceResult: (evidence: { signalId: string; result: unknown; erased?: true | undefined }) => AgentResult;
  createActiveTurnWaiter: () => ActiveTurnWaiter;
  createEmptySynthesisOptions: () => { [AGENT_EXECUTION_OPTION_COMPOSERS]: () => AgentExecutionOptionComposers };
  readonly currentTurnAbortController: AbortController | undefined;
  readonly currentTurnSourceWriteGuard: ObservationalMemorySourceWriteGuard | undefined;
  dispatchClaimStillHeld: (claimExpiresAt: number) => boolean;
  emit: (event: EmitInput) => HarnessEvent;
  emitAgentEnd: (opts: {
    runId: string | undefined;
    finishReason: 'complete' | 'aborted' | 'error' | 'suspended';
    full?: FullOutput<unknown> | undefined;
  }) => void;
  emitAgentStart: (runId: string | undefined, signalId?: string | undefined, wakeNoticeId?: string | undefined) => void;
  emitTurnEvent: (event: EmitInput) => HarnessEvent;
  endTurn: (controller: AbortController) => void;
  ensureThreadSubscription: (
    agent: Agent<string, ToolsInput, undefined, any, AgentEditorConfig | undefined>,
  ) => Promise<AgentThreadSubscription<any, false>>;
  readonly harness: Harness;
  readonly id: string;
  injectSystemReminder: (
    content: string,
    opts?: SessionInjectSystemReminderOptions | undefined,
    internal?:
      | {
          signalId?: string | undefined;
          admissionEpoch?: number | undefined;
          wakeNoticeId?: string | undefined;
          wakeSettlement?:
            | Pick<PendingDelegationSettlementNotice, 'subagentSessionId' | 'taskId' | 'outcome'>
            | undefined;
          wakeReminderRow?: WakeReminderRowOrigin | undefined;
          claimNotice?: (() => Promise<boolean>) | undefined;
          noticeRowDelivered?: (() => boolean) | undefined;
          requireActiveRun?: boolean | undefined;
          assertAdmitted?: (() => void) | undefined;
        }
      | undefined,
  ) => Promise<SessionInjectSystemReminderResult>;
  isExpectedFlushLifecycleError: (err: unknown) => boolean;
  isRejectedTerminalRun: (runId: string, err: unknown, turnAbortSignal: AbortSignal) => boolean;
  isRunning: () => boolean;
  latchDurableTurnFlushError: (err: unknown, full?: FullOutput<unknown> | undefined) => void;
  maybeCaptureSuspend: (
    full: FullOutput<unknown>,
    queuedItemId?: string | undefined,
    modeId?: string,
    modelId?: string,
    opts?: {
      tokenUsageDelta?: TokenUsage | undefined;
      originSignalId?: string | undefined;
      restoreFence?: { expectedKey?: string | undefined } | undefined;
    },
  ) => Promise<void>;
  maybeDrainQueue: () => Promise<void>;
  readonly modeSwitchEpoch: number;
  readonly operationEvidenceSignalIds: Set<string>;
  pendingTokenUsageFlushError: unknown;
  persistTokenUsageOrLatch: () => Promise<void>;
  readonly record: SessionRecord;
  recordMessageTurnCompletion: (
    full: FullOutput<unknown>,
    opts?: { persist?: boolean | undefined },
  ) => { tokenUsageDelta?: TokenUsage | undefined; tokenUsageAccounted: boolean };
  recordTurnCompletion: (
    full: FullOutput<unknown>,
    opts?: { persist?: boolean | undefined; accountedCumulative?: TokenUsage | undefined },
  ) => TokenUsage | undefined;
  releaseWakeTerminalAdmission: (admissionId: string) => void;
  readonly resourceId: string;
  readonly runCompletionWaiters: RunCompletionWaiters;
  runGoalJudge: (turn: FullOutput<unknown>, wasGoalDriven: boolean) => Promise<void>;
  readonly runOutcomes: RunOutcomeCache;
  runUsage: (full?: FullOutput<unknown> | undefined) => TokenUsage;
  scheduleOrphanRecheck: (at: number) => void;
  sessionDeletedError: (err: unknown) => unknown;
  sessionLeaseHeld: () => boolean;
  setCurrentTurnReplacementToolSurface: (
    toolSurface: Pick<AgentExecutionOptionsBase<unknown>, 'toolsets' | 'toolsetsMode'>,
  ) => void;
  readonly signalResultSettlements: Map<string, Promise<AgentSignalResultEvidence | undefined>>;
  stageCompletedTerminalOutput: (
    runId: string,
    full: FullOutput<unknown>,
    opts?: { tolerateUnknownRun?: boolean | undefined; provisional?: boolean | undefined },
  ) => Promise<void>;
  readonly state: SessionLifecycleState;
  readonly storage: HarnessStorage;
  subagentTracingOptions: () => { tracingContext?: TracingContext | undefined };
  readonly terminalWakeFinalizer: HarnessTerminalFinalizer | undefined;
  readonly threadId: string;
  tokenUsageDeltaFromFullOutput: (
    full: FullOutput<unknown>,
    accountedCumulative?: TokenUsage | undefined,
  ) => TokenUsage | undefined;
  trackBackgroundTurnCompletion: <T>(promise: Promise<T>) => Promise<T>;
  readonly wakeAdmissions: WakeAdmissions;
  writeMessageResultEvidence: (
    status: AgentSignalResultStatus & { admissionId?: string | undefined; admissionHash?: string | undefined },
    options?:
      | { compatibleAdmissionHashes?: readonly string[] | undefined; leaseOwner?: { ownerId: string } | undefined }
      | undefined,
  ) => Promise<{
    created: boolean;
    applied: boolean;
    evidence?: AgentSignalResultEvidence | OperationAdmissionTombstone | undefined;
  }>;
  writeMessageResultEvidenceBestEffort: (
    status: AgentSignalResultStatus & { admissionId?: string | undefined; admissionHash?: string | undefined },
    options?: { compatibleAdmissionHashes?: readonly string[] | undefined } | undefined,
  ) => Promise<
    | {
        created: boolean;
        applied: boolean;
        evidence?: AgentSignalResultEvidence | OperationAdmissionTombstone | undefined;
      }
    | undefined
  >;
}

const SIGNAL_DISPATCH_CLAIM_RENEW_MS = 10_000;

const SIGNAL_ACCEPTED_RECOVERY_STALE_MS = 30_000;

/**
 * PF-7631: admit a settlement wake turn under its Harness-minted grant
 * (`harness-wake:<wakeNoticeId>`, generation 1) before it dispatches, through
 * the same three durable steps an admitted message() terminal turn takes:
 * the pending result reservation (lease-fenced), the terminal admission
 * under the wake finalizer (grant lock, tombstones, winner check), and the
 * dispatch claim stamp. Its staged output and settlement then follow the
 * message() terminal path, and dispatch recovery settles it after a crash.
 * Any refusal throws before dispatch, so no run starts under the grant.
 */
export async function admitWakeTerminalTurn(
  port: SignalsSessionPort,
  input: {
    wakeNoticeId: string;
    signalId: string;
    settlement: Pick<PendingDelegationSettlementNotice, 'taskId' | 'subagentSessionId' | 'outcome'>;
    modeId: string;
    modelId: string;
  },
): Promise<{ identity: HarnessTerminalIdentity; claimExpiresAt: number }> {
  const finalizer = port.terminalWakeFinalizer!;
  const sessionIncarnation = port.record.sessionIncarnation;
  if (sessionIncarnation === undefined || sessionIncarnation.length === 0) {
    throw new HarnessTerminalHandoffFencedError(port.id);
  }
  const executionGrant = harnessWakeExecutionGrant(input.wakeNoticeId);
  const seed: HarnessWakeTerminalSeed = {
    origin: 'wake',
    wakeNoticeId: input.wakeNoticeId,
    taskId: input.settlement.taskId,
    subagentSessionId: input.settlement.subagentSessionId,
    outcome: input.settlement.outcome,
  };
  const digest = sha256CanonicalJson({
    kind: 'wake-admission',
    harnessName: port.record.harnessName,
    sessionId: port.id,
    resourceId: port.resourceId,
    threadId: port.threadId,
    wakeNoticeId: input.wakeNoticeId,
  });
  const identity: HarnessTerminalIdentity = {
    harnessName: port.record.harnessName,
    sessionId: port.id,
    resourceId: port.resourceId,
    threadId: port.threadId,
    sessionIncarnation,
    // The admission id is the grant key: both live in the reserved namespace.
    admissionId: executionGrant.key,
    admissionHash: sha256CanonicalJson({ kind: 'wake-admission-hash', seed }),
    signalId: input.signalId,
    runId: `harness-wake-${digest.slice(0, 32)}`,
    executionGrant,
  };
  validateHarnessTerminalExecutionGrant(executionGrant, 'wake.executionGrant');
  // Another attempt for this notice is admitting or running here.
  if (!port.wakeAdmissions.enter(identity.admissionId)) {
    throw new HarnessBusyError(port.id, 'in_flight');
  }
  try {
    const reservation = await port.writeMessageResultEvidence(
      {
        status: 'pending',
        signalId: identity.signalId,
        runId: identity.runId,
        modeId: input.modeId,
        modelId: input.modelId,
        operationKind: 'message',
        admissionId: identity.admissionId,
        admissionHash: identity.admissionHash,
      },
      { leaseOwner: { ownerId: port.harness.ownerId } },
    );
    // An existing reservation means another attempt owns this notice's wake.
    if (!reservation.created) throw new HarnessBusyError(port.id, 'in_flight');
    await port.admitTerminalHandoff(identity, seed, undefined, finalizer);
    const claimExpiresAt = Date.now() + SIGNAL_DISPATCH_CLAIM_TTL_MS;
    const stamped = await port.storage.compareAndSwapSignalDispatch({
      harnessName: port.record.harnessName,
      sessionId: port.id,
      resourceId: port.resourceId,
      threadId: port.threadId,
      signalId: identity.signalId,
      admissionId: identity.admissionId,
      admissionHash: identity.admissionHash,
      operationKind: 'message',
      expected: { state: 'reserved' },
      next: {
        state: 'dispatching',
        attemptId: `wake-dispatch-${randomUUID()}`,
        claimExpiresAt,
        delivery: 'idle',
        runId: identity.runId,
      },
      leaseOwner: { ownerId: port.harness.ownerId },
      updatedAt: Date.now(),
    });
    if (!stamped.applied) throw new HarnessBusyError(port.id, 'in_flight');
    return { identity, claimExpiresAt };
  } catch (error) {
    port.wakeAdmissions.leave(identity.admissionId);
    // A reservation left behind is settled interrupted by dispatch recovery.
    port.scheduleOrphanRecheck(Date.now());
    // The notice was acknowledged before this wake, so a refusal other than
    // a duplicate (busy) loses the wake: surface it rather than drop it. Its
    // reminder row is persisted and still reaches the next parent turn.
    if (!(error instanceof HarnessBusyError)) {
      port.emitTurnEvent({
        type: 'error',
        runId: identity.runId,
        signalId: identity.signalId,
        error: projectHarnessPublicError(error),
      });
    }
    throw error;
  }
}

export async function commitWakeTerminalWithRetry(
  port: SignalsSessionPort,
  identity: HarnessTerminalIdentity,
  outcome: FullOutput<unknown> | RejectedTerminalRun,
  options: { modeId?: string; modelId?: string },
  activeDeleted: Promise<never>,
  turnAbortSignal: AbortSignal | undefined,
): Promise<void> {
  for (let attempt = 1; ; attempt += 1) {
    try {
      await Promise.race([port.commitTerminalHandoff(identity, outcome, options), activeDeleted]);
      return;
    } catch (error) {
      if (port.state !== 'live' || turnAbortSignal?.aborted) throw error;
      port.emitTurnEvent({
        type: 'error',
        runId: identity.runId,
        signalId: identity.signalId,
        error: projectHarnessPublicError(error),
      });
      if (
        attempt >= WAKE_TERMINAL_COMMIT_MAX_ATTEMPTS ||
        error instanceof HarnessTerminalHandoffFencedError ||
        error instanceof HarnessTerminalHandoffCancelledError ||
        error instanceof HarnessTerminalHandoffIdentityConflictError
      ) {
        throw error;
      }
      const delayMs = Math.min(
        WAKE_TERMINAL_COMMIT_RETRY_BASE_MS * 2 ** (attempt - 1),
        WAKE_TERMINAL_COMMIT_RETRY_MAX_MS,
      );
      let timer: ReturnType<typeof setTimeout> | undefined;
      const backoff = new Promise<void>(resolve => {
        timer = setTimeout(resolve, delayMs);
        (timer as { unref?: () => void }).unref?.();
      });
      await Promise.race([backoff, activeDeleted]).finally(() => clearTimeout(timer));
    }
  }
}

export function signalAdmissionIdentity(port: SignalsSessionPort, admissionId: string): SignalAdmissionIdentity {
  const digest = sha256CanonicalJson({
    kind: 'channel-signal-admission',
    harnessName: port.record.harnessName,
    sessionId: port.id,
    resourceId: port.resourceId,
    threadId: port.threadId,
    admissionId,
  });
  return {
    signalId: `harness-channel-signal-${digest.slice(0, 32)}`,
    idleRunId: `harness-channel-wake-${digest.slice(32, 64)}`,
  };
}

export function settleSignalResult(
  port: SignalsSessionPort,
  signalId: string,
  outcome:
    | { status: 'completed'; runId: string; result: AgentResult }
    | { status: 'failed'; runId?: string; error: { code: string; message: string } },
  admission?: SignalSettlementAdmission,
  options?: { strict?: boolean },
): Promise<AgentSignalResultEvidence | undefined> {
  const inFlight = port.signalResultSettlements.get(signalId);
  if (inFlight !== undefined) {
    if (options?.strict !== true) return inFlight;
    // A strict caller may join a best-effort settlement that swallowed its
    // write failure: once it settles, verify the durable evidence is
    // terminal and otherwise write it strictly (failure propagates).
    return settleSignalResultStrictAfter(port, signalId, inFlight, outcome, admission);
  }
  const settlement = settleSignalResultOnce(port, signalId, outcome, admission, options).finally(() => {
    if (port.signalResultSettlements.get(signalId) === settlement) {
      port.signalResultSettlements.delete(signalId);
    }
  });
  port.signalResultSettlements.set(signalId, settlement);
  return settlement;
}

export async function settleSignalResultStrictAfter(
  port: SignalsSessionPort,
  signalId: string,
  inFlight: Promise<AgentSignalResultEvidence | undefined>,
  outcome:
    | { status: 'completed'; runId: string; result: AgentResult }
    | { status: 'failed'; runId?: string; error: { code: string; message: string } },
  admission?: SignalSettlementAdmission,
): Promise<AgentSignalResultEvidence | undefined> {
  await inFlight.catch(() => undefined);
  const durable = await port.storage.loadMessageResultEvidence({
    harnessName: port.record.harnessName,
    sessionId: port.id,
    resourceId: port.resourceId,
    threadId: port.threadId,
    signalId,
  });
  if (durable && 'status' in durable && (durable.status === 'completed' || durable.status === 'failed')) {
    return durable as AgentSignalResultEvidence;
  }
  return settleSignalResultOnce(port, signalId, outcome, admission, { strict: true });
}

export async function settleSignalResultOnce(
  port: SignalsSessionPort,
  signalId: string,
  outcome:
    | { status: 'completed'; runId: string; result: AgentResult }
    | { status: 'failed'; runId?: string; error: { code: string; message: string } },
  admission?: SignalSettlementAdmission,
  options?: { strict?: boolean },
): Promise<AgentSignalResultEvidence | undefined> {
  // A signal-owned run can suspend and later resume through a rehydrated
  // Session. `PendingResume` intentionally carries only the public
  // correlation signal id, so recover the private admission identity from
  // its durable evidence before terminalizing. Without this, the storage
  // identity fence rejects the terminal update and the admitted row remains
  // pending forever. Non-admitted signal ids keep their historical
  // best-effort behavior.
  let retainedAdmission = admission;
  if (retainedAdmission === undefined) {
    const retained = await port.storage.loadMessageResultEvidence({
      harnessName: port.record.harnessName,
      sessionId: port.id,
      resourceId: port.resourceId,
      threadId: port.threadId,
      signalId,
    });
    if (retained && 'status' in retained) {
      if (retained.status === 'completed' || retained.status === 'failed') {
        return retained as AgentSignalResultEvidence;
      }
      const admitted = retained as AgentSignalResultEvidence;
      if (admitted.admissionId !== undefined && admitted.admissionHash !== undefined) {
        const identity = signalAdmissionIdentity(port, admitted.admissionId);
        retainedAdmission = {
          admissionId: admitted.admissionId,
          admissionHash: admitted.admissionHash,
          modeId: admitted.modeId ?? port.record.modeId,
          modelId: admitted.modelId ?? port.record.modelId,
          dispatch: signalDispatchState(port, admitted, identity),
        };
      }
    }
  }
  if (retainedAdmission !== undefined) {
    // Reserved admissions and synchronous pre-dispatch failures remain
    // retryable. Only a concrete dispatch attempt can own terminal evidence.
    if (retainedAdmission.dispatch.state === 'reserved') {
      const retained = await port.storage.loadMessageResultEvidence({
        harnessName: port.record.harnessName,
        sessionId: port.id,
        resourceId: port.resourceId,
        threadId: port.threadId,
        signalId,
      });
      return retained && 'status' in retained ? (retained as AgentSignalResultEvidence) : undefined;
    }
    const terminal =
      outcome.status === 'completed'
        ? {
            status: 'completed' as const,
            signalId,
            runId: outcome.runId,
            result: outcome.result,
            modeId: retainedAdmission.modeId,
            modelId: retainedAdmission.modelId,
          }
        : {
            status: 'failed' as const,
            signalId,
            ...(outcome.runId !== undefined ? { runId: outcome.runId } : {}),
            error: outcome.error,
            modeId: retainedAdmission.modeId,
            modelId: retainedAdmission.modelId,
          };
    const write = await port.storage
      .compareAndSwapSignalTerminal({
        harnessName: port.record.harnessName,
        sessionId: port.id,
        resourceId: port.resourceId,
        threadId: port.threadId,
        signalId,
        admissionId: retainedAdmission.admissionId,
        admissionHash: retainedAdmission.admissionHash,
        expected: retainedAdmission.dispatch,
        terminal,
        updatedAt: Date.now(),
      })
      .catch((err: unknown) => {
        throw port.sessionDeletedError(err);
      });
    await port.cleanupOperationEvidenceIfDeleted({ signalId });
    // The loser adopts the durable winner by returning without rewriting or
    // projecting its stale run/result as the operation terminal.
    if (!write.applied) return write.evidence;
    if (outcome.status === 'completed') {
      port.emit({ type: 'signal_completed', runId: outcome.runId, signalId, result: outcome.result });
    } else {
      port.emit({
        type: 'signal_failed',
        signalId,
        ...(outcome.runId !== undefined ? { runId: outcome.runId } : {}),
        error: outcome.error,
      });
    }
    return write.evidence;
  } else if (outcome.status === 'completed') {
    await port.writeMessageResultEvidenceBestEffort({
      status: 'completed',
      signalId,
      runId: outcome.runId,
      result: outcome.result,
    });
  } else if (options?.strict === true) {
    // Strict callers (discard completion) must not clear their recovery
    // marker over a lost write, so a failure here propagates.
    await port.writeMessageResultEvidence({
      status: 'failed',
      signalId,
      ...(outcome.runId !== undefined ? { runId: outcome.runId } : {}),
      error: outcome.error,
    });
  } else {
    await port.writeMessageResultEvidenceBestEffort({
      status: 'failed',
      signalId,
      ...(outcome.runId !== undefined ? { runId: outcome.runId } : {}),
      error: outcome.error,
    });
  }
  if (outcome.status === 'completed') {
    port.emit({ type: 'signal_completed', runId: outcome.runId, signalId, result: outcome.result });
    return;
  }
  port.emit({
    type: 'signal_failed',
    signalId,
    ...(outcome.runId !== undefined ? { runId: outcome.runId } : {}),
    error: outcome.error,
  });
  return undefined;
}

export async function adoptSignalSettlement(
  port: SignalsSessionPort,
  signalId: string,
  evidence: AgentSignalResultEvidence | undefined,
  local: { status: 'completed'; result: AgentResult } | { status: 'failed'; error: unknown },
  abortSignal?: AbortSignal,
): Promise<AgentResult> {
  if (evidence === undefined) {
    if (local.status === 'completed') return local.result;
    throw local.error;
  }
  if (evidence.status === 'completed') return port.completedEvidenceResult(evidence);
  if (evidence.status === 'failed') throw publicErrorProjectionToError(evidence.error);
  return awaitDurableSignalResult(port, { signalId }, abortSignal, { waitIndefinitely: true });
}

export function computeSignalAdmissionHash(
  port: SignalsSessionPort,
  opts: Pick<
    SessionSignalOptions,
    'content' | 'mode' | 'observationalMemorySourceWriteGuard' | 'logicalMessageIdentity'
  >,
  attachments: PersistedAttachment[],
  requestContext?: PersistedRequestContextInput,
): string {
  return sha256CanonicalJson({
    kind: 'signal',
    content: opts.content,
    ...(opts.mode !== undefined ? { mode: opts.mode } : {}),
    ...(opts.logicalMessageIdentity !== undefined
      ? { logicalMessageIdentity: { ...opts.logicalMessageIdentity } }
      : {}),
    attachments: attachments.map(attachment => ({
      kind: attachment.kind,
      name: attachment.name,
      mimeType: attachment.mimeType,
      ...(attachment.kind === 'ref'
        ? {
            attachmentId: attachment.attachmentId,
            resourceId: port.resourceId,
            ownerSessionId: attachment.ownerSessionId,
            bytes: attachment.bytes,
            sha256: attachment.sha256,
            source: attachment.source,
            attachmentKind: attachment.attachmentKind ?? 'file',
            ...(attachment.primitiveType ? { primitiveType: attachment.primitiveType } : {}),
            ...(attachment.elementType ? { elementType: attachment.elementType } : {}),
            ...(attachment.renderer ? { renderer: attachment.renderer } : {}),
            ...(attachment.schemaId ? { schemaId: attachment.schemaId } : {}),
            ...(attachment.metadata ? { metadata: cloneAttachmentMetadata(attachment.metadata) } : {}),
            ...(attachment.object ? { object: attachment.object } : {}),
          }
        : { url: attachment.url }),
    })),
    ...(requestContext ? { requestContext: clonePersistedRequestContext(requestContext) } : {}),
    ...(opts.observationalMemorySourceWriteGuard
      ? {
          observationalMemorySourceWriteGuard: normalizeObservationalMemorySourceWriteGuard(
            opts.observationalMemorySourceWriteGuard,
            'signal().observationalMemorySourceWriteGuard',
          ),
        }
      : {}),
  });
}

export function signalDispatchState(
  port: SignalsSessionPort,
  evidence: AgentSignalResultEvidence,
  identity: SignalAdmissionIdentity,
): AgentSignalDispatchState {
  if (evidence.dispatch !== undefined) return evidence.dispatch;
  if (evidence.runId !== undefined) {
    // Rows written before durable dispatch fencing already treated a persisted
    // run id as accepted. Preserve that recovery meaning without allowing new
    // unfenced writes to take this compatibility path.
    return {
      state: 'accepted',
      attemptId: 'legacy',
      delivery: evidence.runId === identity.idleRunId ? 'idle' : 'active',
      runId: evidence.runId,
      acceptedAt: evidence.updatedAt,
    };
  }
  return { state: 'reserved' };
}

export async function compareAndSwapSignalDispatch(
  port: SignalsSessionPort,
  admission: SignalAdmission,
  expected: AgentSignalDispatchState,
  next: AgentSignalDispatchState,
): Promise<{ applied: boolean; evidence: AgentSignalResultEvidence }> {
  const result = await port.storage
    .compareAndSwapSignalDispatch({
      harnessName: port.record.harnessName,
      sessionId: port.id,
      resourceId: port.resourceId,
      threadId: port.threadId,
      signalId: admission.identity.signalId,
      admissionId: admission.admissionId,
      admissionHash: admission.admissionHash,
      expected,
      next,
      updatedAt: Date.now(),
    })
    .catch((err: unknown) => {
      throw port.sessionDeletedError(err);
    });
  admission.dispatch = signalDispatchState(port, result.evidence, admission.identity);
  return result;
}

export async function loadSignalAdmissionEvidence(
  port: SignalsSessionPort,
  admission: SignalAdmission,
): Promise<AgentSignalResultEvidence> {
  const loaded = await port.storage.loadMessageResultEvidence({
    harnessName: port.record.harnessName,
    sessionId: port.id,
    resourceId: port.resourceId,
    threadId: port.threadId,
    signalId: admission.identity.signalId,
  });
  if (!loaded || !('status' in loaded)) {
    throw new HarnessValidationError('signal().admissionId', 'signal admission evidence has expired');
  }
  const evidence = loaded as AgentSignalResultEvidence;
  admission.dispatch = signalDispatchState(port, evidence, admission.identity);
  return evidence;
}

export async function claimSignalDispatch(
  port: SignalsSessionPort,
  admission: SignalAdmission,
  planned: { delivery: 'idle' | 'active'; runId: string },
  canAttachRun: (runId: string) => boolean,
  isRunActive: (runId: string) => boolean,
  abortSignal?: AbortSignal,
): Promise<
  | { kind: 'owned'; dispatch: SignalDispatchingState; recoveringAccepted: boolean }
  | { kind: 'duplicate'; evidence: AgentSignalResultEvidence }
  | { kind: 'active-lost'; dispatch: SignalDispatchingState }
> {
  let evidence = await loadSignalAdmissionEvidence(port, admission);
  while (true) {
    throwIfAborted(abortSignal, 'signal().admissionId');
    if (evidence.status !== 'pending') return { kind: 'duplicate', evidence };
    const dispatch = signalDispatchState(port, evidence, admission.identity);
    if (dispatch.state === 'accepted') {
      if (canAttachRun(dispatch.runId)) return { kind: 'duplicate', evidence };
      const recoverAt = dispatch.acceptedAt + SIGNAL_ACCEPTED_RECOVERY_STALE_MS;
      if (Date.now() < recoverAt) {
        await delay(Math.min(MESSAGE_ADMISSION_DURABLE_WAIT_INTERVAL_MS, recoverAt - Date.now()), abortSignal);
        evidence = await loadSignalAdmissionEvidence(port, admission);
        continue;
      }
      const reclaimed: SignalDispatchingState = {
        state: 'dispatching',
        attemptId: `signal-dispatch-${randomUUID()}`,
        claimExpiresAt: Date.now() + SIGNAL_DISPATCH_CLAIM_TTL_MS,
        delivery: dispatch.delivery,
        runId: dispatch.runId,
      };
      const swapped = await compareAndSwapSignalDispatch(port, admission, dispatch, reclaimed);
      if (swapped.evidence.status !== 'pending') return { kind: 'duplicate', evidence: swapped.evidence };
      if (!swapped.applied) {
        evidence = swapped.evidence;
        continue;
      }
      return dispatch.delivery === 'active'
        ? { kind: 'active-lost', dispatch: reclaimed }
        : { kind: 'owned', dispatch: reclaimed, recoveringAccepted: true };
    }
    if (dispatch.state === 'dispatching' && dispatch.claimExpiresAt > Date.now()) {
      await delay(
        Math.min(MESSAGE_ADMISSION_DURABLE_WAIT_INTERVAL_MS, dispatch.claimExpiresAt - Date.now()),
        abortSignal,
      );
      evidence = await loadSignalAdmissionEvidence(port, admission);
      continue;
    }
    const next: SignalDispatchingState = {
      state: 'dispatching',
      attemptId: `signal-dispatch-${randomUUID()}`,
      claimExpiresAt: Date.now() + SIGNAL_DISPATCH_CLAIM_TTL_MS,
      delivery: dispatch.state === 'dispatching' ? dispatch.delivery : planned.delivery,
      runId: dispatch.state === 'dispatching' ? dispatch.runId : planned.runId,
    };
    const swapped = await compareAndSwapSignalDispatch(port, admission, dispatch, next);
    if (swapped.evidence.status !== 'pending') return { kind: 'duplicate', evidence: swapped.evidence };
    if (!swapped.applied) {
      evidence = swapped.evidence;
      continue;
    }
    if (dispatch.state === 'dispatching' && next.delivery === 'active' && !isRunActive(next.runId)) {
      return { kind: 'active-lost', dispatch: next };
    }
    return { kind: 'owned', dispatch: next, recoveringAccepted: false };
  }
}

export function startSignalDispatchClaimHeartbeat(
  port: SignalsSessionPort,
  admission: SignalAdmission,
  initial: SignalDispatchingState,
): {
  stop: () => Promise<SignalDispatchingState>;
} {
  let current = initial;
  let failure: unknown;
  let tail = Promise.resolve();
  const interval = setInterval(() => {
    tail = tail.then(async () => {
      if (failure !== undefined) return;
      const renewed: SignalDispatchingState = {
        ...current,
        claimExpiresAt: Date.now() + SIGNAL_DISPATCH_CLAIM_TTL_MS,
      };
      try {
        const swapped = await compareAndSwapSignalDispatch(port, admission, current, renewed);
        if (!swapped.applied) {
          failure = new HarnessConfigError('signal()', 'durable signal dispatch claim was lost');
          return;
        }
        current = renewed;
      } catch (error) {
        failure = error;
      }
    });
  }, SIGNAL_DISPATCH_CLAIM_RENEW_MS);
  interval.unref?.();
  return {
    stop: async () => {
      clearInterval(interval);
      await tail;
      if (failure !== undefined) throw failure;
      return current;
    },
  };
}

export async function acceptSignalDispatch(
  port: SignalsSessionPort,
  admission: SignalAdmission,
  dispatching: SignalDispatchingState,
): Promise<AgentSignalResultEvidence> {
  const accepted: SignalAcceptedDispatchState = {
    state: 'accepted',
    attemptId: dispatching.attemptId,
    delivery: dispatching.delivery,
    runId: dispatching.runId,
    acceptedAt: Date.now(),
  };
  const swapped = await compareAndSwapSignalDispatch(port, admission, dispatching, accepted);
  if (!swapped.applied) {
    const current = signalDispatchState(port, swapped.evidence, admission.identity);
    if (
      current.state !== 'accepted' ||
      current.attemptId !== accepted.attemptId ||
      current.delivery !== accepted.delivery ||
      current.runId !== accepted.runId
    ) {
      throw new HarnessConfigError('signal()', 'durable signal acceptance lost its dispatch claim');
    }
  }
  return swapped.evidence;
}

export function awaitSignalNativeAcceptance<T>(
  port: SignalsSessionPort,
  accepted: Promise<T>,
  abortSignal?: AbortSignal,
  interrupted?: Promise<never>,
  validationPath = 'signal().admissionId',
): Promise<T> {
  throwIfAborted(abortSignal, validationPath);
  return new Promise<T>((resolve, reject) => {
    let settled = false;
    const finish = (callback: () => void) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      abortSignal?.removeEventListener('abort', onAbort);
      callback();
    };
    const onAbort = () =>
      finish(() => reject(abortSignal?.reason ?? new HarnessValidationError('signal()', 'operation aborted')));
    const timer = setTimeout(() => {
      finish(() => reject(new NativeSignalAcceptanceTimeoutError(validationPath)));
    }, SIGNAL_NATIVE_ACCEPT_TIMEOUT_MS);
    timer.unref?.();
    abortSignal?.addEventListener('abort', onAbort, { once: true });
    accepted.then(
      value => finish(() => resolve(value)),
      error => finish(() => reject(error)),
    );
    interrupted?.then(
      value => finish(() => resolve(value)),
      error => finish(() => reject(error)),
    );
  });
}

export function signalFromAdmissionEvidence(
  port: SignalsSessionPort,
  evidence: AgentSignalResultEvidence,
  opts: SessionSignalOptions,
): CreatedAgentSignal {
  const createdAt = Number.isFinite(evidence.createdAt) ? new Date(evidence.createdAt) : new Date();
  return createSignal({
    id: evidence.signalId,
    type: 'user-message',
    contents: opts.content,
    ...(opts.logicalMessageIdentity ? { metadata: { logicalMessageId: opts.logicalMessageIdentity.input } } : {}),
    createdAt,
    acceptedAt: createdAt,
  });
}

export async function awaitDurableSignalResult(
  port: SignalsSessionPort,
  evidence: Pick<AgentSignalResultEvidence, 'signalId'>,
  abortSignal?: AbortSignal,
  options: { waitIndefinitely?: boolean } = {},
): Promise<AgentResult> {
  const deadline = options.waitIndefinitely ? undefined : Date.now() + MESSAGE_ADMISSION_DURABLE_WAIT_TIMEOUT_MS;
  let extendedWaitMs = MESSAGE_ADMISSION_DURABLE_WAIT_EXTENDED_INTERVAL_MS;
  while (true) {
    throwIfAborted(abortSignal, 'signal().admissionId');
    const latest = await port.storage.loadMessageResultEvidence({
      harnessName: port.record.harnessName,
      sessionId: port.id,
      resourceId: port.resourceId,
      threadId: port.threadId,
      signalId: evidence.signalId,
    });
    if (!latest || !('status' in latest)) {
      throw new HarnessValidationError('signal().admissionId', 'duplicate signal result evidence has expired');
    }
    if (latest.status === 'completed') return port.completedEvidenceResult(latest);
    if (latest.status === 'failed') throw publicErrorProjectionToError(latest.error);
    if (deadline !== undefined && Date.now() >= deadline) {
      throw new HarnessValidationError('signal().admissionId', 'pending signal admission is not live');
    }
    await delay(options.waitIndefinitely ? extendedWaitMs : MESSAGE_ADMISSION_DURABLE_WAIT_INTERVAL_MS, abortSignal);
    if (options.waitIndefinitely) {
      extendedWaitMs = Math.min(extendedWaitMs * 2, MESSAGE_ADMISSION_DURABLE_WAIT_MAX_INTERVAL_MS);
    }
  }
}

export async function returnDuplicateSignalResult(
  port: SignalsSessionPort,
  evidence: AgentSignalResultEvidence | OperationAdmissionTombstone,
  opts: SessionSignalOptions,
  identity: SignalAdmissionIdentity,
): Promise<SessionSignalResult> {
  if (!('status' in evidence)) {
    throw new HarnessValidationError('signal().admissionId', 'duplicate signal result evidence has expired');
  }
  if (evidence.status === 'pending' && signalDispatchState(port, evidence, identity).state !== 'accepted') {
    throw new HarnessValidationError(
      'signal().admissionId',
      'pending signal admission has not crossed durable native acceptance',
    );
  }
  if (evidence.runId === undefined) {
    if (evidence.status === 'failed') throw publicErrorProjectionToError(evidence.error);
    throw new HarnessValidationError('signal().admissionId', 'pending signal admission was not dispatched');
  }

  let result: Promise<AgentResult>;
  if (evidence.status === 'completed') {
    result = Promise.resolve().then(() => port.completedEvidenceResult(evidence));
  } else if (evidence.status === 'failed') {
    result = Promise.reject(publicErrorProjectionToError(evidence.error));
  } else {
    const duplicateModeId = evidence.modeId ?? opts.mode ?? port.record.modeId;
    const agent = port.harness.getAgentForMode(duplicateModeId);
    const subscription = await port.ensureThreadSubscription(agent);
    const cached = port.runOutcomes.get(evidence.runId);
    if (cached) {
      const admission =
        evidence.admissionId !== undefined && evidence.admissionHash !== undefined
          ? {
              admissionId: evidence.admissionId,
              admissionHash: evidence.admissionHash,
              modeId: duplicateModeId,
              modelId: evidence.modelId ?? port.record.modelId,
              dispatch: signalDispatchState(port, evidence, identity),
            }
          : undefined;
      if (cached.ok) {
        result =
          cached.full.finishReason === 'suspended'
            ? awaitDurableSignalResult(port, evidence, opts.abortSignal, { waitIndefinitely: true })
            : (async () => {
                await port.stageCompletedTerminalOutput(evidence.runId!, cached.full, {
                  tolerateUnknownRun: true,
                });
                const settled = await settleSignalResult(
                  port,
                  evidence.signalId,
                  {
                    status: 'completed',
                    runId: evidence.runId!,
                    result: cached.full as AgentResult,
                  },
                  admission,
                );
                return adoptSignalSettlement(
                  port,
                  evidence.signalId,
                  settled,
                  { status: 'completed', result: cached.full as AgentResult },
                  opts.abortSignal,
                );
              })();
      } else {
        result = (async () => {
          if (cached.err instanceof HarnessTerminalFinalizationPendingError) throw cached.err;
          const settled = await settleSignalResult(
            port,
            evidence.signalId,
            {
              status: 'failed',
              runId: evidence.runId,
              error: projectHarnessPublicError(cached.err),
            },
            admission,
          );
          return adoptSignalSettlement(
            port,
            evidence.signalId,
            settled,
            { status: 'failed', error: cached.err },
            opts.abortSignal,
          );
        })();
      }
    } else if (port.runCompletionWaiters.has(evidence.runId)) {
      const admission =
        evidence.admissionId !== undefined && evidence.admissionHash !== undefined
          ? {
              admissionId: evidence.admissionId,
              admissionHash: evidence.admissionHash,
              modeId: duplicateModeId,
              modelId: evidence.modelId ?? port.record.modelId,
              dispatch: signalDispatchState(port, evidence, identity),
            }
          : undefined;
      result = port.awaitRunCompletion(evidence.runId).then(
        async full => {
          if (full.finishReason === 'suspended') {
            return awaitDurableSignalResult(port, evidence, opts.abortSignal, { waitIndefinitely: true });
          }
          const settled = await settleSignalResult(
            port,
            evidence.signalId,
            { status: 'completed', runId: evidence.runId!, result: full as AgentResult },
            admission,
          );
          return adoptSignalSettlement(
            port,
            evidence.signalId,
            settled,
            { status: 'completed', result: full as AgentResult },
            opts.abortSignal,
          );
        },
        async err => {
          if (err instanceof HarnessTerminalFinalizationPendingError) throw err;
          const settled = await settleSignalResult(
            port,
            evidence.signalId,
            { status: 'failed', runId: evidence.runId, error: projectHarnessPublicError(err) },
            admission,
          );
          return adoptSignalSettlement(
            port,
            evidence.signalId,
            settled,
            { status: 'failed', error: err },
            opts.abortSignal,
          );
        },
      );
    } else if (agent.getRunOutput(evidence.runId) || subscription.activeRunId() === evidence.runId) {
      // Rehydrated accepted run: this Session has no original continuation to
      // terminalize the signal evidence. Attach one to the recovered run so a
      // restart does not leave a durable admitted row pending forever.
      const admission =
        evidence.admissionId !== undefined && evidence.admissionHash !== undefined
          ? {
              admissionId: evidence.admissionId,
              admissionHash: evidence.admissionHash,
              modeId: duplicateModeId,
              modelId: evidence.modelId ?? port.record.modelId,
              dispatch: signalDispatchState(port, evidence, identity),
            }
          : undefined;
      result = port.awaitRunCompletion(evidence.runId).then(
        async full => {
          if (full.finishReason !== 'suspended') {
            const settled = await settleSignalResult(
              port,
              evidence.signalId,
              {
                status: 'completed',
                runId: evidence.runId!,
                result: full as AgentResult,
              },
              admission,
            );
            return adoptSignalSettlement(
              port,
              evidence.signalId,
              settled,
              { status: 'completed', result: full as AgentResult },
              opts.abortSignal,
            );
          }
          return awaitDurableSignalResult(port, evidence, opts.abortSignal, { waitIndefinitely: true });
        },
        async err => {
          if (err instanceof HarnessTerminalFinalizationPendingError) throw err;
          const settled = await settleSignalResult(
            port,
            evidence.signalId,
            {
              status: 'failed',
              runId: evidence.runId,
              error: projectHarnessPublicError(err),
            },
            admission,
          );
          return adoptSignalSettlement(
            port,
            evidence.signalId,
            settled,
            { status: 'failed', error: err },
            opts.abortSignal,
          );
        },
      );
    } else {
      result = awaitDurableSignalResult(port, evidence, opts.abortSignal, {
        waitIndefinitely: port.record.pendingResume?.originSignalId === evidence.signalId,
      });
    }
  }
  result = result.catch(err => {
    throw redactPublicBoundaryRejection(err);
  });
  void result.catch(() => {});

  const signal = signalFromAdmissionEvidence(port, evidence, opts);
  return {
    id: evidence.signalId,
    runId: evidence.runId,
    willInterleave: evidence.runId !== identity.idleRunId,
    accepted: true,
    signal,
    result,
  };
}

export async function injectSystemReminder(
  port: SignalsSessionPort,
  content: string,
  opts?: SessionInjectSystemReminderOptions,
  internal?: { requireActiveRun?: boolean; assertAdmitted?: () => void },
): Promise<SessionInjectSystemReminderResult> {
  return port.injectSystemReminder(
    content,
    opts,
    internal === undefined
      ? undefined
      : {
          ...(internal.requireActiveRun !== undefined ? { requireActiveRun: internal.requireActiveRun } : {}),
          ...(internal.assertAdmitted !== undefined ? { assertAdmitted: internal.assertAdmitted } : {}),
        },
  );
}

export async function sessionInjectSystemReminder(
  port: SignalsSessionPort,
  content: string,
  opts?: SessionInjectSystemReminderOptions,
  internal?: {
    signalId?: string;
    admissionEpoch?: number;
    wakeNoticeId?: string;
    wakeSettlement?: Pick<PendingDelegationSettlementNotice, 'taskId' | 'subagentSessionId' | 'outcome'>;
    wakeReminderRow?: WakeReminderRowOrigin;
    /**
     * PF-7679: the settlement notice claim, awaited after the context build
     * (and the wake admission) so the only work after it is the synchronous
     * re-check and dispatch. Resolves true when this wake won the claim.
     */
    claimNotice?: () => Promise<boolean>;
    /** PF-7679: whether a run already loaded the reminder row into its context. */
    noticeRowDelivered?: () => boolean;
    requireActiveRun?: boolean;
    assertAdmitted?: () => void;
  },
): Promise<SessionInjectSystemReminderResult> {
  const signalId = internal?.signalId;
  port.assertLive('injectSystemReminder()');
  port.assertOpenForTurn('injectSystemReminder()');
  if (typeof content !== 'string' || content.length === 0) {
    throw new HarnessValidationError('injectSystemReminder()', '`content` must be a non-empty string');
  }
  const sourceWriteGuard = normalizeObservationalMemorySourceWriteGuard(
    opts?.observationalMemorySourceWriteGuard,
    'injectSystemReminder().observationalMemorySourceWriteGuard',
  );
  if (sourceWriteGuard) {
    port.assertSourceWriteGuardScope(sourceWriteGuard, 'injectSystemReminder().observationalMemorySourceWriteGuard');
  }

  const effectiveModeId = port.record.modeId;
  const mode = port.harness._getMode(effectiveModeId);
  const agent = port.harness.getAgentForMode(effectiveModeId);

  const subscriptionWaiter = port.createActiveTurnWaiter();
  void subscriptionWaiter.promise.catch(() => {});
  const subscription = port.ensureThreadSubscription(agent);
  void subscription.catch(() => {});
  const sub = await Promise.race([subscription, subscriptionWaiter.promise]).finally(() => {
    subscriptionWaiter.cleanup();
  });
  port.assertLive('injectSystemReminder()');
  port.assertOpenForTurn('injectSystemReminder()');
  const activeRunId = sub.activeRunId();
  const willInterleave = activeRunId !== null;
  // PF-7473: the sender may have lost admission during subscription setup.
  internal?.assertAdmitted?.();
  if (!willInterleave && internal?.requireActiveRun === true) {
    throw new HarnessValidationError('injectSystemReminder()', 'no active run to interleave into');
  }
  const idleOnlyEpoch = internal?.admissionEpoch;
  if (
    idleOnlyEpoch !== undefined &&
    (willInterleave || port.isRunning() || !port.activeWorkAdmissionIsCurrent(idleOnlyEpoch))
  ) {
    throw new HarnessBusyError(port.id, 'in_flight');
  }

  if (!willInterleave) {
    // Owned-turn path: full turn bookkeeping in a background
    // continuation. Caller doesn't get a result handle.
    const turnAbortController = port.beginTurn(undefined, undefined, sourceWriteGuard);
    const turnAbortSignal = turnAbortController.signal;
    const activeTurnWaiter = port.createActiveTurnWaiter();
    void activeTurnWaiter.promise.catch(() => {});
    // PF-7631: the terminal identity of a settlement wake turn, once admitted.
    let wakeTerminal: HarnessTerminalIdentity | undefined;
    // When the admitted wake's durable dispatch claim expires.
    let wakeClaimExpiresAt = 0;
    // The wake admission attempt, kept even when a Stop, delete or eviction
    // wins the race against it, and whether its terminal commit landed.
    let wakeAdmission: Promise<HarnessTerminalIdentity> | undefined;
    let wakeSettled = false;
    // Whether a run may have started under the grant: set once `sendSignal`
    // returns (a synchronous throw comes before any run reservation), and
    // cleared again by a `discard` acceptance, which proves nothing ran.
    let wakeDispatchSent = false;
    const finishOwnedReminderTurn = () => {
      if (wakeAdmission !== undefined) {
        const admission = wakeAdmission;
        wakeAdmission = undefined;
        if (wakeSettled) {
          port.wakeAdmissions.leave(wakeTerminal!.admissionId);
        } else if (!wakeDispatchSent) {
          // A Stop, edit, delete or eviction refused the turn before its
          // dispatch: cancel the admission whenever it lands, as a grant
          // given up at the re-check is. Tracked, so a close waits for it.
          void port.trackBackgroundTurnCompletion(
            admission.then(
              identity => port.cancelWakeTerminalAdmission(identity),
              () => undefined,
            ),
          );
        } else {
          // Release the admission whenever it lands; one that failed has
          // already cleared its own marker. Tracked, so a close waits for it.
          void port.trackBackgroundTurnCompletion(
            admission.then(
              identity => port.releaseWakeTerminalAdmission(identity.admissionId),
              () => undefined,
            ),
          );
        }
      }
      activeTurnWaiter.cleanup();
      port.endTurn(turnAbortController);
    };
    const assertOwnedReminderTurnNotDeleted = () => {
      if (port.state === 'deleted') {
        throw new HarnessSessionDeletedError(port.id, port.record.resourceId, port.record.threadId);
      }
    };
    let dispatched;
    try {
      const toolSurface = port.buildToolSurface(mode, undefined);
      port.setCurrentTurnReplacementToolSurface(toolSurface);
      const [requestContext, turnInstructions] = await Promise.race([
        port.buildTurnContextAndInstructions(
          {
            modeId: effectiveModeId,
            modelId: port.record.modelId,
            abortSignal: turnAbortSignal,
          },
          mode,
        ),
        activeTurnWaiter.promise,
      ]);
      const baseExecOptions: AgentExecutionOptionsBase<unknown> = {
        memory: { thread: port.threadId, resource: port.resourceId },
        abortSignal: turnAbortSignal,
        requestContext,
        maxSteps: HARNESS_SESSION_MAX_STEPS,
        ...port.createEmptySynthesisOptions(),
        ...port.subagentTracingOptions(),
        ...toolSurface,
        ...(turnInstructions ? { instructions: turnInstructions } : {}),
        ...(sourceWriteGuard ? { observationalMemorySourceWriteGuard: sourceWriteGuard } : {}),
      };
      assertOwnedReminderTurnNotDeleted();
      port.assertOpenForTurn('injectSystemReminder()');
      if (
        internal?.wakeNoticeId !== undefined &&
        internal.wakeSettlement !== undefined &&
        signalId !== undefined &&
        port.terminalWakeFinalizer !== undefined &&
        port.currentTurnAbortController === turnAbortController &&
        sub.activeRunId() === null &&
        wakeGrantAllowed(port, internal.wakeReminderRow, agent, effectiveModeId)
      ) {
        const claimed = admitWakeTerminalTurn(port, {
          wakeNoticeId: internal.wakeNoticeId,
          signalId,
          settlement: internal.wakeSettlement,
          modeId: effectiveModeId,
          modelId: port.record.modelId,
        });
        const admission = claimed.then(({ identity }) => identity);
        void admission.catch(() => {});
        wakeAdmission = admission;
        const admitted = await Promise.race([
          claimed.then(
            ({ identity, claimExpiresAt }) => ({ identity, claimExpiresAt }),
            (error: unknown) => ({ error }),
          ),
          activeTurnWaiter.promise,
        ]);
        if ('error' in admitted) {
          wakeAdmission = undefined;
          // PF-7679: the grant key is per notice, so a reservation left by an
          // earlier attempt (given up, cancelling, or orphaned by a crash)
          // refuses this one as busy. That is not a lost slot: the claim, not
          // the grant, keeps the delivery exactly once, so the wake runs
          // without a grant. Any other refusal fails the attempt as before.
          if (internal.claimNotice === undefined || !(admitted.error instanceof HarnessBusyError)) {
            throw admitted.error;
          }
        } else {
          wakeTerminal = admitted.identity;
          wakeClaimExpiresAt = admitted.claimExpiresAt;
        }
        assertOwnedReminderTurnNotDeleted();
      }
      // PF-7679: claim last. Raced, so an abort still releases the turn while
      // the claim write stalls; the delivery itself awaits that write before
      // it decides whether to clear its claim.
      if (internal?.claimNotice !== undefined) {
        const won = await Promise.race([internal.claimNotice(), activeTurnWaiter.promise]);
        if (!won) throw new DelegationNoticeWakeNotSentError('claim_lost');
        assertOwnedReminderTurnNotDeleted();
        port.assertOpenForTurn('injectSystemReminder()');
      }
      // An idle-only wake re-checks in the same tick as its dispatch. A stale
      // admission epoch refuses it: a Stop (`abortActiveWork`) or an
      // edit/regenerate (`resetPlanTasks`, which cancels the owned child
      // sessions) during its awaits (the Stop contract: not stopped or
      // cancelled since the settlement). PF-7679: if a caller turn began or
      // started a run during the awaits, the wake never folds into it: it
      // throws `slot_lost`, and its delivery clears its claim and waits for
      // that turn to end. PF-7631: the wake grant is kept only while its row
      // is still proven to be in the dispatching agent's own memory with no
      // mode change since (`_wakeGrantAllowed`), re-checked here in the same
      // tick as the dispatch. If that no longer holds, the grant is cancelled
      // (no run starts under it, no terminal result or intent is committed)
      // and the wake is sent without it. A kept grant binds the dispatch to
      // the reserved run.
      if (idleOnlyEpoch !== undefined) {
        if (!port.activeWorkAdmissionIsCurrent(idleOnlyEpoch)) {
          if (internal?.claimNotice !== undefined) throw new DelegationNoticeWakeNotSentError('stale');
          throw new HarnessBusyError(port.id, 'in_flight');
        }
        if (port.currentTurnAbortController !== turnAbortController || sub.activeRunId() !== null) {
          throw new DelegationNoticeWakeNotSentError('slot_lost');
        }
      }
      if (internal?.noticeRowDelivered?.() === true) throw new DelegationNoticeWakeNotSentError('delivered');
      if (
        wakeTerminal !== undefined &&
        (port.currentTurnAbortController !== turnAbortController ||
          sub.activeRunId() !== null ||
          !wakeGrantAllowed(port, internal?.wakeReminderRow, agent, effectiveModeId))
      ) {
        const givenUp = wakeTerminal;
        wakeAdmission = undefined;
        wakeTerminal = undefined;
        port.cancelWakeTerminalAdmission(givenUp);
      }

      // Last step before `sendSignal`, as in message(): never dispatch a granted
      // wake on a dispatch claim or session lease that lapsed during the
      // awaits above, when another owner may already have recovered it. The
      // grant is then cancelled or released by the turn's cleanup. A claimed
      // settlement notice keeps its claim until the delivery clears it (a
      // lease-fenced write), or the next lease holder takes it over.
      if (wakeTerminal !== undefined && !port.dispatchClaimStillHeld(wakeClaimExpiresAt)) {
        port.scheduleOrphanRecheck(Date.now());
        if (internal?.claimNotice !== undefined) throw new DelegationNoticeWakeNotSentError('slot_lost');
        throw new HarnessBusyError(port.id, 'in_flight');
      }
      // PF-7679: a claimed settlement wake dispatches only while this owner
      // still holds the session lease, granted or not: once the lease lapses
      // the next holder may take the claim over and deliver the notice.
      if (internal?.claimNotice !== undefined && !port.sessionLeaseHeld()) {
        throw new DelegationNoticeWakeNotSentError('lease_lost');
      }
      const idleOnlyDispatch = wakeTerminal !== undefined || internal?.claimNotice !== undefined;
      dispatched = agent.sendSignal(
        {
          type: 'system-reminder',
          ...(signalId !== undefined ? { id: signalId } : {}),
          contents: content,
          ...(opts?.attributes ? { attributes: opts.attributes } : {}),
          ...(opts?.metadata ? { metadata: opts.metadata } : {}),
        },
        {
          resourceId: port.resourceId,
          threadId: port.threadId,
          // An admitted wake binds its grant to the run this dispatch starts.
          // Neither it nor a claimed settlement wake (PF-7679) may fold into a
          // run that appeared meanwhile, here or in another process.
          ...(wakeTerminal !== undefined ? { runId: wakeTerminal.runId } : {}),
          ...(idleOnlyDispatch ? { ifActive: { behavior: 'discard' } } : {}),
          ifIdle: { behavior: 'wake', streamOptions: baseExecOptions as never },
        },
      );
      wakeDispatchSent = true;
      if (idleOnlyDispatch) {
        // PF-7679: the wake run, its label and agent_start exist only once the
        // runtime accepted this dispatch as a wake on the run it returned.
        const expectedRunId = wakeTerminal?.runId ?? dispatched.runId;
        // Bounded as message() bounds it. On a timeout the turn is aborted,
        // so a native stream that did start never runs without an owner; the
        // admission is then released to dispatch recovery.
        let accepted: Awaited<typeof dispatched.accepted>;
        try {
          accepted = await awaitSignalNativeAcceptance(
            port,
            dispatched.accepted,
            turnAbortSignal,
            activeTurnWaiter.promise,
            'injectSystemReminder().wake',
          );
        } catch (err) {
          if (err instanceof NativeSignalAcceptanceTimeoutError) turnAbortController.abort(err);
          // PF-7679: the dispatch was sent, so a run may have started: a
          // claimed settlement wake must not read this as not sent.
          else if (internal?.claimNotice !== undefined) throw new DelegationNoticeWakeNotSentError('unconfirmed');
          throw err;
        }
        if (accepted.action !== 'wake' || accepted.runId !== expectedRunId) {
          // A `discard` (the dispatch binds `ifActive: 'discard'`) proves no
          // run started: the grant is cancelled like one given up before the
          // dispatch. Any other outcome is left to dispatch recovery, which
          // settles it interrupted.
          if (accepted.action === 'discard') {
            wakeDispatchSent = false;
            // PF-7679: a run took the slot; the delivery clears its claim.
            if (internal?.claimNotice !== undefined) throw new DelegationNoticeWakeNotSentError('slot_lost');
          } else {
            if (wakeTerminal !== undefined) port.scheduleOrphanRecheck(Date.now());
            // PF-7679: a run may have started; never read as a lost slot.
            if (internal?.claimNotice !== undefined) throw new DelegationNoticeWakeNotSentError('unconfirmed');
          }
          throw new HarnessBusyError(port.id, 'in_flight');
        }
      }
    } catch (err) {
      finishOwnedReminderTurn();
      throw err;
    }

    // §10.2 agent_start carries the runId — emit it now the run is dispatched.
    // dispatched.signal.id is the originating signalId — stamped for the run projection.
    // A wake reaches here only while it held the idle slot at dispatch, or
    // as the reserved run its grant bound (verified above: action 'wake' on
    // the reserved runId), so it keeps the label.
    port.emitAgentStart(dispatched.runId, dispatched.signal.id, internal?.wakeNoticeId);

    const completion = port.awaitRunCompletion(dispatched.runId);
    const completionOrDelete = Promise.race([completion, activeTurnWaiter.promise]);
    let agentEndEmitted = false;
    const result = port.trackBackgroundTurnCompletion(
      completionOrDelete
        .then(async full => {
          // A wake terminal turn accounts usage under the terminal fences, as
          // an admitted message turn does, so a recovery commit of its
          // staged output never counts it twice.
          const tokenUsageDelta =
            full.finishReason === 'suspended'
              ? port.tokenUsageDeltaFromFullOutput(full)
              : wakeTerminal !== undefined
                ? port.recordMessageTurnCompletion(full, { persist: false }).tokenUsageDelta
                : port.recordTurnCompletion(full, { persist: false });
          await Promise.race([
            port
              .maybeCaptureSuspend(full, undefined, effectiveModeId, port.record.modelId, {
                tokenUsageDelta: full.finishReason === 'suspended' ? tokenUsageDelta : undefined,
              })
              .catch(err => {
                port.latchDurableTurnFlushError(err, full);
                throw err;
              }),
            activeTurnWaiter.promise,
          ]);
          if (tokenUsageDelta !== undefined && full.finishReason !== 'suspended') {
            await Promise.race([
              port.persistTokenUsageOrLatch().catch(err => {
                if (!port.isExpectedFlushLifecycleError(err)) port.pendingTokenUsageFlushError ??= err;
                throw err;
              }),
              activeTurnWaiter.promise,
            ]);
          }
          // PF-7631: commit the wake turn's terminal intent before agent_end,
          // as a message() terminal turn does. A failed commit leaves the
          // staged output pending for dispatch recovery.
          if (wakeTerminal !== undefined) {
            await commitWakeTerminalWithRetry(
              port,
              wakeTerminal,
              full,
              { modeId: effectiveModeId, modelId: port.record.modelId },
              activeTurnWaiter.promise,
              turnAbortSignal,
            );
            wakeSettled = true;
          }
          port.emitAgentEnd({ runId: full.runId, finishReason: port.agentEndReasonForFullOutput(full), full });
          agentEndEmitted = true;
          await Promise.race([port.runGoalJudge(full, false), activeTurnWaiter.promise]);
        })
        .catch(async err => {
          // PF-7631: a wake run whose own output rejected commits a durable
          // failed terminal; anything else stays pending for recovery.
          if (wakeTerminal !== undefined && !agentEndEmitted) {
            if (port.isRejectedTerminalRun(dispatched.runId, err, turnAbortSignal)) {
              // A wake run has no caller to reject: surface the run's own
              // failure first. If the commit below never lands, recovery
              // settles the admission as interrupted, so this event is the
              // only record of what the run failed with.
              port.emitTurnEvent({
                type: 'error',
                runId: dispatched.runId,
                signalId: wakeTerminal.signalId,
                error: projectHarnessPublicError(err),
              });
              // The same bounded re-drive as a completed wake. If it does
              // not land, the admission is released to dispatch recovery
              // below (finishOwnedReminderTurn), never left 'finished'.
              const committed = await commitWakeTerminalWithRetry(
                port,
                wakeTerminal,
                new RejectedTerminalRun(err),
                { modeId: effectiveModeId, modelId: port.record.modelId },
                activeTurnWaiter.promise,
                turnAbortSignal,
              ).then(
                () => true,
                () => false,
              );
              if (committed) wakeSettled = true;
              else port.scheduleOrphanRecheck(Date.now());
            } else {
              port.scheduleOrphanRecheck(Date.now());
            }
          }
          // A wake whose terminal commit did not land leaves agent_end to
          // its final settlement, as message() does: its failure was already
          // surfaced as an `error` event, and recovery publishes the outcome.
          if (!agentEndEmitted && (wakeTerminal === undefined || wakeSettled)) {
            port.emitTurnEvent({
              type: 'agent_end',
              finishReason: turnAbortSignal.aborted ? 'aborted' : 'error',
              runId: dispatched.runId,
              usage: port.runUsage(),
            });
          }
          throw err;
        })
        .finally(() => {
          finishOwnedReminderTurn();
          void port.maybeDrainQueue();
        }),
    );
    void result.catch(() => {});

    return {
      id: dispatched.signal.id,
      runId: dispatched.runId,
      willInterleave: false,
      accepted: true,
      signal: dispatched.signal,
    };
  }

  // Active-delivery path: drain into the live run.
  port.assertOpenForTurn('injectSystemReminder()');
  if (
    sourceWriteGuard !== undefined &&
    !sameObservationalMemorySourceWriteGuard(sourceWriteGuard, port.currentTurnSourceWriteGuard)
  ) {
    throw new HarnessOverrideConflictError(port.id, activeRunId!, ['observationalMemorySourceWriteGuard']);
  }
  const dispatched = agent.sendSignal(
    {
      type: 'system-reminder',
      ...(signalId !== undefined ? { id: signalId } : {}),
      contents: content,
      ...(opts?.attributes ? { attributes: opts.attributes } : {}),
      ...(opts?.metadata ? { metadata: opts.metadata } : {}),
    },
    {
      resourceId: port.resourceId,
      threadId: port.threadId,
      // PF-7473: an interleave-only reminder must never start a run: it
      // expires with the run it was delivered to, and is discarded when the
      // run already ended between routing and dispatch.
      ...(internal?.requireActiveRun === true ? { ifActive: { behavior: 'deliver', expireWithRun: true } } : {}),
      ifIdle:
        internal?.requireActiveRun === true
          ? { behavior: 'discard' }
          : {
              behavior: 'wake',
              // The active-delivery race can still land idle; a wake here starts a
              // REAL streamed turn, so it must carry the step ceiling and the
              // silent-turn nudge like every other wake site — empty options ran
              // the turn at the provider's 5-step default with no memory identity.
              streamOptions: {
                memory: { thread: port.threadId, resource: port.resourceId },
                maxSteps: HARNESS_SESSION_MAX_STEPS,
                ...port.createEmptySynthesisOptions(),
                ...(port.currentTurnSourceWriteGuard
                  ? { observationalMemorySourceWriteGuard: port.currentTurnSourceWriteGuard }
                  : {}),
                ...port.subagentTracingOptions(),
              } as never,
            },
    },
  );
  if (internal?.requireActiveRun === true && (await dispatched.accepted).action !== 'deliver') {
    throw new HarnessValidationError('injectSystemReminder()', 'the active run ended before the reminder landed');
  }

  return {
    id: dispatched.signal.id,
    runId: dispatched.runId,
    willInterleave: true,
    accepted: true,
    signal: dispatched.signal,
  };
}

export function channelSignalAdmissionHash(
  port: SignalsSessionPort,
  opts: {
    content: string;
    mode?: string;
    observationalMemorySourceWriteGuard?: ObservationalMemorySourceWriteGuard;
  },
  attachments: PersistedAttachment[],
  requestContext: PersistedRequestContextInput,
): string {
  return computeSignalAdmissionHash(port, opts, attachments, requestContext);
}

export async function buildSignalContentsWithAttachments(
  port: SignalsSessionPort,
  content: string,
  attachments: PersistedAttachment[] | undefined,
): Promise<AgentSignalContents> {
  if (attachments === undefined || attachments.length === 0) {
    return content;
  }
  const parts: Array<
    { type: 'text'; text: string } | { type: 'file'; data: string; mediaType: string; filename?: string }
  > = [{ type: 'text', text: content }];
  for (const attachment of attachments) {
    if (attachment.kind === 'url') {
      // A URL-only attachment carries no bytes to inline; surface it as a file
      // reference so the model still sees the link + filename.
      parts.push({ type: 'file', data: attachment.url, mediaType: attachment.mimeType, filename: attachment.name });
      continue;
    }
    const loaded = await port.storage.loadAttachment({
      harnessName: port.record.harnessName,
      sessionId: attachment.ownerSessionId,
      attachmentId: attachment.attachmentId,
    });
    if (!loaded) {
      throw new HarnessAttachmentUnavailableError(attachment.ownerSessionId, 'not_found', attachment.attachmentId);
    }
    const base64 = Buffer.from(loaded.data).toString('base64');
    // Inline both image and non-image attachment bytes through the AI SDK v5
    // file-part shape (a `data:<mime>;base64,<bytes>` URL). The v5 image-part
    // shape keys on `image`/`mediaType`, not `data`/`mimeType`, so a hand-built
    // `{ type: 'image', data, mimeType }` part silently drops its bytes when
    // MessageList runs `convertImageFilePart` (it reads `part.image`, which is
    // undefined). The file shape preserves bytes for image/* mime types too —
    // `convertToDataContent` decodes the data URL regardless of media type — so
    // one code path covers every inline attachment.
    parts.push({
      type: 'file',
      data: `data:${attachment.mimeType};base64,${base64}`,
      mediaType: attachment.mimeType,
      filename: attachment.name,
    });
  }
  // Upstream signals API (#merge): AgentSignalContents is a bare parts array.
  return parts as AgentSignalContents;
}

export function deletedOperationEvidenceSignalIds(port: SignalsSessionPort): string[] {
  return Array.from(port.operationEvidenceSignalIds);
}

export function wakeGrantAllowed(
  port: SignalsSessionPort,
  row: WakeReminderRowOrigin | undefined,
  agent: Agent,
  modeId: string,
): boolean {
  return (
    row !== undefined &&
    row.agent === agent &&
    agent.__fixedMemory() === row.memory &&
    row.modeId === modeId &&
    port.record.modeId === modeId &&
    port.modeSwitchEpoch === row.modeEpoch
  );
}
