import type { StepResult, WorkflowRunState } from '../../../workflows';
import type {
  ClaimRunOwnershipInput,
  ClaimRunOwnershipResult,
  RenewRunOwnershipInput,
  RenewRunOwnershipResult,
  RunFence,
  RunOwnershipRecord,
} from '../../run-fencing';
import { runFencingNotSupportedError } from '../../run-fencing';
import type {
  AdvanceWorkflowTerminalizationInput,
  AdvanceWorkflowTerminalizationResult,
  ClaimWorkflowTerminalizationInput,
  ClaimWorkflowTerminalizationResult,
  DeleteCompletedWorkflowTerminalizationsInput,
  DeleteCompletedWorkflowTerminalizationsResult,
  GetWorkflowTerminalizationInput,
  GetWorkflowTerminalizationResult,
  GetWorkflowRunTerminalStatusInput,
  GetWorkflowRunTerminalStatusResult,
  GetWorkflowTerminalEffectForDispatchInput,
  GetWorkflowTerminalEffectForDispatchResult,
  GetWorkflowTerminalDestinationReceiptInput,
  GetWorkflowTerminalDestinationReceiptResult,
  GetWorkflowTerminalParentContextInput,
  GetWorkflowTerminalParentContextResult,
  GetWorkflowTerminalContinuationPlanInput,
  GetWorkflowTerminalContinuationPlanResult,
  PersistWorkflowTerminalStateInput,
  PersistWorkflowTerminalStateResult,
  PrepareWorkflowTerminalEffectInput,
  PrepareWorkflowTerminalEffectResult,
  ReserveWorkflowTerminalDestinationReceiptInput,
  ReserveWorkflowTerminalDestinationReceiptResult,
  ApplyWorkflowTerminalParentEffectInput,
  ApplyWorkflowTerminalParentEffectResult,
  BindWorkflowNestedRunOwnershipInput,
  BindWorkflowNestedRunOwnershipResult,
  PersistWorkflowTerminalRecoveryAncestryInput,
  PersistWorkflowTerminalRecoveryAncestryResult,
  GetWorkflowTerminalRecoveryAncestryResult,
  AdmitWorkflowNestedRunInput,
  AdmitWorkflowNestedRunResult,
  AdmitWorkflowResumeInput,
  AdmitWorkflowResumeResult,
  ConsumeWorkflowResumeResult,
  ConsumeWorkflowResumeResultInput,
  PersistWorkflowStepUpdateInput,
  PersistWorkflowStepUpdateResult,
  DeleteWorkflowRunIfGenerationInput,
  DeleteWorkflowRunIfGenerationResult,
  FinalizeWorkflowResumeInput,
  FinalizeWorkflowResumeResult,
  RollbackWorkflowResumeInput,
  RollbackWorkflowResumeResult,
  ReleaseWorkflowTerminalizationInput,
  ReleaseWorkflowTerminalizationResult,
  UpdateWorkflowResultsResult,
  UpdateWorkflowStateOptions,
  WorkflowRun,
  WorkflowRuns,
  StorageListWorkflowRunsInput,
  WorkflowTerminalizationCapabilities,
  WorkflowResumeCapabilities,
  WorkflowSnapshotHandoffCapabilities,
  ClaimWorkflowSnapshotHandoffInput,
  ClaimWorkflowSnapshotHandoffResult,
  TransitionWorkflowSnapshotHandoffInput,
  TransitionWorkflowSnapshotHandoffResult,
  CompleteWorkflowSnapshotHandoffInput,
  CompleteWorkflowSnapshotHandoffResult,
  ReleaseWorkflowSnapshotHandoffInput,
  ReleaseWorkflowSnapshotHandoffResult,
  ListWorkflowSnapshotHandoffsInput,
  ListWorkflowSnapshotHandoffsResult,
  DeleteWorkflowRunClosureInput,
  DeleteWorkflowRunClosureResult,
  CancelWorkflowRunForErasureInput,
  CancelWorkflowRunForErasureResult,
  ListWorkflowRunClosuresByResourceInput,
  ListWorkflowRunClosuresByResourceResult,
  ListWorkflowRunsByThreadForErasureInput,
  ListWorkflowRunsByThreadForErasureResult,
  WorkflowExecutionState,
} from '../../types';
import { StorageDomain } from '../base';

export abstract class WorkflowsStorage extends StorageDomain {
  constructor() {
    super({
      component: 'STORAGE',
      name: 'WORKFLOWS',
    });
  }

  abstract supportsConcurrentUpdates(): boolean;

  /** Whether this adapter provides atomic terminalization claim/phase CAS. */
  supportsWorkflowTerminalizationJournal(): boolean {
    return false;
  }

  /** Exact protocol versions implemented by this adapter. */
  getWorkflowTerminalizationCapabilities(): WorkflowTerminalizationCapabilities {
    return {};
  }

  /** Exact atomic resume protocol versions implemented by this adapter. */
  getWorkflowResumeCapabilities(): WorkflowResumeCapabilities {
    return {};
  }

  /** Exact framework-native product snapshot handoff protocol versions. */
  getWorkflowSnapshotHandoffCapabilities(): WorkflowSnapshotHandoffCapabilities {
    return {};
  }

  async claimWorkflowSnapshotHandoff(
    _input: ClaimWorkflowSnapshotHandoffInput,
  ): Promise<ClaimWorkflowSnapshotHandoffResult> {
    return { status: 'unsupported' };
  }

  async transitionWorkflowSnapshotHandoff(
    _input: TransitionWorkflowSnapshotHandoffInput,
  ): Promise<TransitionWorkflowSnapshotHandoffResult> {
    return { status: 'unsupported' };
  }

  async completeWorkflowSnapshotHandoff(
    _input: CompleteWorkflowSnapshotHandoffInput,
  ): Promise<CompleteWorkflowSnapshotHandoffResult> {
    return { status: 'unsupported' };
  }

  /**
   * Releases one claim of the run's handoff, compared by `claimGeneration`:
   * the row is kept and marked released, and no longer fences the run.
   * Adapters implementing this advertise `releaseVersion: 1`.
   */
  async releaseWorkflowSnapshotHandoff(
    _input: ReleaseWorkflowSnapshotHandoffInput,
  ): Promise<ReleaseWorkflowSnapshotHandoffResult> {
    return { status: 'unsupported' };
  }

  async listWorkflowSnapshotHandoffs(
    _input: ListWorkflowSnapshotHandoffsInput = {},
  ): Promise<ListWorkflowSnapshotHandoffsResult> {
    return { records: [], hasMore: false };
  }

  /**
   * Erases a settled run's handoff, snapshot and terminal rows atomically,
   * keeping the parent revision. Adapters implementing this advertise
   * `closureDeleteVersion: 1`.
   */
  async deleteWorkflowRunClosure(_input: DeleteWorkflowRunClosureInput): Promise<DeleteWorkflowRunClosureResult> {
    return { status: 'unsupported' };
  }

  /**
   * Overwrites an unsettled run's snapshot with an empty `canceled` one,
   * atomically refusing a journaled run or one a nested child still needs.
   * Adapters implementing this advertise `closureCancelVersion: 1`.
   */
  async cancelWorkflowRunForErasure(
    _input: CancelWorkflowRunForErasureInput,
  ): Promise<CancelWorkflowRunForErasureResult> {
    return { status: 'unsupported' };
  }

  /** Pages the runs holding rows under a resource, released handoffs included. */
  async listWorkflowRunClosuresByResource(
    _input: ListWorkflowRunClosuresByResourceInput,
  ): Promise<ListWorkflowRunClosuresByResourceResult> {
    return { status: 'unsupported' };
  }

  /**
   * Pages the resource's runs whose snapshot records the thread id anywhere
   * it can appear, in any status. Adapters implementing this advertise
   * `threadErasureListVersion: 1`; others answer `unsupported`.
   */
  async listWorkflowRunsByThreadForErasure(
    _input: ListWorkflowRunsByThreadForErasureInput,
  ): Promise<ListWorkflowRunsByThreadForErasureResult> {
    return { status: 'unsupported' };
  }

  async admitWorkflowResume(_input: AdmitWorkflowResumeInput): Promise<AdmitWorkflowResumeResult> {
    return { status: 'unsupported' };
  }

  async rollbackWorkflowResume(_input: RollbackWorkflowResumeInput): Promise<RollbackWorkflowResumeResult> {
    return { status: 'unsupported' };
  }

  async finalizeWorkflowResume(_input: FinalizeWorkflowResumeInput): Promise<FinalizeWorkflowResumeResult> {
    return { status: 'unsupported' };
  }

  async consumeWorkflowResumeResult(_input: ConsumeWorkflowResumeResultInput): Promise<ConsumeWorkflowResumeResult> {
    return { status: 'unsupported' };
  }

  /** Adapters implementing this advertise `generationDeleteVersion: 1`. */
  async deleteWorkflowRunIfGeneration(
    _input: DeleteWorkflowRunIfGenerationInput,
  ): Promise<DeleteWorkflowRunIfGenerationResult> {
    return { status: 'unsupported' };
  }

  async persistWorkflowStepUpdate(_input: PersistWorkflowStepUpdateInput): Promise<PersistWorkflowStepUpdateResult> {
    return { status: 'unsupported' };
  }

  async claimWorkflowTerminalization(
    _input: ClaimWorkflowTerminalizationInput,
  ): Promise<ClaimWorkflowTerminalizationResult> {
    return { status: 'unsupported' };
  }

  async getWorkflowTerminalization(_input: GetWorkflowTerminalizationInput): Promise<GetWorkflowTerminalizationResult> {
    return { status: 'unsupported' };
  }

  async getWorkflowRunTerminalStatus(
    _input: GetWorkflowRunTerminalStatusInput,
  ): Promise<GetWorkflowRunTerminalStatusResult> {
    return { status: 'unsupported' };
  }

  async advanceWorkflowTerminalization(
    _input: AdvanceWorkflowTerminalizationInput,
  ): Promise<AdvanceWorkflowTerminalizationResult> {
    return { status: 'unsupported' };
  }

  async releaseWorkflowTerminalization(
    _input: ReleaseWorkflowTerminalizationInput,
  ): Promise<ReleaseWorkflowTerminalizationResult> {
    return { status: 'unsupported' };
  }

  async deleteCompletedWorkflowTerminalizations(
    _input: DeleteCompletedWorkflowTerminalizationsInput,
  ): Promise<DeleteCompletedWorkflowTerminalizationsResult> {
    return { status: 'unsupported', count: 0 };
  }

  async persistWorkflowTerminalState(
    _input: PersistWorkflowTerminalStateInput,
  ): Promise<PersistWorkflowTerminalStateResult> {
    return { status: 'unsupported' };
  }

  async persistWorkflowTerminalRecoveryAncestry(
    _input: PersistWorkflowTerminalRecoveryAncestryInput,
  ): Promise<PersistWorkflowTerminalRecoveryAncestryResult> {
    return { status: 'unsupported' };
  }

  async getWorkflowTerminalRecoveryAncestry(
    _input: GetWorkflowTerminalizationInput,
  ): Promise<GetWorkflowTerminalRecoveryAncestryResult> {
    return { status: 'unsupported' };
  }

  async bindWorkflowNestedRunOwnership(
    _input: BindWorkflowNestedRunOwnershipInput,
  ): Promise<BindWorkflowNestedRunOwnershipResult> {
    return { status: 'unsupported' };
  }

  async admitWorkflowNestedRun(_input: AdmitWorkflowNestedRunInput): Promise<AdmitWorkflowNestedRunResult> {
    return { status: 'unsupported' };
  }

  async prepareWorkflowTerminalEffect(
    _input: PrepareWorkflowTerminalEffectInput,
  ): Promise<PrepareWorkflowTerminalEffectResult> {
    return { status: 'unsupported' };
  }

  async getWorkflowTerminalEffectForDispatch(
    _input: GetWorkflowTerminalEffectForDispatchInput,
  ): Promise<GetWorkflowTerminalEffectForDispatchResult> {
    return { status: 'unsupported' };
  }

  async reserveWorkflowTerminalDestinationReceipt(
    _input: ReserveWorkflowTerminalDestinationReceiptInput,
  ): Promise<ReserveWorkflowTerminalDestinationReceiptResult> {
    return { status: 'unsupported' };
  }

  async getWorkflowTerminalDestinationReceipt(
    _input: GetWorkflowTerminalDestinationReceiptInput,
  ): Promise<GetWorkflowTerminalDestinationReceiptResult> {
    return { status: 'unsupported' };
  }

  async getWorkflowTerminalParentContext(
    _input: GetWorkflowTerminalParentContextInput,
  ): Promise<GetWorkflowTerminalParentContextResult> {
    return { status: 'unsupported' };
  }

  async getWorkflowTerminalContinuationPlan(
    _input: GetWorkflowTerminalContinuationPlanInput,
  ): Promise<GetWorkflowTerminalContinuationPlanResult> {
    return { status: 'unsupported' };
  }

  async applyWorkflowTerminalParentEffect(
    _input: ApplyWorkflowTerminalParentEffectInput,
  ): Promise<ApplyWorkflowTerminalParentEffectResult> {
    return { status: 'unsupported' };
  }

  /**
   * Whether this adapter implements run ownership: the claim/renew/release
   * operations below, and rejecting writes whose fence is no longer the run's
   * current claim, atomically with the write itself. A write's fence is its
   * `fence` argument, otherwise the one `resolveRunFence()` returns for the
   * run it writes.
   *
   * This fences workflow writes only. A run is fully fenced only when the
   * memory store it writes to supports run fencing too, which a composite
   * store taking its domains from different adapters, or an agent whose
   * memory has its own storage, does not guarantee. Otherwise an execution
   * that lost the run can no longer write here, but its messages, threads and
   * working memory still land.
   *
   * A rejected write throws a `RunFenceConflictError` itself, not wrapped in
   * another error, and writes nothing.
   *
   * Adapters that return true must pass the run-fencing conformance suite.
   * An adapter that has to probe its backend to know returns a promise, and
   * rejects when the probe fails: it must never answer false and later true.
   */
  supportsRunFencing(): boolean | Promise<boolean> {
    return false;
  }

  /**
   * Claim a run for an execution. Succeeds when the run has no live owner, or
   * when `force` is set, and increments the run's generation. Lease expiry is
   * computed on the store's clock. Every claim, renewal and release sets
   * `updatedAt` from the store's clock.
   *
   * A run with no record starts at the store's clock in epoch milliseconds (at
   * least 1) rather than 1. Retention prunes ownership records and memory
   * fences independently, so a run claimed again after `workflows.runOwnership`
   * retention pruned its record must still outrank any fence or stream event
   * left from before. This relies on the store's clock not moving backward
   * between the two claims.
   *
   * Later claims increment by 1 rather than reading the clock again. A takeover
   * then depends only on the stored generation, which the atomic claim already
   * reads, so clock skew cannot affect it. The generation orders claims; it is
   * not a timestamp.
   */
  async claimRunOwnership(_args: ClaimRunOwnershipInput): Promise<ClaimRunOwnershipResult> {
    throw runFencingNotSupportedError('workflows', this.constructor.name);
  }

  /**
   * Extend the lease. Succeeds only while `ownerId` and `generation` still
   * hold the run and the claim has not been released.
   */
  async renewRunOwnership(_args: RenewRunOwnershipInput): Promise<RenewRunOwnershipResult> {
    throw runFencingNotSupportedError('workflows', this.constructor.name);
  }

  /**
   * Give up a claim by clearing its lease, so the run can be claimed without
   * `force`. The record keeps its generation and owner, and the released
   * owner's late writes are still accepted until the run is claimed again or
   * its record is pruned. Returns false if the claim was no longer current.
   */
  async releaseRunOwnership(_args: RunFence): Promise<boolean> {
    throw runFencingNotSupportedError('workflows', this.constructor.name);
  }

  async getRunOwnership(_args: { runId: string }): Promise<RunOwnershipRecord | null> {
    throw runFencingNotSupportedError('workflows', this.constructor.name);
  }

  /**
   * In-memory, LibSQL, and PostgreSQL storage return an empty object without writing when the workflow run does not
   * exist. Remaining adapters are being aligned under COR-1448; callers must not rely on this method to create the
   * initial run snapshot.
   */
  abstract updateWorkflowResults({
    workflowName,
    runId,
    stepId,
    result,
    requestContext,
    executionGeneration,
    state,
    fence,
  }: {
    workflowName: string;
    runId: string;
    stepId: string;
    result: StepResult<any, any, any, any>;
    requestContext: Record<string, any>;
    executionGeneration?: string;
    /**
     * Workflow state to record as `context.__state` in the same atomic update as
     * the step result, so a crash can't persist one without the other.
     */
    state?: Record<string, any>;
    fence?: RunFence;
  }): Promise<UpdateWorkflowResultsResult>;

  abstract updateWorkflowState({
    workflowName,
    runId,
    opts,
    fence,
  }: {
    workflowName: string;
    runId: string;
    opts: UpdateWorkflowStateOptions;
    fence?: RunFence;
  }): Promise<WorkflowRunState | undefined>;

  /**
   * `expectedExecutionGeneration` guards the upsert compare-and-set style:
   * adapters that enforce it throw `WorkflowStaleSnapshotPersistError` when
   * the stored row is missing or its snapshot generation differs, so a stale
   * execution lifetime cannot resurrect a deleted run or overwrite the
   * reopened run's row. Adapters without the guard ignore the field.
   */
  abstract persistWorkflowSnapshot(_: {
    workflowName: string;
    runId: string;
    resourceId?: string;
    snapshot: WorkflowRunState;
    createdAt?: Date;
    updatedAt?: Date;
    expectedExecutionGeneration?: string;
    fence?: RunFence;
  }): Promise<void>;

  abstract loadWorkflowSnapshot({
    workflowName,
    runId,
  }: {
    workflowName: string;
    runId: string;
  }): Promise<WorkflowRunState | null>;

  /**
   * Load only the workflow state required for lifecycle authority checks.
   * Adapters with status/generation projections should override this method;
   * the default preserves compatibility by projecting a full snapshot read.
   */
  async getWorkflowExecutionState({
    workflowName,
    runId,
  }: {
    workflowName: string;
    runId: string;
  }): Promise<WorkflowExecutionState | null> {
    const snapshot = await this.loadWorkflowSnapshot({ workflowName, runId });
    if (!snapshot) return null;
    return {
      status: snapshot.status,
      ...(snapshot.executionGeneration === undefined ? {} : { executionGeneration: snapshot.executionGeneration }),
      ...(snapshot.cancelRequest === undefined ? {} : { cancelRequest: snapshot.cancelRequest }),
    };
  }

  abstract listWorkflowRuns(args?: StorageListWorkflowRunsInput): Promise<WorkflowRuns>;

  abstract getWorkflowRunById(args: { runId: string; workflowName?: string }): Promise<WorkflowRun | null>;

  abstract deleteWorkflowRunById(args: { runId: string; workflowName: string; fence?: RunFence }): Promise<void>;
}
