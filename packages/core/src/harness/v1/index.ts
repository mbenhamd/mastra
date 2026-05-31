/**
 * Harness v1 — public entry point.
 *
 * Exported as `@mastra/core/harness/v1`. See HARNESS_V1_SPEC.md.
 *
 * The legacy Harness lives at `@mastra/core/harness` and remains the
 * default through v1.0. See §11 for the migration story.
 */

export { Harness, createHarnessOperatorThreadController } from './harness';
export type { HarnessOperatorThreadController } from './harness';
export { Session } from './session';
export type { SessionLifecycleState, SessionDisplayState } from './session';
export { toHarnessDisplayStateSnapshotV1 } from './display-state';
export type {
  HarnessDisplayActiveSubagentSnapshotV1,
  HarnessDisplayActiveToolSnapshotV1,
  HarnessDisplayJsonValue,
  HarnessDisplayPendingSnapshotV1,
  HarnessDisplayStateSnapshotV1,
  HarnessDisplayToolInputBufferSnapshotV1,
} from './display-state';

export type {
  AgentEndEvent,
  AgentStartEvent,
  CustomEvent as HarnessCustomEvent,
  GoalClearedEvent,
  GoalDoneEvent,
  GoalJudgedEvent,
  GoalPausedEvent,
  GoalResumedEvent,
  GoalSetEvent,
  HarnessEvent,
  HarnessEventBase,
  HarnessEventListener,
  HarnessEventUnsubscribe,
  ModeChangedEvent,
  ModelChangedEvent,
  PermissionGrantedEvent,
  PermissionPolicyChangedEvent,
  PermissionRevokedEvent,
  ToolApprovalRequiredEvent,
  ToolSuspensionRequiredEvent,
  QuestionPendingEvent,
  PlanApprovalRequiredEvent,
  SessionClosedEvent,
  SessionClosingEvent,
  AttachmentUploadedEvent,
  AttachmentDeletedEvent,
  StorageErrorEvent,
  SessionCreatedEvent,
  SessionEvictedEvent,
  SessionHydratedEvent,
  HarnessShutdownEvent,
  StateChangedEvent,
  TurnErrorEvent,
  TokenUsageChangedEvent,
  TextDeltaEvent,
  ToolEndEvent,
  ToolStartEvent,
} from './events';

export { HARNESS_EVENT_ID_PREFIX, formatHarnessEventId, parseHarnessEventId } from './events';
export { getHarnessWorkspaceActionPathInput, isHarnessWorkspaceFileMutationTool } from './workspace-actions';

export {
  HarnessAbortedError,
  type HarnessAbortReason,
  HarnessAttachmentInUseError,
  HarnessAttachmentUnavailableError,
  HarnessBusyError,
  HarnessConfigError,
  HarnessAdmissionConflictError,
  HarnessInboxItemNotFoundError,
  HarnessInboxResponseConflictError,
  HarnessLiveSessionLimitError,
  HarnessOutputGenerationError,
  type HarnessOutputGenerationReason,
  HarnessQueueItemExpiredError,
  HarnessQueueFullDroppedError,
  HarnessQueueFullError,
  HarnessRuntimeDriftError,
  HarnessSessionCancelledError,
  HarnessSessionClosedError,
  HarnessSessionClosingError,
  HarnessSessionConflictError,
  HarnessSessionCorruptError,
  HarnessSessionDeleteBlockedError,
  type HarnessSessionDeleteBlocker,
  HarnessSessionDeletedError,
  HarnessSessionLockedError,
  HarnessSessionNotFoundError,
  HarnessSkillNotFoundError,
  HarnessStateConflictError,
  HarnessStateSerializationError,
  HarnessSkillArgsValidationError,
  HarnessStorageError,
  type HarnessStorageOperation,
  type HarnessStorageSubject,
  HarnessSubagentDepthExceededError,
  HarnessThreadNotFoundError,
  HarnessValidationError,
  HarnessResourceWorkspaceInUseError,
  HarnessWorkspaceLostError,
  HarnessWorkspaceProviderMismatchError,
  HarnessWorkspaceProvisioningError,
} from './errors';

export { nonDurableProvider } from './workspace-provider';
export type { WorkspaceOwnershipKind, WorkspaceProvider, WorkspaceProviderContext } from './workspace-provider';
export { evaluateWorkspacePolicy, resolveWorkspacePath } from './workspace-policy';
export type {
  WorkspaceCommandPolicyAction,
  WorkspaceFileOperation,
  WorkspaceFilePolicyAction,
  WorkspaceMcpPolicyAction,
  WorkspaceNetworkPolicyAction,
  WorkspacePolicy,
  WorkspacePolicyAction,
  WorkspacePolicyActionKind,
  WorkspacePolicyEvaluation,
  WorkspacePolicyMatchedRule,
  WorkspacePolicyRule,
  WorkspaceResolvedPath,
  WorkspaceRootDescriptor,
} from './workspace-policy';
export { createWorkspaceRestorePlan, workspaceRestoreEntryMatchesScope } from './workspace-restore';
export type {
  CreateWorkspaceRestorePlanOptions,
  WorkspaceRestoreAffectedPath,
  WorkspaceRestoreConflict,
  WorkspaceRestoreConflictStatus,
  WorkspaceRestorePlan,
  WorkspaceRestorePlanStep,
  WorkspaceRestoreScope,
  WorkspaceRestoreStepKind,
  WorkspaceRestoreStepStatus,
} from './workspace-restore';

export type {
  HarnessAdmissionEvidence,
  HarnessEvidence,
  HarnessEvidenceKind,
  HarnessRun,
  HarnessRunFinishReason,
  HarnessTask,
  HarnessTaskIndexEntry,
  HarnessTaskOrigin,
  HarnessTaskStatus,
  PendingInteraction,
  TaskIdFieldMapping,
} from './contracts';

/**
 * `HarnessMessage` and `HarnessMessageContent` are stable cross-version
 * interfaces (spec §11.1). They are re-exported from v1 and back the same
 * underlying definitions used by the legacy `Harness`, so renderers can
 * import from either entry point and consume the same shape.
 */
export type {
  HarnessMessage,
  HarnessMessageContent,
  HarnessMessageContent as HarnessMessageContentPart,
} from '../types';

/**
 * Goal-loop primitive types (§4.7). `GoalState` lives in `SessionRecord.goal`
 * and is returned by `Session.getGoal()` / `setGoal(...)`. `GoalJudgeDecision`
 * captures one judge verdict.
 */
export type { GoalJudgeDecision, GoalState } from '../../storage/domains/harness';

export type {
  AttachmentDeleteOptions,
  AttachmentRef,
  AttachmentUploadOptions,
  ElementAttachmentUploadOptions,
  FileAttachmentUploadOptions,
  ChannelActionEnvelope,
  ChannelActorContext,
  ChannelConversationKind,
  ChannelIngressEnvelope,
  ChannelIngressTrigger,
  GoalOptions,
  HarnessActionCatalogEntry,
  HarnessActionCatalogEntryStatus,
  HarnessActionCatalogListOptions,
  HarnessActionCatalogMcpServerSource,
  HarnessActionCatalogMcpToolSource,
  HarnessActionCatalogSkillSource,
  HarnessActionCatalogSource,
  HarnessActionCatalogSourceKind,
  HarnessActionCatalogUnavailableReason,
  HarnessChannelAdapter,
  HarnessChannelBinding,
  HarnessChannelConfig,
  HarnessChannelDiagnostics,
  HarnessChannelDiagnosticsOptions,
  HarnessChannelDeliveryContext,
  HarnessChannelDiagnosticError,
  HarnessChannelDiagnosticLease,
  HarnessChannelActionReceiptDiagnostic,
  HarnessChannelActionTokenDiagnostic,
  HarnessChannelInboxDiagnostic,
  HarnessChannelOutboxDiagnostic,
  ChannelOutboxDispatchOptions,
  ChannelOutboxDispatchResult,
  HarnessChannelInboundResult,
  HarnessChannelRouteContext,
  HarnessChannelTransportRequest,
  HarnessConfig,
  HarnessMcpServerDescriptor,
  HarnessMcpToolDescriptor,
  HarnessMode,
  HarnessQueueBackpressurePolicy,
  HarnessSkill,
  HarnessSkillActionMetadata,
  HarnessSkillActionPermissionHints,
  HarnessSkillActionShortcut,
  InboxResponseOptions,
  InboxResponseResult,
  PrimitiveAttachmentUploadOptions,
  UseSkillOptions,
  HarnessWorkspaceConfig,
  ListMessagesOptions,
  MessageAdmissionResult,
  PermissionPolicy,
  QueueAdmissionResult,
  RegisterSandboxAccessParams,
  SessionDeleteOptions,
  SessionListOptions,
  SessionLoadByIdOptions,
  SessionRecord,
  SessionResolveById,
  SessionResolveByIdScoped,
  SessionResolveByThread,
  SessionResolveOptions,
  SetStateOptions,
  ShutdownOptions,
  SubagentDefinition,
  ThreadCloneOptions,
  ThreadCreateOptions,
  ThreadDeleteOptions,
  ThreadGetOptions,
  ThreadListOptions,
  ThreadListResult,
  ThreadRecord,
  ThreadRenameOptions,
  ToolCategory,
} from './types';

export type {
  AttachmentObjectPointer,
  AttachmentRendererDescriptor,
  ChannelDeliverySemantics,
  ChannelOutboxEnqueueOptions,
  ChannelOutboxItem,
  ChannelOutboxKind,
  ChannelOutboxOperationKind,
  ChannelOutboxSource,
  ChannelOutboxTarget,
  ChannelProviderDeliveryReceipt,
  HarnessAttachmentKind,
  HarnessPrimitiveType,
  InboxResponseReceipt,
  JsonValue,
  PermissionRules,
  SessionGrants,
} from '../../storage/domains/harness';
