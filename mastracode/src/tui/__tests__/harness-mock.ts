import { vi } from 'vitest';
import type { MastraCodeHarnessRuntime } from '../../harness/runtime.js';

/** Shared direct-runtime test double for TUI tests. */
type AnyRecord = Record<string, any>;

function deepMerge<T extends AnyRecord>(base: T, overrides?: AnyRecord): T {
  if (!overrides) return base;
  const result: AnyRecord = { ...base };
  for (const [key, value] of Object.entries(overrides)) {
    const existing = result[key];
    if (
      existing &&
      typeof existing === 'object' &&
      !Array.isArray(existing) &&
      value &&
      typeof value === 'object' &&
      !Array.isArray(value) &&
      typeof value !== 'function'
    ) {
      result[key] = deepMerge(existing, value);
    } else {
      result[key] = value;
    }
  }
  return result as T;
}

export interface MockHarnessOptions {
  id?: string;
  resourceId?: string;
  threadId?: string | null;
  /** Deep-merged onto the direct MastraCode runtime mock. */
  harness?: AnyRecord;
}

export function createMockHarness(opts: MockHarnessOptions = {}) {
  let resourceId = opts.resourceId ?? opts.id ?? 'test-harness';
  const defaultResourceId = resourceId;
  let currentThreadId: string | null = opts.threadId ?? null;
  let state: AnyRecord = {};
  let displayState: AnyRecord = { isRunning: false, pendingSuspension: null };

  const base = {
    getResourceId: vi.fn(() => resourceId),
    getDefaultResourceId: vi.fn(() => defaultResourceId),
    setResourceId: vi.fn(({ resourceId: nextResourceId }: { resourceId: string }) => {
      resourceId = nextResourceId;
      currentThreadId = null;
    }),
    getKnownResourceIds: vi.fn(async () => []),

    getCurrentThreadId: vi.fn(() => currentThreadId),
    listThreads: vi.fn(async () => []),
    listMessages: vi.fn(async () => []),
    listMessagesForThread: vi.fn(async () => []),
    getFirstUserMessagesForThreads: vi.fn(async () => new Map()),
    getFirstUserMessageForThread: vi.fn(async () => null),
    setThreadSetting: vi.fn(async () => {}),
    createThread: vi.fn(async () => {
      currentThreadId = 'thread-new';
      return { id: currentThreadId, resourceId, title: 'New thread', createdAt: new Date(), updatedAt: new Date() };
    }),
    switchThread: vi.fn(async ({ threadId }: { threadId: string }) => {
      currentThreadId = threadId;
    }),
    cloneThread: vi.fn(async () => ({ id: 'thread-clone', resourceId })),
    renameThread: vi.fn(async () => {}),
    detachFromCurrentThread: vi.fn(() => {
      currentThreadId = null;
    }),

    isRunning: vi.fn(() => false),
    isCurrentThreadStreamActive: vi.fn(() => false),
    getCurrentRunId: vi.fn(() => null),
    getCurrentTraceId: vi.fn(() => null),
    getFollowUpCount: vi.fn(() => 0),

    getCurrentModelId: vi.fn(() => 'anthropic/claude-sonnet-4-5'),
    getModelName: vi.fn(() => 'claude-sonnet-4-5'),
    getFullModelId: vi.fn(() => 'anthropic/claude-sonnet-4-5'),
    hasModelSelected: vi.fn(() => true),
    getCurrentModelAuthStatus: vi.fn(async () => ({ hasAuth: true, apiKeyEnvVar: undefined })),
    listAvailableModels: vi.fn(async () => []),
    switchModel: vi.fn(async () => {}),

    listModes: vi.fn(() => []),
    getCurrentModeId: vi.fn(() => 'build'),
    getCurrentMode: vi.fn(() => ({ id: 'build', defaultModelId: undefined })),
    switchMode: vi.fn(async () => {}),

    getState: vi.fn(() => state),
    setState: vi.fn(async (updates: AnyRecord) => {
      state = { ...state, ...updates };
    }),
    getDisplayState: vi.fn(() => displayState),
    restoreDisplayTasks: vi.fn(),
    subscribe: vi.fn(() => () => {}),

    sendMessage: vi.fn(async () => {}),
    sendSignal: vi.fn(() => ({
      id: 'signal-1',
      accepted: Promise.resolve({ accepted: true as const, runId: 'run-1' }),
    })),
    steer: vi.fn(async () => {}),
    followUp: vi.fn(async () => {}),
    abort: vi.fn(),
    respondToQuestion: vi.fn(),
    respondToToolApproval: vi.fn(),
    respondToToolSuspension: vi.fn(async () => {}),
    respondToSandboxAccess: vi.fn(async () => {}),
    respondToPlanApproval: vi.fn(async () => {}),
    saveSystemReminderMessage: vi.fn(async () => null),

    getWorkspace: vi.fn(() => undefined),
    hasWorkspace: vi.fn(() => false),
    resolveWorkspace: vi.fn(async () => undefined),
    getKnownResources: vi.fn(async () => []),
    loadOMProgress: vi.fn(async () => {}),
    getObservationalMemoryRecord: vi.fn(async () => null),
    getSessionGrants: vi.fn(() => ({ categories: [], tools: [] })),
    getPermissionRules: vi.fn(() => ({ categories: {}, tools: {} })),
    setPermissionForCategory: vi.fn(),
    setPermissionForTool: vi.fn(),
    grantSessionCategory: vi.fn(),
    grantSessionTool: vi.fn(),

    // Test-only setters for mutable snapshots; production callers never see these.
    _setState: (next: AnyRecord) => {
      state = next;
    },
    _setDisplayState: (next: AnyRecord) => {
      displayState = next;
    },
  };

  return deepMerge(base, opts.harness) as unknown as MastraCodeHarnessRuntime<Record<string, unknown>> & typeof base;
}

export function createMockState(opts: MockHarnessOptions & { extra?: AnyRecord } = {}) {
  const { extra, ...harnessOpts } = opts;
  return {
    harness: createMockHarness(harnessOpts),
    ...extra,
  };
}
