import { beforeEach, describe, expect, it, vi } from 'vitest';

const runtimeConstructorMock = vi.fn();
const createStorageMock = vi.fn((): { storage: unknown; backend?: string } => ({ storage: {} }));
const createVectorStoreMock = vi.fn(() => ({}));
const syncGatewaysMock = vi.fn();

vi.mock('@mastra/core/agent', () => ({
  Agent: class {},
}));

vi.mock('@mastra/core/harness', () => ({
  Harness: class {
    constructor(config: unknown) {
      harnessConstructorMock(config);
    }
    subscribe() {}
  },
}));

vi.mock('@mastra/core/llm', () => ({
  GatewayRegistry: {
    getInstance: vi.fn(() => ({
      syncGateways: syncGatewaysMock,
      getProviders: vi.fn(() => ({})),
    })),
  },
  PROVIDER_REGISTRY: {},
}));

vi.mock('@mastra/core/processors', () => ({
  AgentsMDInjector: class {},
  PrefillErrorHandler: class {},
  ProviderHistoryCompat: class {},
  StreamErrorRetryProcessor: class {},
}));

vi.mock('@mastra/core/storage', () => ({
  InMemoryDB: class {},
  InMemoryHarness: class {},
  MastraCompositeStore: class {
    constructor(readonly config: unknown) {}
  },
}));

vi.mock('@mastra/duckdb', () => ({
  DuckDBStore: class {},
}));

vi.mock('@mastra/observability', () => ({
  Observability: class {
    constructor(readonly config: unknown) {}
  },
  MastraStorageExporter: class {
    constructor(readonly config: unknown) {}
  },
  MastraPlatformExporter: class {
    constructor(readonly config: unknown) {}
  },
  SensitiveDataFilter: class {},
}));

vi.mock('../agents/instructions.js', () => ({ getDynamicInstructions: vi.fn() }));
vi.mock('../agents/memory.js', () => ({ getDynamicMemory: vi.fn(() => vi.fn()) }));
vi.mock('../agents/model.js', () => ({
  createMastraCodeGateway: vi.fn(() => ({})),
  getDynamicModel: vi.fn(),
  getGoalJudgeModel: vi.fn(),
  resolveModel: vi.fn(),
}));
vi.mock('../agents/prompts/agent-instructions.js', () => ({ getStaticallyLoadedInstructionPaths: vi.fn(() => []) }));
vi.mock('../agents/subagents/execute.js', () => ({ executeSubagent: { id: 'execute' } }));
vi.mock('../agents/subagents/explore.js', () => ({ exploreSubagent: { id: 'explore' } }));
vi.mock('../agents/subagents/plan.js', () => ({ planSubagent: { id: 'plan' } }));
vi.mock('../agents/thread-caveman-state.js', () => ({
  attachOMThreadStatePersistence: vi.fn(),
  restoreOMThreadStateForCurrentThread: vi.fn(() => Promise.resolve()),
}));
vi.mock('../agents/tools.js', () => ({
  createDynamicTools: vi.fn(() => ({})),
  createToolHooks: vi.fn(() => ({})),
}));
vi.mock('../agents/workspace.js', () => ({
  getDynamicWorkspace: vi.fn(),
  getGoalJudgeTools: vi.fn(),
}));
vi.mock('../auth/storage.js', () => ({
  AuthStorage: class {
    get() {
      return undefined;
    }
    getStoredApiKey() {
      return undefined;
    }
    hasStoredApiKey() {
      return false;
    }
    isLoggedIn() {
      return false;
    }
    loadStoredApiKeysIntoEnv() {}
  },
}));
vi.mock('../evals/scorers/index.js', () => ({
  createOutcomeScorer: vi.fn(() => ({})),
  createEfficiencyScorer: vi.fn(() => ({})),
}));
vi.mock('../hooks/index.js', () => ({
  HookManager: class {
    setSessionId() {}
  },
}));
vi.mock('../harness/index.js', () => ({
  createHarnessV1SubagentAgents: vi.fn(() => ({})),
  MASTRACODE_HARNESS_NAME: 'mastra-code',
  MastraCodeHarnessRuntime: class {
    constructor(config: unknown) {
      runtimeConstructorMock(config);
    }
    subscribe() {}
  },
}));
vi.mock('../mcp/index.js', () => ({ createMcpManager: vi.fn() }));
vi.mock('../onboarding/packs.js', () => ({
  getAvailableModePacks: vi.fn(() => []),
  getAvailableOmPacks: vi.fn(() => []),
}));
vi.mock('../onboarding/settings.js', () => ({
  getCustomProviderId: vi.fn(),
  loadSettings: vi.fn(() => ({
    models: {
      modeDefaults: {},
      omObservationThreshold: null,
      omReflectionThreshold: null,
      omCavemanObservations: null,
      subagentModels: {},
    },
    preferences: { yolo: null, thinkingLevel: 'off' },
    storage: { backend: 'libsql', libsql: {}, pg: {} },
    customProviders: [],
    modelUseCounts: {},
    memoryGateway: {},
    observability: { resources: {}, localTracing: false },
  })),
  MEMORY_GATEWAY_PROVIDER: 'mastra',
  OBSERVABILITY_AUTH_PREFIX: 'observability:',
  resolveModelDefaults: vi.fn(() => ({})),
  resolveOmRoleModel: vi.fn(() => undefined),
  saveSettings: vi.fn(),
  toCustomProviderModelId: vi.fn(),
}));
vi.mock('../permissions.js', () => ({ getToolCategory: vi.fn() }));
vi.mock('../providers/claude-max.js', () => ({ setAuthStorage: vi.fn() }));
vi.mock('../providers/github-copilot.js', () => ({
  getCopilotModelCatalog: vi.fn(() => Promise.resolve([])),
  setAuthStorage: vi.fn(),
}));
vi.mock('../providers/openai-codex.js', () => ({ setAuthStorage: vi.fn() }));
vi.mock('../schema.js', () => ({ stateSchema: {} }));
vi.mock('../tui/theme.js', () => ({ mastra: { green: 'green', purple: 'purple', orange: 'orange' } }));
vi.mock('../utils/gateway-sync.js', () => ({ syncGateways: syncGatewaysMock }));
vi.mock('../utils/project.js', () => ({
  detectProject: vi.fn(() => ({
    rootPath: process.cwd(),
    resourceId: 'resource-1',
    name: 'project',
    gitBranch: 'main',
  })),
  getObservabilityDatabasePath: vi.fn(() => ':memory:'),
  getStorageConfig: vi.fn(() => ({ type: 'memory' })),
  getResourceIdOverride: vi.fn(() => undefined),
}));
vi.mock('../utils/storage-factory.js', () => ({
  createStorage: createStorageMock,
  createVectorStore: createVectorStoreMock,
}));
describe('createMastraCode cross-process signal startup config', () => {
  beforeEach(() => {
    vi.resetModules();
    runtimeConstructorMock.mockReset();
    createStorageMock.mockClear();
    createVectorStoreMock.mockClear();
    syncGatewaysMock.mockClear();
  });

  it('leaves signal routing unset when cross-process PubSub mode is disabled', async () => {
    const { createMastraCode } = await import('../index.js');

    const result = await createMastraCode();

    expect(runtimeConstructorMock).toHaveBeenCalled();
    expect(result.signalsPubSub).toBeUndefined();
  });

  it('rejects cross-process PubSub mode before storage or runtime setup when PubSub is missing', async () => {
    const { createMastraCode } = await import('../index.js');

    await expect(createMastraCode({ crossProcessPubSub: true })).rejects.toThrow('requires a pubsub instance');

    expect(createStorageMock).not.toHaveBeenCalled();
    expect(runtimeConstructorMock).not.toHaveBeenCalled();
  });

  it('uses configured PubSub for cross-process signal routing', async () => {
    const pubsub = { id: 'shared-pubsub' };
    const { createMastraCode } = await import('../index.js');

    const result = await createMastraCode({ pubsub: pubsub as never, crossProcessPubSub: true });

    expect(runtimeConstructorMock).toHaveBeenCalled();
    expect(result.signalsPubSub).toBe(pubsub);
  });
});
