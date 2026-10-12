import { coreFeatures } from '@mastra/core/features';
import { MockStore, ObservabilityStorage } from '@mastra/core/storage';
import { describe, expect, it } from 'vitest';
import { createTestSuite } from './factory';
import { createMastraStorageCompositionTests } from './composite-tests';
import { createObservabilityVNextTests } from './domains/observability-vnext';

// Test InMemoryStore (MockStore)
createTestSuite(new MockStore(), { scopedTraceDeletion: true });

// Test MastraStorage composition with InMemoryStore backing
createMastraStorageCompositionTests();

// Test the shared observability vNext suite against the in-memory adapter.
// Each test gets a fresh store so delta cursors / feature-flag state don't
// leak between tests.
createObservabilityVNextTests({
  capabilities: {
    label: 'InMemoryStore',
    preferredStrategy: 'batch-with-updates',
  },
  getStorage: async () => {
    const store = new MockStore();
    return (await store.getStore('observability')) as ObservabilityStorage;
  },
});

// Optional-feature contract: a store that predates feature declarations omits
// getFeatures() (the base default), and consumers must read an omitted list as
// an empty set. The in-memory adapter declares its features explicitly, and
// declares delta polling only while its flag is on. Pinned here so
// shared-suite helpers that normalize `getFeatures() ?? []` stay exercised by
// an executable test file in this package.
describe('observability vNext optional-feature contract', () => {
  const deltaPollingFlag = 'observability-delta-polling';

  async function observabilityStorage() {
    const store = new MockStore();
    return (await store.getStore('observability')) as ObservabilityStorage;
  }

  it('reads an omitted getFeatures() result as an empty optional-feature set', async () => {
    // The base domain is what a store without feature declarations inherits.
    const legacy = new ObservabilityStorage();
    expect(legacy.getFeatures()).toBeUndefined();
    expect(legacy.getFeatures() ?? []).toEqual([]);
  });

  it('leaves delta polling out of the declared features while its flag is off', async () => {
    const storage = await observabilityStorage();
    const wasEnabled = coreFeatures.has(deltaPollingFlag);
    coreFeatures.delete(deltaPollingFlag);
    try {
      expect(storage.getFeatures() ?? []).not.toContain('delta-polling');
    } finally {
      if (wasEnabled) coreFeatures.add(deltaPollingFlag);
    }
  });

  it('reports enabled optional features explicitly', async () => {
    const storage = await observabilityStorage();
    const wasEnabled = coreFeatures.has(deltaPollingFlag);
    coreFeatures.add(deltaPollingFlag);
    try {
      expect(await storage.getFeatures()).toContain('delta-polling');
    } finally {
      if (!wasEnabled) coreFeatures.delete(deltaPollingFlag);
    }
  });
});
