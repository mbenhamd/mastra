import type { GetSystemPackagesResponse } from '@mastra/client-js';

const baseSystemPackages: GetSystemPackagesResponse = {
  packages: [],
  isDev: false,
  cmsEnabled: false,
  observabilityEnabled: true,
};

const makeObservabilityCapabilities = (supportsMetrics: boolean) => ({
  tracing: {
    preferredStrategy: 'insert-only' as const,
    supportedStrategies: ['insert-only' as const],
  },
  logs: {
    persist: true,
    list: true,
  },
  metrics: {
    persist: supportsMetrics,
    list: supportsMetrics,
    aggregate: supportsMetrics,
    breakdown: supportsMetrics,
    timeSeries: supportsMetrics,
    percentiles: supportsMetrics,
    discovery: supportsMetrics,
  },
  persistence: 'persistent' as const,
});

export const renamedPostgresWithMetrics: GetSystemPackagesResponse = {
  ...baseSystemPackages,
  observabilityStorageType: '_ObservabilityStoragePostgresVNext',
  observabilityStorageCapabilities: makeObservabilityCapabilities(true),
};

export const legacyPostgresWithoutCapabilities: GetSystemPackagesResponse = {
  ...baseSystemPackages,
  observabilityStorageType: 'ObservabilityStoragePostgresVNext',
};

export const storageWithoutMetrics: GetSystemPackagesResponse = {
  ...baseSystemPackages,
  observabilityStorageType: 'ObservabilityStoragePostgresVNext',
  observabilityStorageCapabilities: makeObservabilityCapabilities(false),
};
