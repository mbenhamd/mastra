import type { GetObservabilityCapabilitiesResponse, GetSystemPackagesResponse } from '@mastra/client-js';

export const renamedPostgresWithMetrics: GetObservabilityCapabilitiesResponse = {
  observabilityStorageType: '_ObservabilityStoragePostgresVNext',
  capabilities: {
    metrics: true,
    logs: true,
    traceQueryDiscovery: false,
  },
};

export const storageWithoutMetrics: GetObservabilityCapabilitiesResponse = {
  observabilityStorageType: 'ObservabilityStoragePostgresVNext',
  capabilities: {
    metrics: false,
    logs: true,
    traceQueryDiscovery: false,
  },
};

export const inMemoryStorage: GetObservabilityCapabilitiesResponse = {
  observabilityStorageType: 'ObservabilityInMemory',
  capabilities: {
    metrics: true,
    logs: true,
    traceQueryDiscovery: false,
  },
};

// PF-268: per-API runtime capabilities reported through the system packages response.
const baseSystemPackages: GetSystemPackagesResponse = {
  packages: [],
  isDev: false,
  cmsEnabled: false,
  observabilityEnabled: true,
};

const makeObservabilityRuntimeCapabilities = (
  supportsMetrics: boolean,
  persistence: 'persistent' | 'memory' = 'persistent',
) => ({
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
  persistence,
});

export const packagesWithRuntimeMetrics: GetSystemPackagesResponse = {
  ...baseSystemPackages,
  observabilityStorageType: '_ObservabilityStoragePostgresVNext',
  observabilityRuntimeCapabilities: makeObservabilityRuntimeCapabilities(true),
};

export const packagesWithoutRuntimeMetrics: GetSystemPackagesResponse = {
  ...baseSystemPackages,
  observabilityStorageType: 'ObservabilityStoragePostgresVNext',
  observabilityRuntimeCapabilities: makeObservabilityRuntimeCapabilities(false),
};

export const packagesWithInMemoryRuntime: GetSystemPackagesResponse = {
  ...baseSystemPackages,
  observabilityStorageType: '_RenamedInMemory',
  observabilityRuntimeCapabilities: makeObservabilityRuntimeCapabilities(true, 'memory'),
};
