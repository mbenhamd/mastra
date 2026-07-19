import { useMastraPackages } from './use-mastra-packages';

const LEGACY_ANALYTICS_OBSERVABILITY_TYPES = new Set([
  'ObservabilityStorageClickhouseVNext',
  'ObservabilityStorageDuckDB',
  'ObservabilityInMemory',
  'ObservabilitySpanner',
  'ObservabilityStoragePostgresVNext',
]);

export const useObservabilityStorageCapabilities = () => {
  const { data, error, isLoading } = useMastraPackages();
  const observabilityType = data?.observabilityStorageType;
  const advertisedCapabilities = data?.observabilityStorageCapabilities;
  const metrics = advertisedCapabilities?.metrics;
  const supportsMetrics = metrics
    ? metrics.persist === true &&
      metrics.list === true &&
      metrics.aggregate === true &&
      metrics.breakdown === true &&
      metrics.timeSeries === true &&
      metrics.percentiles === true &&
      metrics.discovery === true
    : observabilityType
      ? LEGACY_ANALYTICS_OBSERVABILITY_TYPES.has(observabilityType)
      : false;

  return {
    supportsMetrics,
    isInMemory: advertisedCapabilities?.persistence === 'memory' || observabilityType === 'ObservabilityInMemory',
    isLoading,
    error,
  };
};
