import { useMastraPackages } from './use-mastra-packages';
import { useMastraPlatform } from '@/lib/mastra-platform/hooks/use-mastra-platform';

const LEGACY_ANALYTICS_OBSERVABILITY_TYPES = new Set([
  'ObservabilityStorageClickhouseVNext',
  'ObservabilityStorageDuckDB',
  'ObservabilityInMemory',
  'ObservabilitySpanner',
  'ObservabilityStoragePostgresVNext',
]);

export const useObservabilityStorageCapabilities = () => {
  // On the Mastra platform, observability reads are served by the hosted
  // ClickHouse query service rather than the project's storage adapter.
  const { isMastraPlatform } = useMastraPlatform();
  const { data, error, isLoading } = useMastraPackages();
  const observabilityType = data?.observabilityStorageType;
  const advertisedCapabilities = data?.observabilityStorageCapabilities;
  const metrics = advertisedCapabilities?.metrics;
  const storageSupportsMetrics = metrics
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
    supportsMetrics: isMastraPlatform || storageSupportsMetrics,
    isInMemory:
      !isMastraPlatform &&
      (advertisedCapabilities?.persistence === 'memory' || observabilityType === 'ObservabilityInMemory'),
    isLoading: !isMastraPlatform && isLoading,
    error: isMastraPlatform ? undefined : error,
  };
};
