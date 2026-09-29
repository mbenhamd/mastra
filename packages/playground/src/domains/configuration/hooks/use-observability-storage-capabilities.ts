import { useMastraPackages } from './use-mastra-packages';
import { useMastraPlatform } from '@/lib/mastra-platform/hooks/use-mastra-platform';

const LEGACY_ANALYTICS_OBSERVABILITY_TYPES = new Set([
  'ObservabilityStorageClickhouseVNext',
  'ObservabilityStorageDuckDB',
  'ObservabilityInMemory',
  'ObservabilitySpanner',
  'ObservabilityStoragePostgresVNext',
]);

/** Resolves metrics support, preserving lookup failures and the hosted-platform override. */
export const useObservabilityStorageCapabilities = () => {
  // On the Mastra platform, observability reads are served by the hosted
  // ClickHouse query service rather than the project's storage adapter.
  const { isMastraPlatform } = useMastraPlatform();
  const { data, error, isLoading } = useMastraPackages();
  const observabilityType = data?.observabilityStorageType;
  // PF-268 per-API runtime capabilities are authoritative when reported; the
  // endpoint-level capabilities and the legacy type list are fallbacks.
  const runtimeCapabilities = data?.observabilityRuntimeCapabilities;
  const advertisedCapabilities = data?.observabilityStorageCapabilities;
  const metrics = runtimeCapabilities?.metrics;
  const storageSupportsMetrics = metrics
    ? metrics.persist === true &&
      metrics.list === true &&
      metrics.aggregate === true &&
      metrics.breakdown === true &&
      metrics.timeSeries === true &&
      metrics.percentiles === true &&
      metrics.discovery === true
    : (advertisedCapabilities?.metrics ??
      (observabilityType ? LEGACY_ANALYTICS_OBSERVABILITY_TYPES.has(observabilityType) : false));

  return {
    supportsMetrics: isMastraPlatform || storageSupportsMetrics,
    isInMemory:
      !isMastraPlatform &&
      (runtimeCapabilities?.persistence === 'memory' || observabilityType === 'ObservabilityInMemory'),
    isLoading: !isMastraPlatform && isLoading,
    error: isMastraPlatform ? undefined : error,
  };
};
