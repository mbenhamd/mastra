import { useObservabilityCapabilities } from '@mastra/react/hooks/capabilities';
import { useMastraPackages } from '@mastra/react/hooks/configuration';
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
  const { data, isLoading: isCapabilitiesLoading, error: capabilitiesError } = useObservabilityCapabilities();
  const { data: packages, isLoading: isPackagesLoading, error: packagesError } = useMastraPackages();
  const observabilityType = data?.observabilityStorageType ?? packages?.observabilityStorageType;
  // PF-268 per-API runtime capabilities (system packages) are authoritative
  // when reported; the capabilities endpoint and the legacy type list are
  // fallbacks.
  const runtimeCapabilities = packages?.observabilityRuntimeCapabilities;
  const metrics = runtimeCapabilities?.metrics;
  const storageSupportsMetrics = metrics
    ? metrics.persist === true &&
      metrics.list === true &&
      metrics.aggregate === true &&
      metrics.breakdown === true &&
      metrics.timeSeries === true &&
      metrics.percentiles === true &&
      metrics.discovery === true
    : (data?.capabilities?.metrics ??
      (observabilityType ? LEGACY_ANALYTICS_OBSERVABILITY_TYPES.has(observabilityType) : false));

  return {
    supportsMetrics: isMastraPlatform || storageSupportsMetrics,
    isInMemory:
      !isMastraPlatform &&
      (runtimeCapabilities?.persistence === 'memory' || observabilityType === 'ObservabilityInMemory'),
    isLoading: !isMastraPlatform && (isCapabilitiesLoading || isPackagesLoading),
    error: isMastraPlatform ? undefined : (packagesError ?? capabilitiesError),
  };
};
