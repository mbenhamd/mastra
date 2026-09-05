import { useMastraClient } from '@mastra/react';
import { useQuery } from '@tanstack/react-query';
import { DISCOVERY_STALE_TIME } from './discovery-cache';

type UseServiceNamesOptions = {
  enabled?: boolean;
};

export const useServiceNames = ({ enabled = true }: UseServiceNamesOptions = {}) => {
  const client = useMastraClient();

  return useQuery({
    queryKey: ['observability-service-names'],
    queryFn: async () => {
      try {
        return await client.getServiceNames();
      } catch {
        return { serviceNames: [] };
      }
    },
    select: data => data?.serviceNames ?? [],
    retry: false,
    enabled,
    staleTime: DISCOVERY_STALE_TIME,
  });
};
