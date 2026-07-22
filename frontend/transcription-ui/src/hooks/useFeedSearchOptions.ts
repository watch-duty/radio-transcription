import { useQuery } from '@tanstack/react-query';

import { getFeedSearchOptions } from '../service/getFeedSearchOptions';

export function useFeedSearchOptions(token: string | null) {
  return useQuery({
    queryKey: ['getFeedSearchOptions', token],
    queryFn: () => getFeedSearchOptions(token!),
    enabled: !!token,
    refetchOnWindowFocus: false,
  });
}
