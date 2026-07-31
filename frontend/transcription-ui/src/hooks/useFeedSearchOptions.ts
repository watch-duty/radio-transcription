import { useQuery } from '@tanstack/react-query';
import { normalizeFeedStatusOptions } from '@transcription/common';

import { getFeedSearchOptions } from '../service/getFeedSearchOptions';

export function useFeedSearchOptions(token: string | null) {
  return useQuery({
    queryKey: ['getFeedSearchOptions', token],
    queryFn: () => getFeedSearchOptions(token!),
    enabled: !!token,
    refetchOnWindowFocus: false,
    select: (data) => ({
      ...data,
      statuses: normalizeFeedStatusOptions(data.statuses),
    }),
  });
}
