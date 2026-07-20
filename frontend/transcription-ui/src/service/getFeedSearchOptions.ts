import type { FeedSearchOptionsResponse } from '@transcription/common';

import { apiFetch } from '../utils/apiUtils';

export async function getFeedSearchOptions(
  token: string
): Promise<FeedSearchOptionsResponse> {
  return apiFetch<FeedSearchOptionsResponse>(
    `${import.meta.env.VITE_API_BASE_URL}/api/v1/feeds/search-options`,
    {
      headers: {
        Authorization: `Bearer ${token}`,
      },
    }
  );
}
