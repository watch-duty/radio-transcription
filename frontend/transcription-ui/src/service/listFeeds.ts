import type { Feed } from '@transcription/common';

import { apiFetch } from '../utils/apiUtils';

export async function listFeeds(token: string): Promise<Feed[]> {
  return apiFetch<Feed[]>(`${import.meta.env.VITE_API_BASE_URL}/api/v1/feeds`, {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  });
}
