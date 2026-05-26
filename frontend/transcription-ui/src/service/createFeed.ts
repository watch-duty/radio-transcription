import type { Feed, FeedCreate } from '@transcription/common';

import { apiFetch } from '../utils/apiUtils';

export async function createFeed(
  feed: FeedCreate,
  token: string
): Promise<Feed> {
  return apiFetch<Feed>(`${import.meta.env.VITE_API_BASE_URL}/api/v1/feeds`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      Authorization: `Bearer ${token}`,
    },
    body: JSON.stringify(feed),
  });
}
