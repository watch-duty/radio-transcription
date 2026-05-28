import type { Feed, FeedUpdate } from '@transcription/common';

import { apiFetch } from '../utils/apiUtils';

export async function updateFeed(
  feedId: string,
  feed: FeedUpdate,
  token: string
): Promise<Feed> {
  return apiFetch<Feed>(
    `${import.meta.env.VITE_API_BASE_URL}/api/v1/feeds/${feedId}`,
    {
      method: 'PUT',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify(feed),
    }
  );
}
