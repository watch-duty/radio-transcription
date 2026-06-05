import type { Feed, ListFeedsResponse } from '@transcription/common';

import { apiFetch } from '../utils/apiUtils';

export async function listFeedsPage(
  token: string,
  limit?: number,
  nextToken?: string
): Promise<ListFeedsResponse> {
  const queryParams = new URLSearchParams();
  if (limit) queryParams.append('limit', limit.toString());
  if (nextToken) queryParams.append('nextToken', nextToken);

  const url = queryParams.toString()
    ? `${import.meta.env.VITE_API_BASE_URL}/api/v1/feeds?${queryParams.toString()}`
    : `${import.meta.env.VITE_API_BASE_URL}/api/v1/feeds`;

  return apiFetch<Feed[] | ListFeedsResponse>(url, {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  }).then((resp) => {
    if (Array.isArray(resp)) {
      return { feeds: resp };
    } else {
      return resp;
    }
  });
}

// TODO: Update this to allow tanstack query to handle the nextToken pagination.
// This is okay, but temporary given that when we have 10k+ feeds this will be a lot of calls.
export async function listFeeds(token: string): Promise<Feed[]> {
  let allFeeds: Feed[] = [];
  let nextToken: string | undefined = undefined;
  do {
    const response = await listFeedsPage(token, 100, nextToken);
    allFeeds = allFeeds.concat(response.feeds);
    nextToken = response.nextToken;
  } while (nextToken);
  return allFeeds;
}
