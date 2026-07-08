import type { ListFeedHistoryResponse } from '@transcription/common';

import { apiFetch } from '../utils/apiUtils';

export async function listFeedHistory(
  feedId: string,
  token: string,
  limit?: number,
  nextToken?: string
): Promise<ListFeedHistoryResponse> {
  const queryParams = new URLSearchParams();
  if (limit !== undefined) {
    queryParams.append('limit', limit.toString());
  }
  if (nextToken) {
    queryParams.append('nextToken', nextToken);
  }
  const queryString = queryParams.toString();
  const url = `${import.meta.env.VITE_API_BASE_URL}/api/v1/feeds/${feedId}/history${queryString ? `?${queryString}` : ''}`;
  return apiFetch<ListFeedHistoryResponse>(url, {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  });
}
