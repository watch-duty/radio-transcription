import type { ListFeedHistoryResponse } from '@transcription/common';

import { apiFetch } from '../utils/apiUtils';

export async function listFeedHistory(
  feedId: string,
  token: string
): Promise<ListFeedHistoryResponse> {
  const url = `${import.meta.env.VITE_API_BASE_URL}/api/v1/feeds/${feedId}/history?limit=200`;
  return apiFetch<ListFeedHistoryResponse>(url, {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  });
}
