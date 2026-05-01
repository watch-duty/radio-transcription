import type { ListTranscriptsResponse } from '@transcription/common';

import { apiFetch } from '../utils/apiUtils';

export async function listTranscripts(
  feedId: string,
  token: string,
  limit?: number,
  nextToken?: string,
  startTime?: number,
  endTime?: number,
  order?: 'asc' | 'desc'
): Promise<ListTranscriptsResponse> {
  let url = `${import.meta.env.VITE_API_BASE_URL}/api/v1/transcripts/${feedId}`;
  const params = new URLSearchParams();
  if (limit) params.append('limit', limit.toString());
  if (nextToken) params.append('nextToken', nextToken);
  if (startTime) params.append('startTime', startTime.toString());
  if (endTime) params.append('endTime', endTime.toString());
  if (order) params.append('order', order);
  if (params.toString()) {
    url += `?${params.toString()}`;
  }

  const data = await apiFetch<ListTranscriptsResponse>(url, {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  });

  return {
    transcripts: data.transcripts || [],
    nextToken: data.nextToken,
  };
}
