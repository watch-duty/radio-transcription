import type { AudioSegment } from '@transcription/common';

import { apiFetch } from '../utils/apiUtils';

export async function listAudioSegments(
  feedId: string,
  token: string,
  limit?: number,
  nextToken?: string,
  startTime?: number | string,
  endTime?: number | string,
  order?: 'asc' | 'desc',
  isAlert?: boolean
): Promise<{ segments: AudioSegment[]; nextToken: string | undefined }> {
  let url = `${import.meta.env.VITE_API_BASE_URL}/api/v1/audioSegments/${feedId}`;
  const params = new URLSearchParams();
  if (limit) params.append('limit', limit.toString());
  if (nextToken) params.append('nextToken', nextToken);
  if (startTime) {
    const startStr =
      typeof startTime === 'number'
        ? new Date(startTime).toISOString()
        : startTime;
    params.append('startTime', startStr);
  }
  if (endTime) {
    const endStr =
      typeof endTime === 'number' ? new Date(endTime).toISOString() : endTime;
    params.append('endTime', endStr);
  }
  if (order) params.append('order', order);
  // Can be true/false, just not undefined.
  if (isAlert !== undefined) params.append('isAlert', isAlert.toString());
  if (params.toString()) {
    url += `?${params.toString()}`;
  }

  const data = await apiFetch<{
    segments: AudioSegment[];
    nextToken: string | undefined;
  }>(url, {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  });

  return {
    segments: data.segments || [],
    nextToken: data.nextToken,
  };
}
