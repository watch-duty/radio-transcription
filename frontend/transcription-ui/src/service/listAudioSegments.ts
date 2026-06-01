import type { AudioSegment } from '@transcription/common';

import { apiFetch } from '../utils/apiUtils';

export async function listAudioSegments(
  feedId: string,
  token: string,
  limit: number,
  nextToken?: string,
  startTime?: string,
  endTime?: string,
  order?: 'asc' | 'desc',
  isAlert?: boolean
): Promise<{ audioSegments: AudioSegment[]; nextToken: string | undefined }> {
  let url = `${import.meta.env.VITE_API_BASE_URL}/api/v1/audioSegments/${feedId}`;
  const params = new URLSearchParams();
  if (limit) params.append('limit', limit.toString());
  if (nextToken) params.append('nextToken', nextToken);
  if (startTime) params.append('startTime', startTime.toString());
  if (endTime) params.append('endTime', endTime.toString());
  if (order) params.append('order', order);
  // Can be true/false, just not undefined.
  if (isAlert !== undefined) params.append('isAlert', isAlert.toString());
  if (params.toString()) {
    url += `?${params.toString()}`;
  }

  const data = await apiFetch<{
    audioSegments: AudioSegment[];
    nextToken: string | undefined;
  }>(url, {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  });

  return {
    audioSegments: data.audioSegments || [],
    nextToken: data.nextToken,
  };
}
