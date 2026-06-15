import type { HistogramBucket } from '@transcription/common';

import { apiFetch } from '../utils/apiUtils';

export async function getAudioSegmentHistogram(
  feedId: string,
  token: string,
  startTime: number | string,
  endTime: number | string,
  buckets?: number,
  isAlert?: boolean
): Promise<{ buckets: HistogramBucket[] }> {
  let url = `${import.meta.env.VITE_API_BASE_URL}/api/v1/audioSegments/${feedId}/histogram`;
  const params = new URLSearchParams();
  const toIso = (t: number | string) =>
    typeof t === 'number' ? new Date(t).toISOString() : t;
  params.append('startTime', toIso(startTime));
  params.append('endTime', toIso(endTime));
  if (buckets) params.append('buckets', buckets.toString());
  // Can be true/false, just not undefined.
  if (isAlert !== undefined) params.append('isAlert', isAlert.toString());
  url += `?${params.toString()}`;

  const data = await apiFetch<{ buckets: HistogramBucket[] }>(url, {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  });

  return {
    buckets: data.buckets || [],
  };
}
