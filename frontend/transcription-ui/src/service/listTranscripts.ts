import type { Transcript } from '@transcription/common';

export async function listTranscripts(
  feedId: string,
  token: string,
  limit?: number,
  nextToken?: string,
  startTimestamp?: number,
  endTimestamp?: number
): Promise<{ transcripts: Transcript[]; nextToken?: string }> {
  let url = `${import.meta.env.VITE_API_BASE_URL}/api/v1/transcripts/${feedId}`;
  const params = new URLSearchParams();
  if (limit) params.append('limit', limit.toString());
  if (nextToken) params.append('nextToken', nextToken);
  if (startTimestamp) params.append('startTimestamp', startTimestamp.toString());
  if (endTimestamp) params.append('endTimestamp', endTimestamp.toString());
  if (params.toString()) {
    url += `?${params.toString()}`;
  }

  const response = await fetch(url, {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  });

  if (!response.ok) {
    throw new Error(`Error: ${response.status} ${response.statusText}`);
  }

  const data = await response.json();
  return {
    transcripts: data.transcripts || [],
    nextToken: data.nextToken,
  };
}
