export async function listTranscripts(
  feedId: string,
  token: string,
  limit?: number,
  nextToken?: string,
  startTime?: number,
  endTime?: number,
  order?: 'asc' | 'desc'
): Promise<{ transcripts: Transcript[]; nextToken?: string }> {
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
