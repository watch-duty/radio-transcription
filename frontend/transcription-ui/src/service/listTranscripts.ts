export interface Transcript {
  transmissionId: string;
  transcript: string;
  startTimestamp: string;
  canonicalAudioUri: string;
}

export async function listTranscripts(
  feedId: string,
  token: string
): Promise<Transcript[]> {
  const response = await fetch(
    `${import.meta.env.VITE_API_BASE_URL}/api/v1/transcripts/${feedId}`,
    {
      headers: {
        Authorization: `Bearer ${token}`,
      },
    }
  );

  if (!response.ok) {
    throw new Error(`Error: ${response.status} ${response.statusText}`);
  }

  const data = await response.json();
  return data.transcripts || [];
}
