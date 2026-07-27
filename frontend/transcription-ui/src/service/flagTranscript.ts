import { type Annotation } from '@transcription/common';

import { apiFetch } from '../utils/apiUtils';

export async function flagTranscript(
  segmentId: string,
  flaggedByUserIds: string[],
  token: string
): Promise<Annotation> {
  return apiFetch<Annotation>(
    `${import.meta.env.VITE_API_BASE_URL}/api/v1/audioSegments/${segmentId}/flag-transcript`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify({ flaggedByUserIds }),
    }
  );
}
