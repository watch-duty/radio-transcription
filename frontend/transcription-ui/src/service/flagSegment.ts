import { type Annotation } from '@transcription/common';

import { apiFetch } from '../utils/apiUtils';

export async function flagSegment(
  segmentId: string,
  isFlagged: boolean,
  token: string
): Promise<Annotation> {
  return apiFetch<Annotation>(
    `${import.meta.env.VITE_API_BASE_URL}/api/v1/audioSegments/${segmentId}/flagSegment`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify({ isFlagged }),
    }
  );
}
