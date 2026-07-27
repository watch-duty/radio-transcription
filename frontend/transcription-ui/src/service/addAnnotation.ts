import { type Annotation, AnnotationType } from '@transcription/common';

import { apiFetch } from '../utils/apiUtils';

export async function addTranscriptFeedback(
  segmentId: string,
  flaggedByUserIds: string[],
  token: string
): Promise<Annotation> {
  return apiFetch<Annotation>(
    `${import.meta.env.VITE_API_BASE_URL}/api/v1/audioSegments/${segmentId}/annotations`,
    {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        Authorization: `Bearer ${token}`,
      },
      body: JSON.stringify({
        type: AnnotationType.TRANSCRIPT_FLAG,
        data: { flaggedByUserIds },
      }),
    }
  );
}
