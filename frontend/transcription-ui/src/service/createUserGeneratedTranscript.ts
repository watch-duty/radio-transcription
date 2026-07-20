import { type Annotation, AnnotationType } from '@transcription/common';

import { apiFetch } from '../utils/apiUtils';

export async function createUserGeneratedTranscript(
  segmentId: string,
  token: string,
  text: string
): Promise<Annotation> {
  return await apiFetch<Annotation>(
    `${import.meta.env.VITE_API_BASE_URL}/api/v1/audioSegments/${segmentId}/annotations`,
    {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${token}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        type: AnnotationType.USER_GENERATED_TRANSCRIPT,
        data: {
          text: text,
          errors: [],
        },
      }),
    }
  );
}
