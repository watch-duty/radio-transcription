import { type InfiniteData, type QueryClient } from '@tanstack/react-query';
import { type AudioSegment } from '@transcription/common';

export function cacheAudioSegment(
  queryClient: QueryClient,
  segmentId: string,
  updateFn: (segment: AudioSegment) => AudioSegment
) {
  queryClient.setQueriesData(
    { queryKey: ['listAudioSegments'] },
    (
      oldData:
        | InfiniteData<{ segments: AudioSegment[]; nextToken?: string }>
        | undefined
    ) => {
      if (!oldData) return oldData;
      return {
        ...oldData,
        pages: oldData.pages.map((page) => ({
          ...page,
          segments: page.segments.map((segment) =>
            segment.id === segmentId ? updateFn(segment) : segment
          ),
        })),
      };
    }
  );
}
