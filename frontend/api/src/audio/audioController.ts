import { AnnotationType } from '@transcription/common';
import type {
  Annotation,
  AudioClassification,
  AudioSegment,
  RuleAnnotation,
} from '@transcription/common';
import {
  Controller,
  Extension,
  Get,
  Path,
  Queries,
  Response,
  Route,
  Security,
  Tags,
} from 'tsoa';

import { AUDIO_SEGMENTS_API_URL } from '../config.js';
import { HttpError, getServiceClient, handleBackendError } from '../utils.js';

interface BaseAnnotationBackend {
  audio_segment_id: string;
  type: AnnotationType;
  created_at: string;
}

interface TranscriptAnnotationBackend extends BaseAnnotationBackend {
  type: AnnotationType.TRANSCRIPT;
  data: {
    text: string;
    errors: string[];
  };
}

interface TextMatchSpanResponse {
  start: number;
  end: number;
  matched_text: string;
}

interface RuleAnnotationResponse {
  text_match?: TextMatchSpanResponse[];
}

interface EvaluationAnnotationBackend extends BaseAnnotationBackend {
  type: AnnotationType.EVALUATION;
  data: {
    decisions: string[];
    errors: string[];
    rule_annotations?: Record<string, RuleAnnotationResponse>;
  };
}

type AnnotationBackend =
  | TranscriptAnnotationBackend
  | EvaluationAnnotationBackend;

interface AudioSegmentBackend {
  id: string;
  feed_id: string;
  classification: AudioClassification;
  start_timestamp: string;
  end_timestamp: string;
  missing_prior_context: boolean;
  missing_post_context: boolean;
  source_audio_uris: string[];
  canonical_audio_uri: string | null;
  start_audio_offset: string | null;
  end_audio_offset: string | null;
  playback_audio_uri: string | null;
  external_audio_segment_id: string | null;
  created_at: string;
  annotations?: AnnotationBackend[];
}

function convertRuleAnnotation(
  response: RuleAnnotationResponse
): RuleAnnotation {
  return {
    textMatch: response.text_match
      ? response.text_match.map((s) => ({
          startIndex: s.start,
          endIndex: s.end,
          matchedText: s.matched_text,
        }))
      : undefined,
  };
}

function convertAnnotationBackend(response: AnnotationBackend): Annotation {
  if (response.type === AnnotationType.EVALUATION) {
    return {
      type: response.type,
      createdAt: response.created_at,
      data: {
        decisions: response.data.decisions,
        errors: response.data.errors,
        ruleAnnotations: Object.fromEntries(
          Object.entries(response.data.rule_annotations ?? {}).map(
            ([ruleId, annotation]) => [
              ruleId,
              convertRuleAnnotation(annotation),
            ]
          )
        ),
      },
    };
  }

  return {
    type: response.type,
    createdAt: response.created_at,
    data: response.data,
  };
}

function convertAudioSegmentBackend(
  response: AudioSegmentBackend
): AudioSegment {
  return {
    id: response.id,
    feedId: response.feed_id,
    classification: response.classification,
    startTimestamp: response.start_timestamp,
    endTimestamp: response.end_timestamp,
    missingPriorContext: response.missing_prior_context,
    missingPostContext: response.missing_post_context,
    sourceAudioUris: response.source_audio_uris,
    canonicalAudioUri: response.canonical_audio_uri ?? undefined,
    startAudioOffset: response.start_audio_offset ?? undefined,
    endAudioOffset: response.end_audio_offset ?? undefined,
    playbackAudioUri: response.playback_audio_uri ?? undefined,
    externalAudioSegmentId: response.external_audio_segment_id ?? undefined,
    createdAt: response.created_at,
    annotations: (response.annotations || []).map(convertAnnotationBackend),
  };
}

export class ListAudioSegmentsQueryParams {
  /**
   * @isInt
   */
  limit: number = 100;
  nextToken?: string;
  startTime?: string;
  endTime?: string;
  order?: 'asc' | 'desc';
  isAlert?: boolean;
}

@Route('api/v1/audioSegments')
@Tags('Audio Segments')
@Response<{ message: string }>(401, 'Unauthorized')
@Response<{ message: string }>(403, 'Forbidden')
@Response<{ message: string }>(404, 'Not Found')
@Response<{ message: string }>(500, 'Internal Server Error')
export class AudioController extends Controller {
  @Get('{feedId}')
  @Security('google_id_token')
  @Extension('x-google-backend', 'radio-transcription-api')
  public async listAudioSegments(
    @Path() feedId: string,
    @Queries() query: ListAudioSegmentsQueryParams
  ): Promise<{ segments: AudioSegment[]; nextToken: string | undefined }> {
    try {
      const queryParams = new URLSearchParams();
      queryParams.append('feed_ids', [feedId].toString());
      if (query.limit) queryParams.append('limit', query.limit.toString());
      if (query.nextToken) queryParams.append('next_token', query.nextToken);
      if (query.startTime) queryParams.append('start_time', query.startTime);
      if (query.endTime) queryParams.append('end_time', query.endTime);
      if (query.order) queryParams.append('order', query.order);
      // Can be true/false, just not undefined.
      if (query.isAlert !== undefined) {
        queryParams.append('is_alert', query.isAlert.toString());
      }

      const client = await getServiceClient(AUDIO_SEGMENTS_API_URL);
      const response = await client.request({
        url: `${AUDIO_SEGMENTS_API_URL}?${queryParams.toString()}`,
        method: 'GET',
      });

      const data = response.data as {
        segments: AudioSegmentBackend[];
        next_token?: string;
      };
      return {
        segments: data.segments.map(convertAudioSegmentBackend),
        nextToken: data.next_token,
      };
    } catch (error: unknown) {
      const { status, message } = handleBackendError(
        error,
        'fetching audio segments'
      );
      throw new HttpError(status, message);
    }
  }
}
