import type {
  ListTranscriptsResponse,
  Transcript,
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

import { TRANSCRIPTS_API_URL } from '../config.js';
import { HttpError, getServiceClient, handleBackendError } from '../utils.js';

export interface TranscriptResponse {
  feed_id: string;
  segment_id: string;
  transcript: string;
  start_timestamp: string;
  end_timestamp: string;
  missing_prior_context: boolean;
  missing_post_context: boolean;
  source_audio_uris: string[];
  canonical_audio_uri: string;
  playback_audio_uri: string;
  start_audio_offset: string;
  end_audio_offset: string;
  evaluation_decisions: string[];
}

function convertTranscriptResponse(response: TranscriptResponse): Transcript {
  return {
    feedId: response.feed_id,
    segmentId: response.segment_id,
    transcript: response.transcript,
    startTimestamp: response.start_timestamp,
    endTimestamp: response.end_timestamp,
    missingPriorContext: response.missing_prior_context,
    missingPostContext: response.missing_post_context,
    sourceAudioUris: response.source_audio_uris,
    canonicalAudioUri: response.canonical_audio_uri,
    playbackAudioUri: response.playback_audio_uri,
    startAudioOffset: response.start_audio_offset,
    endAudioOffset: response.end_audio_offset,
    evaluationDecisions: response.evaluation_decisions,
  };
}

export class ListTranscriptsQueryParams {
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

@Route('api/v1/transcripts')
@Tags('Transcripts')
@Response<{ message: string }>(401, 'Unauthorized')
@Response<{ message: string }>(403, 'Forbidden')
@Response<{ message: string }>(404, 'Not Found')
@Response<{ message: string }>(500, 'Internal Server Error')
export class TranscriptsController extends Controller {
  @Get('{feedId}')
  @Security('google_id_token')
  @Extension('x-google-backend', 'radio-transcription-api')
  public async listTranscripts(
    @Path() feedId: string,
    @Queries() query: ListTranscriptsQueryParams
  ): Promise<ListTranscriptsResponse> {
    // Get the Authentication token to allow us to call the Cloud Run function.
    try {
      const queryParams = new URLSearchParams();
      queryParams.append('feed_id', feedId);
      if (query.limit) queryParams.append('limit', query.limit.toString());
      if (query.nextToken) queryParams.append('next_token', query.nextToken);
      if (query.startTime) queryParams.append('start_time', query.startTime);
      if (query.endTime) queryParams.append('end_time', query.endTime);
      if (query.order) queryParams.append('order', query.order);
      // Can be true/false, just not undefined.
      if (query.isAlert !== undefined) {
        queryParams.append('is_alert', query.isAlert.toString());
      }

      const client = await getServiceClient(TRANSCRIPTS_API_URL);
      const response = await client.request({
        url: `${TRANSCRIPTS_API_URL}?${queryParams.toString()}`,
        method: 'GET',
      });
      const data = response.data as {
        transcripts: TranscriptResponse[];
        next_token?: string;
      };

      return {
        transcripts: data.transcripts.map(convertTranscriptResponse),
        nextToken: data.next_token,
      };
    } catch (error: unknown) {
      const { status, message } = handleBackendError(
        error,
        `fetching transcripts for feed ${feedId}`
      );
      throw new HttpError(status, message);
    }
  }
}
