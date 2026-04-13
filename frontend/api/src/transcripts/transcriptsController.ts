import type {
  ListTranscriptsResponse,
  Transcript,
} from '@transcription/common';
import { GoogleAuth } from 'google-auth-library';
import {
  Controller,
  Extension,
  Get,
  Path,
  Query,
  Res,
  Response,
  Route,
  Security,
  Tags,
  TsoaResponse,
} from 'tsoa';

import { TRANSCRIPTS_API_URL } from '../config.js';


export interface TranscriptResponse {
  feed_id: string;
  transmission_id: string;
  transcript: string;
  start_timestamp: string;
  end_timestamp: string;
  missing_prior_context: boolean;
  missing_post_context: boolean;
  source_audio_uris: string[];
  canonical_audio_uri: string;
  start_audio_offset: string;
  end_audio_offset: string;
  evaluation_decisions: string[];
}

function convertTranscriptResponse(response: TranscriptResponse): Transcript {
  return {
    feedId: response.feed_id,
    transmissionId: response.transmission_id,
    transcript: response.transcript,
    startTimestamp: response.start_timestamp,
    endTimestamp: response.end_timestamp,
    missingPriorContext: response.missing_prior_context,
    missingPostContext: response.missing_post_context,
    sourceAudioUris: response.source_audio_uris,
    canonicalAudioUri: response.canonical_audio_uri,
    startAudioOffset: response.start_audio_offset,
    endAudioOffset: response.end_audio_offset,
    evaluationDecisions: response.evaluation_decisions,
  };
}

@Route('api/v1/transcripts')
@Tags('Transcripts')
@Response(401, 'Unauthorized')
export class TranscriptsController extends Controller {
  @Get('{feedId}')
  @Security('google_id_token')
  @Extension('x-google-backend', 'radio-transcription-api')
  public async listTranscripts(
    @Path() feedId: string,
    @Res() notFound: TsoaResponse<404, { message: string }>,
    @Query() limit?: number,
    @Query() nextToken?: string,
    @Query() startTime?: string,
    @Query() endTime?: string
  ): Promise<ListTranscriptsResponse> {
    // Get the Authentication token to allow us to call the Cloud Run function.
    const auth = new GoogleAuth();
    const client = await auth.getIdTokenClient(TRANSCRIPTS_API_URL!);

    try {
      const queryParams = new URLSearchParams();
      queryParams.append('feed_id', feedId);
      if (limit) queryParams.append('limit', limit.toString());
      if (nextToken) queryParams.append('next_token', nextToken);
      if (startTime) queryParams.append('start_time', startTime);
      if (endTime) queryParams.append('end_time', endTime);

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
      console.error('Error fetching transcript:', error);
      if (error instanceof Error) {
        throw new Error(`Error fetching transcript: ${error.message}`, {
          cause: error,
        });
      } else {
        throw new Error('Error fetching transcript', { cause: error });
      }
    }
  }
}
