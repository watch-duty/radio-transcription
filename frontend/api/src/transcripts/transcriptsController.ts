import axios from 'axios';
import { GoogleAuth } from 'google-auth-library';
import { TRANSCRIPTS_API_URL } from '../config.js';
import { Route, Controller, Tags, Get, Path, TsoaResponse, Res } from 'tsoa';

export interface Transcript {
  feedId: string;
  transmissionId: string;
  transcript: string;
  startTimestamp: string;
  endTimestamp: string;
  missingPriorContext: boolean;
  missingPostContext: boolean;
  sourceAudioUris: string[];
  canonicalAudioUri: string;
  startAudioOffset: string;
  endAudioOffset: string;
  evaluationDecisions: string[];
}

export interface ListTranscriptsResponse {
  transcripts: Transcript[];
}

@Route('api/v1/transcripts')
@Tags('Transcripts')
export class TranscriptsController extends Controller {
  @Get("{feedId}")
  public async listTranscripts(
    @Path() feedId: string,
    @Res() notFound: TsoaResponse<404, { message: string }>
  ): Promise<ListTranscriptsResponse> {
    // Get the Authentication token to allow us to call the Cloud Run function.
    const auth = new GoogleAuth();
    const client = await auth.getIdTokenClient(TRANSCRIPTS_API_URL!);
    const tokenResponse = await client.getRequestHeaders();
    const token = tokenResponse.get('Authorization');

    try {
      const response = await axios.get(TRANSCRIPTS_API_URL!, { params: { feedId }, headers: { Authorization: token } });
      return response.data;
    } catch (error: unknown) {
      console.error('Error fetching transcript:', error);
      if (error instanceof Error) {
        throw new Error(`Error fetching transcript: ${error.message}`);
      } else {
        throw new Error('Error fetching transcript');
      }
    }
  }
}
