import type { Feed, FeedCreate, SourceType } from '@transcription/common';
import { GoogleAuth } from 'google-auth-library';
import {
  Body,
  Controller,
  Delete,
  Extension,
  Get,
  Path,
  Post,
  Res,
  Response,
  Route,
  Security,
  SuccessResponse,
  Tags,
  TsoaResponse,
} from 'tsoa';

import { FEEDS_STORE_API_URL } from '../config.js';
import { isAxiosError } from '../utils.js';

interface BaseFeedBackend {
  name: string;
  source_type: SourceType;
}

interface FeedBackend extends BaseFeedBackend {
  id: string;
  source_feed_id: string;
  external_id: string;
}

interface FeedCreateBackend extends BaseFeedBackend {
  source_feed_id: string;
  external_id: string;
}

function convertFeedBackend(response: FeedBackend): Feed {
  return {
    id: response.id,
    name: response.name,
    sourceType: response.source_type,
    sourceFeedId: response.source_feed_id,
    externalId: response.external_id,
  };
}

function convertFeedCreate(create: FeedCreate): FeedCreateBackend {
  return {
    name: create.name,
    source_type: create.sourceType,
    source_feed_id: create.sourceFeedId,
    external_id: create.externalId,
  };
}

@Route('api/v1/feeds')
@Tags('Feeds')
@Response(401, 'Unauthorized')
export class FeedsController extends Controller {
  private async getClient() {
    const auth = new GoogleAuth();
    return await auth.getIdTokenClient(FEEDS_STORE_API_URL);
  }

  @Get('')
  @Security('google_id_token')
  @Extension('x-google-backend', 'radio-transcription-api')
  public async listFeeds(): Promise<Feed[]> {
    const client = await this.getClient();
    try {
      const response = await client.request({
        url: FEEDS_STORE_API_URL,
        method: 'GET',
      });
      const data = response.data as FeedBackend[];
      return data.map(convertFeedBackend);
    } catch (error: unknown) {
      if (isAxiosError(error)) {
        const status = error.response?.status || 500;
        const data = JSON.stringify(error.response?.data);
        console.error(
          JSON.stringify({
            level: 'ERROR',
            message: `Backend API error: ${status}`,
            data: error.response?.data,
          })
        );
        throw new Error(`Backend API error ${status}: ${data}`, {
          cause: error,
        });
      }
      console.error(
        JSON.stringify({
          level: 'ERROR',
          message: 'Unexpected error fetching feeds',
          error: error instanceof Error ? error.message : String(error),
        })
      );
      throw new Error('Error fetching feeds', { cause: error });
    }
  }

  @Get('{feedId}')
  @Security('google_id_token')
  @Response<{ message: string }>(404, 'Not Found')
  @Extension('x-google-backend', 'radio-transcription-api')
  public async getFeed(
    @Path() feedId: string,
    @Res() notFound: TsoaResponse<404, { message: string }>
  ): Promise<Feed> {
    const client = await this.getClient();
    try {
      const response = await client.request({
        url: `${FEEDS_STORE_API_URL}/${feedId}`,
        method: 'GET',
      });
      return convertFeedBackend(response.data as FeedBackend);
    } catch (error: unknown) {
      if (isAxiosError(error)) {
        if (error.response?.status === 404) {
          return notFound(404, { message: `Feed ${feedId} not found` });
        }
        const status = error.response?.status || 500;
        const data = JSON.stringify(error.response?.data);
        console.error(
          JSON.stringify({
            level: 'ERROR',
            message: `Backend API error: ${status}`,
            data: error.response?.data,
          })
        );
        throw new Error(`Backend API error ${status}: ${data}`, {
          cause: error,
        });
      }
      console.error(
        JSON.stringify({
          level: 'ERROR',
          message: `Unexpected error fetching feed ${feedId}`,
          error: error instanceof Error ? error.message : String(error),
        })
      );
      throw new Error(`Error fetching feed ${feedId}`, { cause: error });
    }
  }

  @Post('')
  @Security('google_id_token')
  @SuccessResponse('201', 'Created')
  @Extension('x-google-backend', 'radio-transcription-api')
  public async createFeed(@Body() requestBody: FeedCreate): Promise<Feed> {
    const client = await this.getClient();
    try {
      const response = await client.request({
        url: FEEDS_STORE_API_URL,
        method: 'POST',
        data: convertFeedCreate(requestBody),
      });
      return convertFeedBackend(response.data as FeedBackend);
    } catch (error: unknown) {
      if (isAxiosError(error)) {
        const status = error.response?.status || 500;
        const data = JSON.stringify(error.response?.data);
        console.error(
          JSON.stringify({
            level: 'ERROR',
            message: `Backend API error: ${status}`,
            data: error.response?.data,
          })
        );
        throw new Error(`Backend API error ${status}: ${data}`, {
          cause: error,
        });
      }
      console.error(
        JSON.stringify({
          level: 'ERROR',
          message: 'Unexpected error creating feed',
          error: error instanceof Error ? error.message : String(error),
        })
      );
      throw new Error('Error creating feed', { cause: error });
    }
  }

  @Delete('{feedId}')
  @Security('google_id_token')
  @SuccessResponse('204', 'No Content')
  @Response<{ message: string }>(404, 'Not Found')
  @Extension('x-google-backend', 'radio-transcription-api')
  public async deleteFeed(
    @Path() feedId: string,
    @Res() notFound: TsoaResponse<404, { message: string }>
  ): Promise<void> {
    const client = await this.getClient();
    try {
      await client.request({
        url: `${FEEDS_STORE_API_URL}/${feedId}`,
        method: 'DELETE',
      });
    } catch (error: unknown) {
      if (isAxiosError(error)) {
        if (error.response?.status === 404) {
          return notFound(404, { message: `Feed ${feedId} not found` });
        }
        const status = error.response?.status || 500;
        const data = JSON.stringify(error.response?.data);
        console.error(
          JSON.stringify({
            level: 'ERROR',
            message: `Backend API error: ${status}`,
            data: error.response?.data,
          })
        );
        throw new Error(`Backend API error ${status}: ${data}`, {
          cause: error,
        });
      }
      console.error(
        JSON.stringify({
          level: 'ERROR',
          message: `Unexpected error deleting feed ${feedId}`,
          error: error instanceof Error ? error.message : String(error),
        })
      );
      throw new Error(`Error deleting feed ${feedId}`, { cause: error });
    }
  }
}
