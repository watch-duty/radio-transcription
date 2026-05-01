import type {
  BackendFeedStatus,
  Feed,
  FeedCreate,
  FeedStatus,
  SourceType,
} from '@transcription/common';
import { GoogleAuth } from 'google-auth-library';
import {
  Body,
  Controller,
  Delete,
  Extension,
  Get,
  Path,
  Post,
  Response,
  Route,
  Security,
  SuccessResponse,
  Tags,
} from 'tsoa';

import { FEEDS_STORE_API_URL } from '../config.js';
import { HttpError, handleBackendError } from '../utils.js';

interface BaseFeedBackend {
  name: string;
  source_type: SourceType;
}

interface FeedBackend extends BaseFeedBackend {
  id: string;
  source_feed_id: string;
  external_id: string;
  status: BackendFeedStatus;
  last_heartbeat: string | null;
}

interface FeedCreateBackend extends BaseFeedBackend {
  source_feed_id: string;
  external_id: string;
}

function getSourceUrl(
  sourceType: SourceType,
  sourceFeedId: string | undefined
): string | undefined {
  if (!sourceFeedId) return undefined;
  switch (sourceType) {
    case 'bcfy_feeds':
      return `https://partner.broadcastify.com/${sourceFeedId}`;
    case 'bcfy_calls':
      return `https://www.broadcastify.com/calls/tg/${sourceFeedId.replace(/-/g, '/')}`;
    case 'openmhz':
      return `https://openmhz.com/system/${sourceFeedId}`;
    case 'echo':
      return undefined;
    default:
      throw new Error(`Unsupported source type: ${sourceType}`);
  }
}

function getArchiveUrl(
  sourceType: SourceType,
  sourceFeedId: string | undefined
): string | undefined {
  if (!sourceFeedId) return undefined;

  switch (sourceType) {
    case 'bcfy_feeds':
      return `https://www.broadcastify.com/archives/feed/${sourceFeedId}`;
    case 'bcfy_calls':
      return `https://www.broadcastify.com/calls/tg/${sourceFeedId.replace(/-/g, '/')}/archives`;
    case 'openmhz':
      return undefined;
    case 'echo':
      return undefined;
    default:
      throw new Error(`Unsupported source type: ${sourceType}`);
  }
}

function convertFeedStatusBackend(status: BackendFeedStatus): FeedStatus {
  return status === 'active' ? 'active' : 'inactive';
}

function convertFeedBackend(response: FeedBackend): Feed {
  return {
    id: response.id,
    name: response.name,
    sourceType: response.source_type,
    sourceFeedId: response.source_feed_id,
    externalId: response.external_id,
    sourceUrl: getSourceUrl(response.source_type, response.source_feed_id),
    archiveUrl: getArchiveUrl(response.source_type, response.source_feed_id),
    status: convertFeedStatusBackend(response.status),
    lastHeartbeat: response.last_heartbeat ?? undefined,
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
  @Response<{ message: string }>(401, 'Unauthorized')
  @Response<{ message: string }>(403, 'Forbidden')
  @Response<{ message: string }>(500, 'Internal Server Error')
  @Extension('x-google-backend', 'radio-transcription-api')
  public async listFeeds(): Promise<Feed[]> {
    try {
      const client = await this.getClient();
      const response = await client.request<FeedBackend[]>({
        url: FEEDS_STORE_API_URL,
        method: 'GET',
      });
      return response.data.map(convertFeedBackend);
    } catch (error: unknown) {
      const { status, message } = handleBackendError(error, 'fetching feeds');
      throw new HttpError(status, message);
    }
  }

  @Get('{feedId}')
  @Security('google_id_token')
  @Response<{ message: string }>(401, 'Unauthorized')
  @Response<{ message: string }>(403, 'Forbidden')
  @Response<{ message: string }>(404, 'Not Found')
  @Response<{ message: string }>(500, 'Internal Server Error')
  @Extension('x-google-backend', 'radio-transcription-api')
  public async getFeed(@Path() feedId: string): Promise<Feed> {
    try {
      const client = await this.getClient();
      const response = await client.request<FeedBackend>({
        url: `${FEEDS_STORE_API_URL}/${feedId}`,
        method: 'GET',
      });
      return convertFeedBackend(response.data);
    } catch (error: unknown) {
      const { status, message } = handleBackendError(
        error,
        `fetching feed ${feedId}`
      );
      throw new HttpError(status, message);
    }
  }

  @Post('')
  @Security('google_id_token')
  @SuccessResponse('201', 'Created')
  @Response<{ message: string }>(401, 'Unauthorized')
  @Response<{ message: string }>(403, 'Forbidden')
  @Response<{ message: string }>(500, 'Internal Server Error')
  @Extension('x-google-backend', 'radio-transcription-api')
  public async createFeed(@Body() requestBody: FeedCreate): Promise<Feed> {
    try {
      const client = await this.getClient();
      const response = await client.request<FeedBackend>({
        url: FEEDS_STORE_API_URL,
        method: 'POST',
        data: convertFeedCreate(requestBody),
      });
      return convertFeedBackend(response.data);
    } catch (error: unknown) {
      const { status, message } = handleBackendError(error, 'creating feed');
      throw new HttpError(status, message);
    }
  }

  @Post('{feedId}/reset')
  @Security('google_id_token')
  @Response<{ message: string }>(401, 'Unauthorized')
  @Response<{ message: string }>(403, 'Forbidden')
  @Response<{ message: string }>(404, 'Not Found')
  @Response<{ message: string }>(500, 'Internal Server Error')
  @Extension('x-google-backend', 'radio-transcription-api')
  public async resetFeed(@Path() feedId: string): Promise<Feed> {
    const client = await this.getClient();
    try {
      const response = await client.request<FeedBackend>({
        url: `${FEEDS_STORE_API_URL}/${feedId}/reset`,
        method: 'POST',
      });
      return convertFeedBackend(response.data);
    } catch (error: unknown) {
      const { status, message } = handleBackendError(
        error,
        `resetting feed ${feedId}`
      );
      throw new HttpError(status, message);
    }
  }

  @Delete('{feedId}')
  @Security('google_id_token')
  @SuccessResponse('204', 'No Content')
  @Response<{ message: string }>(401, 'Unauthorized')
  @Response<{ message: string }>(403, 'Forbidden')
  @Response<{ message: string }>(404, 'Not Found')
  @Response<{ message: string }>(500, 'Internal Server Error')
  @Extension('x-google-backend', 'radio-transcription-api')
  public async deleteFeed(@Path() feedId: string): Promise<void> {
    const client = await this.getClient();
    try {
      await client.request({
        url: `${FEEDS_STORE_API_URL}/${feedId}`,
        method: 'DELETE',
      });
    } catch (error: unknown) {
      const { status, message } = handleBackendError(
        error,
        `deleting feed ${feedId}`
      );
      throw new HttpError(status, message);
    }
  }
}
