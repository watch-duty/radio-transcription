import type {
  AuditTrailValues,
  BackendFeedStatus,
  BackendFeedStatusReason,
  Feed,
  FeedCreate,
  FeedHistoryEvent,
  FeedSearchOptionsResponse,
  FeedUpdate,
  ListFeedHistoryResponse,
  ListFeedsResponse,
  Tag,
} from '@transcription/common';
import {
  SourceType,
  convertFeedStatusBackend,
  convertFeedStatusReason,
} from '@transcription/common';
import {
  Body,
  Controller,
  Delete,
  Extension,
  Get,
  Path,
  Post,
  Put,
  Queries,
  Request,
  Response,
  Route,
  Security,
  SuccessResponse,
  Tags,
} from 'tsoa';

import { mutationActorHeaders } from '../actorHeaders.js';
import { AuthenticatedRequest } from '../authentication.js';
import { FEEDS_STORE_API_URL } from '../config.js';
import {
  HttpError,
  getServiceClient,
  handleBackendError,
  parseTimestamp,
  toCamel,
  toSnake,
} from '../utils.js';

interface BaseFeedBackend {
  name: string;
  source_type: SourceType;
}

interface FeedBackend extends BaseFeedBackend {
  id: string;
  source_feed_id: string;
  status: BackendFeedStatus;
  last_heartbeat: string | null;
  tags?: Tag[];
  status_reason_detail?: string | null;
  status_reason: BackendFeedStatusReason | null;
  last_speech_segment_timestamp: string | null;
}

interface FeedCreateBackend extends BaseFeedBackend {
  source_feed_id: string;
  tags?: Tag[];
}

interface FeedUpdateBackend {
  name: string;
  tags?: Tag[];
}

export class ListFeedsQueryParams {
  /**
   * @isInt
   */
  limit?: number;
  nextToken?: string;
  order?: 'asc' | 'desc';
  sourceTypes?: string;
  statuses?: string;
  // Tag strings must be in the format of {"key": "<val>", "value": "<val>"}
  tags?: string[];
  name?: string;
}

export class ListFeedHistoryQueryParams {
  /**
   * @isInt
   */
  limit: number = 100;
  nextToken?: string;
  order?: 'asc' | 'desc';
}

interface ListFeedsBackendResponse {
  feeds: FeedBackend[];
  next_token?: string;
  total: number;
}

interface FeedHistoryEventBackend {
  id: string;
  feed_id: string;
  action: string;
  actor: string;
  occurred_at: string;
  feed_revision_num: number;
  before_values: AuditTrailValues;
  after_values: AuditTrailValues;
}

function convertFeedHistoryEventBackend(
  response: FeedHistoryEventBackend
): FeedHistoryEvent {
  return {
    id: response.id,
    feedId: response.feed_id,
    action: response.action,
    actor: response.actor,
    occurredAt: parseTimestamp(response.occurred_at) ?? 0,
    feedRevision: response.feed_revision_num,
    beforeValues: toCamel(response.before_values),
    afterValues: toCamel(response.after_values),
  };
}

function getSourceUrl(
  sourceType: SourceType,
  sourceFeedId: string | undefined
): string | undefined {
  if (!sourceFeedId) return undefined;
  switch (sourceType) {
    case SourceType.BCFY_FEEDS:
      return `https://www.broadcastify.com/listen/feed/${sourceFeedId}`;
    case SourceType.BCFY_CALLS:
      return `https://www.broadcastify.com/calls/tg/${sourceFeedId.replace(/-/g, '/')}`;
    case SourceType.OPENMHZ:
      return `https://openmhz.com/system/${sourceFeedId}`;
    case SourceType.ECHO:
      return `https://storage.googleapis.com/wd-echo-recordings-prod/index.html#${sourceFeedId}/`;
    case SourceType.FIRE_NOTIFICATIONS: {
      const cleanSourceId = sourceFeedId.startsWith('/')
        ? sourceFeedId.slice(1)
        : sourceFeedId;
      return `https://audioplay.textmefires.info/audioplay/folder_play?dir=${encodeURIComponent(cleanSourceId)}`;
    }
    default:
      return undefined;
  }
}

function getArchiveUrl(
  sourceType: SourceType,
  sourceFeedId: string | undefined
): string | undefined {
  if (!sourceFeedId) return undefined;

  switch (sourceType) {
    case SourceType.BCFY_FEEDS:
      return `https://www.broadcastify.com/archives/feed/${sourceFeedId}`;
    case SourceType.BCFY_CALLS:
      return `https://www.broadcastify.com/calls/tg/${sourceFeedId.replace(/-/g, '/')}/archives`;
    case SourceType.OPENMHZ:
      return undefined;
    case SourceType.ECHO:
      return undefined;
    case SourceType.FIRE_NOTIFICATIONS: {
      const cleanSourceId = sourceFeedId.startsWith('/')
        ? sourceFeedId.slice(1)
        : sourceFeedId;
      const archivePath = `${cleanSourceId}/Archive`;
      return `https://audioplay.textmefires.info/audioplay/folder_play?dir=${encodeURIComponent(archivePath)}`;
    }
    default:
      return undefined;
  }
}

function convertFeedBackend(response: FeedBackend): Feed {
  return {
    id: response.id,
    name: response.name,
    sourceType: response.source_type,
    sourceFeedId: response.source_feed_id,
    sourceUrl: getSourceUrl(response.source_type, response.source_feed_id),
    archiveUrl: getArchiveUrl(response.source_type, response.source_feed_id),
    status: convertFeedStatusBackend(response.status),
    substatus: response.status,
    lastHeartbeat: parseTimestamp(response.last_heartbeat),
    tags: response.tags,
    statusReasonDetail: response.status_reason_detail ?? undefined,
    statusReason: convertFeedStatusReason(response.status_reason),
    lastSpeechSegmentTimestamp: parseTimestamp(
      response.last_speech_segment_timestamp
    ),
  };
}

function appendTagFilters(queryParams: URLSearchParams, tags: string[]): void {
  let parsedTags: unknown[];
  try {
    parsedTags = tags.flatMap((tag) => {
      const parsed = JSON.parse(tag) as unknown;
      return Array.isArray(parsed) ? parsed : [parsed];
    });
  } catch {
    throw new HttpError(400, 'Invalid tags query parameter');
  }
  queryParams.append('tags', JSON.stringify(parsedTags));
}

function convertFeedCreate(create: FeedCreate): FeedCreateBackend {
  return toSnake<FeedCreateBackend>(create);
}

function convertFeedUpdate(update: FeedUpdate): FeedUpdateBackend {
  return toSnake<FeedUpdateBackend>(update);
}

@Route('api/v1/feeds')
@Tags('Feeds')
@Response(401, 'Unauthorized')
export class FeedsController extends Controller {
  @Get('')
  @Security('google_id_token')
  @Response<{ message: string }>(401, 'Unauthorized')
  @Response<{ message: string }>(403, 'Forbidden')
  @Response<{ message: string }>(500, 'Internal Server Error')
  @Extension('x-google-backend', 'radio-transcription-api')
  public async listFeeds(
    @Queries() query?: ListFeedsQueryParams
  ): Promise<ListFeedsResponse | Feed[]> {
    try {
      const queryParams = new URLSearchParams();
      if (query?.limit) queryParams.append('limit', query.limit.toString());
      if (query?.nextToken) queryParams.append('next_token', query.nextToken);
      if (query?.order) queryParams.append('order', query.order);
      if (query?.sourceTypes) {
        queryParams.append('source_types', query.sourceTypes);
      }
      if (query?.statuses) {
        queryParams.append('statuses', query.statuses);
      }
      if (query?.tags?.length) {
        appendTagFilters(queryParams, query.tags);
      }
      if (query?.name) {
        queryParams.append('name', query.name);
      }

      const client = await getServiceClient(FEEDS_STORE_API_URL);
      const response = await client.request<
        FeedBackend[] | ListFeedsBackendResponse
      >({
        url: queryParams.toString()
          ? `${FEEDS_STORE_API_URL}?${queryParams.toString()}`
          : FEEDS_STORE_API_URL,
        method: 'GET',
      });

      const data = response.data;
      return Array.isArray(data)
        ? data.map(convertFeedBackend)
        : {
            feeds: data.feeds.map(convertFeedBackend),
            nextToken: data.next_token,
            total: data.total,
          };
    } catch (error: unknown) {
      if (error instanceof HttpError) throw error;
      const { status, message } = handleBackendError(error, 'fetching feeds');
      throw new HttpError(status, message);
    }
  }

  @Get('search-options')
  @Security('google_id_token')
  @Response<{ message: string }>(401, 'Unauthorized')
  @Response<{ message: string }>(403, 'Forbidden')
  @Response<{ message: string }>(500, 'Internal Server Error')
  @Extension('x-google-backend', 'radio-transcription-api')
  public async getFeedSearchOptions(): Promise<FeedSearchOptionsResponse> {
    try {
      const client = await getServiceClient(FEEDS_STORE_API_URL);
      const response = await client.request<{
        source_types: SourceType[];
        statuses: string[];
        tags: Tag[];
      }>({
        url: `${FEEDS_STORE_API_URL}/search-options`,
        method: 'GET',
      });
      const data = response.data;
      return {
        sourceTypes: data.source_types,
        statuses: data.statuses,
        tags: data.tags,
      };
    } catch (error: unknown) {
      if (error instanceof HttpError) throw error;
      const { status, message } = handleBackendError(
        error,
        'fetching feed search options'
      );
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
      const client = await getServiceClient(FEEDS_STORE_API_URL);
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

  @Get('{feedId}/history')
  @Security('google_id_token')
  @Response<{ message: string }>(401, 'Unauthorized')
  @Response<{ message: string }>(403, 'Forbidden')
  @Response<{ message: string }>(404, 'Not Found')
  @Response<{ message: string }>(500, 'Internal Server Error')
  @Extension('x-google-backend', 'radio-transcription-api')
  public async listFeedHistory(
    @Request() request: AuthenticatedRequest,
    @Path() feedId: string,
    @Queries() query: ListFeedHistoryQueryParams
  ): Promise<ListFeedHistoryResponse> {
    try {
      const queryParams = new URLSearchParams();
      if (query.limit) queryParams.append('limit', query.limit.toString());
      if (query.nextToken) queryParams.append('next_token', query.nextToken);
      if (query.order) queryParams.append('order', query.order);

      const client = await getServiceClient(FEEDS_STORE_API_URL);
      const response = await client.request<{
        history_events: FeedHistoryEventBackend[];
        next_token?: string;
        total: number;
      }>({
        url: `${FEEDS_STORE_API_URL}/${feedId}/history?${queryParams.toString()}`,
        method: 'GET',
      });

      const data = response.data;
      return {
        historyEvents: data.history_events.map(convertFeedHistoryEventBackend),
        nextToken: data.next_token,
        total: data.total,
      };
    } catch (error: unknown) {
      const { status, message } = handleBackendError(
        error,
        `fetching history for feed ${feedId}`
      );
      throw new HttpError(status, message);
    }
  }

  @Post('')
  @Security('google_id_token')
  @SuccessResponse('201', 'Created')
  @Response<{ message: string }>(401, 'Unauthorized')
  @Response<{ message: string }>(403, 'Forbidden')
  @Response<{ message: string }>(409, 'Conflict')
  @Response<{ message: string }>(500, 'Internal Server Error')
  @Extension('x-google-backend', 'radio-transcription-api')
  public async createFeed(
    @Request() request: AuthenticatedRequest,
    @Body() requestBody: FeedCreate
  ): Promise<Feed> {
    if (!request.user?.isAdmin) {
      throw new HttpError(403, 'Forbidden');
    }

    const actorHeaders = mutationActorHeaders(request);
    try {
      const client = await getServiceClient(FEEDS_STORE_API_URL);
      const response = await client.request<FeedBackend>({
        url: FEEDS_STORE_API_URL,
        method: 'POST',
        headers: actorHeaders,
        data: convertFeedCreate(requestBody),
      });
      return convertFeedBackend(response.data);
    } catch (error: unknown) {
      const { status, message } = handleBackendError(error, 'creating feed');
      throw new HttpError(status, message);
    }
  }

  /**
   * Update an existing feed (Full override).
   * The fields passed here will fully override the fields stored. There is no coalesing done, so make sure these are the final desired fields.
   */
  @Put('{feedId}')
  @Security('google_id_token')
  @Response<{ message: string }>(401, 'Unauthorized')
  @Response<{ message: string }>(403, 'Forbidden')
  @Response<{ message: string }>(404, 'Not Found')
  @Response<{ message: string }>(409, 'Conflict')
  @Response<{ message: string }>(500, 'Internal Server Error')
  @Extension('x-google-backend', 'radio-transcription-api')
  public async updateFeed(
    @Request() request: AuthenticatedRequest,
    @Path() feedId: string,
    @Body() requestBody: FeedUpdate
  ): Promise<Feed> {
    if (!request.user?.isAdmin) {
      throw new HttpError(403, 'Forbidden');
    }

    const actorHeaders = mutationActorHeaders(request);
    try {
      const client = await getServiceClient(FEEDS_STORE_API_URL);
      const response = await client.request<FeedBackend>({
        url: `${FEEDS_STORE_API_URL}/${feedId}`,
        method: 'PUT',
        headers: actorHeaders,
        data: convertFeedUpdate(requestBody),
      });
      return convertFeedBackend(response.data);
    } catch (error: unknown) {
      const { status, message } = handleBackendError(
        error,
        `updating feed ${feedId}`
      );
      throw new HttpError(status, message);
    }
  }

  @Post('{feedId}/reset')
  @Security('google_id_token')
  @Response<{ message: string }>(401, 'Unauthorized')
  @Response<{ message: string }>(403, 'Forbidden')
  @Response<{ message: string }>(404, 'Not Found')
  @Response<{ message: string }>(409, 'Conflict')
  @Response<{ message: string }>(500, 'Internal Server Error')
  @Extension('x-google-backend', 'radio-transcription-api')
  public async resetFeed(
    @Path() feedId: string,
    @Request() request: AuthenticatedRequest
  ): Promise<Feed> {
    if (!request.user?.isAdmin) {
      throw new HttpError(403, 'Forbidden');
    }

    const actorHeaders = mutationActorHeaders(request);
    try {
      const client = await getServiceClient(FEEDS_STORE_API_URL);
      const response = await client.request<FeedBackend>({
        url: `${FEEDS_STORE_API_URL}/${feedId}/reset`,
        method: 'POST',
        headers: actorHeaders,
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

  /**
   * Deactivate a feed (soft delete).
   * Marks the feed as deactivated to preserve historical transcripts.
   */
  @Post('{feedId}/deactivate')
  @Security('google_id_token')
  @SuccessResponse('204', 'No Content')
  @Response<{ message: string }>(401, 'Unauthorized')
  @Response<{ message: string }>(403, 'Forbidden')
  @Response<{ message: string }>(404, 'Not Found')
  @Response<{ message: string }>(500, 'Internal Server Error')
  @Extension('x-google-backend', 'radio-transcription-api')
  public async deactivateFeed(
    @Path() feedId: string,
    @Request() request: AuthenticatedRequest
  ): Promise<void> {
    if (!request.user?.isAdmin) {
      throw new HttpError(403, 'Forbidden');
    }

    const actorHeaders = mutationActorHeaders(request);
    try {
      const client = await getServiceClient(FEEDS_STORE_API_URL);
      await client.request({
        url: `${FEEDS_STORE_API_URL}/${feedId}/deactivate`,
        method: 'POST',
        headers: actorHeaders,
      });
    } catch (error: unknown) {
      const { status, message } = handleBackendError(
        error,
        `deactivating feed ${feedId}`
      );
      throw new HttpError(status, message);
    }
  }

  /**
   * Hard delete a feed (permanent delete).
   * Deletes the feed, corresponding transcripts, and audio segments.
   */
  @Delete('{feedId}')
  @Security('google_id_token')
  @SuccessResponse('204', 'No Content')
  @Response<{ message: string }>(401, 'Unauthorized')
  @Response<{ message: string }>(403, 'Forbidden')
  @Response<{ message: string }>(404, 'Not Found')
  @Response<{ message: string }>(409, 'Conflict')
  @Response<{ message: string }>(500, 'Internal Server Error')
  @Extension('x-google-backend', 'radio-transcription-api')
  public async deleteFeed(
    @Path() feedId: string,
    @Request() request: AuthenticatedRequest
  ): Promise<void> {
    if (!request.user?.isAdmin) {
      throw new HttpError(403, 'Forbidden');
    }

    const actorHeaders = mutationActorHeaders(request);
    try {
      const client = await getServiceClient(FEEDS_STORE_API_URL);
      await client.request({
        url: `${FEEDS_STORE_API_URL}/${feedId}`,
        method: 'DELETE',
        headers: actorHeaders,
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
