import { mapFeedStatusToBackendStatuses } from '@transcription/common';
import type { Feed, ListFeedsResponse } from '@transcription/common';

import { apiFetch } from '../utils/apiUtils';

export interface ListFeedsParams {
  limit?: number;
  nextToken?: string;
  name?: string;
  sourceTypes?: string[];
  statuses?: string[];
  tags?: { key: string; value: string }[];
}

export async function listFeedsPage(
  token: string,
  params?: ListFeedsParams
): Promise<ListFeedsResponse> {
  const queryParams = new URLSearchParams();
  if (params?.limit) queryParams.append('limit', params.limit.toString());
  if (params?.nextToken) queryParams.append('nextToken', params.nextToken);
  if (params?.name) queryParams.append('name', params.name);
  if (params?.sourceTypes && params.sourceTypes.length > 0) {
    queryParams.append('sourceTypes', params.sourceTypes.join(','));
  }
  if (params?.statuses && params.statuses.length > 0) {
    const backendStatuses = params.statuses.flatMap((status) =>
      mapFeedStatusToBackendStatuses(status)
    );
    if (backendStatuses.length > 0) {
      queryParams.append('statuses', backendStatuses.join(','));
    }
  }
  if (params?.tags && params.tags.length > 0) {
    for (const tag of params.tags) {
      queryParams.append('tags', JSON.stringify(tag));
    }
  }

  const url = queryParams.toString()
    ? `${import.meta.env.VITE_API_BASE_URL}/api/v1/feeds?${queryParams.toString()}`
    : `${import.meta.env.VITE_API_BASE_URL}/api/v1/feeds`;

  return apiFetch<Feed[] | ListFeedsResponse>(url, {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  }).then((resp) => {
    if (Array.isArray(resp)) {
      return {
        feeds: resp,
        total: resp.length,
      };
    } else {
      return resp;
    }
  });
}

export async function listFeeds(
  token: string,
  params?: Omit<ListFeedsParams, 'limit' | 'nextToken'>
): Promise<Omit<ListFeedsResponse, 'nextToken'>> {
  return listFeedsPage(token, params);
}
