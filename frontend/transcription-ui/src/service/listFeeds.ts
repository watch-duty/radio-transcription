import type {
  FeedStatus,
  ListFeedsResponse,
  SourceType,
  Tag,
} from '@transcription/common';

import { apiFetch } from '../utils/apiUtils';

export interface ListFeedsParams {
  limit?: number;
  nextToken?: string;
  order?: 'asc' | 'desc';
  sourceTypes?: SourceType[];
  statuses?: FeedStatus[];
  tags?: Tag[];
}

export async function listFeeds(
  token: string,
  params?: ListFeedsParams
): Promise<ListFeedsResponse> {
  const queryParams = new URLSearchParams();
  if (params?.limit !== undefined) {
    queryParams.append('limit', params.limit.toString());
  }
  if (params?.nextToken) {
    queryParams.append('nextToken', params.nextToken);
  }
  if (params?.order) {
    queryParams.append('order', params.order);
  }
  if (params?.sourceTypes) {
    params.sourceTypes.forEach((st) => queryParams.append('sourceTypes', st));
  }
  if (params?.statuses) {
    params.statuses.forEach((status) => queryParams.append('statuses', status));
  }
  if (params?.tags) {
    params.tags.forEach((tag) => {
      queryParams.append('tags', JSON.stringify(tag));
    });
  }

  const url = queryParams.toString()
    ? `${import.meta.env.VITE_API_BASE_URL}/api/v1/feeds?${queryParams.toString()}`
    : `${import.meta.env.VITE_API_BASE_URL}/api/v1/feeds`;

  return apiFetch<ListFeedsResponse>(url, {
    headers: {
      Authorization: `Bearer ${token}`,
    },
  });
}
