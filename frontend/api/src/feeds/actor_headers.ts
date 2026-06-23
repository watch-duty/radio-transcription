import type { AuthenticatedRequest } from '../authentication.js';
import { HttpError } from '../utils.js';

const ACTOR_ID_HEADER = 'X-WD-Actor-Id';
const GOOGLE_USER_ACTOR_PREFIX = 'user:google:';

export function feedMutationActorHeaders(
  request: AuthenticatedRequest
): Record<string, string> {
  const sub = request.user?.sub;
  if (!sub || /\s/.test(sub)) {
    throw new HttpError(403, 'Forbidden');
  }

  return {
    [ACTOR_ID_HEADER]: `${GOOGLE_USER_ACTOR_PREFIX}${sub}`,
  };
}
