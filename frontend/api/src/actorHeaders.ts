import type { AuthenticatedRequest } from './authentication.js';
import { HttpError } from './utils.js';

const ACTOR_ID_HEADER = 'X-WD-Actor-Id';
const GOOGLE_USER_ACTOR_PREFIX = 'user:google:';

export function mutationActorHeaders(
  request: AuthenticatedRequest
): Record<string, string> {
  const email = request.user?.email?.trim().toLowerCase();
  if (!email || /\s/.test(email)) {
    throw new HttpError(403, 'Forbidden');
  }

  return {
    [ACTOR_ID_HEADER]: `${GOOGLE_USER_ACTOR_PREFIX}${email}`,
  };
}
