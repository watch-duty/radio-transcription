import { SourceType } from '@transcription/common';

/**
 * Validates a feed source ID based on its SourceType.
 * Returns an error message string if invalid, or null if valid.
 */
export function validateFeedSourceId(
  sourceType: SourceType,
  sourceId: string
): string | null {
  const trimmedId = sourceId.trim();
  if (!trimmedId) {
    return 'Source feed ID is required.';
  }

  switch (sourceType) {
    case SourceType.BCFY_CALLS:
      if (!/^\d+-\d+$/.test(trimmedId)) {
        return 'Must only contain numbers formatted as sid-talkgroup.';
      }
      break;
    case SourceType.BCFY_FEEDS:
      if (!/^\d+$/.test(trimmedId)) {
        return 'Must be a number.';
      }
      break;
    case SourceType.ECHO:
      if (!/^[a-zA-Z0-9_-]+$/.test(trimmedId)) {
        return 'Must only contain letters, numbers, and the following special characters: - _';
      }
      break;
    case SourceType.FIRE_NOTIFICATIONS:
      if (!/^[A-Z0-9_\-/()]+$/.test(trimmedId)) {
        return 'Must only contain uppercase letters, numbers, and the following special characters: / - ( )';
      }
      break;
    case SourceType.OPENMHZ:
      if (!/^\w+$/.test(trimmedId)) {
        return 'Must be alphanumeric.';
      }
      break;
    default:
      break;
  }

  return null;
}
