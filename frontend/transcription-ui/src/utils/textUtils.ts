import { SourceType } from '@transcription/common';

export function toSourceTypeString(type: SourceType) {
  switch (type) {
    case SourceType.BCFY_FEEDS:
      return 'Broadcastify Feeds';
    case SourceType.GENERIC_ICECAST:
      return 'Generic Icecast';
    case SourceType.BCFY_CALLS:
      return 'Broadcastify Calls';
    case SourceType.ECHO:
      return 'Echo';
    case SourceType.OPENMHZ:
      return 'OpenMHz';
    case SourceType.FIRE_NOTIFICATIONS:
      return 'Fire Notifications';
    default:
      return type || 'Unknown';
  }
}
