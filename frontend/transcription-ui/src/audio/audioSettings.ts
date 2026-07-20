// User-facing audio settings: the volume/pan/speed values the operator can
// tune and the ranges/options the controls expose. These are the "what the user
// picks" constants — distinct from the engine's internal computation constants
// (e.g. mute threshold, ramp time), which live with the player in
// `WebAudioPlayer.ts`. Grouped below by purpose.

// Bounds for the continuous volume control.
export const VOLUME_MIN_DB = -30;
export const VOLUME_MAX_DB = 20;

// Discrete choices the pan/speed toggles offer.
export const SPEED_OPTIONS = [0.5, 1.0, 1.25, 1.5, 2.0] as const;
export const PAN_OPTIONS = [-1, 0, 1] as const;

// The single source of truth for "default" audio settings. A future global
// settings UI replaces these, and both persistence and the off-default badges
// on the speaker button follow automatically.
export const DEFAULT_VOLUME_DB = 0;
export const DEFAULT_PAN = 0;
export const DEFAULT_SPEED = 1;

// Width of the volume snap zone: a value within this many dB of the default is
// pulled to exactly the default so the slider settles there when dragged near
// it. See `snapVolumeToDefault` in `WebAudioPlayer.ts`.
export const VOLUME_SNAP_DB = 1;

// Persistence keys for audio and application settings in localStorage.
export const STORAGE_KEYS = {
  themeMode: 'radio.themeMode',
  volumeDb: 'radio.audio.volumeDb',
  pan: 'radio.audio.pan',
  speed: 'radio.audio.speed',
} as const;

export function feedKey(base: string, feedId?: string): string {
  return feedId ? `${base}.${feedId}` : base;
}
