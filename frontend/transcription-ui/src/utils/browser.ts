import { UAParser } from 'ua-parser-js';

export function isSafari(): boolean {
  if (typeof navigator === 'undefined') return false;
  const name = new UAParser(navigator.userAgent).getBrowser().name;
  return name === 'Safari' || name === 'Mobile Safari';
}
