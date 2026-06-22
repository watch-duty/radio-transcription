export function isSafari(): boolean {
  if (typeof navigator === 'undefined') return false;
  const ua = navigator.userAgent;
  // Chrome/Edge/Firefox (incl. their iOS WebKit builds) all include "Safari";
  // exclude them so only genuine Safari matches.
  return /^((?!chrome|android|crios|fxios|edg).)*safari/i.test(ua);
}
