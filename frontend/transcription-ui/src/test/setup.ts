import '@testing-library/jest-dom/vitest';

// JSDOM does not implement HTMLMediaElement methods (like play, pause, load)
// since it lacks a media engine. Stub them to prevent 'Not implemented' console warnings.
if (typeof window !== 'undefined') {
  window.HTMLMediaElement.prototype.play = async () => {};
  window.HTMLMediaElement.prototype.pause = () => {};
  window.HTMLMediaElement.prototype.load = () => {};

  // JSDOM implements no scroll methods; react-virtuoso calls scrollBy from an
  // rAF on prepend, which otherwise throws an unhandled "not a function" error.
  window.HTMLElement.prototype.scrollBy = () => {};
  window.HTMLElement.prototype.scrollTo = () => {};
  window.HTMLElement.prototype.scrollIntoView = () => {};
}
