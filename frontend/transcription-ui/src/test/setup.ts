import '@testing-library/jest-dom/vitest';

// JSDOM does not implement HTMLMediaElement methods (like play, pause, load)
// since it lacks a media engine. Stub them to prevent 'Not implemented' console warnings.
if (typeof window !== 'undefined') {
  window.HTMLMediaElement.prototype.play = async () => {};
  window.HTMLMediaElement.prototype.pause = () => {};
  window.HTMLMediaElement.prototype.load = () => {};
}
