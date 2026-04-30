/**
 * Converts a Google Cloud Storage URI to a URL that can be used by the browser.
 *
 * Most audio loading libraries cannot load from a `gs://` URI. Google provides a public endpoint
 * for reading public GCS objects, which we can use here.
 *
 * For localhost development, we use a proxy endpoint to avoid CORS issues, which is `/gcs/*`.
 * The vite config sets this up to proxy to `https://storage.googleapis.com/`.
 *
 * @param uri The Google Cloud Storage URI to convert.
 * @returns The URL that can be used by the browser.
 */
export function getAudioUrl(uri: string): string {
  if (!uri) return uri;
  const base = import.meta.env.DEV
    ? '/gcs/'
    : 'https://storage.googleapis.com/';
  return uri.replace('gs://', base);
}
