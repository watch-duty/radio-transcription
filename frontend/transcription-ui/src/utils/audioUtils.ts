export function getAudioUrl(uri: string): string {
  const base = import.meta.env.DEV ? '/gcs/' : 'https://storage.googleapis.com/';
  return uri.replace('gs://', base);
}
