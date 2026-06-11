export interface Transcript {
  feedId: string;
  segmentId: string;
  transcript: string;
  startTimestamp: string;
  endTimestamp: string;
  missingPriorContext: boolean;
  missingPostContext: boolean;
  sourceAudioUris: string[];
  canonicalAudioUri: string;
  playbackAudioUri: string;
  startAudioOffset: string;
  endAudioOffset: string;
  evaluationDecisions: string[];
}

export interface ListTranscriptsResponse {
  transcripts: Transcript[];
  nextToken?: string;
}
