export interface TextMatchSpan {
  start: number;
  end: number;
  matchedText: string;
}

export interface TextMatchAnnotation {
  spans: TextMatchSpan[];
}

export interface RuleAnnotation {
  ruleId: string;
  textMatch?: TextMatchAnnotation;
}

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
  ruleAnnotations: RuleAnnotation[];
}

export interface ListTranscriptsResponse {
  transcripts: Transcript[];
  nextToken?: string;
}
