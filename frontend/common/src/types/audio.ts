export enum AudioClassification {
  UNSPECIFIED = 'UNSPECIFIED',
  SPEECH = 'SPEECH',
  OTHER = 'OTHER',
}

export enum AnnotationType {
  TRANSCRIPT = 'TRANSCRIPT',
  EVALUATION = 'EVALUATION',
  WAVEFORM = 'WAVEFORM',
  TRANSCRIPT_FLAG = 'TRANSCRIPT_FLAG',
}

export interface TranscriptAnnotationData {
  text: string;
  errors: string[];
}

export interface TextMatchSpan {
  startIndex: number;
  endIndex: number;
  matchedText: string;
}

export interface RuleAnnotation {
  textMatch?: TextMatchSpan[];
}

export interface RuleAnnotationMap {
  [ruleId: string]: RuleAnnotation;
}

export interface EvaluationAnnotationData {
  decisions: string[];
  errors: string[];
  ruleAnnotations?: RuleAnnotationMap;
}

export interface WaveformAnnotationData {
  peaks: number[][];
  durationSeconds: number;
}

export interface TranscriptFlagAnnotationData {
  flaggedByUserIds: string[];
}

export interface Annotation {
  type: AnnotationType;
  createdAt: string;
  data:
    | TranscriptAnnotationData
    | EvaluationAnnotationData
    | WaveformAnnotationData
    | TranscriptFlagAnnotationData;
}

export type AddAnnotationRequest = Omit<Annotation, 'createdAt'>;

export interface AudioSegment {
  id: string;
  feedId: string;
  classification: AudioClassification;
  startTimestamp: string;
  endTimestamp: string;
  missingPriorContext: boolean;
  missingPostContext: boolean;
  sourceAudioUris: string[];
  canonicalAudioUri?: string;
  startAudioOffset?: string;
  endAudioOffset?: string;
  playbackAudioUri?: string;
  externalAudioSegmentId?: string;
  createdAt: string;
  annotations: Annotation[];
}
