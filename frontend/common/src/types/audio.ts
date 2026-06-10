export enum AudioClassification {
  SPEECH_DETECTED = 'SPEECH_DETECTED',
  UNCLASSIFIED = 'UNCLASSIFIED',
}

export enum AnnotationType {
  TRANSCRIPT = 'TRANSCRIPT',
  EVALUATION = 'EVALUATION',
}

export interface TranscriptAnnotationData {
  text: string;
  errors: string[];
}

export interface EvaluationAnnotationData {
  decisions: string[];
  errors: string[];
}

export interface Annotation {
  type: AnnotationType;
  createdAt: string;
  data: TranscriptAnnotationData | EvaluationAnnotationData;
}

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
