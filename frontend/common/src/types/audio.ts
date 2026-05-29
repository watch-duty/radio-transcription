export type AudioClassification = 'SPEECH_DETECTED' | 'UNCLASSIFIED';

export type AnnotationType = 'TRANSCRIPT' | 'EVALUATION';

export interface TranscriptAnnotationData {
  text: string;
  errors: string[];
}

export interface EvaluationAnnotationData {
  decisions: string[];
  errors: string[];
}

export interface BaseAnnotation {
  audioSegmentId: string;
  type: AnnotationType;
  createdAt: string;
}

export interface TranscriptAnnotation extends BaseAnnotation {
  type: 'TRANSCRIPT';
  data: TranscriptAnnotationData;
}

export interface EvaluationAnnotation extends BaseAnnotation {
  type: 'EVALUATION';
  data: EvaluationAnnotationData;
}

export type Annotation = TranscriptAnnotation | EvaluationAnnotation;

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
  createdAt: string;
  annotations: Annotation[];
}
