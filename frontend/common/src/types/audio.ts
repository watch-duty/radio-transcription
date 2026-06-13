import type { RuleAnnotation } from './transcripts.js';

export enum AudioClassification {
  UNSPECIFIED = 'UNSPECIFIED',
  SPEECH = 'SPEECH',
  OTHER = 'OTHER',
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
  ruleAnnotations: Record<string, RuleAnnotation>;
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
