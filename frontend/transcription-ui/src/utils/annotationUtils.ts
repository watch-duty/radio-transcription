import {
  type Annotation,
  AnnotationType,
  AudioClassification,
  type AudioSegment,
  type EvaluationAnnotationData,
  type TranscriptAnnotationData,
  type TranscriptFlagAnnotationData,
  type WaveformAnnotationData,
} from '@transcription/common';

export function findEvaluationAnnotationData(
  annotations: Annotation[]
): EvaluationAnnotationData | null {
  for (const annotation of annotations) {
    if (annotation.type === AnnotationType.EVALUATION) {
      return annotation.data as EvaluationAnnotationData;
    }
  }
  return null;
}

// A segment is an "alert" when its evaluation annotation fired at least one rule
// decision; used to tint the timeline overview.
export function segmentHasAlert(segment: AudioSegment): boolean {
  const evaluation = findEvaluationAnnotationData(segment.annotations);
  return !!evaluation && evaluation.decisions.length > 0;
}

export function findTranscriptAnnotationData(
  annotations: Annotation[]
): TranscriptAnnotationData | null {
  for (const annotation of annotations) {
    if (annotation.type === AnnotationType.TRANSCRIPT) {
      return annotation.data as TranscriptAnnotationData;
    }
  }
  return null;
}

export function findWaveformAnnotationData(
  annotations: Annotation[]
): WaveformAnnotationData | null {
  for (const annotation of annotations) {
    if (annotation.type === AnnotationType.WAVEFORM) {
      return annotation.data as WaveformAnnotationData;
    }
  }
  return null;
}

export function findTranscriptFlagAnnotationData(
  annotations: Annotation[]
): TranscriptFlagAnnotationData | null {
  for (const annotation of annotations) {
    if (annotation.type === AnnotationType.TRANSCRIPT_FLAG) {
      return annotation.data as TranscriptFlagAnnotationData;
    }
  }
  return null;
}

// Speech if classified as speech or carrying a transcript (the backend
// transcribes some UNSPECIFIED segments).
export function segmentHasSpeech(segment: AudioSegment): boolean {
  return (
    segment.classification === AudioClassification.SPEECH ||
    !!findTranscriptAnnotationData(segment.annotations)
  );
}
