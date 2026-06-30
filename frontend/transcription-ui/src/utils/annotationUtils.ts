import {
  type Annotation,
  AnnotationType,
  AudioClassification,
  type AudioSegment,
  type EvaluationAnnotationData,
  type TranscriptAnnotationData,
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

// Speech if classified as speech or carrying a transcript (the backend
// transcribes some UNSPECIFIED segments).
export function segmentHasSpeech(segment: AudioSegment): boolean {
  return (
    segment.classification === AudioClassification.SPEECH ||
    !!findTranscriptAnnotationData(segment.annotations)
  );
}
