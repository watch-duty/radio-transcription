import {
  type Annotation,
  AnnotationType,
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
