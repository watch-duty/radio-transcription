import {
  type Annotation,
  AnnotationType,
  type EvaluationAnnotationData,
  type TranscriptAnnotationData,
} from '@transcription/common';

export function findEvaluationAnnotationData(
  annotations: Annotation[]
): EvaluationAnnotationData | null {
  for (const annotation of annotations) {
    if (annotation.type === AnnotationType.EVALUATION) {
      return annotation as unknown as EvaluationAnnotationData;
    }
  }
  return null;
}

export function findTranscriptAnnotationData(
  annotations: Annotation[]
): TranscriptAnnotationData | null {
  for (const annotation of annotations) {
    if (annotation.type === AnnotationType.TRANSCRIPT) {
      return annotation as unknown as TranscriptAnnotationData;
    }
  }
  return null;
}
