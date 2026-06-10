import type { Transcript } from '@transcription/common';

// Single home for how a transcript's evaluation results are interpreted, so
// display changes (e.g. what counts as an alert) live in one place.
export function hasEvaluationAlert(
  evaluationDecisions: Transcript['evaluationDecisions'] | undefined
): boolean {
  return (evaluationDecisions?.length ?? 0) > 0;
}
