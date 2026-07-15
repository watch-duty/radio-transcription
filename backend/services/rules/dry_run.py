from datetime import UTC, datetime, timedelta

from pydantic import BaseModel, Field

from backend.pipeline.common.evaluation.annotations import (
    TextMatchSpan,
)
from backend.pipeline.common.rules.models import Rule, RuleBase
from backend.pipeline.evaluation.rules_evaluation.evaluator import (
    StaticTextEvaluator,
)
from backend.pipeline.storage.audio_segment_store import AudioSegmentStore
from backend.services.audio_segments.models import AudioSegment

_DB_PAGINATION_CHUNK_SIZE = 500


class DryRunRequest(BaseModel):
    """Request payload for dry-running a prospective rule against
    historical transcripts.
    """

    rule: RuleBase
    evaluation_limit: int = Field(
        default=5000,
        ge=1,
        le=10000,
        description=(
            "The maximum number of recent historical transcripts "
            "to evaluate for hit rate calculation."
        ),
    )
    max_examples: int = Field(
        default=50,
        ge=1,
        le=100,
        description=(
            "The maximum number of matching transcripts to return "
            "in the response payload."
        ),
    )
    feed_ids: list[str] | None = Field(
        default=None,
        description=(
            "List of feed IDs to restrict the dry-run to. "
            "If omitted, evaluates globally across all feeds."
        ),
    )
    # TODO(jake): Tag search is not supported at the moment.
    # https://linear.app/watchduty/issue/GOO-794
    tag: str | None = None
    days_lookback: int | None = None


class DryRunMatchExample(BaseModel):
    """A matched transcript example to display in the UI."""

    audio_segment_id: str
    feed_id: str
    text: str
    matched_spans: list[TextMatchSpan]


class DryRunResponse(BaseModel):
    """Response containing hit rate statistics and matched examples."""

    hit_count: int
    total_evaluated: int
    examples: list[DryRunMatchExample]


def _extract_transcript_text(segment: AudioSegment) -> str:
    """Extract transcript text from an audio segment's annotations."""
    return next(
        (
            getattr(ann.data, "text", "")
            for ann in segment.annotations
            if ann.type == "TRANSCRIPT"
        ),
        "",
    )


async def execute_dry_run(
    request: DryRunRequest, audio_store: AudioSegmentStore
) -> DryRunResponse:
    """Execute the full dry-run process chunk-by-chunk."""
    start_time = None
    if request.days_lookback is not None:
        start_time = datetime.now(UTC) - timedelta(days=request.days_lookback)

    rule = Rule(rule_id="dry-run", **request.rule.model_dump())
    evaluator = StaticTextEvaluator()

    examples: list[DryRunMatchExample] = []
    hit_count = 0
    total_evaluated = 0
    next_token = None

    # Loop through audio segments and evaluate chunk-by-chunk to save memory
    while total_evaluated < request.evaluation_limit:
        chunk_limit = min(
            _DB_PAGINATION_CHUNK_SIZE,
            request.evaluation_limit - total_evaluated,
        )
        paginated = await audio_store.list_audio_segments(
            feed_ids=request.feed_ids,
            limit=chunk_limit,
            next_token=next_token,
            start_time=start_time,
        )

        for seg in paginated.segments:
            if total_evaluated >= request.evaluation_limit:
                break

            # Find the transcript text for this segment
            transcript_text = _extract_transcript_text(seg)

            if not transcript_text:
                continue

            total_evaluated += 1

            # Evaluate immediately
            result = evaluator.evaluate_ruleset(
                [rule], transcript_text, str(seg.feed_id)
            )

            if not result["is_flagged"]:
                continue

            hit_count += 1

            if len(examples) < request.max_examples:
                annotation = result["rule_annotations"].get(rule.rule_id)
                matched_spans = []
                if annotation and annotation.text_match:
                    matched_spans = annotation.text_match

                examples.append(
                    DryRunMatchExample(
                        audio_segment_id=str(seg.id),
                        feed_id=str(seg.feed_id),
                        text=transcript_text,
                        matched_spans=matched_spans,
                    )
                )

        next_token = paginated.next_token
        if not next_token:
            break

    return DryRunResponse(
        hit_count=hit_count,
        total_evaluated=total_evaluated,
        examples=examples,
    )
