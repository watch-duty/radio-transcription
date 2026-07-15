"""Context preparation utilities for sequential audio transcription."""

from collections import defaultdict
from typing import Any


def add_prior_context_to_manifest(
    rows: list[dict[str, Any]],
    context_turns: int = 8,
) -> list[dict[str, Any]]:
    """Group rows by source_group, sort chronologically, and append prior context.

    Mutates the rows in-place by adding a 'prior_context' key containing a list
    of up to `context_turns` prior transcript texts.

    Args:
        rows: List of canonical row dictionaries.
        context_turns: Maximum number of prior turns to include in the context.

    Returns:
        The mutated list of rows.
    """
    # 1. Group rows by source_group
    grouped_rows = defaultdict(list)
    for row in rows:
        source_group = row.get("source_group") or "unknown_group"
        grouped_rows[source_group].append(row)

    # 2. Process each group and gather all sorted rows
    sorted_rows = []
    for source_group, group in grouped_rows.items():
        # Sort chronologically by original_offset
        group.sort(key=lambda r: float(r.get("original_offset") or 0.0))

        # Attach prior context transcripts
        for i, row in enumerate(group):
            # Fetch up to context_turns preceding rows
            start_idx = max(0, i - context_turns)
            prior_segments = group[start_idx:i]

            # Extract transcript texts
            prior_transcripts = [
                s.get("text") or ""
                for s in prior_segments
                if s.get("text") is not None
            ]
            row["prior_context"] = prior_transcripts

        sorted_rows.extend(group)

    return sorted_rows
