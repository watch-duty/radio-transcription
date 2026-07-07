import logging
import sys
from collections import defaultdict

logger = logging.getLogger(__name__)


def find_duplicates(feeds):
    """Assign has_duplicate and duplicate_sources via canonical_id grouping.

    Mutates feeds in-place. No fuzzy matching — canonical_id only.
    """
    groups = defaultdict(list)
    for f in feeds:
        if f.canonical_id:
            groups[f.canonical_id].append(f)

    cross_source_dups = 0
    for canonical_id, group in groups.items():
        if len(group) <= 1:
            continue

        sources = sorted({f.source for f in group})
        if len(sources) <= 1:
            continue  # same source, not a cross-platform duplicate

        cross_source_dups += 1
        for f in group:
            other_sources = [s for s in sources if s != f.source]
            f.has_duplicate = "true"
            f.duplicate_sources = ",".join(other_sources)

    print(
        f"  dedup: {cross_source_dups} cross-source duplicate canonical_ids found",
        file=sys.stderr,
    )
