from bisect import bisect_left

from config import (
    AGENCY_TIER_DEFAULT,
    AGENCY_TIER_PATTERNS,
    KEYWORDS_HIGH,
    KEYWORDS_MEDIUM,
    TIER_1_THRESHOLD,
    TIER_2_THRESHOLD,
    WEIGHT_AGENCY_TIER,
    WEIGHT_GEO_RISK,
    WEIGHT_KEYWORD,
    WEIGHT_QUALITY,
)


def score_keywords(text):
    """Return (score, matched_keywords) for the given text."""
    if not text:
        return 0.0, []

    text_lower = text.lower()
    matched = []
    best = 0.0

    for pattern, label in KEYWORDS_HIGH:
        if pattern.search(text_lower):
            matched.append(label)
            best = 1.0

    for pattern, label in KEYWORDS_MEDIUM:
        if pattern.search(text_lower):
            matched.append(label)
            if best < 0.5:
                best = 0.5

    return best, matched


def score_agency_tier(text):
    """Return agency tier score based on name pattern matching."""
    if not text:
        return AGENCY_TIER_DEFAULT

    for pattern, score in AGENCY_TIER_PATTERNS:
        if pattern.search(text):
            return score

    return AGENCY_TIER_DEFAULT


def build_listener_percentiles(feeds):
    """Build a sorted list of listener counts for percentile lookup."""
    counts = sorted(
        f.listener_count for f in feeds
        if f.listener_count is not None and f.listener_count > 0
    )
    return counts


def score_quality(listener_count, sorted_counts):
    """Return quality score as a listener count percentile (0-1)."""
    if listener_count is None or not sorted_counts:
        return 0.5

    pos = bisect_left(sorted_counts, listener_count)
    return pos / len(sorted_counts)


def compute_composite(keyword, geo_risk, agency_tier, quality):
    """Return (composite_score, priority_tier)."""
    composite = (
        keyword * WEIGHT_KEYWORD
        + geo_risk * WEIGHT_GEO_RISK
        + agency_tier * WEIGHT_AGENCY_TIER
        + quality * WEIGHT_QUALITY
    )
    composite = round(composite, 4)

    if composite >= TIER_1_THRESHOLD:
        tier = 1
    elif composite >= TIER_2_THRESHOLD:
        tier = 2
    else:
        tier = 3

    return composite, tier
