import uuid
from dataclasses import dataclass, field


@dataclass
class CatalogFeed:
    source: str  # bcfy_feeds | bcfy_calls | openmhz | echo
    source_feed_id: str
    feed_name: str
    description: str = ""
    state: str = ""
    county: str = ""
    county_fips: str = ""
    category: str = "other"
    agency_name: str = ""
    agency_type: str = "unknown"
    service_tags: str = ""

    # Scores (populated during scoring pass)
    keyword_score: float = 0.0
    geographic_risk_score: float = 0.0
    agency_tier_score: float = 0.0
    quality_score: float = 0.5
    composite_priority: float = 0.0
    priority_tier: int = 3

    # Status
    is_encrypted: str = "unknown"
    is_online: str = "unknown"
    listener_count: int | None = None

    # Dedup + cross-platform linkage
    canonical_id: str = ""
    has_duplicate: str = "false"
    duplicate_sources: str = ""
    bcfy_calls_group_id: str = ""  # {sid}-{tgid} when RR ID available

    # Admin
    admin_selected: str = "false"
    admin_notes: str = ""

    # Internal (not in CSV output, used for classification log)
    matched_keywords: list = field(default_factory=list)
    raw_metadata: dict = field(default_factory=dict)

    # Auto-generated
    catalog_id: str = field(default_factory=lambda: str(uuid.uuid4()))
