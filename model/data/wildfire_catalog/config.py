import re

# ── API URLs ──────────────────────────────────────────────────────────
BCFY_API_BASE = "https://api.bcfy.io"
OPENMHZ_API_BASE = "https://api.openmhz.com"

# ── Rate limits (requests per second) ────────────────────────────────
# Hard cap of 2 QPS per endpoint, per user instruction.
BCFY_QPS = 2
OPENMHZ_QPS = 2

# ── Geographic risk data sources ─────────────────────────────────────
# FEMA NRI URL often serves an HTML landing page; the loader tries this
# first but falls back to USDA WRC (wildfirerisk.org) and then to the
# state heuristic. See geo_risk.py.
FEMA_NRI_URL = (
    "https://hazards.fema.gov/nri/Content/StaticDocuments/"
    "DataDownload/NRI_Table_Counties/NRI_Table_Counties.csv"
)

# ── Keyword lists (pattern, label) ───────────────────────────────────
# Score 1.0 — dedicated fire / wildland keywords
KEYWORDS_HIGH = [
    (re.compile(r"\bfire\b", re.I), "fire"),
    (re.compile(r"\bforestry\b", re.I), "forestry"),
    (re.compile(r"\bwildland\b", re.I), "wildland"),
    (re.compile(r"\bwildfire\b", re.I), "wildfire"),
    (re.compile(r"\bsuppression\b", re.I), "suppression"),
    (re.compile(r"\bbrush\b", re.I), "brush"),
    (re.compile(r"\bcal\s?fire\b", re.I), "calfire"),
    (re.compile(r"\busfs\b", re.I), "usfs"),
    (re.compile(r"\bforest\s+service\b", re.I), "forest_service"),
    (re.compile(r"\bblm\b", re.I), "blm"),
    (re.compile(r"\bdnr\b", re.I), "dnr"),
    (re.compile(r"\bodf\b", re.I), "odf"),
    (re.compile(r"\bdcnr\b", re.I), "dcnr"),
    (re.compile(r"\bmabas\b", re.I), "mabas"),
    (re.compile(r"\bifern\b", re.I), "ifern"),
    (re.compile(r"\bicall\b", re.I), "icall"),
    (re.compile(r"\bitac\b", re.I), "itac"),
    (re.compile(r"\bvfire\b", re.I), "vfire"),
    (re.compile(r"\bfireground\b", re.I), "fireground"),
    (re.compile(r"\bnifc\b", re.I), "nifc"),
    (re.compile(r"\bhotshot\b", re.I), "hotshot"),
    (re.compile(r"\bhelitack\b", re.I), "helitack"),
    (re.compile(r"\bair\s?attack\b", re.I), "air_attack"),
    (re.compile(r"\bair\s?tanker\b", re.I), "air_tanker"),
    (re.compile(r"\bsmokejumper\b", re.I), "smokejumper"),
    (re.compile(r"\bfire.?tac\b", re.I), "fire_tac"),
    (re.compile(r"\bfire.?dispatch\b", re.I), "fire_dispatch"),
    (re.compile(r"\bfire.?talk\b", re.I), "fire_talk"),
    (re.compile(r"\bcommand\b", re.I), "command"),
    (re.compile(r"\btactical\b", re.I), "tactical"),
]

# Score 0.5 — fire-adjacent keywords
KEYWORDS_MEDIUM = [
    (re.compile(r"\bpublic\s+safety\b", re.I), "public_safety"),
    (re.compile(r"\bemergency\b", re.I), "emergency"),
    (re.compile(r"\bems\b", re.I), "ems"),
    (re.compile(r"\brescue\b", re.I), "rescue"),
    (re.compile(r"\bcounty\s+dispatch\b", re.I), "county_dispatch"),
    (re.compile(r"\bmutual\s+aid\b", re.I), "mutual_aid"),
    (re.compile(r"\binterop\b", re.I), "interop"),
]

# ── Agency tier patterns (ordered, first-match → score) ──────────────
AGENCY_TIER_PATTERNS = [
    # Federal wildland — 1.0
    (re.compile(r"\busfs\b|\bforest\s+service\b|\bblm\b|\bnps\b|\busfws\b|\bnifc\b", re.I), 1.0),
    # State forestry — 0.9
    (re.compile(r"\bcal\s?fire\b|\bodf\b|\bdnr\b|\bdcnr\b|\bstate\s+forest", re.I), 0.9),
    # County fire — 0.7
    (re.compile(r"\bcounty\s+fire\b|\bcounty.*\bfire\s+(?:dispatch|dept|department)\b", re.I), 0.7),
    # Municipal fire — 0.6
    (re.compile(r"\bcity.*fire\b|\bfire\s+(?:dept|department)\b|\bfd\b", re.I), 0.6),
    # Mixed / EMS — 0.4
    (re.compile(r"\bems\b|\brescue\b|\bmutual\s+aid\b|\binterop\b|\bpublic\s+safety\b", re.I), 0.4),
]
AGENCY_TIER_DEFAULT = 0.3

# ── Composite score weights ──────────────────────────────────────────
WEIGHT_KEYWORD = 0.30
WEIGHT_GEO_RISK = 0.35
WEIGHT_AGENCY_TIER = 0.20
WEIGHT_QUALITY = 0.15

# ── Tier thresholds ──────────────────────────────────────────────────
TIER_1_THRESHOLD = 0.7
TIER_2_THRESHOLD = 0.4

# ── Fire-relevant service tags (RadioReference / Broadcastify) ───────
FIRE_RELEVANT_TAGS = frozenset({
    "Fire Dispatch",
    "Fire-Tac",
    "Fire-Talk",
    "EMS Dispatch",
    "EMS-Tac",
    "EMS-Talk",
    "Multi-Dispatch",
    "Multi-Tac",
    "Multi-Talk",
    "Interop",
    "Emergency Ops",
})

# Same tags by numeric ID (from GET /common/v1/tags). Lets us filter
# bcfy_calls groups by the `tagId` field without string matching.
FIRE_RELEVANT_TAG_IDS = frozenset({
    1,   # Multi-Dispatch
    3,   # Fire Dispatch  (highest-priority fire)
    4,   # EMS Dispatch
    6,   # Multi-Tac
    8,   # Fire-Tac       (highest-priority fire)
    9,   # EMS-Tac
    11,  # Interop
    22,  # Multi-Talk
    24,  # Fire-Talk
    25,  # EMS-Talk
    29,  # Emergency Ops
})

# ── State wildfire risk heuristic (fallback when FEMA NRI unavailable)
HIGH_RISK_STATES = frozenset({
    "CA", "OR", "WA", "CO", "MT", "ID", "AZ", "NM",
    "NV", "UT", "TX", "FL", "GA", "SC", "NC",
})
HIGH_RISK_SCORE = 0.8
DEFAULT_RISK_SCORE = 0.3

# ── State FIPS → abbreviation (first 2 digits of a 5-digit county FIPS)
STATE_FIPS_TO_ABBREV = {
    "01": "AL", "02": "AK", "04": "AZ", "05": "AR", "06": "CA",
    "08": "CO", "09": "CT", "10": "DE", "11": "DC", "12": "FL",
    "13": "GA", "15": "HI", "16": "ID", "17": "IL", "18": "IN",
    "19": "IA", "20": "KS", "21": "KY", "22": "LA", "23": "ME",
    "24": "MD", "25": "MA", "26": "MI", "27": "MN", "28": "MS",
    "29": "MO", "30": "MT", "31": "NE", "32": "NV", "33": "NH",
    "34": "NJ", "35": "NM", "36": "NY", "37": "NC", "38": "ND",
    "39": "OH", "40": "OK", "41": "OR", "42": "PA", "44": "RI",
    "45": "SC", "46": "SD", "47": "TN", "48": "TX", "49": "UT",
    "50": "VT", "51": "VA", "53": "WA", "54": "WV", "55": "WI",
    "56": "WY", "60": "AS", "66": "GU", "69": "MP", "72": "PR",
    "78": "VI",
}

# ── Cache directory ──────────────────────────────────────────────────
CACHE_DIR = "cache"
