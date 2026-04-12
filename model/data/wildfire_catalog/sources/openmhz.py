import logging
import sys

from curl_cffi import requests as curl_requests
from tenacity import retry, stop_after_attempt, wait_exponential

import cache
from catalog_types import CatalogFeed
from config import FIRE_RELEVANT_TAGS, OPENMHZ_API_BASE
from scoring import score_keywords

logger = logging.getLogger(__name__)

_CACHE_SOURCE = "openmhz"
_TIMEOUT = 30
_IMPERSONATE = "chrome"  # bypass Cloudflare TLS fingerprint


@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
def _get(url, rate_limiter):
    rate_limiter.wait()
    resp = curl_requests.get(url, timeout=_TIMEOUT, impersonate=_IMPERSONATE)
    resp.raise_for_status()
    return resp.json()


def _fetch_systems(rate_limiter):
    cached = cache.get(_CACHE_SOURCE, "systems")
    if cached is not None:
        return cached

    data = _get(f"{OPENMHZ_API_BASE}/systems", rate_limiter)
    systems = data.get("systems", []) if isinstance(data, dict) else data
    cache.put(_CACHE_SOURCE, "systems", systems)
    return systems


def _fetch_talkgroups(short_name, rate_limiter):
    cache_key = f"talkgroups_{short_name}"
    cached = cache.get(_CACHE_SOURCE, cache_key)
    if cached is not None:
        return cached

    data = _get(
        f"{OPENMHZ_API_BASE}/{short_name}/talkgroups", rate_limiter
    )
    # OpenMHZ returns {"talkgroups": {tg_num_str: tg_dict, ...}}
    talkgroups = data.get("talkgroups", {}) if isinstance(data, dict) else {}
    cache.put(_CACHE_SOURCE, cache_key, talkgroups)
    return talkgroups


_FIRE_CATEGORIES = frozenset({
    "fire", "fire dispatch", "fire-tac", "fire-talk",
})


def _is_fire_relevant(alpha, description, tag, category):
    """Return True if a talkgroup is fire-relevant by metadata or keywords.

    A talkgroup is kept if any of:
      1. tag is in FIRE_RELEVANT_TAGS (Fire Dispatch, Fire-Tac, EMS, Multi,
         Interop, Emergency Ops)
      2. category matches a fire-category (case-insensitive)
      3. alpha or description contains a fire-relevant high-confidence keyword

    This catches talkgroups that lack tag/category metadata but have
    clear fire naming (e.g., alpha="Fire Dispatch" with no tag field).
    """
    if tag and tag in FIRE_RELEVANT_TAGS:
        return True
    if category and category.lower() in _FIRE_CATEGORIES:
        return True
    # Keyword scan of alpha + description
    text = f"{alpha} {description}"
    score, _ = score_keywords(text)
    return score > 0


def fetch(rate_limiter):
    systems = _fetch_systems(rate_limiter)
    print(f"  openmhz: {len(systems)} systems found", file=sys.stderr)

    feeds = []
    total_talkgroups = 0

    for i, sys_info in enumerate(systems, 1):
        short_name = sys_info.get("shortName") or sys_info.get("short_name", "")
        sys_name = sys_info.get("name", "")
        sys_state = sys_info.get("state", "")
        sys_city = sys_info.get("city", "")
        sys_desc = sys_info.get("description", "")
        sys_rr_id = (
            sys_info.get("systemId")
            or sys_info.get("system_id")
            or sys_info.get("rrSystemId", "")
        )

        if not short_name:
            continue

        if i % 50 == 0 or i == len(systems):
            print(
                f"  openmhz: system {i}/{len(systems)}: {short_name} "
                f"(kept {len(feeds)}/{total_talkgroups} so far)",
                file=sys.stderr,
            )

        try:
            talkgroups = _fetch_talkgroups(short_name, rate_limiter)
        except Exception as e:
            logger.warning(
                "Failed to fetch talkgroups for %s: %s", short_name, e
            )
            continue

        # talkgroups is a dict {tg_num_str: tg_dict}
        if isinstance(talkgroups, list):
            tg_items = [(tg.get("num", ""), tg) for tg in talkgroups]
        else:
            tg_items = list(talkgroups.items())

        for tg_num, tg in tg_items:
            total_talkgroups += 1
            num = tg.get("num", tg_num)
            alpha = tg.get("alpha") or tg.get("alpha_tag", "")
            description = tg.get("description", "")
            tag = tg.get("tag", "")
            category = tg.get("category", "")
            group = tg.get("group", "")

            # Pre-filter: only keep fire-relevant talkgroups
            if not _is_fire_relevant(alpha, description, tag, category):
                continue

            text_parts = [alpha, description, tag, category, group]
            combined_name = " | ".join(p for p in text_parts if p)

            # Build canonical_id + bcfy_calls_group_id if RadioReference
            # system ID is available on the system. As of 2026-04, OpenMHZ's
            # /systems endpoint does NOT expose RR system IDs (only shortName,
            # name, city, state, county). So these remain empty for all
            # OpenMHZ entries — admins can fill them in manually when they
            # know the RR mapping, enabling cross-platform pipeline polling
            # via /calls/v1/live/{sid}-{tgDec}.
            canonical_id = ""
            bcfy_calls_group_id = ""
            if sys_rr_id and num:
                canonical_id = f"rr:{sys_rr_id}:{num}"
                bcfy_calls_group_id = f"{sys_rr_id}-{num}"

            feed = CatalogFeed(
                source="openmhz",
                source_feed_id=f"{short_name}:{num}",
                feed_name=alpha or combined_name or f"TG {num}",
                description=combined_name[:500],
                state=sys_state,
                service_tags=tag,
                canonical_id=canonical_id,
                bcfy_calls_group_id=bcfy_calls_group_id,
                raw_metadata={
                    "short_name": short_name,
                    "system_name": sys_name,
                    "system_description": sys_desc,
                    "city": sys_city,
                    "talkgroup_num": num,
                    "alpha": alpha,
                    "description": description,
                    "tag": tag,
                    "category": category,
                    "group": group,
                    "system_rr_id": sys_rr_id,
                },
            )
            feeds.append(feed)

    print(
        f"  openmhz: kept {len(feeds)} fire-relevant talkgroups out of "
        f"{total_talkgroups} total (filtered by tag/category/keywords)",
        file=sys.stderr,
    )
    return feeds
