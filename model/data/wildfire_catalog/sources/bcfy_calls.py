"""Broadcastify Calls catalog source.

Two-phase enumeration using the official (under-development) Calls
Management endpoints:

1. **Public playlists** (`GET /calls/v1/playlists_public` + per-playlist
   `GET /calls/v1/playlist_get/{uuid}`) — human-curated groupings,
   high signal, ~224 API calls total.

2. **Geographic crawl** (`POST /calls/v1/groups_ctid/{ctid}` per county)
   — systematic coverage. Filter locally by the fire-relevant tagIds
   enumerated in `GET /common/v1/tags`. ~3,200 API calls, ~27 min at
   2 QPS.

Groups are deduplicated by `groupId` across phases. Each surviving
group becomes one `CatalogFeed` with `source="bcfy_calls"` and
`source_feed_id=groupId` (matching the production collector contract).
"""
import logging
import sys

import cache
from catalog_types import CatalogFeed
from config import BCFY_API_BASE, FIRE_RELEVANT_TAG_IDS
from sources.bcfy_auth import bcfy_get, bcfy_post

logger = logging.getLogger(__name__)

_CACHE_SOURCE = "bcfy_calls"
_COMMON_CACHE_SOURCE = "bcfy_common"

# Fallback fire keywords when tagId is missing/unknown
_FALLBACK_FIRE_KEYWORDS = (
    "fire", "forestry", "wildland", "wildfire", "brush",
    "blm", "usfs", "calfire", "cal fire", "nifc", "dnr",
    "odf", "dcnr", "mabas", "vfire", "ifern",
)


def _fetch_states(rate_limiter):
    cached = cache.get(_COMMON_CACHE_SOURCE, "states")
    if cached is not None:
        return cached
    data = bcfy_get(f"{BCFY_API_BASE}/common/v1/states/1", rate_limiter)
    cache.put(_COMMON_CACHE_SOURCE, "states", data)
    return data


def _fetch_counties(stid, rate_limiter):
    key = f"counties_{stid}"
    cached = cache.get(_COMMON_CACHE_SOURCE, key)
    if cached is not None:
        return cached
    data = bcfy_get(
        f"{BCFY_API_BASE}/common/v1/counties/{stid}", rate_limiter
    )
    cache.put(_COMMON_CACHE_SOURCE, key, data)
    return data


def _fetch_public_playlists(rate_limiter):
    cached = cache.get(_CACHE_SOURCE, "playlists_public")
    if cached is not None:
        return cached
    data = bcfy_get(
        f"{BCFY_API_BASE}/calls/v1/playlists_public", rate_limiter
    )
    cache.put(_CACHE_SOURCE, "playlists_public", data)
    return data


def _fetch_playlist(uuid, rate_limiter):
    key = f"playlist_{uuid}"
    cached = cache.get(_CACHE_SOURCE, key)
    if cached is not None:
        return cached
    data = bcfy_get(
        f"{BCFY_API_BASE}/calls/v1/playlist_get/{uuid}", rate_limiter
    )
    cache.put(_CACHE_SOURCE, key, data)
    return data


def _fetch_county_groups(ctid, rate_limiter):
    key = f"groups_ctid_{ctid}"
    cached = cache.get(_CACHE_SOURCE, key)
    if cached is not None:
        return cached
    # POST with no tagId filter — fetch everything, filter locally.
    # Filtering at API level would require 11 separate calls per county
    # (one per fire-relevant tag), making the crawl ~5 hours.
    data = bcfy_post(
        f"{BCFY_API_BASE}/calls/v1/groups_ctid/{ctid}",
        rate_limiter,
        data={},
    )
    cache.put(_CACHE_SOURCE, key, data)
    return data


def _is_fire_relevant(group):
    """Return True if a group should be included in the fire catalog."""
    try:
        tag_id = int(group.get("tagId", -1))
    except (ValueError, TypeError):
        tag_id = -1

    if tag_id in FIRE_RELEVANT_TAG_IDS:
        return True

    # Fallback: keyword match on group name/description when tag is
    # missing, deprecated, or unexpected.
    text = " ".join(
        str(group.get(k, "")) for k in
        ("tgCname", "tgDescr", "tgDisplay", "sName", "tagDescr")
    ).lower()
    return any(k in text for k in _FALLBACK_FIRE_KEYWORDS)


def _is_fire_playlist(name):
    n = (name or "").lower()
    return any(k in n for k in (
        "fire", "forestry", "wildland", "wildfire", "blm",
        "usfs", "cal fire", "calfire", "nifc", "dnr",
    ))


def _merge(acc, groupId, **kwargs):
    """Merge group metadata into the accumulator, preserving richer fields."""
    existing = acc.setdefault(groupId, {"groupId": groupId})
    for k, v in kwargs.items():
        if v and not existing.get(k):
            existing[k] = v


def _collect_playlists(rate_limiter, acc):
    """Phase A: collect groups from public playlists."""
    print("  bcfy_calls: Phase A — public playlists", file=sys.stderr)
    try:
        playlists = _fetch_public_playlists(rate_limiter)
    except Exception as e:
        logger.warning("Failed to fetch public playlists: %s", e)
        return

    if not isinstance(playlists, list):
        logger.warning("playlists_public unexpected shape: %s", type(playlists).__name__)
        return

    print(f"  bcfy_calls: {len(playlists)} public playlists", file=sys.stderr)

    fire_pl_count = 0
    for i, pl in enumerate(playlists, 1):
        uuid = pl.get("uuid")
        name = pl.get("name", "")
        if not uuid:
            continue
        pl_is_fire = _is_fire_playlist(name)
        if pl_is_fire:
            fire_pl_count += 1

        try:
            detail = _fetch_playlist(uuid, rate_limiter)
        except Exception as e:
            logger.warning("Failed to fetch playlist %s: %s", uuid, e)
            continue

        for g in detail.get("groups", []):
            gid = g.get("groupId")
            if not gid:
                continue
            _merge(
                acc,
                gid,
                from_playlist=True,
                playlist_is_fire=pl_is_fire,
                playlist_name=name,
            )

        if i % 50 == 0 or i == len(playlists):
            print(
                f"  bcfy_calls: playlist {i}/{len(playlists)} "
                f"({len(acc)} unique groups collected)",
                file=sys.stderr,
            )

    print(
        f"  bcfy_calls: Phase A complete — {len(acc)} unique groups "
        f"(of which {fire_pl_count} playlists were fire-named)",
        file=sys.stderr,
    )


def _collect_county_groups(rate_limiter, acc):
    """Phase B: geographic crawl of all US counties."""
    print("  bcfy_calls: Phase B — geographic crawl", file=sys.stderr)

    states = _fetch_states(rate_limiter)
    if isinstance(states, dict):
        states = states.get("states", states.get("data", []))
    if not isinstance(states, list):
        logger.warning("states unexpected shape")
        return

    print(f"  bcfy_calls: {len(states)} states", file=sys.stderr)

    total_counties = 0
    total_groups_seen = 0

    for si, state in enumerate(states, 1):
        stid = state.get("stid")
        state_name = state.get("state_name", "")
        state_code = state.get("state_code", "")
        if not stid:
            continue

        try:
            counties = _fetch_counties(stid, rate_limiter)
        except Exception as e:
            logger.warning("Failed to fetch counties for %s: %s", state_name, e)
            continue

        if not isinstance(counties, list):
            continue

        total_counties += len(counties)

        if si % 5 == 0 or si == len(states):
            print(
                f"  bcfy_calls: state {si}/{len(states)}: "
                f"{state_name} ({len(counties)} counties, "
                f"{len(acc)} groups in accumulator)",
                file=sys.stderr,
            )

        for county in counties:
            ctid = county.get("ctid")
            county_name = county.get("county_name", "")
            if not ctid:
                continue

            try:
                groups = _fetch_county_groups(ctid, rate_limiter)
            except Exception as e:
                logger.warning(
                    "Failed to fetch groups for ctid=%s (%s): %s",
                    ctid, county_name, e,
                )
                continue

            if not isinstance(groups, list):
                continue

            for g in groups:
                gid = g.get("groupId")
                if not gid:
                    continue
                total_groups_seen += 1

                if not _is_fire_relevant(g):
                    continue

                _merge(
                    acc, gid,
                    tagId=g.get("tagId"),
                    tagDescr=g.get("tagDescr", ""),
                    tgCname=g.get("tgCname", ""),
                    tgDescr=g.get("tgDescr", ""),
                    tgDisplay=g.get("tgDisplay", ""),
                    sid=g.get("sid", ""),
                    tgDec=g.get("tgDec", ""),
                    sName=g.get("sName", ""),
                    state=state_code,
                    county=county_name,
                    ctid=ctid,
                )

    print(
        f"  bcfy_calls: Phase B complete — crawled {total_counties} counties, "
        f"saw {total_groups_seen} total groups, "
        f"{len(acc)} unique fire-relevant groups in accumulator",
        file=sys.stderr,
    )


def _build_feed(gid, meta):
    """Convert an accumulator entry to a CatalogFeed."""
    name = (
        meta.get("tgDisplay")
        or meta.get("tgCname")
        or meta.get("sName")
        or gid
    )
    descr_parts = [
        meta.get("sName", ""),
        meta.get("tgDescr", ""),
        meta.get("tagDescr", ""),
    ]
    description = " | ".join(p for p in descr_parts if p)

    sid = meta.get("sid", "")
    tg_dec = meta.get("tgDec", "")
    # Canonical ID used across sources for cross-platform dedup.
    canonical_id = f"rr:{sid}:{tg_dec}" if sid and tg_dec else ""

    return CatalogFeed(
        source="bcfy_calls",
        source_feed_id=gid,
        feed_name=str(name)[:200],
        description=description[:500],
        state=meta.get("state", ""),
        county=meta.get("county", ""),
        service_tags=meta.get("tagDescr", ""),
        canonical_id=canonical_id,
        raw_metadata=meta,
    )


def fetch(rate_limiter):
    acc = {}  # groupId → metadata dict
    _collect_playlists(rate_limiter, acc)
    _collect_county_groups(rate_limiter, acc)

    # After both phases: drop playlist-only groups that aren't on a
    # fire-named playlist AND lack tag metadata (we have no evidence
    # they're fire-relevant).
    fire_relevant = {}
    for gid, meta in acc.items():
        has_fire_tag = meta.get("tagId") and int(meta.get("tagId", -1)) in FIRE_RELEVANT_TAG_IDS
        has_fallback_kw = _is_fire_relevant(meta)
        has_fire_playlist = meta.get("playlist_is_fire")
        if has_fire_tag or has_fallback_kw or has_fire_playlist:
            fire_relevant[gid] = meta

    feeds = [_build_feed(gid, meta) for gid, meta in fire_relevant.items()]
    print(
        f"  bcfy_calls: {len(feeds)} fire-relevant groups "
        f"(from {len(acc)} total collected)",
        file=sys.stderr,
    )
    return feeds
