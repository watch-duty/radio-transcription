import logging
import sys

import cache
from catalog_types import CatalogFeed
from config import BCFY_API_BASE
from sources.bcfy_auth import bcfy_get

logger = logging.getLogger(__name__)

_ALL_FEEDS_URL = f"{BCFY_API_BASE}/feeds/v1/feeds/"
_STATES_URL = f"{BCFY_API_BASE}/common/v1/states/1"
_COUNTIES_URL_TMPL = f"{BCFY_API_BASE}/common/v1/counties/{{stid}}"

_CACHE_SOURCE = "bcfy_feeds"
_CTID_CACHE_SOURCE = "bcfy_common"


def _load_ctid_state_lookup(rate_limiter):
    """Build ctid → state_abbrev lookup from /common/v1/states + /counties.

    The Broadcastify Feed API returns internal ctid values (e.g., 2290 for
    Perry County, TN) in the `counties` field — NOT 5-digit FIPS codes.
    This lookup lets us derive the correct state abbreviation for each feed.
    """
    # States (cached as a single JSON)
    states = cache.get(_CTID_CACHE_SOURCE, "states")
    if states is None:
        print("  bcfy_feeds: fetching states...", file=sys.stderr)
        states = bcfy_get(_STATES_URL, rate_limiter)
        cache.put(_CTID_CACHE_SOURCE, "states", states)

    # Map stid → state_code
    stid_to_state = {}
    for st in states:
        stid = st.get("stid", "")
        code = st.get("state_code", "")
        if stid and code:
            stid_to_state[str(stid)] = code

    # Counties per state
    ctid_to_state = {}
    for stid, state_code in stid_to_state.items():
        cache_key = f"counties_{stid}"
        counties = cache.get(_CTID_CACHE_SOURCE, cache_key)
        if counties is None:
            counties = bcfy_get(
                _COUNTIES_URL_TMPL.format(stid=stid), rate_limiter
            )
            cache.put(_CTID_CACHE_SOURCE, cache_key, counties)

        if not isinstance(counties, list):
            continue
        for county in counties:
            ctid = str(county.get("ctid", ""))
            name = county.get("county_name", "")
            if ctid:
                ctid_to_state[ctid] = (state_code, name)

    print(
        f"  bcfy_feeds: ctid lookup built "
        f"({len(stid_to_state)} states, {len(ctid_to_state)} counties)",
        file=sys.stderr,
    )
    return ctid_to_state


def fetch(rate_limiter):
    # Step 1: fetch all feeds (unfiltered — scoring handles relevance)
    cached = cache.get(_CACHE_SOURCE, "all_feeds")
    if cached is not None:
        print(
            f"  bcfy_feeds: loaded {len(cached)} feeds from cache",
            file=sys.stderr,
        )
        raw_feeds = cached
    else:
        print("  bcfy_feeds: fetching all feeds (unfiltered)...", file=sys.stderr)
        raw_feeds = bcfy_get(_ALL_FEEDS_URL, rate_limiter)
        cache.put(_CACHE_SOURCE, "all_feeds", raw_feeds)
        print(f"  bcfy_feeds: fetched {len(raw_feeds)} feeds", file=sys.stderr)

    # Step 2: build ctid → state lookup
    ctid_lookup = _load_ctid_state_lookup(rate_limiter)

    # Step 3: convert to CatalogFeed
    feeds = []
    no_state = 0
    for f in raw_feeds:
        ctids = [str(c) for c in (f.get("counties") or [])]

        # Derive state from first ctid that we can map
        state = ""
        county_name = ""
        for ctid in ctids:
            if ctid in ctid_lookup:
                state, county_name = ctid_lookup[ctid]
                break
        if not state and ctids:
            no_state += 1

        feed = CatalogFeed(
            source="bcfy_feeds",
            source_feed_id=str(f["feedId"]),
            feed_name=f.get("descr", ""),
            description=f.get("descr", "")[:500],
            state=state,
            county=county_name,
            # county_fips left empty: Broadcastify `counties` are internal
            # ctids, not real FIPS. geo_risk falls back to state_avg.
            county_fips="",
            is_online="true" if f.get("online") == 1 else "false",
            listener_count=f.get("listeners", 0),
            raw_metadata={**f, "bcfy_ctids": ctids},
        )
        feed.canonical_id = f"bcfy:{f['feedId']}"
        feeds.append(feed)

    if no_state:
        print(
            f"  bcfy_feeds: WARNING {no_state}/{len(feeds)} feeds have "
            f"ctids that don't map to any state",
            file=sys.stderr,
        )
    print(f"  bcfy_feeds: {len(feeds)} total feeds", file=sys.stderr)
    return feeds
