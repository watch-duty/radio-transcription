"""Broadcastify Calls catalog source.

NOTE: No working enumeration endpoint found in the public Broadcastify
Calls API (api.bcfy.io/calls/v1/*). Attempted endpoints:

- GET /calls/v1/county_groups/{ctid}       → 404
- GET /calls/v1/systems                    → 404
- GET /calls/v1/groups                     → 404
- GET /calls/v1/group_list/{ctid}          → 404
- POST /calls/v1/county_groups             → 404

Only `GET /calls/v1/group_get/{groupId}` works, but it requires the
groupId a priori (format: {sid}-{tgDec}). Without an enumeration
endpoint, we cannot discover which groups exist.

The existing production collector (backend/.../bcfy_calls_collector.py)
only polls /calls/v1/live/ for real-time calls of *already-known* groups.

To add bcfy_calls coverage to this catalog, we'd need either:
1. A new enumeration endpoint (ask Broadcastify support)
2. A RadioReference trunked system export (separate subscription/data)
3. Scraping the broadcastify.com/calls/ web UI

For now: return an empty list with a clear log message.
"""
import logging
import sys

logger = logging.getLogger(__name__)


def fetch(rate_limiter):
    print(
        "  bcfy_calls: SKIPPED — no catalog enumeration endpoint "
        "available in Broadcastify Calls API. "
        "See sources/bcfy_calls.py for details.",
        file=sys.stderr,
    )
    return []
