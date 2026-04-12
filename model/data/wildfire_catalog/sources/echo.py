import csv
import logging
import os
import sys

from catalog_types import CatalogFeed

logger = logging.getLogger(__name__)

_SCRIPT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_ECHO_CSV = os.path.join(
    _SCRIPT_DIR, "..", "echo", "all_echo_mono_streams.csv"
)
_STATE_MAP_CSV = os.path.join(_SCRIPT_DIR, "echo_device_states.csv")


def _load_state_map():
    states = {}
    with open(_STATE_MAP_CSV) as f:
        reader = csv.DictReader(f)
        for row in reader:
            name = row["device_name"].strip()
            st = row["state"].strip()
            if name and st:
                states[name] = st
    return states


def fetch():
    state_map = _load_state_map()
    print(f"  echo: loaded state map ({len(state_map)} devices)", file=sys.stderr)

    feeds = []
    seen = set()

    with open(_ECHO_CSV) as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) < 2:
                continue
            device_name = row[0].strip().strip('"')
            prefix = row[1].strip().strip('"')
            key = (device_name, prefix)
            if key in seen:
                continue
            seen.add(key)

            state = state_map.get(device_name, "")
            if not state and device_name != "echo_mobile":
                logger.warning("No state mapping for echo device: %s", device_name)

            feed = CatalogFeed(
                source="echo",
                source_feed_id=f"{device_name}/{prefix}",
                feed_name=prefix.replace("_", " "),
                description=f"Echo device: {device_name}, channel: {prefix}",
                state=state,
                quality_score=0.5,
                raw_metadata={"device_name": device_name, "prefix": prefix},
            )
            feed.canonical_id = f"echo:{device_name}:{prefix}"
            feeds.append(feed)

    print(f"  echo: {len(feeds)} feeds from CSV", file=sys.stderr)
    return feeds
