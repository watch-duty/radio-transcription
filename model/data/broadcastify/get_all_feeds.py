import csv
import sys

from bcfy_api import fetch_all_feeds
from bcfy_types import BroadcastifyFeedGenre

all_feeds = fetch_all_feeds(BroadcastifyFeedGenre.PUBLIC_SAFETY)

# retain only mono mp3 feeds that have archives
#
# as of jan 2026:
#
# - public safety feed count: 6320
# - w/ archives: 6303
# - only mono: 5350
# - only mp3: 5843
# - all constraints: 5347
filtered_feeds = [
    f
    for f in all_feeds
    if f.data_format == "mp3" and f.channel_mode == "mono" and f.archive_feed
]

# print out a CSV
writer = csv.writer(sys.stdout)
writer.writerow(["feedId", "descr", "trim_audio"])
for feed in filtered_feeds:
    writer.writerow([feed.feedId, feed.descr, feed.trim_audio])
