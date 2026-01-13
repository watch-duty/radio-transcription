#!python


import sys

from bcfy_api import fetch_archive_files

if len(sys.argv) < 2:
    print("Usage: python sample_broadcastify_archive.py <feed_id>")
    sys.exit(1)

feed_id = int(sys.argv[1])

# uses the 12-hour "trial" API for now
# archive_days = fetch_archive_days(feed_id)
files = fetch_archive_files(feed_id)
for f in files:
    print(f)
