#!python


import csv
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed

from bcfy_api import fetch_archive_files

if len(sys.argv) < 2:
    print("Usage: python sample_broadcastify_archive.py <feed_id>")
    sys.exit(1)

input_csv = str(sys.argv[1])

# read the input csv. each row is: feed ID, feed name, is audio trimmed
feeds = []
with open(input_csv) as f:
    reader = csv.reader(f)
    reader.__next__()  # skip header row
    for row in reader:
        feed_id, feed_name, is_audio_trimmed = row
        feed_id = int(feed_id)
        is_audio_trimmed = is_audio_trimmed.strip().lower() == "true"
        feeds.append((feed_id, feed_name, is_audio_trimmed))


def fetch_all_archives_for_feed(feed_id) -> list[str]:
    # uses the 12-hour "trial" API for now
    return fetch_archive_files(feed_id)


# use a threadpool to get archive URLs for all of the feeds
with ThreadPoolExecutor(max_workers=10) as executor:
    future_to_feed = {
        executor.submit(fetch_all_archives_for_feed, feed_id): (
            feed_id,
            feed_name,
            is_audio_trimmed,
        )
        for feed_id, feed_name, is_audio_trimmed in feeds
    }

    writer = csv.writer(sys.stdout)
    for future in as_completed(future_to_feed):
        feed_id, feed_name, is_audio_trimmed = future_to_feed[future]

        for url in future.result():
            writer.writerow([feed_id, url, is_audio_trimmed])
