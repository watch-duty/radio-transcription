# Broadcastify data selection

## Selecting useful streams

**Selecting candidate streams**

We only want mono audio files that have a single radio channel stream in them, have archives, and are mp3 encoded. To get a list of those, run:

```sh
python get_all_feeds.py > output.csv
```

A snapshot from 2026-01 is in `all_bcfy_feeds_202601.csv`.

**Selecting candidate files**

```sh
# pass output from above
python fetch_all_broadcastify_archive_urls.py <feeds_csv>
# outputs a new CSV file with the feed ID, URL, and is_audio_trimmed columns
```

A snapshot from 2026-01 is in `archive_urls_sample_20260114_12hrs.csv`.
