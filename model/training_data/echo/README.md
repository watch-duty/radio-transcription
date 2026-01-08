# Echo training data selection

## Selecting useful streams

**Selecting candidate streams**

We only want mono audio files that have a single radio channel stream in them. To get a list of those, run the following in the `watch-duty/watchduty-echo` GitHub repo:

```sh
for f in site_configs/rtl_airband-*; do python make_mono_echo_feeds_csv.py $f; done > output.csv
```

A snapshot from 2026-01 is in `all_echo_mono_streams.csv`.

**Selecting candidate files**

* We only want files that are over a certain size (indicating they likely have some speech audio).
* Bias towards a broader distribution of streams, versus broader coverage of a single stream.

The following script pulls size data for all echo recordings from a CSV like above, for a specific date range, and filters them down to only those over a certain file size:

```sh
# only those > 10kb
python s3_file_scanner.py --start-date 20251208 --end-date 20251209 --input-csv all_echo_mono_streams.csv --min-size 10000
```
