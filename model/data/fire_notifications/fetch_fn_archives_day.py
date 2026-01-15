#!/usr/bin/env python3
"""
Scans FN archives for files on a specific archive date, and over a minimum file size.

Usage:
    python fetch_fn_archives_day.py --date 20240101 --min-size 10000
"""

import argparse
import csv
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

from fn_api import list_fn_dir, make_fn_audio_url
from fn_types import FNFile

TOPLEVEL_DIRS = [
    "RECORDINGS",
    "TEXAS",
    "COLORADO",
    "EAST",
]

ARCHIVE_DIR_NAME = "Archive"

def parse_args():
    parser = argparse.ArgumentParser(
        description="Scan FireNotifications for mp3 files, output CSV with stream name, URL, size"
    )
    parser.add_argument(
        "--date",
        default=datetime.strftime(datetime.now() - timedelta(days=5), "%Y%m%d"),
        help="Date (YYYYMMDD)",
    )
    parser.add_argument(
        "--min-size",
        type=int,
        default=100 * 1024,
        help="Minimum file size in bytes (default: 100k)",
    )
    parser.add_argument(
        "--output-csv",
        default=None,
        help="Output CSV file (default: stdout)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=10,
        help="Number of parallel workers for HTTP requests (default: 10)",
    )
    return parser.parse_args()

def list_archive_urls_for_stream(path: str, date_dir: str) -> list[FNFile]:
    stream_dir_results = list_fn_dir(path)
    if not any(d.endswith(ARCHIVE_DIR_NAME) for d in stream_dir_results.dirs):
        print(f"WARNING: no {ARCHIVE_DIR_NAME}/ dir in {path}. skipping.", file=sys.stderr)

    date_dir_results = list_fn_dir(f"{path}/{ARCHIVE_DIR_NAME}/{date_dir}")
    return date_dir_results.files

def main():
    args = parse_args()

    # Generate a list of candidate streams.
    stream_dirs = []
    for dir in TOPLEVEL_DIRS:
        result = list_fn_dir(dir)
        stream_dirs.extend(result.dirs)

    print(f"Found {len(stream_dirs)} streams", file=sys.stderr)

    checked = 0
    found = 0
    all_files: list[FNFile] = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = [
            executor.submit(list_archive_urls_for_stream, path, args.date)
            for path in stream_dirs[:5]
        ]

        for future in as_completed(futures):
            checked += 1

            if checked % 1000 == 0:
                print(
                    f"Progress: {checked}/{len(stream_dirs)} streams checked, {found} found files, {len(all_files)} > min size",
                    file=sys.stderr,
                )

            result = future.result()
            found += len(result)
            all_files.extend([f for f in result if f.size_bytes > args.min_size])

    print(f"Scan complete: {found} files, {len(all_files)} matching criteria", file=sys.stderr)

    # Write output
    output_file = (
        open(args.output_csv, "w", newline="") if args.output_csv else sys.stdout
    )
    try:
        writer = csv.writer(output_file)
        for fn_file in all_files:
            stream_name = "/".join(fn_file.path.split("/")[:2])
            url = make_fn_audio_url(fn_file.path)
            writer.writerow([stream_name, url, fn_file.size_bytes])
    finally:
        if args.output_csv:
            output_file.close()


if __name__ == "__main__":
    main()
