#!/usr/bin/env python3
"""
Scans S3 for echo recording files based on device/prefix combinations from a CSV,
filtering by date range and minimum file size.

Usage:
    python s3_file_scanner.py --start-date 2024-01-01 --end-date 2024-01-31 --min-size 10000
"""

import argparse
import csv
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

import boto3
from botocore.exceptions import ClientError

BUCKET_NAME = "echo-recordings"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Scan S3 for echo recording files and output paths with sizes"
    )
    parser.add_argument(
        "--input-csv",
        default="all_echo_mono_streams.csv",
        help="Input CSV file with device_name,filename_prefix columns (default: all_echo_mono_streams.csv)",
    )
    parser.add_argument(
        "--start-date",
        required=True,
        type=lambda s: datetime.strptime(s, "%Y%m%d"),
        help="Start date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--end-date",
        required=True,
        type=lambda s: datetime.strptime(s, "%Y%m%d"),
        help="End date (YYYY-MM-DD)",
    )
    parser.add_argument(
        "--min-size",
        type=int,
        default=0,
        help="Minimum file size in bytes (default: 0)",
    )
    parser.add_argument(
        "--output-csv",
        default=None,
        help="Output CSV file (default: stdout)",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=50,
        help="Number of parallel workers for S3 requests (default: 50)",
    )
    return parser.parse_args()


def read_input_csv(filepath):
    """Read the input CSV and return list of (device_name, filename_prefix) tuples."""
    entries = []
    with open(filepath, "r") as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) >= 2:
                device_name = row[0].strip().strip('"')
                filename_prefix = row[1].strip().strip('"')
                entries.append((device_name, filename_prefix))
    return entries


def generate_s3_paths(entries, start_date, end_date):
    """
    Generate all candidate S3 paths for the given entries and date range.

    S3 path format: s3://echo-recordings/<device_name>/<date>/<filename_prefix>_<date>_<hour>.mp3
    where date is YYYYMMDD and hour is 00-23
    """
    paths = []
    current_date = start_date

    while current_date <= end_date:
        date_str = current_date.strftime("%Y%m%d")

        for device_name, filename_prefix in entries:
            for hour in range(24):
                hour_str = f"{hour:02d}"
                filename = f"{filename_prefix}_{date_str}_{hour_str}.mp3"
                s3_key = f"{device_name}/{date_str}/{filename}"
                full_path = f"https://{BUCKET_NAME}.s3.us-east-1.amazonaws.com/{s3_key}"
                paths.append((full_path, s3_key))

        current_date += timedelta(days=1)

    return paths


def check_file_exists_and_size(s3_client, s3_key):
    """Check if a file exists in S3 and return its size, or None if it doesn't exist."""
    try:
        response = s3_client.head_object(Bucket=BUCKET_NAME, Key=s3_key)
        return response["ContentLength"]
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return None
        raise


def main():
    args = parse_args()

    # Read input CSV
    entries = read_input_csv(args.input_csv)
    print(f"Read {len(entries)} device/prefix combinations from {args.input_csv}", file=sys.stderr)

    # Generate all candidate S3 paths
    paths = generate_s3_paths(entries, args.start_date, args.end_date)
    num_days = (args.end_date - args.start_date).days + 1
    print(
        f"Generated {len(paths)} candidate paths ({len(entries)} streams x {num_days} days x 24 hours)",
        file=sys.stderr,
    )

    # Set up S3 client
    s3_client = boto3.client("s3")

    # Check files in parallel
    results = []
    checked = 0
    found = 0

    print("Scanning S3 for files...", file=sys.stderr)

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_to_path = {
            executor.submit(check_file_exists_and_size, s3_client, s3_key): (full_path, s3_key)
            for full_path, s3_key in paths
        }

        for future in as_completed(future_to_path):
            full_path, s3_key = future_to_path[future]
            checked += 1

            if checked % 10000 == 0:
                print(f"Progress: {checked}/{len(paths)} checked, {found} found", file=sys.stderr)

            try:
                size = future.result()
                if size is not None and size >= args.min_size:
                    results.append((full_path, size))
                    found += 1
            except Exception as e:
                print(f"Error checking {s3_key}: {e}", file=sys.stderr)

    print(f"Scan complete: {found} files found matching criteria", file=sys.stderr)

    # Sort results by path for consistent output
    results.sort(key=lambda x: x[0])

    # Write output
    output_file = open(args.output_csv, "w", newline="") if args.output_csv else sys.stdout
    try:
        writer = csv.writer(output_file)
        for full_path, size in results:
            writer.writerow([full_path, size])
    finally:
        if args.output_csv:
            output_file.close()


if __name__ == "__main__":
    main()
