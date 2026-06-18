"""Filters coverage reports out of test stdout streams.

Basic script to stream test output to the console line-by-line while extracting
the coverage report tables and saving them to a separate output file.
It supports both pytest and vitest coverage report patterns.
"""

import argparse
import sys


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Filter coverage report from stdout to a file."
    )
    parser.add_argument(
        "-o",
        "--output",
        required=True,
        help="The output file path to write the coverage report to.",
    )
    args = parser.parse_args()

    is_cov = False
    cov = []

    for line in sys.stdin:
        is_pytest_cov = (
            "================================ tests coverage ================================"
            in line
        )
        is_vitest_cov = (
            "-------------------|---------|----------|---------|---------|-------------------"
            in line
        )

        if is_pytest_cov or is_vitest_cov:
            is_cov = True

        if is_cov:
            cov.append(line)
        else:
            sys.stdout.write(line)
            sys.stdout.flush()

    with open(args.output, "w", encoding="utf-8") as f:
        f.writelines(cov)


if __name__ == "__main__":
    main()
