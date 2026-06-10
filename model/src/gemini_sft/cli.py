"""Command-line entrypoint for the packaged Gemini SFT workflow."""

from __future__ import annotations

import argparse
import logging

from gemini_sft.evaluate import evaluate
from gemini_sft.prepare import prepare
from gemini_sft.tune import tune


def build_parser() -> argparse.ArgumentParser:
    """Build the ``gemini-sft`` CLI parser."""
    parser = argparse.ArgumentParser(
        prog="gemini-sft",
        description="Gemini supervised fine-tuning workflow for radio transcription.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare_parser = subparsers.add_parser(
        "prepare", help="Prepare and preflight Gemini SFT artifacts"
    )
    prepare_parser.add_argument("--config", required=True, help="Run TOML path")
    prepare_parser.set_defaults(func=prepare)

    tune_parser = subparsers.add_parser(
        "tune", help="Submit or resume a Gemini SFT tuning job"
    )
    tune_parser.add_argument("--config", required=True, help="Run TOML path")
    tune_parser.add_argument(
        "--confirm",
        action="store_true",
        help="Skip interactive paid-run confirmation",
    )
    tune_parser.set_defaults(func=tune)

    eval_parser = subparsers.add_parser(
        "eval", help="Batch-infer and score a Gemini SFT run"
    )
    eval_parser.add_argument("--config", required=True, help="Run TOML path")
    eval_parser.add_argument(
        "--base-only",
        action="store_true",
        help="Evaluate only the base model even when a tuned endpoint exists",
    )
    eval_parser.set_defaults(func=evaluate)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the CLI and return a process-style exit code."""
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    parser = build_parser()
    args = parser.parse_args(argv)
    return int(args.func(args))
