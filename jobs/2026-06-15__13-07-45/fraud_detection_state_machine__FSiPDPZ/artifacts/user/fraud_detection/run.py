"""Entry point for the fraud detection Bytewax dataflow.

Usage:
    python run.py --input input.jsonl --output output.jsonl
"""

import argparse
import sys

from bytewax.testing import run_main

from fraud_detection import build_dataflow


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Bytewax fraud detection state machine."
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to input JSONlines file.",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Path to output JSONlines file.",
    )
    args = parser.parse_args()

    flow = build_dataflow(args.input, args.output)
    run_main(flow)


if __name__ == "__main__":
    main()
