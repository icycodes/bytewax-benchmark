#!/usr/bin/env python3
"""Entry point for the fraud detection dataflow."""

import argparse
import sys
import os

# Add project root to Python path so fraud_detector can be imported
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fraud_detector import build_flow
from bytewax.testing import run_main


def main():
    parser = argparse.ArgumentParser(
        description="Fraud detection using Bytewax stateful stream processing"
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to input JSONlines file",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Path to output JSONlines file",
    )
    args = parser.parse_args()

    flow = build_flow(args.input, args.output)
    run_main(flow)


if __name__ == "__main__":
    main()