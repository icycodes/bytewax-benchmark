"""
Bytewax Fraud Detection Dataflow
=================================
Reads user events from a JSONlines file and uses a per-user state machine to
detect fraudulent transaction patterns, writing alerts to an output JSONlines
file.

Fraud pattern
-------------
A user triggers a FRAUD_ALERT when they make 3 or more transactions each with
amount >= 1000 within 300 seconds of their most recent login event.

State machine transitions (per user_id)
----------------------------------------
LOGGED_OUT  --login-->         LOGGED_IN   (record login_ts, reset counter)
LOGGED_IN   --transaction-->   SUSPICIOUS  (amount >= 1000, increment counter)
                                           If counter reaches 3 AND within 300s
                                           -> emit alert, go to LOGGED_OUT
LOGGED_IN   --transaction-->   LOGGED_IN   (amount < 1000, no state change)
SUSPICIOUS  --transaction-->   SUSPICIOUS  (amount >= 1000, increment counter)
                                           If counter reaches 3 AND within 300s
                                           -> emit alert, go to LOGGED_OUT
*           --logout-->        LOGGED_OUT
*           --transaction-->   LOGGED_OUT  if event_ts > login_ts + 300s
                                           (session expired, ignore tx)

Usage
-----
    python run.py --input input.jsonl --output output.jsonl
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Optional, Tuple

import bytewax.operators as op
from bytewax.connectors.files import FileSource, FileSink
from bytewax.dataflow import Dataflow
from bytewax.testing import run_main


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

LARGE_TX_THRESHOLD = 1000       # Minimum amount to be considered a large transaction
SESSION_WINDOW_SECS = 300       # 5-minute window from login
FRAUD_TX_COUNT = 3              # Number of large transactions to trigger alert
SENTINEL_NO_ALERT = "__NO_ALERT__"  # Internal sentinel – filtered out before output


# ---------------------------------------------------------------------------
# State machine
# ---------------------------------------------------------------------------

class UserStatus(Enum):
    LOGGED_OUT = auto()
    LOGGED_IN = auto()
    SUSPICIOUS = auto()


@dataclass
class UserState:
    """Immutable-style state for a single user.  Always return a new instance
    from the mapper so Bytewax can snapshot it correctly."""

    status: UserStatus = UserStatus.LOGGED_OUT
    login_ts: Optional[int] = None          # Unix timestamp of latest login
    large_tx_count: int = 0                 # Consecutive large-tx counter


def _initial_state() -> UserState:
    return UserState()


def fraud_state_machine(
    state: Optional[UserState],
    event: dict,
) -> Tuple[Optional[UserState], str]:
    """Bytewax stateful_map mapper.

    Parameters
    ----------
    state:
        Current ``UserState``, or ``None`` if this is the first event for the
        key (Bytewax passes None when no prior state exists).
    event:
        Parsed event dictionary with keys ``event_type``, ``timestamp``, and
        optionally ``amount``.

    Returns
    -------
    (new_state, output_value)
        ``new_state``    – updated ``UserState`` (never ``None``;
                           Bytewax will store it for subsequent events).
        ``output_value`` – either ``"FRAUD_ALERT"`` or the sentinel string
                           ``SENTINEL_NO_ALERT`` which is filtered downstream.
    """
    if state is None:
        state = _initial_state()

    event_type: str = event.get("event_type", "")
    timestamp: int = int(event.get("timestamp", 0))
    amount: float = float(event.get("amount", 0))

    # ------------------------------------------------------------------
    # LOGOUT – always resets regardless of current status
    # ------------------------------------------------------------------
    if event_type == "logout":
        new_state = UserState(
            status=UserStatus.LOGGED_OUT,
            login_ts=None,
            large_tx_count=0,
        )
        return new_state, SENTINEL_NO_ALERT

    # ------------------------------------------------------------------
    # LOGIN – record timestamp, move to LOGGED_IN, reset counter
    # ------------------------------------------------------------------
    if event_type == "login":
        new_state = UserState(
            status=UserStatus.LOGGED_IN,
            login_ts=timestamp,
            large_tx_count=0,
        )
        return new_state, SENTINEL_NO_ALERT

    # ------------------------------------------------------------------
    # TRANSACTION
    # ------------------------------------------------------------------
    if event_type == "transaction":
        # Ignore transactions when already logged out
        if state.status == UserStatus.LOGGED_OUT:
            return state, SENTINEL_NO_ALERT

        # Session-expiry check: more than 300 s since login → reset
        if state.login_ts is not None and (timestamp - state.login_ts) > SESSION_WINDOW_SECS:
            new_state = UserState(
                status=UserStatus.LOGGED_OUT,
                login_ts=None,
                large_tx_count=0,
            )
            return new_state, SENTINEL_NO_ALERT

        # Only LOGGED_IN or SUSPICIOUS states reach here
        if amount >= LARGE_TX_THRESHOLD:
            new_count = state.large_tx_count + 1

            if new_count >= FRAUD_TX_COUNT:
                # Fraud pattern complete – emit alert, reset user
                new_state = UserState(
                    status=UserStatus.LOGGED_OUT,
                    login_ts=None,
                    large_tx_count=0,
                )
                return new_state, "FRAUD_ALERT"
            else:
                # Accumulating – become / stay SUSPICIOUS
                new_state = UserState(
                    status=UserStatus.SUSPICIOUS,
                    login_ts=state.login_ts,
                    large_tx_count=new_count,
                )
                return new_state, SENTINEL_NO_ALERT
        else:
            # Small transaction – stay in current active state, counter unchanged
            return state, SENTINEL_NO_ALERT

    # Unknown event type – leave state unchanged
    return state, SENTINEL_NO_ALERT


# ---------------------------------------------------------------------------
# Dataflow builder
# ---------------------------------------------------------------------------

def build_dataflow(input_path: Path, output_path: Path) -> Dataflow:
    """Construct and return the Bytewax Dataflow."""

    flow = Dataflow("fraud_detection")

    # --- Input: read raw lines from JSONlines file ---
    raw_lines: op.Stream = op.input("file_input", flow, FileSource(input_path))

    # --- Parse each line into (user_id, event_dict) tuples ---
    def parse_line(line: str) -> Optional[Tuple[str, dict]]:
        line = line.strip()
        if not line:
            return None
        try:
            event = json.loads(line)
            user_id = event.get("user_id")
            if user_id is None:
                return None
            return (user_id, event)
        except json.JSONDecodeError:
            return None

    # filter_map discards None returns automatically
    keyed_events: op.Stream = op.filter_map("parse_and_key", raw_lines, parse_line)

    # --- Stateful processing: run the state machine per user ---
    # stateful_map expects a Stream[Tuple[str, V]] and returns Stream[Tuple[str, W]]
    results: op.Stream = op.stateful_map(
        "fraud_state_machine",
        keyed_events,
        fraud_state_machine,
    )

    # --- Keep only real fraud alerts (drop sentinels) ---
    def is_real_alert(item: Tuple[str, str]) -> bool:
        _user_id, alert = item
        return alert != SENTINEL_NO_ALERT

    alerts: op.Stream = op.filter("filter_alerts", results, is_real_alert)

    # --- Format alerts as JSONlines strings.
    #     FileSink expects (key, value) tuples; it writes the value part.
    #     op.map receives the full (user_id, alert) tuple and must return a
    #     (user_id, json_string) tuple so FileSink routes and writes correctly.
    def format_alert(item: Tuple[str, str]) -> Tuple[str, str]:
        user_id, alert = item
        return (user_id, json.dumps({"user_id": user_id, "alert": alert}))

    alert_lines: op.Stream = op.map("format_output", alerts, format_alert)

    # --- Output: write to JSONlines file ---
    # FileSink requires the output path to exist as a file; create/truncate it.
    output_path.touch()
    op.output("file_output", alert_lines, FileSink(output_path))

    return flow


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Bytewax fraud detection dataflow",
    )
    parser.add_argument(
        "--input",
        required=True,
        metavar="FILE",
        help="Path to the input JSONlines file of user events.",
    )
    parser.add_argument(
        "--output",
        required=True,
        metavar="FILE",
        help="Path to the output JSONlines file where fraud alerts are written.",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.exists():
        print(f"Error: input file '{input_path}' does not exist.", file=sys.stderr)
        sys.exit(1)

    flow = build_dataflow(input_path, output_path)
    run_main(flow)


if __name__ == "__main__":
    main()
