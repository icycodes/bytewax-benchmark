"""Fraud detection state machine using Bytewax stateful operators."""

import json
from dataclasses import dataclass
from typing import Optional, Tuple

import bytewax.operators as op
from bytewax.dataflow import Dataflow

# ── State machine constants ──────────────────────────────────────────────

LOGGED_OUT = "LOGGED_OUT"
LOGGED_IN = "LOGGED_IN"
SUSPICIOUS = "SUSPICIOUS"

LARGE_TXN_THRESHOLD = 1000
MAX_LARGE_TXNS = 3
TIME_WINDOW_SECONDS = 300


@dataclass(frozen=True)
class UserState:
    """Immutable state for each user in the state machine.

    Attributes:
        state: Current state (LOGGED_OUT, LOGGED_IN, or SUSPICIOUS).
        login_timestamp: Timestamp of the last login event, or None.
        large_txn_count: Number of large transactions (>=1000) since login.
    """

    state: str = LOGGED_OUT
    login_timestamp: Optional[int] = None
    large_txn_count: int = 0


def state_machine(
    current_state: Optional[UserState],
    event: dict,
) -> Tuple[Optional[UserState], Optional[dict]]:
    """Stateful mapper for fraud detection.

    Processes a single event for a user and returns the new state plus
    any alert that should be emitted.

    Args:
        current_state: The current state for this user, or None if first event.
        event: The event dict with keys: user_id, event_type, amount?, timestamp.

    Returns:
        A tuple of (new_state, alert_to_emit_or_None).
    """
    # Initialize state on first event
    if current_state is None:
        current_state = UserState()

    event_type = event["event_type"]
    timestamp = event["timestamp"]
    amount = event.get("amount", 0)

    # ── LOGOUT: immediately transition to LOGGED_OUT ─────────────────
    if event_type == "logout":
        return (UserState(state=LOGGED_OUT), None)

    # ── LOGIN: transition to LOGGED_IN, record timestamp, reset count
    if event_type == "login":
        return (
            UserState(
                state=LOGGED_IN,
                login_timestamp=timestamp,
                large_txn_count=0,
            ),
            None,
        )

    # ── TRANSACTION ──────────────────────────────────────────────────
    if event_type == "transaction":
        # If logged out, ignore the transaction entirely
        if current_state.state == LOGGED_OUT:
            return (current_state, None)

        # Check if we're still within the time window from login
        login_ts = current_state.login_timestamp
        if login_ts is not None and (timestamp - login_ts) > TIME_WINDOW_SECONDS:
            # Outside the window: reset to LOGGED_OUT, ignore this transaction
            return (UserState(state=LOGGED_OUT), None)

        # Check if this is a large transaction
        if amount >= LARGE_TXN_THRESHOLD:
            new_count = current_state.large_txn_count + 1

            if new_count >= MAX_LARGE_TXNS:
                # Fraud pattern detected: emit alert, reset to LOGGED_OUT
                alert = {"user_id": event["user_id"], "alert": "FRAUD_ALERT"}
                return (UserState(state=LOGGED_OUT), alert)
            else:
                # Increment count, move to SUSPICIOUS
                return (
                    UserState(
                        state=SUSPICIOUS,
                        login_timestamp=current_state.login_timestamp,
                        large_txn_count=new_count,
                    ),
                    None,
                )

        # Transaction under threshold: no state change
        return (current_state, None)

    # Unknown event type: no state change
    return (current_state, None)


def build_dataflow(
    input_path: str,
    output_path: str,
) -> Dataflow:
    """Build the fraud detection Bytewax dataflow.

    Args:
        input_path: Path to the input JSONlines file.
        output_path: Path to the output JSONlines file.

    Returns:
        A configured Dataflow ready to be executed.
    """
    flow = Dataflow("fraud_detection")

    # ── Input: read JSONlines file ───────────────────────────────────
    from bytewax.connectors.files import FileSource

    raw_lines = op.input("input", flow, FileSource(input_path))

    # ── Parse JSON ───────────────────────────────────────────────────
    def parse_json(line: str) -> dict:
        return json.loads(line)

    events = op.map("parse_json", raw_lines, parse_json)

    # ── Key by user_id ───────────────────────────────────────────────
    keyed = op.key_on("key_on_user", events, lambda e: e["user_id"])

    # ── Stateful fraud detection ─────────────────────────────────────
    results = op.stateful_map("fraud_state_machine", keyed, state_machine)

    # ── Filter out None (no alert) results ───────────────────────────
    def extract_alert(alert: Optional[dict]) -> Optional[dict]:
        return alert  # None values are filtered out automatically

    alerts = op.filter_map_value("filter_alerts", results, extract_alert)

    # ── Serialize to JSONlines ───────────────────────────────────────
    def serialize(alert: dict) -> str:
        return json.dumps(alert)

    lines = op.map_value("serialize", alerts, serialize)

    # ── Output: write to JSONlines file ──────────────────────────────
    from bytewax.connectors.files import FileSink

    op.output("output", lines, FileSink(output_path))

    return flow
