"""Bytewax pipeline: IoT telemetry alert filter with dynamic thresholds."""

import json
import os
from pathlib import Path
from typing import Optional, Tuple

import bytewax.operators as op
from bytewax.connectors.files import FileSource, FileSink
from bytewax.dataflow import Dataflow

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

INPUT_FILE = Path(__file__).parent / "input.jsonl"

run_id = os.environ["ZEALT_RUN_ID"]
OUTPUT_FILE = Path(__file__).parent / f"output-{run_id}.jsonl"

DEFAULT_THRESHOLD = 100.0

# ---------------------------------------------------------------------------
# Step helpers
# ---------------------------------------------------------------------------


def parse_and_validate(line: str) -> Optional[Tuple[str, dict]]:
    """Parse a JSON line and return (device_id, event) or None on any error.

    Drops lines that are:
    - Invalid JSON
    - Missing any of the required top-level fields: type, device_id, payload
    """
    try:
        event = json.loads(line)
    except json.JSONDecodeError:
        return None

    if not isinstance(event, dict):
        return None

    if not all(k in event for k in ("type", "device_id", "payload")):
        return None

    device_id = event["device_id"]
    if not isinstance(device_id, str):
        return None

    return (device_id, event)


def process_event(
    state: Optional[float], event: dict
) -> Tuple[Optional[float], Optional[dict]]:
    """Stateful mapper: maintain threshold per device, emit alerts for metrics.

    State is the current threshold (float). When state is None (first time
    this device key is seen) we initialise it to DEFAULT_THRESHOLD.

    Returns:
        (new_state, alert_dict | None)
    """
    threshold: float = state if state is not None else DEFAULT_THRESHOLD

    event_type = event.get("type")
    payload = event.get("payload", {})

    if event_type == "config":
        new_threshold = payload.get("threshold")
        if isinstance(new_threshold, (int, float)):
            threshold = float(new_threshold)
        # Config events never emit an alert
        return (threshold, None)

    elif event_type == "metric":
        temperature = payload.get("temperature")
        if isinstance(temperature, (int, float)):
            temperature = float(temperature)
            if temperature > threshold:
                alert = {
                    "device_id": event["device_id"],
                    "alert_type": "temperature_high",
                    "value": temperature,
                    "threshold": threshold,
                }
                return (threshold, alert)
        # Temperature within bounds or missing — no alert
        return (threshold, None)

    # Unknown event type — no state change, no alert
    return (threshold, None)


def serialize_alert(
    keyed_alert: Tuple[str, Optional[dict]]
) -> Optional[Tuple[str, str]]:
    """Convert (device_id, alert_dict | None) to (device_id, JSON string) or None.

    FileSink (FixedPartitionedSink) requires a keyed 2-tuple from upstream.
    Returning None causes filter_map to drop the item (non-alert events).
    """
    key, alert = keyed_alert
    if alert is None:
        return None
    return (key, json.dumps(alert))


# ---------------------------------------------------------------------------
# Dataflow
# ---------------------------------------------------------------------------

flow = Dataflow("iot-alert-pipeline")

# 1. Read raw lines from the input file
raw = op.input("read", flow, FileSource(INPUT_FILE))

# 2. Parse JSON, validate required fields, key by device_id
#    filter_map drops None returns automatically
parsed = op.filter_map("parse_and_validate", raw, parse_and_validate)

# 3. Stateful processing per device: threshold tracking + alert generation
#    stateful_map requires the stream to already be keyed (Tuple[str, V])
processed = op.stateful_map("process_event", parsed, process_event)

# 4. Serialise alert dicts to JSON strings, drop None (non-alert events)
alerts = op.filter_map("serialize_alert", processed, serialize_alert)

# 5. Write alert JSON strings to the output file
op.output("write", alerts, FileSink(OUTPUT_FILE))
