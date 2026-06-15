import os
import json
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSource, FileSink
from bytewax.operators import StatefulLogic

class CartLogic(StatefulLogic):
    def __init__(self, resume_state):
        if resume_state is not None:
            self.user_id, self.items, self.first_added_at = resume_state
        else:
            self.user_id = None
            self.items = []
            self.first_added_at = None

    def on_item(self, value):
        self.user_id = value["user_id"]
        event_type = value["type"]
        if event_type == "add_to_cart":
            if not self.items:
                self.first_added_at = datetime.now(timezone.utc)
            self.items.append(value["item"])
            return ([], StatefulLogic.RETAIN)
        elif event_type == "checkout":
            self.items = []
            self.first_added_at = None
            return ([], StatefulLogic.DISCARD)
        return ([], StatefulLogic.RETAIN)

    def notify_at(self):
        if self.first_added_at and self.items:
            timeout_seconds = int(os.getenv("CART_TIMEOUT_SECONDS", "900"))
            return self.first_added_at + timedelta(seconds=timeout_seconds)
        return None

    def on_notify(self):
        if self.items:
            abandoned_event = {
                "user_id": self.user_id,
                "abandoned_items": list(self.items)
            }
            self.items = []
            self.first_added_at = None
            return ([abandoned_event], StatefulLogic.DISCARD)
        return ([], StatefulLogic.DISCARD)

    def on_eof(self):
        if self.items:
            abandoned_event = {
                "user_id": self.user_id,
                "abandoned_items": list(self.items)
            }
            self.items = []
            self.first_added_at = None
            return ([abandoned_event], StatefulLogic.DISCARD)
        return ([], StatefulLogic.DISCARD)

    def snapshot(self):
        return (self.user_id, list(self.items), self.first_added_at)

# Create the dataflow
flow = Dataflow("cart_pipeline")

# Read INPUT_FILE env var
input_file_path = os.getenv("INPUT_FILE")
if not input_file_path:
    raise ValueError("INPUT_FILE environment variable is not set")

# Read lines from input file
lines = op.input("input_step", flow, FileSource(Path(input_file_path)))

# Parse JSON lines
def parse_line(line: str) -> Optional[dict]:
    line = line.strip()
    if not line:
        return None
    try:
        return json.loads(line)
    except json.JSONDecodeError:
        return None

parsed = op.filter_map("parse_input", lines, parse_line)

# Key the stream by user_id
keyed = op.key_on("key_by_user", parsed, lambda event: event["user_id"])

# Apply stateful logic to detect abandoned carts
stateful_stream = op.stateful("detect_abandoned", keyed, CartLogic)

# Format output as JSON string
def format_output(item: Tuple[str, dict]) -> Tuple[str, str]:
    user_id, event = item
    return (user_id, json.dumps(event))

formatted = op.map("format_output", stateful_stream, format_output)

# Read OUTPUT_FILE env var
output_file_path = os.getenv("OUTPUT_FILE")
if not output_file_path:
    raise ValueError("OUTPUT_FILE environment variable is not set")

# Write output to file
op.output("output_step", formatted, FileSink(Path(output_file_path)))
