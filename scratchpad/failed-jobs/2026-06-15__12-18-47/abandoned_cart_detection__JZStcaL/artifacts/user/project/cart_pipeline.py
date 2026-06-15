import json
import os
from datetime import datetime, timezone, timedelta
from bytewax.dataflow import Dataflow
from bytewax import operators as op
from bytewax.connectors.files import FileSource, FileSink
from bytewax.operators import StatefulLogic

INPUT_FILE = os.environ.get("INPUT_FILE")
OUTPUT_FILE = os.environ.get("OUTPUT_FILE")
CART_TIMEOUT_SECONDS = int(os.environ.get("CART_TIMEOUT_SECONDS", "900"))

flow = Dataflow("abandoned_cart")

# Read from input file
stream = op.input("in", flow, FileSource(INPUT_FILE))

# Parse JSON and key by user_id
def parse_event(line):
    event = json.loads(line)
    return event["user_id"], event

stream = op.map("parse", stream, parse_event)

class CartLogic(StatefulLogic):
    def __init__(self, resume_state):
        if resume_state is not None:
            self.items, self.first_add_time = resume_state
        else:
            self.items = []
            self.first_add_time = None

    def on_item(self, value):
        event_type = value.get("type")
        if event_type == "add_to_cart":
            if not self.items:
                # First item added
                self.first_add_time = datetime.now(timezone.utc)
            self.items.append(value["item"])
            return ([], StatefulLogic.RETAIN)
        elif event_type == "checkout":
            # Clear cart on checkout
            self.items = []
            self.first_add_time = None
            return ([], StatefulLogic.DISCARD)
        return ([], StatefulLogic.RETAIN)

    def on_notify(self):
        # Timeout reached
        abandoned_items = self.items
        self.items = []
        self.first_add_time = None
        return ([{"abandoned_items": abandoned_items}], StatefulLogic.DISCARD)

    def on_eof(self):
        # Flush pending carts at EOF
        if self.items:
            abandoned_items = self.items
            self.items = []
            self.first_add_time = None
            return ([{"abandoned_items": abandoned_items}], StatefulLogic.DISCARD)
        return ([], StatefulLogic.DISCARD)

    def notify_at(self):
        if self.first_add_time is not None:
            return self.first_add_time + timedelta(seconds=CART_TIMEOUT_SECONDS)
        return None

    def snapshot(self):
        return (list(self.items), self.first_add_time)

# Apply stateful logic
stream = op.stateful("cart_logic", stream, lambda resume_state: CartLogic(resume_state))

# Format the output as JSON string
def format_output(key_value):
    user_id, data = key_value
    out = {"user_id": user_id, "abandoned_items": data["abandoned_items"]}
    return (user_id, json.dumps(out))

stream = op.map("format", stream, format_output)

# Write to output file
op.output("out", stream, FileSink(OUTPUT_FILE))
