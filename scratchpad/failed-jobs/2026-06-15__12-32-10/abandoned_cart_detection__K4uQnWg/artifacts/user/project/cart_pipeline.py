"""Bytewax dataflow for detecting abandoned shopping carts."""

import json
import os
from datetime import datetime, timedelta, timezone

import bytewax.operators as op
from bytewax.connectors.files import FileSink, FileSource
from bytewax.dataflow import Dataflow
from bytewax.operators import StatefulLogic

CART_TIMEOUT_SECONDS = int(os.environ.get("CART_TIMEOUT_SECONDS", "900"))
INPUT_FILE = os.environ["INPUT_FILE"]
OUTPUT_FILE = os.environ["OUTPUT_FILE"]


class CartLogic(StatefulLogic):
    """Stateful logic to track per-user shopping carts and detect abandonment.

    Maintains a list of items per user and tracks an expiration time based on
    when the first item was added. If the cart is not checked out before the
    timeout, it is considered abandoned and an event is emitted.

    State schema (for snapshot/resume):
        {
            "items": list[str],       # items in the cart
            "expire_at": datetime | None,  # when the cart expires
        }
    """

    def __init__(self, resume_state, timeout_seconds):
        self.timeout_seconds = timeout_seconds
        if resume_state is not None:
            self.items = resume_state["items"]
            self.expire_at = resume_state["expire_at"]
        else:
            self.items = []
            self.expire_at = None

    def on_item(self, value):
        event_type = value["type"]

        if event_type == "add_to_cart":
            self.items.append(value["item"])
            # Start the timeout only when the first item is added
            if len(self.items) == 1:
                self.expire_at = datetime.now(timezone.utc) + timedelta(
                    seconds=self.timeout_seconds
                )
            return ([], StatefulLogic.RETAIN)

        if event_type == "checkout":
            # Cart is paid for; clear state and discard this logic
            self.items = []
            self.expire_at = None
            return ([], StatefulLogic.DISCARD)

        # Unknown event type — ignore
        return ([], StatefulLogic.RETAIN)

    def on_notify(self):
        # The cart has timed out — emit an abandonment event
        result = {"abandoned_items": list(self.items)}
        self.items = []
        self.expire_at = None
        return ([result], StatefulLogic.DISCARD)

    def on_eof(self):
        # Stream has ended; flush any remaining non-empty carts
        if self.items:
            result = {"abandoned_items": list(self.items)}
            self.items = []
            self.expire_at = None
            return ([result], StatefulLogic.DISCARD)
        return ([], StatefulLogic.DISCARD)

    def notify_at(self):
        return self.expire_at

    def snapshot(self):
        return {
            "items": list(self.items),
            "expire_at": self.expire_at,
        }


def build_flow():
    """Build and return the Bytewax dataflow."""
    flow = Dataflow("cart_abandonment")

    # Read JSON-encoded events line-by-line from the input file
    inp = op.input("read_input", flow, FileSource(INPUT_FILE))

    # Parse each line as JSON
    events = op.map("parse_json", inp, json.loads)

    # Key the stream by user_id for per-user stateful processing
    keyed = op.key_on("key_by_user", events, lambda e: e["user_id"])

    # Apply the stateful cart-logic operator
    abandoned = op.stateful(
        "cart_logic",
        keyed,
        lambda resume_state: CartLogic(resume_state, CART_TIMEOUT_SECONDS),
    )

    # Format the output: (user_id, {"abandoned_items": [...]}) → (user_id, JSON string)
    # FileSink requires a keyed stream and writes the value part to the file.
    def format_output(keyed_value):
        user_id, data = keyed_value
        return (user_id, json.dumps({"user_id": user_id, **data}))

    output_stream = op.map("format_output", abandoned, format_output)

    # Write results to the output file (FileSink writes the value part)
    op.output("write_output", output_stream, FileSink(OUTPUT_FILE))

    return flow


flow = build_flow()