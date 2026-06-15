import json
import os
from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional, Tuple

from bytewax.connectors.files import FileSink, FileSource
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.operators import StatefulLogic


class CartState:
    """Immutable snapshot of cart state for recovery."""

    def __init__(self, items: list, first_added_at: Optional[datetime]):
        self.items = list(items)
        self.first_added_at = first_added_at

    def __repr__(self):
        return f"CartState(items={self.items}, first_added_at={self.first_added_at})"


class CartAbandonmentLogic(StatefulLogic[dict, dict, CartState]):
    """Stateful logic that tracks a user's cart and detects abandonment.

    The cart is considered abandoned if it remains unpaid for
    `timeout_seconds` after the first item was added.
    """

    def __init__(self, timeout_seconds: int, resume_state: Optional[CartState]):
        self._timeout = timedelta(seconds=timeout_seconds)
        if resume_state is not None:
            self._items: list = resume_state.items
            self._first_added_at: Optional[datetime] = resume_state.first_added_at
        else:
            self._items: list = []
            self._first_added_at: Optional[datetime] = None

    def notify_at(self) -> Optional[datetime]:
        if self._first_added_at is not None:
            return self._first_added_at + self._timeout
        return None

    def on_item(self, value: dict) -> Tuple[Iterable[dict], bool]:
        event_type = value.get("type")

        if event_type == "add_to_cart":
            item = value.get("item")
            if item is not None:
                self._items.append(item)
            if self._first_added_at is None:
                self._first_added_at = datetime.now(timezone.utc)
            return ([], StatefulLogic.RETAIN)

        elif event_type == "checkout":
            # Clear the cart — checkout occurred, no abandonment
            self._items.clear()
            self._first_added_at = None
            return ([], StatefulLogic.DISCARD)

        else:
            # Unknown event type — ignore
            return ([], StatefulLogic.RETAIN)

    def on_notify(self) -> Tuple[Iterable[dict], bool]:
        # Timer fired: cart is abandoned
        if self._items:
            result = {
                "user_id": None,  # will be filled by the key from the stream
                "abandoned_items": list(self._items),
            }
            self._items.clear()
            self._first_added_at = None
            return ([result], StatefulLogic.DISCARD)
        return ([], StatefulLogic.DISCARD)

    def on_eof(self) -> Tuple[Iterable[dict], bool]:
        # Stream ended: any remaining items are abandoned
        if self._items:
            result = {
                "user_id": None,
                "abandoned_items": list(self._items),
            }
            self._items.clear()
            self._first_added_at = None
            return ([result], StatefulLogic.DISCARD)
        return ([], StatefulLogic.DISCARD)

    def snapshot(self) -> CartState:
        return CartState(
            items=list(self._items),
            first_added_at=self._first_added_at,
        )


def _build_logic(timeout_seconds: int):
    """Factory that closes over the timeout config."""

    def builder(resume_state: Optional[CartState]) -> CartAbandonmentLogic:
        return CartAbandonmentLogic(timeout_seconds, resume_state)

    return builder


flow = Dataflow("cart_abandonment")

# Read configuration from environment
input_file = os.environ.get("INPUT_FILE", "")
output_file = os.environ.get("OUTPUT_FILE", "")
timeout_seconds = int(os.environ.get("CART_TIMEOUT_SECONDS", "900"))

# Step 1: Read lines from the input file
lines = op.input("file_input", flow, FileSource(input_file))

# Step 2: Parse JSON lines
events = op.map("parse_json", lines, json.loads)

# Step 3: Key the stream by user_id
keyed = op.key_on("key_by_user", events, lambda e: e["user_id"])

# Step 4: Stateful cart tracking with timeout-based abandonment detection
abandoned = op.stateful("cart_tracker", keyed, _build_logic(timeout_seconds))

# Step 5: Fill in the user_id from the key and serialize to JSON lines
def fill_user_id(key__result):
    key, result = key__result
    result["user_id"] = key
    return [json.dumps(result)]


formatted = op.flat_map("format_output", abandoned, fill_user_id)

# Step 6: Re-key the stream for the output sink
keyed_output = op.key_on("key_output", formatted, lambda s: "")

# Step 7: Write to the output file
op.output("file_output", keyed_output, FileSink(path=output_file))
