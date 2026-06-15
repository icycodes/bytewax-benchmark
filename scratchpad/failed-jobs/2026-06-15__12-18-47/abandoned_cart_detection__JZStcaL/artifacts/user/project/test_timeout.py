import json
import os
import time
from datetime import datetime, timezone, timedelta
from bytewax.dataflow import Dataflow
from bytewax import operators as op
from bytewax.connectors.files import FileSink
from bytewax.operators import StatefulLogic
from bytewax.inputs import DynamicSource, StatelessSourcePartition

class SlowSourcePartition(StatelessSourcePartition):
    def __init__(self):
        self.events = [
            '{"user_id": "u1", "type": "add_to_cart", "item": "laptop"}',
            "SLEEP",
            '{"user_id": "u1", "type": "add_to_cart", "item": "mouse"}',
            "EOF"
        ]
        self.idx = 0

    def next_batch(self):
        if self.idx >= len(self.events):
            raise StopIteration()
        ev = self.events[self.idx]
        self.idx += 1
        if ev == "SLEEP":
            time.sleep(2)
            return []
        elif ev == "EOF":
            raise StopIteration()
        return [ev]

class SlowSource(DynamicSource):
    def build(self, worker_index, worker_count):
        return SlowSourcePartition()

os.environ["OUTPUT_FILE"] = "test_out_timeout.txt"
os.environ["CART_TIMEOUT_SECONDS"] = "1"

flow = Dataflow("abandoned_cart")

stream = op.input("in", flow, SlowSource())

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
                self.first_add_time = datetime.now(timezone.utc)
            self.items.append(value["item"])
            return ([], StatefulLogic.RETAIN)
        elif event_type == "checkout":
            self.items = []
            self.first_add_time = None
            return ([], StatefulLogic.DISCARD)
        return ([], StatefulLogic.RETAIN)

    def on_notify(self):
        abandoned_items = self.items
        self.items = []
        self.first_add_time = None
        return ([{"abandoned_items": abandoned_items}], StatefulLogic.DISCARD)

    def on_eof(self):
        if self.items:
            abandoned_items = self.items
            self.items = []
            self.first_add_time = None
            return ([{"abandoned_items": abandoned_items}], StatefulLogic.DISCARD)
        return ([], StatefulLogic.DISCARD)

    def notify_at(self):
        if self.first_add_time is not None:
            return self.first_add_time + timedelta(seconds=1)
        return None

    def snapshot(self):
        return (list(self.items), self.first_add_time)

stream = op.stateful("cart_logic", stream, lambda resume_state: CartLogic(resume_state))

def format_output(key_value):
    user_id, data = key_value
    out = {"user_id": user_id, "abandoned_items": data["abandoned_items"]}
    return (user_id, json.dumps(out))

stream = op.map("format", stream, format_output)
op.output("out", stream, FileSink(os.environ["OUTPUT_FILE"]))
