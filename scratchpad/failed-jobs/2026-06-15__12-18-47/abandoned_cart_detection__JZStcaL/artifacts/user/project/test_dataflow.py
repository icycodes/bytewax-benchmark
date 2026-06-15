import json
import os
from datetime import datetime, timezone, timedelta
from bytewax.dataflow import Dataflow
from bytewax import operators as op
from bytewax.connectors.files import FileSource, FileSink
from bytewax.operators import StatefulLogic

with open("test_in.txt", "w") as f:
    f.write('{"user_id": "u1", "type": "add_to_cart", "item": "laptop"}\n')
    f.write('{"user_id": "u2", "type": "add_to_cart", "item": "mouse"}\n')
    f.write('{"user_id": "u1", "type": "checkout"}\n')

os.environ["INPUT_FILE"] = "test_in.txt"
os.environ["OUTPUT_FILE"] = "test_out.txt"
os.environ["CART_TIMEOUT_SECONDS"] = "1"

flow = Dataflow("test")
stream = op.input("in", flow, FileSource(os.environ["INPUT_FILE"]))

def parse(line):
    d = json.loads(line)
    return d["user_id"], d

stream = op.map("parse", stream, parse)

class CartLogic(StatefulLogic):
    def __init__(self, resume_state):
        if resume_state:
            self.items, self.first_add_time = resume_state
        else:
            self.items = []
            self.first_add_time = None

    def on_item(self, value):
        if value["type"] == "add_to_cart":
            if not self.items:
                self.first_add_time = datetime.now(timezone.utc)
            self.items.append(value["item"])
            return ([], StatefulLogic.RETAIN)
        elif value["type"] == "checkout":
            self.items = []
            self.first_add_time = None
            return ([], StatefulLogic.DISCARD)
        return ([], StatefulLogic.RETAIN)

    def on_notify(self):
        abandoned = self.items
        self.items = []
        self.first_add_time = None
        return ([{"abandoned_items": abandoned}], StatefulLogic.DISCARD)

    def on_eof(self):
        if self.items:
            abandoned = self.items
            self.items = []
            self.first_add_time = None
            return ([{"abandoned_items": abandoned}], StatefulLogic.DISCARD)
        return ([], StatefulLogic.DISCARD)

    def notify_at(self):
        if self.first_add_time is not None:
            return self.first_add_time + timedelta(seconds=int(os.environ["CART_TIMEOUT_SECONDS"]))
        return None

    def snapshot(self):
        return (list(self.items), self.first_add_time)

stream = op.stateful("state", stream, lambda resume_state: CartLogic(resume_state))

def format_out(kv):
    k, v = kv
    return (k, json.dumps({"user_id": k, "abandoned_items": v["abandoned_items"]}))

stream = op.map("format", stream, format_out)
op.output("out", stream, FileSink(os.environ["OUTPUT_FILE"]))
