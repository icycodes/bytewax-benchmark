import json
from datetime import datetime, timezone, timedelta
import bytewax.operators as op
import bytewax.operators.windowing as win
from bytewax.dataflow import Dataflow
from bytewax.connectors.stdio import StdOutSink
from bytewax.connectors.files import FileSource

# Load products
try:
    with open("products.json", "r") as f:
        products = json.load(f)
except FileNotFoundError:
    products = {}

def parse_transaction(line):
    try:
        return json.loads(line)
    except Exception:
        return None

def enrich(tx):
    if tx is None:
        return None
    product_id = tx.get("product_id")
    if product_id not in products:
        return None
    product = products[product_id]
    tx["category"] = product["category"]
    tx["price"] = product["price"]
    return tx

def get_event_time(tx):
    ts_str = tx["timestamp"]
    if ts_str.endswith("Z"):
        ts_str = ts_str[:-1] + "+00:00"
    return datetime.fromisoformat(ts_str)

flow = Dataflow("enrichment")

stream = op.input("input", flow, FileSource("transactions.jsonl"))
stream = op.map("parse", stream, parse_transaction)
stream = op.filter_map("enrich", stream, enrich)

keyed_stream = op.key_on("key", stream, lambda tx: tx["category"])

clock = win.EventClock(
    ts_getter=get_event_time,
    wait_for_system_duration=timedelta(seconds=0)
)
window = win.TumblingWindower(
    length=timedelta(minutes=1),
    align_to=datetime(2026, 1, 1, tzinfo=timezone.utc)
)

windowed = win.fold_window(
    "revenue_window",
    keyed_stream,
    clock,
    window,
    builder=lambda: 0.0,
    folder=lambda acc, tx: acc + (tx["price"] * tx["quantity"]),
    merger=lambda a, b: a + b
)

def rekey_down(item):
    k, (w_id, rev) = item
    return (f"{k}_{w_id}", rev)

def rekey_meta(item):
    k, (w_id, meta) = item
    return (f"{k}_{w_id}", meta)

down_rekey = op.map("rekey_down", windowed.down, rekey_down)
meta_rekey = op.map("rekey_meta", windowed.meta, rekey_meta)

joined = op.join("join", down_rekey, meta_rekey)

def format_output(item):
    k_wid, (rev, meta) = item
    category = k_wid.rsplit("_", 1)[0]
    
    window_start = meta.open_time.isoformat()
    if window_start.endswith("+00:00"):
        window_start = window_start.replace("+00:00", "Z")
        
    return json.dumps({
        "category": category,
        "window_start": window_start,
        "revenue": rev
    })

out_stream = op.map("format", joined, format_output)

op.output("out", out_stream, StdOutSink())
