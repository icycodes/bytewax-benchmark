from bytewax.dataflow import Dataflow
import bytewax.operators as op
import bytewax.operators.windowing as win
from bytewax.connectors.files import FileSource
from bytewax.connectors.stdio import StdOutSink
from datetime import datetime, timedelta, timezone
import json

# Read static products metadata at initialization
products_path = "products.json"
with open(products_path, "r") as f:
    products = json.load(f)

# Initialize Dataflow
flow = Dataflow("enrichment_flow")

# Ingest transaction events from transactions.jsonl
up = op.input("input", flow, FileSource("transactions.jsonl"))

# Enrich each transaction with category and price, drop if product_id not found
def enrich_and_filter(line):
    try:
        tx = json.loads(line)
    except Exception:
        return None
    
    product_id = tx.get("product_id")
    if product_id in products:
        prod = products[product_id]
        tx["category"] = prod["category"]
        tx["price"] = prod["price"]
        return tx
    return None

enriched = op.filter_map("enrich", up, enrich_and_filter)

# Group (key) by category
keyed = op.key_on("key_by_category", enriched, lambda tx: tx["category"])

# EventClock to retrieve timestamp from the transaction
def get_tx_timestamp(tx):
    return datetime.fromisoformat(tx["timestamp"])

clock = win.EventClock(
    ts_getter=get_tx_timestamp,
    wait_for_system_duration=timedelta(seconds=0)
)

# 1-minute tumbling window aligned to the start of the hour
windower = win.TumblingWindower(
    length=timedelta(minutes=1),
    align_to=datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
)

# Calculate total revenue per category per window
windowed = win.fold_window(
    "fold",
    keyed,
    clock,
    windower,
    builder=lambda: 0.0,
    folder=lambda acc, tx: acc + (tx["price"] * tx["quantity"]),
    merger=lambda a, b: a + b
)

# Map down and meta streams to use f"{category}:{window_id}" as key for joining
def map_down(item):
    category, (window_id, revenue) = item
    return (f"{category}:{window_id}", (category, revenue))

down_mapped = op.map("map_down", windowed.down, map_down)

def map_meta(item):
    category, (window_id, metadata) = item
    return (f"{category}:{window_id}", metadata)

meta_mapped = op.map("map_meta", windowed.meta, map_meta)

# Join the revenue and metadata streams
joined = op.join("join", down_mapped, meta_mapped)

# Format the output into JSON strings
def format_output(item):
    join_key, ((category, revenue), metadata) = item
    window_start = metadata.open_time.isoformat()
    out_dict = {
        "category": category,
        "window_start": window_start,
        "revenue": revenue
    }
    return json.dumps(out_dict)

formatted = op.map("format", joined, format_output)

# Output the results to standard output
op.output("out", formatted, StdOutSink())
