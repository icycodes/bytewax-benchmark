import json
from pathlib import Path

from bytewax.dataflow import Dataflow
from bytewax.connectors.files import FileSource, FileSink
import bytewax.operators as op

flow = Dataflow("join_impressions_clicks")

# Read impressions stream from JSONL file
impressions = op.input("imp_input", flow, FileSource(Path("impressions.jsonl")))
# Parse each line as JSON
impressions = op.map("imp_parse", impressions, json.loads)
# Key by user_id for the join
impressions = op.key_on("imp_key", impressions, lambda x: x["user_id"])

# Read clicks stream from JSONL file
clicks = op.input("click_input", flow, FileSource(Path("clicks.jsonl")))
# Parse each line as JSON
clicks = op.map("click_parse", clicks, json.loads)
# Key by user_id for the join
clicks = op.key_on("click_key", clicks, lambda x: x["user_id"])

# Perform a complete inner join on user_id
# emit_mode="complete" (default) emits when both sides have a value for the key
joined = op.join("join_on_user", impressions, clicks)

# Format the joined result as a JSON string
# joined items are (user_id, (impression, click)) tuples
# Use map_value to transform the value while preserving the key
def format_result(item):
    impression, click = item
    result = {"user_id": impression["user_id"], "impression": impression, "click": click}
    return json.dumps(result)

joined = op.map_value("format_output", joined, format_result)

# Write joined results to JSONL file
# The stream is (user_id, json_string); FileSink receives the string values
op.output("out", joined, FileSink(Path("joined.jsonl")))