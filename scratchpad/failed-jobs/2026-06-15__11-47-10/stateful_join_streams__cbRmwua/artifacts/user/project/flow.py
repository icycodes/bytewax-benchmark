import json
from pathlib import Path
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSource, FileSink

# Define the dataflow
flow = Dataflow("stateful_join_flow")

# Define paths
impressions_path = Path("impressions.jsonl")
clicks_path = Path("clicks.jsonl")
output_path = Path("joined.jsonl")

# 1. Read input streams from local JSONL files
impressions_stream = op.input("impressions_in", flow, FileSource(impressions_path))
clicks_stream = op.input("clicks_in", flow, FileSource(clicks_path))

# 2. Parse and validate JSON lines
def parse_and_validate(line):
    line = line.strip()
    if not line:
        return None
    try:
        data = json.loads(line)
        if isinstance(data, dict) and "user_id" in data:
            return data
    except (json.JSONDecodeError, TypeError):
        pass
    return None

parsed_impressions = op.filter_map("parse_impressions", impressions_stream, parse_and_validate)
parsed_clicks = op.filter_map("parse_clicks", clicks_stream, parse_and_validate)

# 3. Key the streams on user_id
keyed_impressions = op.key_on("key_impressions", parsed_impressions, lambda x: x["user_id"])
keyed_clicks = op.key_on("key_clicks", parsed_clicks, lambda x: x["user_id"])

# 4. Stateful complete join on user_id
joined_stream = op.join("join_streams", keyed_impressions, keyed_clicks, insert_mode="last", emit_mode="complete")

# 5. Format the joined results as JSON strings
def format_joined(item):
    user_id, (impression, click) = item
    output_record = {
        "user_id": user_id,
        "impression": impression,
        "click": click
    }
    return (user_id, json.dumps(output_record))

formatted_stream = op.map("format_output", joined_stream, format_joined)

# 6. Write the joined results to joined.jsonl
op.output("joined_out", formatted_stream, FileSink(output_path))
