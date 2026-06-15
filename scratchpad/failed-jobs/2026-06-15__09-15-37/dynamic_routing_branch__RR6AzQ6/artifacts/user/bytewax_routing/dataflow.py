import json
from pathlib import Path

import bytewax.operators as op
from bytewax.connectors.files import FileSource, FileSink
from bytewax.dataflow import Dataflow
from bytewax.testing import run_main

# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def parse_line(line: str):
    """Return (tag, payload) where tag is 'valid' or 'error'."""
    raw = line.rstrip("\n")
    try:
        record = json.loads(raw)
    except json.JSONDecodeError:
        error = json.dumps({"error": "invalid json", "raw": raw})
        return ("error", error)

    if "sensor_type" not in record:
        error = json.dumps({"error": "missing sensor_type", "raw": raw})
        return ("error", error)

    return ("valid", record)


def is_error(item):
    tag, _ = item
    return tag == "error"


def is_temperature(item):
    tag, record = item
    return tag == "valid" and record.get("sensor_type") == "temperature"


def celsius_to_fahrenheit(item):
    _, record = item
    record = dict(record)           # shallow copy – avoid mutating the original
    value_c = record.pop("value_c")
    record["value_f"] = round(value_c * 9 / 5 + 32, 10)
    return json.dumps(record)


def extract_humidity(item):
    _, record = item
    return json.dumps(record)


def extract_error(item):
    _, payload = item
    return payload  # already a JSON string


# ---------------------------------------------------------------------------
# Dataflow definition
# ---------------------------------------------------------------------------

flow = Dataflow("sensor_routing")

# 1. Read raw lines from sensors.json
lines = op.input("read", flow, FileSource(Path("/home/user/bytewax_routing/sensors.json")))

# 2. Parse every line → ("error"|"valid", payload)
parsed = op.map("parse", lines, parse_line)

# 3. Split errors from valid records
error_branch = op.branch("split_errors", parsed, is_error)
errors_raw   = error_branch.trues    # tag == "error"
valid        = error_branch.falses   # tag == "valid"

# 4. Split temperature from humidity (everything that is not temperature)
temp_branch    = op.branch("split_temp", valid, is_temperature)
temperature    = temp_branch.trues   # sensor_type == "temperature"
humidity       = temp_branch.falses  # sensor_type == "humidity" (or anything else valid)

# 5. Transform temperature records (C → F) and serialise all streams to JSON strings
temp_json     = op.map("to_temp_json",     temperature, celsius_to_fahrenheit)
humidity_json = op.map("to_humidity_json", humidity,    extract_humidity)
errors_json   = op.map("to_error_json",    errors_raw,  extract_error)

# 6. FileSink is a FixedPartitionedSink and expects (key, value) 2-tuples.
#    We attach a static key so routing works correctly.
temp_keyed     = op.key_on("key_temp",     temp_json,     lambda _: "temp")
humidity_keyed = op.key_on("key_humidity", humidity_json, lambda _: "hum")
errors_keyed   = op.key_on("key_errors",   errors_json,   lambda _: "err")

# 7. Write to individual files
op.output("write_temperature", temp_keyed,
          FileSink(Path("/home/user/bytewax_routing/temperature.json")))
op.output("write_humidity", humidity_keyed,
          FileSink(Path("/home/user/bytewax_routing/humidity.json")))
op.output("write_errors", errors_keyed,
          FileSink(Path("/home/user/bytewax_routing/errors.json")))

# ---------------------------------------------------------------------------
# Entry point – allows `python dataflow.py` in addition to
# `python -m bytewax.run dataflow:flow`
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    run_main(flow)
