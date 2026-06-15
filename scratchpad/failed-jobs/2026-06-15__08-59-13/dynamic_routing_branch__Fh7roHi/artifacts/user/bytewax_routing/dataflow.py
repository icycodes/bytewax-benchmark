import json
from pathlib import Path
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSource, FileSink
from bytewax.testing import run_main

def parse_line(line: str):
    line = line.strip()
    try:
        data = json.loads(line)
        if not isinstance(data, dict):
            return ("error", json.dumps({"error": "invalid json", "raw": line}))
        if "sensor_type" not in data:
            return ("error", json.dumps({"error": "missing sensor_type", "raw": line}))
        return (data["sensor_type"], data)
    except Exception:
        return ("error", json.dumps({"error": "invalid json", "raw": line}))

def transform_temp(item):
    key, data = item
    value_c = data.pop("value_c")
    data["value_f"] = value_c * 9 / 5 + 32
    return (key, json.dumps(data))

def transform_humidity(item):
    key, data = item
    return (key, json.dumps(data))

# Create the Dataflow
flow = Dataflow("sensor_routing")

# Input stream
raw_stream = op.input("input", flow, FileSource(Path("/home/user/bytewax_routing/sensors.json")))

# Parse each line
parsed_stream = op.map("parse", raw_stream, parse_line)

# Branch 1: Valid vs Invalid (error)
b_valid_invalid = op.branch("valid_invalid", parsed_stream, lambda x: x[0] != "error")
valid_stream = b_valid_invalid.trues
error_stream = b_valid_invalid.falses

# Branch 2: Temperature vs Humidity/Other
b_temp_humidity = op.branch("temp_humidity", valid_stream, lambda x: x[0] == "temperature")
temp_stream = b_temp_humidity.trues
other_stream = b_temp_humidity.falses

# Branch 3: Humidity vs Other
b_humidity = op.branch("humidity_only", other_stream, lambda x: x[0] == "humidity")
humidity_stream = b_humidity.trues

# Format/transform streams to (key, value) where value is the JSON string
temp_lines = op.map("transform_temp", temp_stream, transform_temp)
humidity_lines = op.map("transform_humidity", humidity_stream, transform_humidity)

# Output streams to sinks
op.output("error_sink", error_stream, FileSink(Path("/home/user/bytewax_routing/errors.json")))
op.output("temp_sink", temp_lines, FileSink(Path("/home/user/bytewax_routing/temperature.json")))
op.output("humidity_sink", humidity_lines, FileSink(Path("/home/user/bytewax_routing/humidity.json")))

if __name__ == "__main__":
    run_main(flow)
