import json
from pathlib import Path

from bytewax.connectors.files import FileSink, FileSource
from bytewax.dataflow import Dataflow
from bytewax.operators import branch, input, map, merge, output

BASE_DIR = Path("/home/user/bytewax_routing")

flow = Dataflow("sensor_routing")

# 1. Read lines from sensors.json
lines = input("read_sensors", flow, FileSource(BASE_DIR / "sensors.json"))


# 2. Parse each line as JSON. If parsing fails, produce an error dict.
def parse_line(line: str) -> dict:
    try:
        return json.loads(line)
    except json.JSONDecodeError:
        return {"error": "invalid json", "raw": line}


parsed = map("parse_json", lines, parse_line)


# 3. Branch: separate valid records (no "error" key) from error records.
def is_valid(record: dict) -> bool:
    return "error" not in record


branch_out = branch("split_valid_error", parsed, is_valid)
valid = branch_out.trues
errors = branch_out.falses


# 4. Branch valid records: separate those missing sensor_type from those that have it.
def has_sensor_type(record: dict) -> bool:
    return "sensor_type" in record


branch_type = branch("split_missing_type", valid, has_sensor_type)
has_type = branch_type.trues
missing_type = branch_type.falses


# 5. Convert missing_type records into error dicts.
def missing_type_error(record: dict) -> dict:
    raw = json.dumps(record)
    return {"error": "missing sensor_type", "raw": raw}


missing_type_errors = map("make_missing_type_error", missing_type, missing_type_error)


# 6. Branch has_type: separate temperature from humidity.
def is_temperature(record: dict) -> bool:
    return record["sensor_type"] == "temperature"


branch_sensor = branch("split_temp_humidity", has_type, is_temperature)
temperature_stream = branch_sensor.trues
humidity_stream = branch_sensor.falses


# 7. Transform temperature: convert value_c to value_f.
def convert_temperature(record: dict) -> dict:
    value_c = record.pop("value_c")
    record["value_f"] = value_c * 9.0 / 5.0 + 32.0
    return record


temperature_transformed = map("convert_temp", temperature_stream, convert_temperature)


# 8. Serialize to JSON strings and wrap in (key, value) tuples for FileSink.
def to_kv(item: dict) -> tuple:
    json_str = json.dumps(item)
    return (json_str, json_str)


error_kv = map("serialize_errors", errors, to_kv)
missing_type_kv = map("serialize_missing_type", missing_type_errors, to_kv)
temperature_kv = map("serialize_temp", temperature_transformed, to_kv)
humidity_kv = map("serialize_humidity", humidity_stream, to_kv)


# 9. Merge error streams and write to output files.
all_errors = merge("merge_errors", error_kv, missing_type_kv)
output("write_errors", all_errors, FileSink(BASE_DIR / "errors.json"))
output("write_temperature", temperature_kv, FileSink(BASE_DIR / "temperature.json"))
output("write_humidity", humidity_kv, FileSink(BASE_DIR / "humidity.json"))

# 10. Run the dataflow.
from bytewax.testing import run_main

run_main(flow)
