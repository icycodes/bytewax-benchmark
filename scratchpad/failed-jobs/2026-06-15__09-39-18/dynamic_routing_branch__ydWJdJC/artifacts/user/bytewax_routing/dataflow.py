import json
from bytewax.dataflow import Dataflow
from bytewax.connectors.files import FileSource, FileSink
from bytewax.testing import run_main
import bytewax.operators as op

BASE = "/home/user/bytewax_routing"

flow = Dataflow("sensor_routing")

# Step 1: Read lines from sensors.json
lines = op.input("read", flow, FileSource(f"{BASE}/sensors.json"))


# Step 2: Parse each line, tagging valid/invalid
def try_parse(line):
    try:
        data = json.loads(line)
        return ("valid_json", data, line)
    except (json.JSONDecodeError, ValueError):
        return ("invalid_json", None, line)


tagged = op.map("parse", lines, try_parse)

# Step 3: Branch valid vs invalid JSON
valid_vs_invalid = op.branch(
    "valid_vs_invalid", tagged, lambda x: x[0] == "valid_json"
)
valid = valid_vs_invalid.trues
invalid = valid_vs_invalid.falses

# Step 4: Format invalid JSON errors
def format_invalid(item):
    _, _, raw = item
    return json.dumps({"error": "invalid json", "raw": raw})


errors_invalid = op.map("format_invalid", invalid, format_invalid)

# Step 5: Branch has sensor_type vs missing sensor_type
has_type = op.branch(
    "has_sensor_type", valid, lambda x: "sensor_type" in x[1]
)
with_type = has_type.trues
without_type = has_type.falses

# Step 6: Format missing sensor_type errors
def format_missing(item):
    _, _, raw = item
    return json.dumps({"error": "missing sensor_type", "raw": raw})


errors_missing = op.map("format_missing", without_type, format_missing)

# Step 7: Merge error streams
errors = op.merge("merge_errors", errors_invalid, errors_missing)

# Step 8: Branch temperature vs humidity
temp_hum = op.branch(
    "temp_hum", with_type, lambda x: x[1]["sensor_type"] == "temperature"
)
temperature = temp_hum.trues
humidity = temp_hum.falses

# Step 9: Transform temperature (value_c -> value_f)
def transform_temp(item):
    _, data, _ = item
    value_c = data.pop("value_c")
    data["value_f"] = round(value_c * 9 / 5 + 32, 1)
    return json.dumps(data)


temp_output = op.map("transform_temp", temperature, transform_temp)

# Step 10: Format humidity output
def format_humidity(item):
    _, data, _ = item
    return json.dumps(data)


hum_output = op.map("format_humidity", humidity, format_humidity)

# Step 11: Add keys and write outputs to files
temp_keyed = op.key_on("temp_key", temp_output, lambda x: "temp")
hum_keyed = op.key_on("hum_key", hum_output, lambda x: "hum")
err_keyed = op.key_on("err_key", errors, lambda x: "err")

op.output("temp_out", temp_keyed, FileSink(f"{BASE}/temperature.json"))
op.output("hum_out", hum_keyed, FileSink(f"{BASE}/humidity.json"))
op.output("err_out", err_keyed, FileSink(f"{BASE}/errors.json"))

if __name__ == "__main__":
    run_main(flow)
