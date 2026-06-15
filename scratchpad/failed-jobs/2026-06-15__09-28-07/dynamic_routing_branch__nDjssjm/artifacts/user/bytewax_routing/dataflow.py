import json
import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.connectors.files import FileSource, FileSink
from bytewax.testing import run_main

flow = Dataflow("sensor_routing")

lines = op.input("input", flow, FileSource("sensors.json"))

def parse_line(line):
    try:
        data = json.loads(line)
        if not isinstance(data, dict):
            return "error", {"error": "invalid json", "raw": line}
        if "sensor_type" not in data:
            return "error", {"error": "missing sensor_type", "raw": line}
        return "valid", data
    except json.JSONDecodeError:
        return "error", {"error": "invalid json", "raw": line}

parsed = op.map("parse", lines, parse_line)

b1 = op.branch("valid_or_error", parsed, lambda x: x[0] == "valid")
valids = op.map("get_valid", b1.trues, lambda x: x[1])
errors = op.map("get_error", b1.falses, lambda x: x[1])

errors_json = op.map("json_errors", errors, lambda x: ("error", json.dumps(x)))
op.output("out_errors", errors_json, FileSink("errors.json"))

b2 = op.branch("temp_or_hum", valids, lambda x: x.get("sensor_type") == "temperature")
temps_raw = b2.trues
hums_raw = b2.falses

hums_filtered = op.filter("filter_hum", hums_raw, lambda x: x.get("sensor_type") == "humidity")

def transform_temp(data):
    new_data = dict(data)
    new_data["value_f"] = new_data["value_c"] * 9/5 + 32
    del new_data["value_c"]
    return new_data

temps = op.map("transform_temp", temps_raw, transform_temp)

temps_json = op.map("json_temps", temps, lambda x: ("temp", json.dumps(x)))
op.output("out_temps", temps_json, FileSink("temperature.json"))

hums_json = op.map("json_hums", hums_filtered, lambda x: ("hum", json.dumps(x)))
op.output("out_hums", hums_json, FileSink("humidity.json"))

if __name__ == "__main__":
    run_main(flow)
