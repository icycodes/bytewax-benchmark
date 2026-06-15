import bytewax.operators as op
from bytewax.connectors.files import FileSource, FileSink
from bytewax.dataflow import Dataflow


def parse_line(line):
    """Parse a CSV line 'sensor_id,temperature' into (sensor_id, float)."""
    sensor_id, temp = line.strip().split(",")
    return (sensor_id, float(temp))


def moving_average(state, value):
    """Compute the moving average of the last 3 temperature readings.

    Args:
        state: List of recent temperature readings (max 3), or None.
        value: New temperature reading (float).

    Returns:
        (new_state, moving_average): Updated state list and the
        moving average rounded to 2 decimal places.
    """
    if state is None:
        state = []
    # Build a new list (do not mutate in-place)
    new_state = state + [value]
    # Keep only the last 3 readings
    if len(new_state) > 3:
        new_state = new_state[-3:]
    avg = round(sum(new_state) / len(new_state), 2)
    return (new_state, avg)


def format_output(item):
    """Format (sensor_id, moving_average) as (sensor_id, csv_line)."""
    sensor_id, avg = item
    return (sensor_id, f"{sensor_id},{avg:.2f}")


flow = Dataflow("moving_average")

# Read lines from input.csv
inp = op.input("read_input", flow, FileSource("input.csv"))

# Parse each line into (sensor_id, temperature)
parsed = op.map("parse", inp, parse_line)

# Key on sensor_id for stateful processing
keyed = op.key_on("key_on_sensor", parsed, lambda x: x[0])

# Extract the temperature value as the value for stateful_map
temps = op.map_value("extract_temp", keyed, lambda x: x[1])

# Compute moving average statefully
averages = op.stateful_map("moving_avg", temps, moving_average)

# Format output: keep (key, value) structure with value being the CSV line
formatted = op.map("format_output", averages, format_output)

# Write to output.csv
op.output("write_output", formatted, FileSink("output.csv"))