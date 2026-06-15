import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.connectors.files import FileSource, FileSink
from pathlib import Path

def parse_line(line):
    # line is like "s1,20.5"
    sensor_id, temp_str = line.split(",")
    return sensor_id, float(temp_str)

def moving_average_mapper(state, temp):
    if state is None:
        state = []
    
    # We must return a new state object, not mutate in place
    new_state = state + [temp]
    if len(new_state) > 3:
        new_state = new_state[-3:]
    
    avg = sum(new_state) / len(new_state)
    avg_rounded = round(avg, 2)
    
    return (new_state, avg_rounded)

def format_output(key_value):
    sensor_id, avg = key_value
    # Format as "s1,20.50"
    return (sensor_id, f"{sensor_id},{avg:.2f}")

flow = Dataflow("moving_average")

# Read from input.csv
lines = op.input("read_input", flow, FileSource("input.csv"))

# Parse lines to (sensor_id, temperature)
parsed = op.map("parse", lines, parse_line)

# Calculate moving average
averages = op.stateful_map("moving_avg", parsed, moving_average_mapper)

# Format for output
formatted = op.map("format", averages, format_output)

# Write to output.csv
op.output("write_output", formatted, FileSink(Path("output.csv")))
