"""Bytewax stateful streaming pipeline: moving average of sensor temperatures."""

from pathlib import Path
from typing import List, Optional, Tuple

import bytewax.operators as op
from bytewax.connectors.files import FileSink, FileSource
from bytewax.dataflow import Dataflow

# ---------------------------------------------------------------------------
# Dataflow definition
# ---------------------------------------------------------------------------

flow = Dataflow("moving_avg")

# 1. Read raw lines from input.csv (each line: "sensor_id,temperature")
raw: op.Stream = op.input("read_csv", flow, FileSource(Path("input.csv")))

# 2. Parse each line into a (key, value) tuple required by stateful_map.
#    key  → sensor_id  (must be a str)
#    value → temperature as float
def parse_line(line: str) -> Tuple[str, float]:
    sensor_id, temp = line.strip().split(",")
    return (sensor_id, float(temp))

keyed = op.map("parse", raw, parse_line)

# 3. Stateful logic: maintain a sliding window of the last 3 readings.
#    State is an immutable tuple of floats so that Bytewax's recovery
#    snapshots always receive a fresh copy.
WINDOW = 3

def moving_avg(
    state: Optional[Tuple[float, ...]],
    temp: float,
) -> Tuple[Tuple[float, ...], float]:
    """Append the new reading, keep only the last WINDOW values, return avg."""
    if state is None:
        state = ()
    # Build a new tuple (no in-place mutation) with the latest reading appended
    new_state: Tuple[float, ...] = (state + (temp,))[-WINDOW:]
    avg = sum(new_state) / len(new_state)
    return (new_state, avg)

averaged = op.stateful_map("moving_avg", keyed, moving_avg)

# 4. Format output as "sensor_id,moving_average" with 2 decimal places.
#    FileSink expects (key, value) tuples where value is the string to write.
def format_output(item: Tuple[str, float]) -> Tuple[str, str]:
    sensor_id, avg = item
    return (sensor_id, f"{sensor_id},{avg:.2f}")

formatted = op.map("format", averaged, format_output)

# 5. Write results to output.csv
op.output("write_csv", formatted, FileSink(Path("output.csv")))
