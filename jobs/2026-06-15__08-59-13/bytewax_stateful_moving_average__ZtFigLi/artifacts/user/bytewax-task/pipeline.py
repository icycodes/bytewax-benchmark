"""Bytewax pipeline that computes the stateful moving average of temperature readings."""

from pathlib import Path
from typing import List
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSource
from bytewax.outputs import DynamicSink, StatelessSinkPartition

# Global line counter to assign sequential indices to valid input lines
_line_counter = 0


def parse_line(line: str):
    """Parse a CSV line into a keyed tuple (sensor_id, (line_index, temperature))."""
    global _line_counter
    line = line.strip()
    if not line:
        return None
    try:
        parts = line.split(",")
        if len(parts) == 2:
            sensor_id, temp_str = parts
            sensor_id = sensor_id.strip()
            if sensor_id:
                idx = _line_counter
                _line_counter += 1
                return (sensor_id, (idx, float(temp_str)))
    except Exception:
        pass
    return None


def update_moving_average(state, item):
    """Compute moving average of the last 3 readings for a sensor.
    
    Returns a new state object (tuple) to ensure compatibility with recovery snapshots,
    along with the computed average.
    """
    idx, temp = item
    if state is None:
        state = ()
    
    # Return a new state object (do not mutate in-place)
    new_state = state + (temp,)
    if len(new_state) > 3:
        new_state = new_state[-3:]
    
    # Compute the average of the available readings
    avg = round(sum(new_state) / len(new_state), 2)
    return (new_state, (idx, avg))


class OrderedFileSinkPartition(StatelessSinkPartition):
    """A sink partition that collects items and writes them sorted by original index."""
    
    def __init__(self, path: Path):
        self._path = path
        self._items = []

    def write_batch(self, items: List):
        self._items.extend(items)

    def close(self):
        # Sort items by the original line index (item[1][0])
        self._items.sort(key=lambda x: x[1][0])
        # Write to file
        with open(self._path, "w") as f:
            for sensor_id, (idx, avg) in self._items:
                f.write(f"{sensor_id},{avg:.2f}\n")


class OrderedFileSink(DynamicSink):
    """A custom Bytewax sink that preserves the original line order in the output file."""
    
    def __init__(self, path: Path):
        self._path = path

    def build(self, step_id: str, worker_index: int, worker_count: int):
        return OrderedFileSinkPartition(self._path)


# 1. Define the dataflow
flow = Dataflow("moving_average_pipeline")

# 2. Read sensor readings from input.csv in the current directory
input_path = Path("input.csv")
stream = op.input("input_step", flow, FileSource(input_path))

# 3. Parse input lines and filter out invalid/empty lines
keyed_stream = op.filter_map("parse_step", stream, parse_line)

# 4. Compute moving average statefully
moving_avg_stream = op.stateful_map("moving_avg_step", keyed_stream, update_moving_average)

# 5. Write output using the custom OrderedFileSink to preserve original line order
output_path = Path("output.csv")
op.output("output_step", moving_avg_stream, OrderedFileSink(output_path))
