import pathlib
from typing import Optional, Tuple, Union
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSource
from bytewax.outputs import DynamicSink, StatelessSinkPartition

# Create the Bytewax dataflow
flow = Dataflow("sensor_moving_average")

# Define input and output paths relative to the current directory
input_path = pathlib.Path("input.csv")
output_path = pathlib.Path("output.csv")

# Read lines from input.csv
lines = op.input("read_input", flow, FileSource(input_path))

# Global line counter to preserve original line order in the output file
line_counter = 0

# Parse each line of the CSV and attach its original index
def parse_line(line: str) -> Optional[Tuple[str, Tuple[int, Union[float, str]]]]:
    global line_counter
    line = line.strip()
    if not line:
        return None
    
    index = line_counter
    line_counter += 1
    
    # Check if this is the header line
    if line.startswith("sensor_id,"):
        return ("HEADER", (index, "sensor_id,moving_average"))
        
    parts = line.split(",")
    if len(parts) != 2:
        return None
        
    sensor_id = parts[0].strip()
    try:
        temp = float(parts[1].strip())
    except ValueError:
        return None
        
    return (sensor_id, (index, temp))

parsed_stream = op.filter_map("parse_lines", lines, parse_line)

# Compute stateful moving average
def compute_moving_avg(
    state: Optional[Tuple[float, ...]], 
    val: Tuple[int, Union[float, str]]
) -> Tuple[Optional[Tuple[float, ...]], Tuple[int, Union[float, str]]]:
    index, actual_val = val
    if isinstance(actual_val, str):
        # Header pass-through
        return (state, (index, actual_val))
        
    # actual_val is a float temperature
    if state is None:
        state = ()
    
    # Append the new reading and keep only the last 3 readings
    new_state = (state + (actual_val,))[-3:]
    
    # Compute the average of the available readings (up to 3) rounded to 2 decimal places
    avg = round(sum(new_state) / len(new_state), 2)
    return (new_state, (index, avg))

moving_avg_stream = op.stateful_map("moving_average", parsed_stream, compute_moving_avg)

# Format the output stream as tuples of (original_index, formatted_string)
def format_output(item: Tuple[str, Tuple[int, Union[float, str]]]) -> Tuple[int, str]:
    key, (index, val) = item
    if key == "HEADER":
        return (index, str(val))
    else:
        # val is the moving average float, formatted to 2 decimal places
        return (index, f"{key},{val:.2f}")

formatted_stream = op.map("format_output", moving_avg_stream, format_output)

# Custom DynamicSink to collect and sort output items to match the input order
class SimpleFileSinkPartition(StatelessSinkPartition[Tuple[int, str]]):
    def __init__(self, file_path: pathlib.Path):
        self._file_path = file_path
        self._items = []
        
    def write_batch(self, items):
        self._items.extend(items)
            
    def close(self):
        # Sort items by their original index to preserve line-by-line order
        self._items.sort(key=lambda x: x[0])
        with open(self._file_path, "w") as f:
            for index, formatted_line in self._items:
                f.write(formatted_line + "\n")

class SimpleFileSink(DynamicSink[Tuple[int, str]]):
    def __init__(self, file_path: pathlib.Path):
        self._file_path = file_path
        
    def build(self, step_id: str, worker_index: int, worker_count: int) -> SimpleFileSinkPartition:
        return SimpleFileSinkPartition(self._file_path)

# Write output to output.csv
op.output("write_output", formatted_stream, SimpleFileSink(output_path))
