"""
Bytewax dataflow for computing running maximum per key with state recovery.

Reads key,value pairs from a CSV input file, maintains a stateful running
maximum for each key, and writes key,running_max to an output file.
State is persisted via Bytewax's SQLite-based recovery system.
"""

import os
import sys
from pathlib import Path

from bytewax.connectors.files import FileSource, FileSink
from bytewax.dataflow import Dataflow
from bytewax.operators import filter_map, input as op_input
from bytewax.operators import key_on, map, map_value, stateful_map
from bytewax.operators import output as op_output


def parse_line(line: str) -> tuple[str, int] | None:
    """Parse a CSV line into a (key, value) pair. Returns None for empty lines."""
    line = line.strip()
    if not line:
        return None
    parts = line.split(",")
    key = parts[0].strip()
    value = int(parts[1].strip())
    return (key, value)


def running_max_mapper(
    current_max: int | None, value: int
) -> tuple[int | None, int]:
    """
    Stateful mapper that maintains the running maximum for a key.

    Args:
        current_max: The current maximum stored in state (None on first call).
        value: The new value to compare against.

    Returns:
        A tuple of (new_state, output_value) where both are the new maximum.
    """
    if current_max is None:
        new_max = value
    else:
        new_max = max(current_max, value)
    return (new_max, new_max)


def build_dataflow(input_path: str, output_path: str) -> Dataflow:
    """Build and return the Bytewax dataflow."""
    flow = Dataflow("running_max")

    # Read lines from the input file
    lines = op_input("input", flow, FileSource(Path(input_path)))

    # Parse CSV lines into (key, value) tuples
    parsed = map("parse", lines, parse_line)

    # Filter out None values (empty lines)
    parsed = filter_map("filter_none", parsed, lambda x: x)

    # Key the stream by the key field.
    # After key_on, items are (key, (key, value)) where the value is the
    # original parsed tuple. We then use map_value to extract just the int.
    keyed = key_on("key", parsed, lambda kv: kv[0])

    # Extract just the integer value for stateful processing.
    # keyed items: (key_str, (key_str, value_int))
    # After map_value: (key_str, value_int)
    valued = map_value("extract_value", keyed, lambda kv_tuple: kv_tuple[1])

    # Stateful map: maintain running maximum per key.
    # Items in valued: (key_str, value_int)
    # stateful_map calls running_max_mapper(state, value_int)
    # Output items: (key_str, new_max_int)
    maxed = stateful_map("running_max_state", valued, running_max_mapper)

    # Format output as "key,running_max" string.
    # maxed items: (key_str, max_int)
    # Use map to transform the full (key, max_int) item into (key, csv_string)
    def format_item(key_max: tuple[str, int]) -> tuple[str, str]:
        key, max_val = key_max
        return (key, f"{key},{max_val}")

    formatted = map("format_output", maxed, format_item)

    # Write to output file.
    # FileSink is a FixedPartitionedSink which expects (key, value) tuples.
    op_output("output", formatted, FileSink(Path(output_path)))

    return flow


# Module-level flow variable for bytewax.run to discover
# The input/output paths are set via environment variables
_input_path = os.environ.get("BYTEWAX_INPUT_PATH", "")
_output_path = os.environ.get("BYTEWAX_OUTPUT_PATH", "")

if not _input_path or not _output_path:
    print(
        "Error: BYTEWAX_INPUT_PATH and BYTEWAX_OUTPUT_PATH environment "
        "variables must be set.",
        file=sys.stderr,
    )
    sys.exit(1)

flow = build_dataflow(_input_path, _output_path)
