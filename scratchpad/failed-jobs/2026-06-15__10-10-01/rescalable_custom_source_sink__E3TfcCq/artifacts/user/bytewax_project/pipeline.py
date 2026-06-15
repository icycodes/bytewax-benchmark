"""Bytewax dataflow: read .txt files, uppercase lines, write to output."""

import os

from bytewax import operators as op
from bytewax.dataflow import Dataflow

from connectors import CustomFileSource, CustomFileSink

INPUT_DIR = os.path.join(os.path.dirname(__file__), "input_data")
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), "output_data")

# Create the dataflow
flow = Dataflow("file_uppercase")

# Input source
source = CustomFileSource(INPUT_DIR)

# Input: read (filename, line) tuples from CustomFileSource
stream = op.input("read_files", flow, source)

# Map: convert each (filename, line) to (filename, line.upper())
def uppercase(item: tuple) -> tuple:
    filename, line = item
    return (filename, line.upper())


stream = op.map("uppercase", stream, uppercase)

# Output: write (filename, line) to the appropriate file
# We pass the same partition keys as the source so the sink knows
# which files to create.
sink = CustomFileSink(OUTPUT_DIR, source.list_parts())
op.output("write_files", stream, sink)
