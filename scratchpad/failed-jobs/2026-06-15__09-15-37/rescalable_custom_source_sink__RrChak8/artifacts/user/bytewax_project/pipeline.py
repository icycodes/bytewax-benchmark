"""
pipeline.py – Bytewax dataflow: uppercase every line from input files.

First-time setup (initialise recovery store):
    python -c "
    from pathlib import Path; from bytewax.recovery import init_db_dir
    init_db_dir(Path('recovery'), 1)
    "

Run (single worker, no recovery):
    python -m bytewax.run pipeline:flow

Run (single worker, with recovery):
    python -m bytewax.run pipeline:flow -r recovery/ -s 1 -b 1

Run (two workers, with recovery):
    python -m bytewax.run pipeline:flow -w2 -r recovery/ -s 1 -b 1

The dataflow:
    CustomFileSource  →  map(uppercase)  →  CustomFileSink

* Source emits ``(filename, line)`` tuples.
* The map step uppercases the *line* part while keeping the *filename*
  key intact so the sink can route each item to the correct output file.
* Sink writes the uppercased lines to ``output_data/<filename>``.
"""

from pathlib import Path

import bytewax.operators as op
from bytewax.dataflow import Dataflow

from connectors import CustomFileSource, CustomFileSink

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_HERE = Path(__file__).parent
INPUT_DIR = _HERE / "input_data"
OUTPUT_DIR = _HERE / "output_data"

# ---------------------------------------------------------------------------
# Dataflow definition
# ---------------------------------------------------------------------------

flow = Dataflow("file_uppercase")

# 1. Input – yields (filename, line) tuples
lines = op.input("read_files", flow, CustomFileSource(INPUT_DIR))


# 2. Transform – uppercase the line, preserve the filename key
def uppercase_line(item: tuple) -> tuple:
    filename, line = item
    return (filename, line.upper())


uppercased = op.map("to_uppercase", lines, uppercase_line)

# 3. Output – routes (filename, line) to the matching output file
op.output("write_files", uppercased, CustomFileSink(OUTPUT_DIR, INPUT_DIR))
