from pathlib import Path
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSource, FileSink
from bytewax.testing import run_main

with open("input.jsonl", "w") as f:
    f.write('{"timestamp": "2023-10-01T10:00:00Z", "service": "auth", "response_time_ms": 200}\n')
    f.write('{"timestamp": "2023-10-01T10:00:05Z", "service": "auth", "response_time_ms": 600}\n')

flow = Dataflow("files")
up = op.input("in", flow, FileSource("input.jsonl"))
op.output("out", up, FileSink(Path("output.jsonl")))

run_main(flow)
with open("output.jsonl") as f:
    print("OUTPUT:", f.read())
