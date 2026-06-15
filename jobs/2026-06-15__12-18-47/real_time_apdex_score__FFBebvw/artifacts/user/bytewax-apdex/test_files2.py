from pathlib import Path
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSource, FileSink
from bytewax.testing import run_main

flow = Dataflow("files")
up = op.input("in", flow, FileSource("input.jsonl"))
keyed = op.map("key", up, lambda x: (str(x), x))
op.output("out", keyed, FileSink(Path("output.jsonl")))

run_main(flow)
with open("output.jsonl") as f:
    print("OUTPUT:", f.read())
