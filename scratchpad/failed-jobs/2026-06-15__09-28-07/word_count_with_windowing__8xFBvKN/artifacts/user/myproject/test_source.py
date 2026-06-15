from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSource
from bytewax.testing import run_main

flow = Dataflow("test_source")
lines = op.input("read", flow, FileSource("input.jsonl"))
op.inspect("lines", lines)
run_main(flow)
