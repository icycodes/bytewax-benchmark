import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.connectors.files import FileSource, FileSink
from bytewax.testing import run_main

flow = Dataflow("test")
stream = op.input("inp", flow, FileSource("test.jsonl"))
op.output("out", stream, FileSink("out.jsonl"))
run_main(flow)
