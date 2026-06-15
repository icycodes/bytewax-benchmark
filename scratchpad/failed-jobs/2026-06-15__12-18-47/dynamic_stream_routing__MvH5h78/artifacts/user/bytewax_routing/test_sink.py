import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.connectors.files import FileSource, FileSink
from bytewax.testing import run_main, TestingSource

flow = Dataflow("test")
stream = op.input("inp", flow, TestingSource(["hello", "world"]))
stream = op.map("add_key", stream, lambda x: ("key", x))
op.output("out", stream, FileSink("out.jsonl"))
run_main(flow)
