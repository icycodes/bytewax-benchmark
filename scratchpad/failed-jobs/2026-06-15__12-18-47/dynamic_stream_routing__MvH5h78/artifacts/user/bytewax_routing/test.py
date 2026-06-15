import bytewax
from bytewax.dataflow import Dataflow
from bytewax.connectors.files import FileSource, FileSink
from bytewax.testing import run_main

flow = Dataflow("test")
flow.input("inp", FileSource("test.jsonl"))
flow.output("out", FileSink("out.jsonl"))
run_main(flow)
