import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.connectors.files import FileSource
from bytewax.testing import run_main

flow = Dataflow("test")
inp = op.input("inp", flow, FileSource("input.jsonl"))
op.inspect("inspect", inp)

if __name__ == "__main__":
    run_main(flow)
