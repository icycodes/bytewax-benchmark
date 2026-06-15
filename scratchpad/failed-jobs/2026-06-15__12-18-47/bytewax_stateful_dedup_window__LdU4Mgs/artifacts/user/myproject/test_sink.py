import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.connectors.files import FileSink
from bytewax.testing import run_main, TestingSource
from pathlib import Path

flow = Dataflow("test")
inp = op.input("inp", flow, TestingSource(["a", "b", "c"]))
keyed = op.map("key", inp, lambda x: ("mykey", f"val:{x}"))
op.output("out", keyed, FileSink(Path("test_out.txt")))

if __name__ == "__main__":
    run_main(flow)
