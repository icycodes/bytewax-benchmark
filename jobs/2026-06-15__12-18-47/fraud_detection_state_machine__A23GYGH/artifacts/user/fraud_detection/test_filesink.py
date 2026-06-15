from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSink
from bytewax.testing import run_main, TestingSource

flow = Dataflow("test")
inp = op.input("inp", flow, TestingSource(["a", "b"]))
keyed = op.map("key", inp, lambda x: ("key", x))
op.output("out", keyed, FileSink("/home/user/fraud_detection/test_out.txt"))
run_main(flow)
