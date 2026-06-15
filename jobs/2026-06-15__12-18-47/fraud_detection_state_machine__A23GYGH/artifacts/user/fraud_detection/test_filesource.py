from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSource
from bytewax.testing import run_main

flow = Dataflow("test")
inp = op.input("inp", flow, FileSource("/home/user/fraud_detection/input.jsonl"))
op.inspect("out", inp)
run_main(flow)
