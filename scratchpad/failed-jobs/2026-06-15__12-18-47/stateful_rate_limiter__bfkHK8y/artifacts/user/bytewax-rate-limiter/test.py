from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.testing import run_main, TestingSource, TestingSink

flow = Dataflow("test")
stream = op.input("in", flow, TestingSource([1, 2, 3]))
out = []
op.output("out", stream, TestingSink(out))
run_main(flow)
print(out)
