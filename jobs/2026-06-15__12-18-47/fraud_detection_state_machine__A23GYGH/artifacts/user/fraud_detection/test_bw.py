import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import run_main

flow = Dataflow("test")
inp = op.input("inp", flow, lambda: [("a", 1), ("a", 2)])
def builder():
    return 0
def mapper(state, val):
    return state + val, state + val
out = op.stateful_map("sm", inp, builder, mapper)
op.output("out", out, lambda x: print(x))

run_main(flow)
