from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSink
from bytewax.testing import TestingSource

flow = Dataflow("test")
inp = op.input("in", flow, TestingSource([("key", "hello"), ("key", "world")]))
op.output("out", inp, FileSink("test_out.txt"))
