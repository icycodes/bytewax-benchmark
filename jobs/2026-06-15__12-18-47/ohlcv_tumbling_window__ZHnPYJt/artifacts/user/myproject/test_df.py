from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import CSVSource
from bytewax.testing import run_main
from pathlib import Path

flow = Dataflow("test")
src = CSVSource(Path("test.csv"))
stream = op.input("inp", flow, src)
op.inspect("insp", stream)

run_main(flow)
