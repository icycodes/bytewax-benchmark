from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.inputs import DynamicSource, StatelessSourcePartition
from bytewax.testing import run_main
import csv

class CSVPartition(StatelessSourcePartition):
    def __init__(self, path):
        self.f = open(path, "r")
        self.reader = csv.DictReader(self.f)
    def next_batch(self):
        try:
            return [next(self.reader)]
        except StopIteration:
            raise
class MyCSVSource(DynamicSource):
    def __init__(self, path):
        self.path = path
    def build(self, step_id, worker_index, worker_count):
        return CSVPartition(self.path)

flow = Dataflow("test")
stream = op.input("inp", flow, MyCSVSource("test.csv"))
op.inspect("insp", stream)
run_main(flow)
