import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import run_main, TestingSource
from bytewax.outputs import FixedPartitionedSink, StatefulSinkPartition

class TestSinkPart(StatefulSinkPartition):
    def __init__(self, name):
        self.name = name
    def write_batch(self, values):
        print(f"Sink {self.name} received: {values}")
    def snapshot(self):
        return None

class TestSink(FixedPartitionedSink):
    def list_parts(self):
        return ["B", "A", "C"]
    def part_fn(self, item_key):
        parts = sorted(["B", "A", "C"])
        return parts.index(item_key)
    def build_part(self, step_id, for_part, resume_state):
        return TestSinkPart(for_part)

flow = Dataflow("test")
src = op.input("in", flow, TestingSource([("A", "valA"), ("B", "valB"), ("C", "valC")]))
op.output("out", src, TestSink())

run_main(flow)
