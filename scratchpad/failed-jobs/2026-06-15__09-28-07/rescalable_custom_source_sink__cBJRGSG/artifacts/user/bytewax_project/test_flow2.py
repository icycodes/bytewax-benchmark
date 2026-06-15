import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import run_main
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.outputs import FixedPartitionedSink, StatefulSinkPartition

class TestSourcePart(StatefulSourcePartition):
    def __init__(self):
        self.i = 0
    def next_batch(self):
        if self.i < 2:
            self.i += 1
            return [("my_key", f"val{self.i}")]
        raise StopIteration()
    def snapshot(self):
        return self.i

class TestSource(FixedPartitionedSource):
    def list_parts(self):
        return ["part1"]
    def build_part(self, step_id, for_part, resume_state):
        return TestSourcePart()

class TestSinkPart(StatefulSinkPartition):
    def write_batch(self, values):
        print(f"Sink received values: {values}")
    def snapshot(self):
        return None

class TestSink(FixedPartitionedSink):
    def list_parts(self):
        return ["part1"]
    def part_fn(self, item_key):
        print(f"part_fn received key: {item_key}")
        return 0
    def build_part(self, step_id, for_part, resume_state):
        return TestSinkPart()

flow = Dataflow("test")
src = op.input("in", flow, TestSource())
op.output("out", src, TestSink())

run_main(flow)
