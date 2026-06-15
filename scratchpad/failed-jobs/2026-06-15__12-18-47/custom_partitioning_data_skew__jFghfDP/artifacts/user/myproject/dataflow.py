import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.connectors.files import FileSink
from pathlib import Path

class FilePartition(StatefulSourcePartition):
    def __init__(self, filename, partition_id, num_partitions, resume_state=None):
        self.filename = filename
        self.partition_id = int(partition_id)
        self.num_partitions = num_partitions
        self.current_line = resume_state if resume_state is not None else 0
        self.f = open(filename, 'r')
        # Skip to the current line
        for _ in range(self.current_line):
            self.f.readline()

    def next_batch(self):
        while True:
            line = self.f.readline()
            if not line:
                raise StopIteration()
            
            line_idx = self.current_line
            self.current_line += 1
            
            if line_idx % self.num_partitions == self.partition_id:
                return [line.strip("\n")]

    def snapshot(self):
        return self.current_line

    def close(self):
        self.f.close()

class CustomFileSource(FixedPartitionedSource):
    def __init__(self, filename):
        self.filename = filename
        self.num_partitions = 3

    def list_parts(self):
        return ["0", "1", "2"]

    def build_part(self, step_id, for_part, resume_state):
        return FilePartition(self.filename, for_part, self.num_partitions, resume_state)

flow = Dataflow("flow")
stream = op.input("input", flow, CustomFileSource("input.jsonl"))
keyed_stream = op.map("add_key", stream, lambda x: ("", x))
op.output("output", keyed_stream, FileSink(Path("output.jsonl")))
