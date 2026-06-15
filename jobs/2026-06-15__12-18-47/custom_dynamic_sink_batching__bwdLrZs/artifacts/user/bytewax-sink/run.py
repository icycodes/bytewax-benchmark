import os
import json
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource
from bytewax.outputs import DynamicSink, StatelessSinkPartition
import bytewax.operators as op

class RotatingFileSinkPartition(StatelessSinkPartition):
    def __init__(self, worker_index, run_id, out_dir="out"):
        self.worker_index = worker_index
        self.run_id = run_id
        self.out_dir = out_dir
        self.part_number = 0
        self.records_in_current_file = 0
        self.max_records_per_file = 20
        self.current_file = None
        os.makedirs(self.out_dir, exist_ok=True)

    def _open_next_file(self):
        if self.current_file is not None:
            self.current_file.close()
        
        filename = f"output-{self.run_id}-worker-{self.worker_index}-part-{self.part_number}.jsonl"
        filepath = os.path.join(self.out_dir, filename)
        self.current_file = open(filepath, "w")
        self.records_in_current_file = 0

    def write_batch(self, items):
        for item in items:
            if self.current_file is None or self.records_in_current_file >= self.max_records_per_file:
                if self.current_file is not None:
                    self.part_number += 1
                self._open_next_file()
            
            record = {"worker": self.worker_index, "value": item}
            self.current_file.write(json.dumps(record) + "\n")
            self.records_in_current_file += 1

    def close(self):
        if self.current_file is not None:
            self.current_file.close()

class RotatingFileSink(DynamicSink):
    def __init__(self, run_id, out_dir="out"):
        self.run_id = run_id
        self.out_dir = out_dir

    def build(self, step_id, worker_index, worker_count):
        return RotatingFileSinkPartition(worker_index, self.run_id, self.out_dir)


flow = Dataflow("rotating_file_sink_flow")
inp = op.input("input", flow, TestingSource(range(200)))

run_id = os.environ.get("ZEALT_RUN_ID", "default-run-id")
op.output("output", inp, RotatingFileSink(run_id=run_id, out_dir="/home/user/bytewax-sink/out"))
