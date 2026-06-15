import os
import json
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.testing import TestingSource
from bytewax.outputs import DynamicSink, StatelessSinkPartition

class JSONLPartition(StatelessSinkPartition[int]):
    def __init__(self, worker_index: int, run_id: str, out_dir: str):
        self.worker_index = worker_index
        self.run_id = run_id
        self.out_dir = out_dir
        self.part_number = 0
        self.records_written = 0
        self.current_file = None
        
        # Ensure output directory exists
        os.makedirs(self.out_dir, exist_ok=True)

    def _get_file(self):
        if self.current_file is None:
            file_path = os.path.join(
                self.out_dir,
                f"output-{self.run_id}-worker-{self.worker_index}-part-{self.part_number}.jsonl"
            )
            self.current_file = open(file_path, "w", encoding="utf-8")
        return self.current_file

    def write_batch(self, items: list[int]) -> None:
        for item in items:
            record = {"worker": self.worker_index, "value": item}
            line = json.dumps(record) + "\n"
            
            f = self._get_file()
            f.write(line)
            self.records_written += 1
            
            if self.records_written == 20:
                f.close()
                self.current_file = None
                self.records_written = 0
                self.part_number += 1

    def close(self) -> None:
        if self.current_file is not None:
            self.current_file.close()
            self.current_file = None

class JSONLDynamicSink(DynamicSink[int]):
    def __init__(self, run_id: str, out_dir: str):
        self.run_id = run_id
        self.out_dir = out_dir

    def build(self, step_id: str, worker_index: int, worker_count: int) -> JSONLPartition:
        return JSONLPartition(worker_index, self.run_id, self.out_dir)

# Initialize the dataflow
flow = Dataflow("flow")

# Generate the 200 integers
nums = op.input("input", flow, TestingSource(range(200)))

# Redistribute items across workers to ensure parallel processing
redistributed = op.redistribute("redistribute", nums)

# Read the run ID from the environment variable
run_id = os.environ.get("ZEALT_RUN_ID", "test-run")

# Output to the custom dynamic sink
op.output("output", redistributed, JSONLDynamicSink(run_id, "/home/user/bytewax-sink/out"))
