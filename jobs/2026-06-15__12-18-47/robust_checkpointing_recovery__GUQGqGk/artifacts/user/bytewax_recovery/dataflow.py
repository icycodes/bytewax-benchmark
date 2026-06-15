import os
from typing import Iterable, Optional

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.inputs import DynamicSource, StatelessSourcePartition
from bytewax.outputs import DynamicSink, StatelessSinkPartition

class _FileSourcePartition(StatelessSourcePartition):
    def __init__(self, path: str):
        self._f = open(path, "r")
        
    def next_batch(self) -> Iterable[str]:
        lines = self._f.readlines(1000)
        if not lines:
            raise StopIteration()
        return lines
        
    def close(self):
        self._f.close()

class MyFileSource(DynamicSource):
    def __init__(self, path: str):
        self._path = path
        
    def build(self, step_id: str, worker_index: int, worker_count: int) -> _FileSourcePartition:
        if worker_index == 0:
            return _FileSourcePartition(self._path)
        else:
            # Other workers read nothing
            class _EmptyPartition(StatelessSourcePartition):
                def next_batch(self):
                    raise StopIteration()
            return _EmptyPartition()

class _FileSinkPartition(StatelessSinkPartition):
    def __init__(self, path: str):
        self._f = open(path, "a")
        
    def write_batch(self, items: Iterable[str]):
        for item in items:
            self._f.write(item + "\n")
        self._f.flush()
        
    def close(self):
        self._f.close()

class MyFileSink(DynamicSink):
    def __init__(self, path: str):
        self._path = path
        
    def build(self, step_id: str, worker_index: int, worker_count: int) -> _FileSinkPartition:
        if worker_index == 0:
            # Truncate if we are the only worker writing
            open(self._path, "w").close()
            return _FileSinkPartition(self._path)
        else:
            class _EmptySink(StatelessSinkPartition):
                def write_batch(self, items):
                    pass
            return _EmptySink()

input_file = os.environ["INPUT_FILE"]
output_file = os.environ["OUTPUT_FILE"]

flow = Dataflow("running_max")

# Read from file
stream = op.input("input", flow, MyFileSource(input_file))

# FileSource yields lines as strings
def parse_line(line):
    k, v = line.strip().split(",")
    return k, int(v)
    
stream = op.map("parse", stream, parse_line)

# Stateful map to keep running max
def update_max(current_max, new_val):
    if current_max is None:
        current_max = new_val
    else:
        current_max = max(current_max, new_val)
    return current_max, current_max
    
stream = op.stateful_map("running_max_map", stream, update_max)

# Format for output
def format_output(key_max):
    k, v = key_max
    return f"{k},{v}"
    
stream = op.map("format", stream, format_output)

# Write to file
op.output("output", stream, MyFileSink(output_file))
