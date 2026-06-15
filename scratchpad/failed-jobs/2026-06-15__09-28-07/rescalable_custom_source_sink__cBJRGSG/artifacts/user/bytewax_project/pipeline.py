import os
import glob
from pathlib import Path

from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.outputs import FixedPartitionedSink, StatefulSinkPartition
import bytewax.operators as op
from bytewax.dataflow import Dataflow

class FileSourcePartition(StatefulSourcePartition):
    def __init__(self, filepath, resume_state):
        self.filepath = filepath
        self.filename = os.path.basename(filepath)
        self._current_line = resume_state or 0
        self._f = open(self.filepath, 'r')
        # Skip lines up to resume_state
        for _ in range(self._current_line):
            self._f.readline()

    def next_batch(self):
        line = self._f.readline()
        if not line:
            raise StopIteration()
        self._current_line += 1
        return [(self.filename, line.rstrip('\n'))]

    def snapshot(self):
        return self._current_line

    def close(self):
        self._f.close()

class CustomFileSource(FixedPartitionedSource):
    def __init__(self, input_dir):
        self.input_dir = input_dir

    def list_parts(self):
        files = glob.glob(os.path.join(self.input_dir, '*.txt'))
        return [os.path.basename(f) for f in files]

    def build_part(self, step_id, for_part, resume_state):
        filepath = os.path.join(self.input_dir, for_part)
        return FileSourcePartition(filepath, resume_state)

class FileSinkPartition(StatefulSinkPartition):
    def __init__(self, filepath, resume_state):
        self.filepath = filepath
        self._lines_written = resume_state or 0
        
        # Ensure directory exists
        os.makedirs(os.path.dirname(self.filepath), exist_ok=True)
        
        # If file exists, truncate it to the number of lines written
        if os.path.exists(self.filepath):
            with open(self.filepath, 'r') as f:
                lines = f.readlines()
            
            with open(self.filepath, 'w') as f:
                f.writelines(lines[:self._lines_written])
        else:
            open(self.filepath, 'w').close()
            
        self._f = open(self.filepath, 'a')

    def write_batch(self, values):
        for value in values:
            self._f.write(value + '\n')
            self._lines_written += 1

    def snapshot(self):
        return self._lines_written

    def close(self):
        self._f.close()

class CustomFileSink(FixedPartitionedSink):
    def __init__(self, output_dir, input_dir):
        self.output_dir = output_dir
        self.input_dir = input_dir

    def list_parts(self):
        files = glob.glob(os.path.join(self.input_dir, '*.txt'))
        return [os.path.basename(f) for f in files]

    def part_fn(self, item_key):
        parts = sorted(self.list_parts())
        return parts.index(item_key)

    def build_part(self, step_id, for_part, resume_state):
        filepath = os.path.join(self.output_dir, for_part)
        return FileSinkPartition(filepath, resume_state)

def to_uppercase(item):
    filename, line = item
    return (filename, line.upper())

flow = Dataflow("file_processor")
input_dir = "/home/user/bytewax_project/input_data"
output_dir = "/home/user/bytewax_project/output_data"

src = op.input("file_input", flow, CustomFileSource(input_dir))
uppercased = op.map("uppercase", src, to_uppercase)
op.output("file_output", uppercased, CustomFileSink(output_dir, input_dir))
