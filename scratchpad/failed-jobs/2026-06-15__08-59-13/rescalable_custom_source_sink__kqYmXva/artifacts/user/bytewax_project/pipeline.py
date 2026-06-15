import os
import glob
import time
from typing import List, Optional, Tuple
import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.outputs import FixedPartitionedSink, StatefulSinkPartition

def truncate_to_lines(filepath: str, num_lines: int) -> None:
    if not os.path.exists(filepath):
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'w') as f:
            pass
        return

    if num_lines == 0:
        with open(filepath, 'w') as f:
            pass
        return

    byte_offset = 0
    lines_found = 0
    with open(filepath, 'rb') as f:
        for line in f:
            byte_offset += len(line)
            lines_found += 1
            if lines_found == num_lines:
                break

    with open(filepath, 'r+b') as f:
        f.truncate(byte_offset)

class FileSourcePartition(StatefulSourcePartition[Tuple[str, str], int]):
    def __init__(self, filepath: str, resume_state: Optional[int]):
        self.filepath = filepath
        self.filename = os.path.basename(filepath)
        self.lines_read = resume_state or 0
        self.file = open(filepath, 'r')
        for _ in range(self.lines_read):
            self.file.readline()

    def next_batch(self) -> List[Tuple[str, str]]:
        line = self.file.readline()
        if not line:
            raise StopIteration
        line_content = line.rstrip('\r\n')
        self.lines_read += 1
        return [(self.filename, line_content)]

    def snapshot(self) -> int:
        return self.lines_read

    def close(self) -> None:
        if self.file:
            self.file.close()

class CustomFileSource(FixedPartitionedSource[Tuple[str, str], int]):
    def __init__(self, input_dir: str):
        self.input_dir = input_dir

    def list_parts(self) -> List[str]:
        pattern = os.path.join(self.input_dir, "*.txt")
        files = glob.glob(pattern)
        return sorted([os.path.basename(f) for f in files])

    def build_part(
        self, step_id: str, for_part: str, resume_state: Optional[int]
    ) -> FileSourcePartition:
        filepath = os.path.join(self.input_dir, for_part)
        return FileSourcePartition(filepath, resume_state)

class FileSinkPartition(StatefulSinkPartition[str, int]):
    def __init__(self, filepath: str, resume_state: Optional[int]):
        self.filepath = filepath
        self.lines_written = resume_state or 0
        truncate_to_lines(self.filepath, self.lines_written)
        self.file = open(self.filepath, 'a')

    def write_batch(self, values: List[str]) -> None:
        for val in values:
            self.file.write(val + '\n')
            self.lines_written += 1
        self.file.flush()

    def snapshot(self) -> int:
        return self.lines_written

    def close(self) -> None:
        if self.file:
            self.file.close()

class CustomFileSink(FixedPartitionedSink[str, int]):
    def __init__(self, input_dir: str, output_dir: str):
        self.input_dir = input_dir
        self.output_dir = output_dir

    def list_parts(self) -> List[str]:
        pattern = os.path.join(self.input_dir, "*.txt")
        files = glob.glob(pattern)
        return sorted([os.path.basename(f) for f in files])

    def part_fn(self, item_key: str) -> int:
        parts = sorted(self.list_parts())
        if item_key in parts:
            return parts.index(item_key)
        import zlib
        return zlib.adler32(item_key.encode())

    def build_part(
        self, step_id: str, for_part: str, resume_state: Optional[int]
    ) -> FileSinkPartition:
        filepath = os.path.join(self.output_dir, for_part)
        return FileSinkPartition(filepath, resume_state)

# Define the dataflow
flow = Dataflow("rescalable_pipeline")

input_dir = "/home/user/bytewax_project/input_data"
output_dir = "/home/user/bytewax_project/output_data"

stream = op.input("input", flow, CustomFileSource(input_dir))

def to_uppercase(item: Tuple[str, str]) -> Tuple[str, str]:
    filename, line_content = item
    
    # Optional slowdown for testing recovery
    if os.environ.get("SLOW_DOWN") == "true":
        time.sleep(0.5)
        
    # Optional crash for testing recovery
    if os.environ.get("CRASH_ON_LINE_6") == "true" and line_content == "line 6":
        raise ValueError("Intentional crash on line 6")
        
    return filename, line_content.upper()

upper_stream = op.map("uppercase", stream, to_uppercase)

op.output("output", upper_stream, CustomFileSink(input_dir, output_dir))
