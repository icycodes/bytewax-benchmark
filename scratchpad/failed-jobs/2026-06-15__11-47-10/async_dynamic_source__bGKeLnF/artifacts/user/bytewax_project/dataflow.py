import asyncio
import queue
import threading
from pathlib import Path
from typing import List

from bytewax.inputs import DynamicSource, StatelessSourcePartition
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.stdio import StdOutSink


class AsyncFileSourcePartition(StatelessSourcePartition[str]):
    def __init__(self, files: List[Path]):
        self._files = files
        self._queue = queue.Queue()
        self._finished = False
        
        # Spawn background thread running asyncio event loop
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()

    def _run_loop(self):
        try:
            asyncio.set_event_loop(self._loop)
            self._loop.run_until_complete(self._read_files_async())
        finally:
            self._loop.close()
            self._finished = True

    async def _read_files_async(self):
        for file_path in self._files:
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    for line in f:
                        cleaned_line = line.rstrip("\r\n")
                        # Simulate an async client/operation delay
                        await asyncio.sleep(0.01)
                        self._queue.put(cleaned_line)
            except Exception as e:
                # Silently ignore or log errors
                pass

    def next_batch(self) -> List[str]:
        batch = []
        while True:
            try:
                item = self._queue.get_nowait()
                batch.append(item)
            except queue.Empty:
                break
        
        if not batch and self._finished:
            raise StopIteration()
            
        return batch


class AsyncFileSource(DynamicSource[str]):
    def __init__(self, dir_path: str):
        self.dir_path = Path(dir_path)

    def build(self, step_id: str, worker_index: int, worker_count: int) -> AsyncFileSourcePartition:
        # List all .txt files in the directory and sort them alphabetically
        all_files = sorted(list(self.dir_path.glob("*.txt")))
        # Assign a disjoint subset of files to the current worker
        assigned_files = all_files[worker_index::worker_count]
        return AsyncFileSourcePartition(assigned_files)


# Define the Bytewax Dataflow
flow = Dataflow("async_file_dataflow")

# Use AsyncFileSource to read files from 'input_data' directory
source = AsyncFileSource("input_data")
stream = op.input("input_step", flow, source)

# Apply a stateless op.map to convert each line to uppercase
upper_stream = op.map("uppercase_step", stream, lambda line: line.upper())

# Output the results using op.output with a StdOutSink
op.output("output_step", upper_stream, StdOutSink())
