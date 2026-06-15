import asyncio
import os
import queue
import threading
from typing import List

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.inputs import DynamicSource, StatelessSourcePartition
from bytewax.connectors.stdio import StdOutSink

class AsyncFilePartition(StatelessSourcePartition):
    def __init__(self, files: List[str]):
        self.files = files
        self.queue = queue.Queue()
        self.is_done = False
        
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self._start_loop, daemon=True)
        self.thread.start()

    def _start_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self._read_files())
        self.is_done = True
        self.loop.close()

    async def _read_files(self):
        for file_path in self.files:
            with open(file_path, "r") as f:
                for line in f:
                    self.queue.put(line.strip())
                    await asyncio.sleep(0.01)

    def next_batch(self) -> List[str]:
        items = []
        while True:
            try:
                item = self.queue.get(timeout=0.05)
                items.append(item)
            except queue.Empty:
                break
        
        if not items and self.is_done and self.queue.empty():
            raise StopIteration
        
        return items

    def close(self):
        pass

class AsyncFileSource(DynamicSource):
    def __init__(self, directory: str):
        self.directory = directory

    def build(self, step_id: str, worker_index: int, worker_count: int) -> StatelessSourcePartition:
        all_files = []
        for filename in os.listdir(self.directory):
            if filename.endswith(".txt"):
                all_files.append(os.path.join(self.directory, filename))
        
        all_files.sort()
        
        assigned_files = all_files[worker_index::worker_count]
        
        return AsyncFilePartition(assigned_files)

flow = Dataflow("async_file_reader")
stream = op.input("read_files", flow, AsyncFileSource("input_data"))
upper_stream = op.map("uppercase", stream, lambda line: line.upper())
op.output("stdout", upper_stream, StdOutSink())
