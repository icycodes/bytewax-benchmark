"""Bytewax dataflow with an async file source that reads .txt files from a directory."""

import asyncio
import os
import queue
import threading
from typing import Iterable, List

from bytewax.inputs import DynamicSource, StatelessSourcePartition
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
from bytewax import operators as op

# Sentinel value to signal the source is exhausted
_DONE = object()


class _AsyncFilePartition(StatelessSourcePartition[str]):
    """Stateless source partition that reads assigned files asynchronously.

    A background thread runs an asyncio event loop that opens and reads
    each assigned file line-by-line, pushing lines into a thread-safe queue.
    The synchronous ``next_batch`` method pulls from that queue.
    """

    def __init__(self, files: List[str], directory: str):
        self._q: queue.Queue = queue.Queue()
        self._eof = False  # set to True once the sentinel has been consumed

        # Start a background thread running the asyncio event loop
        self._thread = threading.Thread(
            target=self._run_async_loop,
            args=(files, directory),
            daemon=True,
        )
        self._thread.start()

    # ---- async background work ----

    def _run_async_loop(self, files: List[str], directory: str) -> None:
        """Run the async file-reading coroutine in a new event loop."""
        asyncio.run(self._read_files(files, directory))

    async def _read_files(self, files: List[str], directory: str) -> None:
        """Asynchronously read every assigned file and push lines to the queue."""
        for filename in files:
            filepath = os.path.join(directory, filename)
            # Read the entire file in a thread (simulating an async I/O client)
            content = await asyncio.to_thread(self._read_file_sync, filepath)
            for line in content:
                self._q.put(line)
                # Small sleep to simulate network latency in an async client
                await asyncio.sleep(0.01)
        # Signal that all files have been fully read
        self._q.put(_DONE)

    @staticmethod
    def _read_file_sync(filepath: str) -> List[str]:
        """Synchronous helper that reads a file and returns stripped lines."""
        with open(filepath, "r") as f:
            return [line.rstrip("\n") for line in f if line.strip()]

    # ---- synchronous interface consumed by Bytewax ----

    def next_batch(self) -> Iterable[str]:
        """Return a batch of lines from the queue.

        Uses a short timeout so the method is responsive while still
        non-blocking when data is available.
        """
        if self._eof:
            raise StopIteration()

        batch: List[str] = []
        # Drain whatever is currently available (up to 100 items per batch)
        for _ in range(100):
            try:
                item = self._q.get(timeout=0.1)
            except queue.Empty:
                break
            if item is _DONE:
                self._eof = True
                break
            batch.append(item)

        if not batch and self._eof:
            raise StopIteration()

        return batch

    def close(self) -> None:
        """Wait for the background thread to finish."""
        self._thread.join(timeout=5)


class AsyncFileSource(DynamicSource):
    """A DynamicSource that reads all ``.txt`` files in a directory.

    Files are sorted alphabetically and assigned to workers in a
    round-robin (stride) fashion so that each worker gets a disjoint
    subset of files.
    """

    def __init__(self, directory: str):
        self.directory = directory

    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> _AsyncFilePartition:
        # List and sort .txt files, then pick the ones for this worker
        all_files = sorted(
            f for f in os.listdir(self.directory) if f.endswith(".txt")
        )
        assigned_files = all_files[worker_index::worker_count]
        return _AsyncFilePartition(assigned_files, self.directory)


# ---- Dataflow definition ----

flow = Dataflow("async-file-example")

# 1. Read lines from .txt files in the input_data directory
stream = op.input("read-files", flow, AsyncFileSource("input_data"))

# 2. Convert each line to uppercase
stream = op.map("uppercase", stream, lambda line: line.upper())

# 3. Print to stdout
op.output("stdout", stream, StdOutSink())