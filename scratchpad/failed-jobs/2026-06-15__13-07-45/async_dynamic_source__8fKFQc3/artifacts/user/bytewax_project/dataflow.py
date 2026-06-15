"""Bytewax dataflow with an async-backed DynamicSource.

AsyncFileSource reads .txt files from a directory, partitions them across
workers, and bridges an asyncio event loop (running in a background thread)
to the synchronous next_batch API via a thread-safe queue.Queue.
"""

import asyncio
import os
import queue
import threading
from typing import List

from bytewax.dataflow import Dataflow
from bytewax.inputs import DynamicSource, StatelessSourcePartition
from bytewax.operators import input as op_input
from bytewax.operators import map as op_map
from bytewax.operators import output as op_output
from bytewax.outputs import DynamicSink, StatelessSinkPartition


class _AsyncFilePartition(StatelessSourcePartition):
    """A partition that reads its assigned .txt files asynchronously.

    A background thread runs an asyncio event loop which reads every line
    from the assigned files and pushes them into a thread-safe queue.Queue.
    The synchronous ``next_batch`` drains the queue and returns a list of
    lines, raising ``StopIteration`` when the source is exhausted.
    """

    def __init__(self, file_paths: List[str]):
        self._queue: queue.Queue = queue.Queue()
        self._done = False

        # Start a background thread that runs the asyncio event loop.
        self._thread = threading.Thread(
            target=self._run_async_loop,
            args=(file_paths,),
            daemon=True,
        )
        self._thread.start()

    def _run_async_loop(self, file_paths: List[str]) -> None:
        """Entry point for the background thread.

        Creates a new event loop, schedules the async file reader, and
        runs the loop until completion.
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            loop.run_until_complete(self._read_files(file_paths))
        finally:
            loop.close()

    async def _read_files(self, file_paths: List[str]) -> None:
        """Asynchronously read every line from the assigned files.

        Each file is read sequentially (simulating an async client call for
        each file). Every line is pushed into the thread-safe queue.
        """
        for path in file_paths:
            await self._async_read_single_file(path)
        # Signal that all files have been fully read.
        self._done = True

    async def _async_read_single_file(self, file_path: str) -> None:
        """Simulate an async file read by yielding to the event loop."""
        # Simulate async I/O — yield control to the event loop before reading.
        await asyncio.sleep(0)
        with open(file_path, "r") as f:
            for line in f:
                line = line.rstrip("\n")
                self._queue.put(line)
                # Yield after each line to simulate cooperative async work.
                await asyncio.sleep(0)

    def next_batch(self) -> List[str]:
        """Synchronously return the next batch of lines.

        Returns:
            A list of strings (may be empty if no data is available yet).

        Raises:
            StopIteration: When all files have been read and the queue is
                empty.
        """
        batch: List[str] = []
        # Drain as many items as are immediately available.
        while True:
            try:
                item = self._queue.get_nowait()
            except queue.Empty:
                break
            batch.append(item)

        if not batch:
            if self._done:
                raise StopIteration
            # Return an empty list so the runtime can poll again later.
            return []

        return batch


class _StdOutPartition(StatelessSinkPartition[str]):
    """A simple sink partition that prints each item to stdout."""

    def write_batch(self, items: List[str]) -> None:
        for item in items:
            print(item)

    def close(self) -> None:
        pass


class StdOutSink(DynamicSink):
    """A dynamic sink that writes each item to stdout."""

    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> _StdOutPartition:
        return _StdOutPartition()


class AsyncFileSource(DynamicSource):
    """A DynamicSource that reads .txt files from a directory.

    Files are partitioned across workers using round-robin assignment
    (``files[worker_index::worker_count]``) so that each worker processes a
    disjoint subset.
    """

    def __init__(self, directory: str):
        self._directory = directory

    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> _AsyncFilePartition:
        """Build a partition for the current worker.

        Args:
            step_id: Unique identifier for this step in the dataflow.
            worker_index: 0-based index of the current worker.
            worker_count: Total number of workers.

        Returns:
            A ``_AsyncFilePartition`` that will read the assigned files.
        """
        all_files = sorted(
            os.path.join(self._directory, f)
            for f in os.listdir(self._directory)
            if f.endswith(".txt")
        )
        assigned = all_files[worker_index::worker_count]
        return _AsyncFilePartition(assigned)


# ---------------------------------------------------------------------------
# Build the dataflow
# ---------------------------------------------------------------------------

flow = Dataflow("async_file_upper")

# 1. Input — async-backed file source
inp = op_input("input", flow, AsyncFileSource("input_data"))

# 2. Transform — convert each line to uppercase
upper = op_map("uppercase", inp, str.upper)

# 3. Output — print to stdout
op_output("output", upper, StdOutSink())
