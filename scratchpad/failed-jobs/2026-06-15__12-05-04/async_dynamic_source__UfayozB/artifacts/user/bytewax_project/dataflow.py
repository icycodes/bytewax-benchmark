"""
AsyncFileSource — a Bytewax DynamicSource that reads .txt files from a
directory using an async background thread and feeds lines into the
synchronous next_batch pipeline via a thread-safe queue.Queue.
"""

import asyncio
import os
import queue
import threading
from typing import List, Optional

import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.inputs import DynamicSource, StatelessSourcePartition
from bytewax.outputs import DynamicSink, StatelessSinkPartition


# ---------------------------------------------------------------------------
# Sink — simple stdout writer (StdOutSink is not in bytewax 0.21)
# ---------------------------------------------------------------------------

class _StdOutPartition(StatelessSinkPartition):
    """Write every item to stdout."""

    def write_batch(self, items: List) -> None:
        for item in items:
            print(item, flush=True)


class StdOutSink(DynamicSink):
    """A DynamicSink that prints every item to stdout."""

    def build(
        self,
        step_id: str,
        worker_index: int,
        worker_count: int,
    ) -> _StdOutPartition:
        return _StdOutPartition()


# ---------------------------------------------------------------------------
# Async helper — reads a list of files line-by-line, pushing into a Queue
# ---------------------------------------------------------------------------

async def _read_files_async(files: List[str], q: queue.Queue) -> None:
    """
    Asynchronously open each file in *files*, read its lines, and put each
    non-empty stripped line into *q*.  A sentinel ``None`` is placed at the
    end to signal completion.
    """
    loop = asyncio.get_event_loop()

    for filepath in files:
        # Offload the blocking file I/O onto a thread-pool executor so the
        # event loop remains responsive (simulates a real async client).
        def _read(path: str) -> List[str]:
            with open(path, "r") as fh:
                return fh.readlines()

        lines: List[str] = await loop.run_in_executor(None, _read, filepath)
        for line in lines:
            stripped = line.strip()
            if stripped:
                q.put(stripped)

    # Sentinel: signals that all files have been fully read.
    q.put(None)


def _run_event_loop(files: List[str], q: queue.Queue) -> None:
    """Entry point for the background thread; owns its own event loop."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(_read_files_async(files, q))
    finally:
        loop.close()


# ---------------------------------------------------------------------------
# Partition
# ---------------------------------------------------------------------------

class _AsyncFilePartition(StatelessSourcePartition):
    """
    Reads lines from a fixed set of files via an async background thread and
    exposes them through the synchronous next_batch / StopIteration contract
    required by Bytewax.
    """

    def __init__(self, files: List[str]) -> None:
        self._q: queue.Queue = queue.Queue()
        self._done: bool = False  # set when the sentinel None is dequeued

        # Spawn a daemon thread that runs the asyncio event loop; it will
        # push lines (and a final None sentinel) into self._q.
        self._thread = threading.Thread(
            target=_run_event_loop,
            args=(files, self._q),
            daemon=True,
        )
        self._thread.start()

    # ------------------------------------------------------------------
    def next_batch(self) -> List[str]:
        """
        Drain as many items as are immediately available from the queue.

        Returns an empty list if nothing is ready yet (cooperative
        multi-tasking — Bytewax will back off and try again shortly).

        Raises StopIteration once the sentinel has been received *and*
        the queue is completely empty.
        """
        if self._done:
            raise StopIteration()

        batch: List[str] = []

        # Pull the first item with a short timeout so we don't spin at 100 %
        # CPU when the producer is slow.
        try:
            item = self._q.get(timeout=0.05)
            if item is None:            # sentinel received
                self._done = True
                # Drain any remaining real items that arrived before sentinel
                # (unlikely, but possible under scheduling races).
                while True:
                    try:
                        leftover = self._q.get_nowait()
                        if leftover is not None:
                            batch.append(leftover)
                    except queue.Empty:
                        break
                if batch:
                    return batch
                raise StopIteration()
            batch.append(item)
        except queue.Empty:
            # Nothing ready yet — return an empty batch; Bytewax will retry.
            return []

        # Drain any additional items that are already queued (non-blocking).
        while True:
            try:
                item = self._q.get_nowait()
                if item is None:        # sentinel mid-drain
                    self._done = True
                    break
                batch.append(item)
            except queue.Empty:
                break

        return batch

    # ------------------------------------------------------------------
    def next_awake(self) -> Optional[object]:
        # Let Bytewax use its default back-off (1 ms delay on empty batches).
        return None

    def close(self) -> None:
        self._thread.join(timeout=5)


# ---------------------------------------------------------------------------
# Source
# ---------------------------------------------------------------------------

class AsyncFileSource(DynamicSource):
    """
    A DynamicSource that discovers all .txt files in *directory*, sorts them
    alphabetically, and assigns a disjoint slice to each worker.

    Worker *i* of *n* receives ``files[i::n]``.
    """

    def __init__(self, directory: str) -> None:
        self._directory = directory

    def build(
        self,
        step_id: str,
        worker_index: int,
        worker_count: int,
    ) -> _AsyncFilePartition:
        all_files = sorted(
            os.path.join(self._directory, f)
            for f in os.listdir(self._directory)
            if f.endswith(".txt")
        )
        # Assign a disjoint subset to this worker.
        my_files = all_files[worker_index::worker_count]
        return _AsyncFilePartition(my_files)


# ---------------------------------------------------------------------------
# Dataflow
# ---------------------------------------------------------------------------

flow = Dataflow("async_file_source")

# Ingest lines from all .txt files in input_data/
stream = op.input("file_input", flow, AsyncFileSource("input_data"))

# Transform: uppercase every line
upper_stream = op.map("to_upper", stream, str.upper)

# Output to stdout
op.output("stdout", upper_stream, StdOutSink())
