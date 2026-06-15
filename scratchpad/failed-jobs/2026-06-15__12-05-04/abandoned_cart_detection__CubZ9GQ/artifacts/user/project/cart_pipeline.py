"""Bytewax dataflow for abandoned shopping cart detection.

Reads JSON-encoded cart events line-by-line from INPUT_FILE, detects
carts that have not been checked out within CART_TIMEOUT_SECONDS after
the first item is added, and writes abandoned-cart records to OUTPUT_FILE.

Each output line is a JSON object:
    {"user_id": "<id>", "abandoned_items": ["item1", "item2", ...]}

Run with:
    python -m bytewax.run cart_pipeline:flow
"""

from __future__ import annotations

import copy
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, List, Optional, Tuple

from bytewax.connectors.files import FileSource
from bytewax.dataflow import Dataflow
from bytewax.operators import StatefulLogic
from bytewax.outputs import DynamicSink, StatelessSinkPartition
import bytewax.operators as op

# ---------------------------------------------------------------------------
# Configuration from environment variables
# ---------------------------------------------------------------------------

INPUT_FILE: str = os.environ["INPUT_FILE"]
OUTPUT_FILE: str = os.environ.get("OUTPUT_FILE", "output.jsonl")
CART_TIMEOUT_SECONDS: int = int(os.environ.get("CART_TIMEOUT_SECONDS", "900"))


# ---------------------------------------------------------------------------
# Custom file sink that writes one JSON line per item
# ---------------------------------------------------------------------------

class _JsonLinesSinkPartition(StatelessSinkPartition[str]):
    """Writes string items to a file, one per line."""

    def __init__(self, path: str) -> None:
        # Open in append mode so multiple partitions don't clobber each other.
        # In practice the dataflow runs with one worker here, but this is safe.
        self._fh = open(path, "a", encoding="utf-8")  # noqa: WPS515

    def write_batch(self, items: List[str]) -> None:
        for item in items:
            self._fh.write(item + "\n")
        self._fh.flush()

    def close(self) -> None:
        self._fh.close()


class JsonLinesSink(DynamicSink[str]):
    """A DynamicSink that appends JSON lines to a single file.

    Because this is a DynamicSink each worker opens the file in append
    mode, which is safe for single-worker (batch) execution.
    """

    def __init__(self, path: str) -> None:
        self._path = path
        # Truncate/create the output file before the run begins so we start
        # from a clean slate on every execution.
        open(path, "w", encoding="utf-8").close()

    def build(
        self, step_id: str, worker_index: int, worker_count: int
    ) -> _JsonLinesSinkPartition:
        return _JsonLinesSinkPartition(self._path)


# ---------------------------------------------------------------------------
# Stateful cart logic
# ---------------------------------------------------------------------------

# State snapshot type: (items, expiry_iso_str | None)
_Snapshot = Tuple[List[str], Optional[str]]


class CartLogic(StatefulLogic[dict, str, _Snapshot]):
    """Per-user stateful logic tracking cart items and timeout.

    State lifecycle:
    - First ``add_to_cart`` event starts the expiry timer.
    - Subsequent ``add_to_cart`` events accumulate items; the timer is
      NOT reset (timeout is measured from the *first* add).
    - A ``checkout`` event clears state and discards this logic instance.
    - When ``on_notify`` fires (timeout elapsed) we emit an abandonment
      record and discard this logic instance.
    - ``on_eof`` flushes any non-empty cart as abandoned immediately.
    """

    def __init__(
        self,
        timeout_seconds: int,
        resume_state: Optional[_Snapshot] = None,
    ) -> None:
        self._timeout_seconds = timeout_seconds

        if resume_state is not None:
            items, expiry_str = resume_state
            self._items: List[str] = list(items)
            self._expiry: Optional[datetime] = (
                datetime.fromisoformat(expiry_str) if expiry_str else None
            )
        else:
            self._items = []
            self._expiry = None

    # ------------------------------------------------------------------
    # StatefulLogic interface
    # ------------------------------------------------------------------

    def on_item(self, event: dict) -> Tuple[Iterable[str], bool]:
        event_type = event.get("type")

        if event_type == "add_to_cart":
            item = event.get("item")
            if item:
                self._items.append(item)
            # Start the expiry clock only on the *first* item added.
            if self._expiry is None:
                self._expiry = datetime.now(tz=timezone.utc) + timedelta(
                    seconds=self._timeout_seconds
                )
            return ([], StatefulLogic.RETAIN)

        if event_type == "checkout":
            # Cart was paid — clear state, no abandonment record emitted.
            self._items = []
            self._expiry = None
            return ([], StatefulLogic.DISCARD)

        # Unknown event type — ignore and retain.
        return ([], StatefulLogic.RETAIN)

    def on_notify(self) -> Tuple[Iterable[str], bool]:
        """Called by the runtime when the scheduled notification time arrives."""
        if self._items:
            record = json.dumps(
                {"user_id": None, "abandoned_items": self._items},
                # user_id is injected by the operator as the stream key;
                # we patch it in the downstream map step.
            )
            self._items = []
            self._expiry = None
            return ([record], StatefulLogic.DISCARD)
        # Nothing to emit (e.g. checkout cleared state before notify fired).
        return ([], StatefulLogic.DISCARD)

    def on_eof(self) -> Tuple[Iterable[str], bool]:
        """Flush remaining cart items when the input stream is exhausted."""
        if self._items:
            record = json.dumps(
                {"user_id": None, "abandoned_items": self._items}
            )
            self._items = []
            self._expiry = None
            return ([record], StatefulLogic.DISCARD)
        return ([], StatefulLogic.DISCARD)

    def notify_at(self) -> Optional[datetime]:
        """Return the scheduled wake-up time for this cart."""
        return self._expiry

    def snapshot(self) -> _Snapshot:
        """Return an immutable copy of state for recovery."""
        return (
            copy.copy(self._items),
            self._expiry.isoformat() if self._expiry else None,
        )


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _parse_event(line: str) -> Optional[Tuple[str, dict]]:
    """Parse a JSON line into a (user_id, event) keyed pair.

    Returns ``None`` for blank lines or malformed JSON so they can be
    filtered downstream.
    """
    line = line.strip()
    if not line:
        return None
    try:
        event = json.loads(line)
        user_id = event.get("user_id")
        if user_id is None:
            return None
        return (str(user_id), event)
    except json.JSONDecodeError:
        return None


def _inject_user_id(keyed_record: Tuple[str, str]) -> str:
    """Replace the placeholder ``user_id: null`` with the stream key."""
    user_id, raw_json = keyed_record
    obj = json.loads(raw_json)
    obj["user_id"] = user_id
    return json.dumps(obj)


def _build_cart_logic(resume_state: Optional[_Snapshot]) -> CartLogic:
    return CartLogic(CART_TIMEOUT_SECONDS, resume_state)


# ---------------------------------------------------------------------------
# Dataflow definition
# ---------------------------------------------------------------------------

flow = Dataflow("abandoned_cart")

# 1. Read raw lines from the input file.
raw_lines = op.input("read_file", flow, FileSource(INPUT_FILE))

# 2. Parse each line into (user_id, event) pairs; drop invalid lines.
parsed = op.filter_map("parse_event", raw_lines, _parse_event)

# 3. Apply stateful cart logic keyed by user_id.
#    Emits (user_id, raw_json_with_null_user_id) for abandoned carts.
abandoned_keyed = op.stateful(
    "cart_state",
    parsed,
    _build_cart_logic,
)

# 4. Inject the real user_id into each emitted JSON record.
abandoned_json = op.map("inject_user_id", abandoned_keyed, _inject_user_id)

# 5. Write one JSON line per abandoned cart to the output file.
op.output("write_output", abandoned_json, JsonLinesSink(OUTPUT_FILE))
