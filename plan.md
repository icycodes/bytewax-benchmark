# Bytewax Benchmark Research & Dataset Creation Plan

This research plan provides a deep technical analysis of **Bytewax (v0.21.1)**, a Python-native stateful stream processing framework. It is designed to guide the creation of robust evaluation datasets and benchmark tasks for AI coding agents.

---

## 1. Library Overview

### Description
**Bytewax** is a Python-native framework and Rust-based distributed processing engine for stateful event and stream processing. Built on top of the Rust-based **Timely Dataflow** engine, Bytewax brings streaming semantics—such as windowing, stateful map, joins, and fault-tolerant recovery—to Python. It is designed to bridge the gap between heavyweight, operationally complex JVM-based stream processors (e.g., Apache Flink, Spark Streaming) and simple, non-scalable Python scripts. Bytewax uses up to 25x less memory than a comparable JVM-based stream processing cluster.

### Ecosystem Role
* **Primary Value Proposition:** Allows Python-centric teams (data engineers, data scientists, ML engineers) to build real-time vectorization, streaming ETL, and anomaly detection pipelines using Python libraries (e.g., Hugging Face, Pandas, NumPy, Spacy) without managing JVM infrastructure, Scala/Java code, or Zookeeper quorums.
* **Integrations:** Integrates natively with stream systems like **Apache Kafka** and **Redpanda**, databases/lakehouses (via standard Python clients), and AI vectors/stores.

### Project Setup (v0.21.1)
Bytewax runs in a standard Python environment. To ensure a non-interactive, container-compatible execution environment, use the following commands:

1. **Install Bytewax and Dependencies:**
   ```bash
   pip install bytewax==0.21.1
   ```

2. **Initialize SQLite Recovery Partitions (Optional, for fault-tolerance):**
   Stateful dataflows require pre-initialized SQLite files to store progress snapshots.
   ```bash
   # Create a recovery directory and initialize 4 partitions (allowing scaling up to 4 workers)
   mkdir -p ./recovery_dir
   python -m bytewax.recovery ./recovery_dir 4
   ```

3. **Execute a Dataflow:**
   Bytewax dataflows are executed using the `bytewax.run` CLI module. This is completely non-interactive and Docker-friendly:
   ```bash
   # Run locally with a single worker
   python -m bytewax.run my_pipeline:flow

   # Run locally with 4 parallel worker threads
   python -m bytewax.run my_pipeline:flow -w 4

   # Run with recovery enabled (snapshot state every 10 seconds)
   python -m bytewax.run my_pipeline:flow -r ./recovery_dir -s 10 -b 0
   ```

---

## 2. Core Primitives & APIs

Bytewax uses a functional, declarative style where operators are imported from `bytewax.operators` and applied to stream objects.

### Key Concepts & API Reference Links
* **[`Dataflow`](https://docs.bytewax.io/stable/api/bytewax/bytewax.dataflow.html)**: The container representing the directed acyclic graph (DAG) of the processing pipeline.
* **[`Stream`](https://docs.bytewax.io/stable/api/bytewax/bytewax.dataflow.html#bytewax.dataflow.Stream)**: A handle to a specific flow of items. Streams can be branched, merged, or duplicated.
* **[`bytewax.operators` (Stateless & Stateful Operators)](https://docs.bytewax.io/stable/api/bytewax/bytewax.operators.html)**: Core processing primitives such as `op.input`, `op.output`, `op.map`, `op.filter`, `op.flat_map`, `op.branch`, `op.key_on`, `op.stateful_map`, and `op.join`.
* **[`bytewax.operators.windowing` (Time-Based Windowing)](https://docs.bytewax.io/stable/api/bytewax/bytewax.operators.windowing.html)**: Time-based aggregations using clocks (`SystemClock`, `EventClock`) and windowers (`TumblingWindower`, `SlidingWindower`, `SessionWindower`).
* **[`bytewax.inputs` (Custom Sources)](https://docs.bytewax.io/stable/api/bytewax/bytewax.inputs.html)**: Framework for defining custom stateful (`FixedPartitionedSource`) or stateless (`DynamicSource`) data inputs.
* **[`bytewax.outputs` (Custom Sinks)](https://docs.bytewax.io/stable/api/bytewax/bytewax.outputs.html)**: Framework for defining custom data destinations (`FixedPartitionedSink`, `DynamicSink`).
* **[`bytewax.recovery` (Fault-Tolerance)](https://docs.bytewax.io/stable/api/bytewax/bytewax.recovery.html)**: Module for programmatically initializing recovery databases.

---

### Deep-Dive API Explanations & Code Snippets

#### 1. Basic Dataflow with Stateless Operators, Joins, and Branching
This snippet demonstrates creating a dataflow, reading from a testing source, converting streams into keyed streams, joining them, branching based on a predicate, and writing to standard output.

```python
import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource
from bytewax.connectors.stdio import StdOutSink

# 1. Initialize the Dataflow
flow = Dataflow("user_profile_pipeline")

# 2. Define mock input sources
users_raw = [{"user_id": "u1", "name": "Alice"}, {"user_id": "u2", "name": "Bob"}]
emails_raw = [{"user_id": "u1", "email": "alice@example.com"}, {"user_id": "u2", "email": "bob@example.com"}]

users_stream = op.input("users_in", flow, TestingSource(users_raw))
emails_stream = op.input("emails_in", flow, TestingSource(emails_raw))

# 3. Convert streams to Keyed Streams (Required for joins and stateful operations)
# Keyed Streams are streams of 2-tuples: (key_string, value)
keyed_users = op.key_on("key_users", users_stream, lambda x: str(x["user_id"]))
keyed_emails = op.key_on("key_emails", emails_stream, lambda x: str(x["user_id"]))

# 4. Join the streams (Inner Join by default: waits for values on all sides)
# Output shape: Stream of (key, (val_from_side1, val_from_side2))
joined_profiles = op.join("join_profiles", keyed_users, keyed_emails)

# 5. Branch the stream based on a predicate
# Returns a BranchOut object containing .trues and .falses streams
branch_out = op.branch(
    "filter_alice", 
    joined_profiles, 
    lambda item: item[0] == "u1"  # item is (user_id, (user_dict, email_dict))
)

alice_stream = branch_out.trues
other_users_stream = branch_out.falses

# 6. Re-merge the streams
merged_stream = op.merge("merge_back", alice_stream, other_users_stream)

# 7. Output the stream to stdout
op.output("stdout_out", merged_stream, StdOutSink())
```

#### 2. Stateful Stream Processing with `op.stateful_map`
Stateful operations partition state by key. The `mapper` function takes the current state (which is `None` if uninitialized) and the new value, returning a 2-tuple of `(updated_state, emit_value)`.

```python
import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource
from bytewax.connectors.stdio import StdOutSink

flow = Dataflow("running_average_pipeline")

# Input stream of (sensor_id, reading)
sensor_readings = [
    ("sensor_a", 10.0),
    ("sensor_b", 20.0),
    ("sensor_a", 12.0),
    ("sensor_b", 24.0),
    ("sensor_a", 14.0),
]
readings_stream = op.input("readings_in", flow, TestingSource(sensor_readings))

# Stateful mapper function
def calculate_running_average(state, new_value):
    # state shape: (sum_of_readings, count_of_readings)
    if state is None:
        state = (0.0, 0)
    
    current_sum, count = state
    updated_sum = current_sum + new_value
    updated_count = count + 1
    updated_state = (updated_sum, updated_count)
    
    running_avg = updated_sum / updated_count
    # Return (new_state, value_to_emit_downstream)
    return updated_state, {"avg": running_avg, "count": updated_count}

# Apply stateful_map (input must be a Keyed Stream)
avg_stream = op.stateful_map("running_avg", readings_stream, calculate_running_average)

op.output("stdout_out", avg_stream, StdOutSink())
```

#### 3. Event-Time Windowing and Aggregating with `win.fold_window`
Windowing groups elements based on time. This example uses `EventClock` (using a timestamp in the data) and a `TumblingWindower` to sum values within 10-second windows.

```python
from datetime import datetime, timedelta, timezone
import bytewax.operators as op
import bytewax.operators.windowing as win
from bytewax.dataflow import Dataflow
from bytewax.testing import TestingSource
from bytewax.connectors.stdio import StdOutSink

flow = Dataflow("windowed_sum_pipeline")

# Keyed events with UTC timestamps
events = [
    ("user_1", {"val": 10, "time": datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)}),
    ("user_1", {"val": 20, "time": datetime(2026, 1, 1, 12, 0, 5, tzinfo=timezone.utc)}),
    ("user_1", {"val": 15, "time": datetime(2026, 1, 1, 12, 0, 15, tzinfo=timezone.utc)}), # Next window
]
input_stream = op.input("events_in", flow, TestingSource(events))

# 1. Define the EventClock (extracts time from the dictionary)
clock = win.EventClock(
    ts_getter=lambda item: item["time"], 
    wait_for_system_duration=timedelta(seconds=0)
)

# 2. Define the Windower (10-second tumbling windows aligned to a fixed epoch)
align_to = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
windower = win.TumblingWindower(length=timedelta(seconds=10), align_to=align_to)

# 3. Define fold functions
def builder():
    return 0  # Initial accumulator value

def folder(accumulator, item):
    return accumulator + item["val"]  # Combine new item value into accumulator

def merger(acc1, acc2):
    return acc1 + acc2  # Combine two window accumulators if windows merge (e.g., Session windows)

# 4. Apply fold_window
# Returns a WindowedStream object. The `.down` attribute is the stream of window results.
windowed_out = win.fold_window(
    "sum_window",
    input_stream,
    clock,
    windower,
    builder=builder,
    folder=folder,
    merger=merger
)

# Output shape on windowed_out.down: (key, (window_id, final_accumulator))
op.output("stdout_out", windowed_out.down, StdOutSink())
```

---

## 3. Real-World Use Cases & Templates

1. **Real-Time Embedding and Vectorization Pipelines:**
   * *Pattern:* Ingests text documents or clickstreams from Kafka/S3, batches them using custom stateful batching, generates vector embeddings using `sentence-transformers` or Hugging Face, and streams them directly into vector databases (e.g., Qdrant, Pinecone).
   * *Example Guide:* [LLM twin course pipeline](https://github.com/decodingml/llm-twin-course/blob/test-module-3/course/module-3/data_flow/bytewax_pipeline.py).
2. **Recoverable Shopping Cart Aggregator:**
   * *Pattern:* Tracks real-time user shopping cart additions, updates, and checkouts. Uses `op.stateful_map` to maintain the active cart state per user and can gracefully recover state from SQLite partitions in case of worker crashes.
   * *Official Tutorial:* [Recoverable Shopping Cart Tutorial](https://docs.bytewax.io/stable/tutorials/recoverable-shopping-cart/index.html).
3. **Session-Based Log Analytics:**
   * *Pattern:* Groups log events into dynamic session windows based on inactivity gaps (e.g., 30 minutes of inactivity closes the session), enabling real-time computation of metrics like average session duration and search success rate.
   * *Official Tutorial:* [Search Logs Sessionization](https://docs.bytewax.io/stable/tutorials/search-logs/index.html).
4. **Time-Series Profiling and Anomaly Detection:**
   * *Pattern:* Ingests IoT sensor readings, groups them into tumbling time windows, and calculates statistical profiles (mean, variance, min, max) using NumPy/Pandas to detect anomalies.
   * *Official Tutorial:* [Profiling Time Series Data](https://docs.bytewax.io/stable/tutorials/profiling-time-series-data/index.html).

---

## 4. Developer Friction Points & Edge Cases

These common pitfalls make excellent test cases for evaluation benchmarks, as they represent realistic stream-processing errors.

### Friction Point 1: Strict String Requirement for Keys
* **Description:** In Bytewax, all stateful operators (e.g., `stateful_map`, `join`, windowing operators) require the input stream to be a Keyed Stream of 2-tuples `(key, value)`. The key **must be a Python string**.
* **Symptom / Error String:**
  ```python
  TypeError: key must be a string
  # Or during execution, a Rust-layer panic / ValueError regarding routing keys.
  ```
* **Underlying Cause:** Bytewax v0.17+ removed arbitrary object hashing (`bytewax.exhash`) and strictly enforces that keys be strings. This allows Bytewax's underlying Rust engine to consistently hash and route partitions across distributed workers.
* **Resolution:** Explicitly cast keys to strings before passing them to stateful operators (e.g., mapping `(123, val)` to `("123", val)` or using `op.key_on("key_step", stream, lambda x: str(x["id"]))`).
* **References:** [Bytewax Changelog](https://github.com/bytewax/bytewax/blob/main/CHANGELOG.md), [Migration Guide](https://docs.bytewax.io/stable/guide/reference/migration.html).

### Friction Point 2: State Pickling Failures Under Recovery
* **Description:** When recovery is enabled, Bytewax periodically snapshots internal states. If any state object contains unpicklable Python objects, the pipeline will crash when snapshotting.
* **Symptom / Error String:**
  ```python
  _pickle.PicklingError: Can't pickle <class '...'>: attribute lookup ... failed
  ```
* **Underlying Cause:** Bytewax v0.17+ replaced `JsonPickle` with Python's native `pickle` module for performance. Stateful operators that retain open file handles, database connections, thread locks, lambdas, or unpicklable custom class instances in their states will fail during snapshotting.
* **Resolution:** Ensure state structures are pure, serializable data structures (e.g., dicts, lists, dataclasses). Initialize database clients or heavy ML models lazily inside a stateless operator or within a custom source/sink partition, separate from the recovery state.
* **References:** [Bytewax Pull Request #222](https://github.com/bytewax/bytewax/pull/222), [Changelog](https://github.com/bytewax/bytewax/blob/main/CHANGELOG.md).

### Friction Point 3: Silent State Corruption Due to In-Place Mutation
* **Description:** Users write stateful logic functions that mutate states in-place and return them, resulting in silent data corruption or recovery failures.
* **Symptom / Error String:** No explicit error is thrown, but state values are incorrect after a crash-and-resume or during parallel execution.
* **Underlying Cause:** Bytewax requires state objects returned by custom logic functions to be effectively immutable or cloned. Mutating a list or dictionary in-place and returning the same reference causes Bytewax's internal snapshotting engine to capture outdated references or miss state updates entirely.
* **Resolution:** Always return a copy or deep-copy of the state when mutating (e.g., `return state.copy(), emit_val` or `copy.deepcopy(state)`).
* **References:** [Bytewax Operator Docs (StatefulLogic Warning)](https://docs.bytewax.io/stable/api/bytewax/bytewax.operators.html).

---

## 5. Evaluation Ideas

These high-level evaluation ideas can be expanded into concrete coding tasks to benchmark AI agents:

* **Tumbling Window Event-Time Counter** `[Difficulty: Simple]`
  Implement a dataflow that reads timestamped click events, groups them into 5-minute tumbling windows using event-time, and outputs the count of clicks per user.
* **Stateful Deduplication Pipeline** `[Difficulty: Simple]`
  Create a dataflow that filters out duplicate messages from a stream using `op.stateful_map` to remember seen message IDs, expiring them after a specific count.
* **Multi-Stream Inner Join with Missing Data Handling** `[Difficulty: Medium]`
  Build a pipeline that joins two streams (orders and payments) on `order_id` (handling integer-to-string key casting), filtering out orders that do not receive payments within a timeout.
* **Session-Based Clickstream Aggregator** `[Difficulty: Medium]`
  Implement a sessionization pipeline that groups user click events using a `SessionWindower` with a 10-second inactivity gap, returning the total pages visited per session.
* **Sliding-Window Outlier Detector with Stateful Recovery** `[Difficulty: High]`
  Design a pipeline that calculates the sliding-window standard deviation of sensor temperatures, identifies outliers, and runs successfully with SQLite recovery enabled (requiring state to be fully picklable).
* **Rescalable Custom Source with Exactly-Once Semantics** `[Difficulty: High]`
  Implement a custom `FixedPartitionedSource` that reads files from a directory, maintaining the read offset in the `resume_state` to guarantee exactly-once processing across pipeline restarts and worker scaling.

---

## 6. Sources

1. [Bytewax GitHub Repository](https://github.com/bytewax/bytewax): Primary source code, changelogs, and architecture discussions.
2. [Bytewax Stable Documentation](https://docs.bytewax.io/stable/): Official guide, tutorials, and API reference for v0.21.1.
3. [Bytewax Migration Reference](https://docs.bytewax.io/stable/guide/reference/migration.html): Details on breaking changes, API evolution, and key constraints.
4. [Bytewax Recovery Documentation](https://docs.bytewax.io/stable/guide/concepts/recovery.html): In-depth guide on SQLite recovery initialization, snapshotting, and rescaling.
5. [Tessl PyPI Bytewax Registry Documentation](https://tessl.io/registry/tessl/pypi-bytewax/0.21.0/files/docs/index.md): Dense API signatures and typing details for Bytewax operators.
6. [Bytewax Substack - Stream Processing with Python](https://bytewax.substack.com/): Official blog posts on architecture, patterns, and cheatsheets.


## Notes for Task Generation

* When a test requires a version check, use `importlib.metadata.version('bytewax')` — the standard way to retrieve installed package versions. Do **not** use `bytewax.__version__`, as Bytewax does not expose a `__version__` attribute.

