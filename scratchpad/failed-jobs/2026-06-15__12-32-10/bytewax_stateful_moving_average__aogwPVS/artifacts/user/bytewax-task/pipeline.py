from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition
from bytewax.outputs import DynamicSink, StatelessSinkPartition


class CsvSourcePartition(StatefulSourcePartition):
    """Reads lines from a CSV file as a single partition source."""

    def __init__(self, path):
        self._path = path
        self._file = open(path, "r")
        self._iterator = iter(self._file)

    def next_batch(self):
        items = []
        try:
            line = next(self._iterator)
            line = line.strip()
            if line:
                sensor_id, temp = line.split(",")
                items.append((sensor_id.strip(), float(temp.strip())))
        except StopIteration:
            raise StopIteration()
        return items

    def snapshot(self):
        return None

    def close(self):
        self._file.close()


class CsvSource(FixedPartitionedSource):
    """Single-partition CSV source that reads (sensor_id, temperature) tuples."""

    def __init__(self, path):
        self._path = path

    def list_parts(self):
        return ["single"]

    def build_part(self, step_id, for_part, resume_state):
        return CsvSourcePartition(self._path)


class CsvSinkPartition(StatelessSinkPartition):
    """Writes formatted strings as lines to a CSV file."""

    def __init__(self, path):
        self._path = path
        self._file = open(path, "w")

    def write_batch(self, items):
        for item in items:
            self._file.write(f"{item}\n")
            self._file.flush()

    def close(self):
        self._file.close()


class CsvSink(DynamicSink):
    """Dynamic sink that writes to a single CSV file."""

    def __init__(self, path):
        self._path = path

    def build(self, step_id, worker_index, worker_count):
        return CsvSinkPartition(self._path)


def moving_avg(state, value):
    """Compute the moving average of the last 3 temperature readings.

    Args:
        state: List of recent temperature readings (or None for first call).
        value: New temperature reading.

    Returns:
        Tuple of (updated_state, moving_average) where updated_state is a new
        list containing the last 3 readings and moving_average is rounded to
        2 decimal places.
    """
    if state is None:
        state = []
    # Create a new list to avoid mutating state in-place (for recovery snapshots)
    state = state + [value]
    # Keep only the last 3 readings
    if len(state) > 3:
        state = state[-3:]
    # Compute average of available readings
    avg = round(sum(state) / len(state), 2)
    return (state, avg)


# Build the dataflow
flow = Dataflow("moving_avg")

# Read input: produces (sensor_id, temperature) tuples
inp = op.input("input", flow, CsvSource("input.csv"))

# Key by sensor_id for stateful processing
keyed = op.key_on("key", inp, lambda x: x[0])

# Extract just the temperature value from (sensor_id, temperature)
temps = op.map_value("extract_temp", keyed, lambda x: x[1])

# Compute moving average using stateful map
averages = op.stateful_map("moving_avg", temps, moving_avg)

# Format output as "sensor_id,moving_average"
formatted = op.map("format", averages, lambda kv: f"{kv[0]},{kv[1]:.2f}")

# Write output to CSV
op.output("output", formatted, CsvSink("output.csv"))