import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from dataclasses import dataclass

from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.operators.windowing import EventClock, TumblingWindower, fold_window
from bytewax.connectors.files import FileSource, FileSink

@dataclass
class ApdexAccumulator:
    satisfied: int = 0
    tolerating: int = 0
    total: int = 0

def builder():
    return ApdexAccumulator()

def folder(acc: ApdexAccumulator, item: dict) -> ApdexAccumulator:
    response_time_ms = item["response_time_ms"]
    acc.total += 1
    if response_time_ms <= 500:
        acc.satisfied += 1
    elif response_time_ms <= 2000:
        acc.tolerating += 1
    return acc

def merger(acc1: ApdexAccumulator, acc2: ApdexAccumulator) -> ApdexAccumulator:
    return ApdexAccumulator(
        satisfied=acc1.satisfied + acc2.satisfied,
        tolerating=acc1.tolerating + acc2.tolerating,
        total=acc1.total + acc2.total
    )

def extract_timestamp(item: dict) -> datetime:
    return datetime.fromisoformat(item["timestamp"])

# 1. Initialize the Dataflow
flow = Dataflow("apdex_flow")

# 2. Read lines from input.jsonl using FileSource
lines = op.input("input", flow, FileSource("input.jsonl"))

# 3. Parse each line as JSON
def parse_json(line: str):
    line = line.strip()
    if not line:
        return None
    try:
        return json.loads(line)
    except Exception:
        return None

parsed = op.filter_map("parse_json", lines, parse_json)

# 4. Key the stream by service
keyed = op.key_on("key_on_service", parsed, lambda x: x["service"])

# 5. Define clock and windower
clock = EventClock(
    ts_getter=extract_timestamp,
    wait_for_system_duration=timedelta(seconds=0),
)

align_to = datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc)
windower = TumblingWindower(
    length=timedelta(seconds=10),
    align_to=align_to,
)

# 6. Aggregate values in windows
windowed = fold_window(
    "fold_window",
    keyed,
    clock,
    windower,
    builder=builder,
    folder=folder,
    merger=merger,
)

# 7. Map down and meta streams to be keyed by f"{service}-{window_id}"
def map_down(item):
    service, (window_id, acc) = item
    return f"{service}-{window_id}", (service, window_id, acc)

down_mapped = op.map("map_down", windowed.down, map_down)

def map_meta(item):
    service, (window_id, metadata) = item
    return f"{service}-{window_id}", metadata

meta_mapped = op.map("map_meta", windowed.meta, map_meta)

# 8. Join down and meta streams
joined = op.join("join_down_meta", down_mapped, meta_mapped)

# 9. Format the output as JSON lines
def format_result(item):
    key, ((service, window_id, acc), metadata) = item
    apdex_score = 0.0
    if acc.total > 0:
        apdex_score = (acc.satisfied + (acc.tolerating / 2.0)) / acc.total
    apdex_score = round(apdex_score, 2)
    
    # Format open_time to match expected UTC format
    window_start = metadata.open_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    
    result = {
        "window_start": window_start,
        "service": service,
        "apdex_score": apdex_score
    }
    return json.dumps(result)

formatted_results = op.map("format_result", joined, format_result)

# 10. Write results to output.jsonl
output_path = Path("output.jsonl")
sink_stream = op.map("prepare_for_sink", formatted_results, lambda x: (str(output_path), x))
op.output("output", sink_stream, FileSink(output_path))

if __name__ == "__main__":
    from bytewax.testing import run_main
    run_main(flow)
