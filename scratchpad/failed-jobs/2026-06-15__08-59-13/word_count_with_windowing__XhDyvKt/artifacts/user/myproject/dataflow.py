import sys
from pathlib import Path
import json
from datetime import datetime, timedelta, timezone
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.operators.windowing import EventClock, TumblingWindower, count_window
from bytewax.connectors.files import FileSource, FileSink
from bytewax.testing import run_main

def main():
    if len(sys.argv) < 3:
        print("Usage: python dataflow.py <input_file> <output_file>", file=sys.stderr)
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    flow = Dataflow("word_count_dataflow")

    # 1. Read input JSONL file
    up = op.input("input", flow, FileSource(input_file))

    # 2. Parse input JSON lines
    def parse_line(line_str):
        line_str = line_str.strip()
        if not line_str:
            return None
        try:
            data = json.loads(line_str)
            word = data["word"]
            time_str = data["time"]
            dt = datetime.fromisoformat(time_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return (word, dt)
        except Exception as e:
            return None

    parsed = op.filter_map("parse_input", up, parse_line)

    # 3. Define clock and windower
    clock = EventClock(
        ts_getter=lambda x: x[1],
        wait_for_system_duration=timedelta(seconds=0),
    )

    windower = TumblingWindower(
        length=timedelta(hours=1),
        align_to=datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc),
    )

    # 4. Use count_window
    windowed = count_window(
        step_id="count_window",
        up=parsed,
        clock=clock,
        windower=windower,
        key=lambda x: x[0],
    )

    # 5. Format output to JSON Lines
    def format_output(item):
        word, (window_id, count) = item
        align_to = datetime(2023, 1, 1, 0, 0, tzinfo=timezone.utc)
        length = timedelta(hours=1)
        open_time = align_to + window_id * length
        window_id_str = open_time.isoformat().replace("+00:00", "Z")
        out_dict = {
            "word": word,
            "window_id": window_id_str,
            "count": count
        }
        return (word, json.dumps(out_dict))

    formatted = op.map("format_output", windowed.down, format_output)

    # 6. Write to output file
    op.output("output", formatted, FileSink(Path(output_file)))

    # Execute dataflow
    run_main(flow)

if __name__ == "__main__":
    main()
