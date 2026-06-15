import os
import subprocess
import json
import pytest

PROJECT_DIR = "/home/user/myproject"
INPUT_CSV = os.path.join(PROJECT_DIR, "input.csv")
OUTPUT_JSONL = os.path.join(PROJECT_DIR, "output.jsonl")

CSV_CONTENT = """timestamp,symbol,price,volume
2023-10-01T10:00:05Z,AAPL,150.0,100
2023-10-01T10:00:25Z,AAPL,151.0,50
2023-10-01T10:00:45Z,AAPL,149.0,200
2023-10-01T10:00:55Z,AAPL,150.5,150
2023-10-01T10:00:10Z,MSFT,300.0,50
2023-10-01T10:01:05Z,AAPL,152.0,100
2023-10-01T10:01:30Z,MSFT,305.0,200
2023-10-01T10:01:45Z,MSFT,302.0,100
"""

@pytest.fixture(scope="session", autouse=True)
def setup_and_run_dataflow():
    # Setup
    if os.path.exists(OUTPUT_JSONL):
        os.remove(OUTPUT_JSONL)
    
    with open(INPUT_CSV, "w") as f:
        f.write(CSV_CONTENT)
    
    # Run the Dataflow
    result = subprocess.run(
        ["python", "run.py", "--input", "input.csv", "--output", "output.jsonl"],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True
    )
    assert result.returncode == 0, f"Dataflow execution failed: {result.stderr}"
    
    yield

def test_output_exists():
    assert os.path.isfile(OUTPUT_JSONL), f"Output file {OUTPUT_JSONL} does not exist."

def get_record(symbol, window_start):
    records = []
    with open(OUTPUT_JSONL, "r") as f:
        for line in f:
            if not line.strip():
                continue
            record = json.loads(line)
            if record.get("symbol") == symbol and record.get("window_start") == window_start:
                records.append(record)
    return records

def test_aapl_window_1():
    records = get_record("AAPL", "2023-10-01T10:00:00Z")
    assert len(records) == 1, f"Expected 1 record for AAPL at 10:00:00Z, found {len(records)}"
    record = records[0]
    assert record.get("open") == 150.0, f"Expected open 150.0, got {record.get('open')}"
    assert record.get("high") == 151.0, f"Expected high 151.0, got {record.get('high')}"
    assert record.get("low") == 149.0, f"Expected low 149.0, got {record.get('low')}"
    assert record.get("close") == 150.5, f"Expected close 150.5, got {record.get('close')}"
    assert record.get("volume") == 500, f"Expected volume 500, got {record.get('volume')}"

def test_msft_window_1():
    records = get_record("MSFT", "2023-10-01T10:00:00Z")
    assert len(records) == 1, f"Expected 1 record for MSFT at 10:00:00Z, found {len(records)}"
    record = records[0]
    assert record.get("open") == 300.0, f"Expected open 300.0, got {record.get('open')}"
    assert record.get("high") == 300.0, f"Expected high 300.0, got {record.get('high')}"
    assert record.get("low") == 300.0, f"Expected low 300.0, got {record.get('low')}"
    assert record.get("close") == 300.0, f"Expected close 300.0, got {record.get('close')}"
    assert record.get("volume") == 50, f"Expected volume 50, got {record.get('volume')}"

def test_aapl_window_2():
    records = get_record("AAPL", "2023-10-01T10:01:00Z")
    assert len(records) == 1, f"Expected 1 record for AAPL at 10:01:00Z, found {len(records)}"
    record = records[0]
    assert record.get("open") == 152.0, f"Expected open 152.0, got {record.get('open')}"
    assert record.get("high") == 152.0, f"Expected high 152.0, got {record.get('high')}"
    assert record.get("low") == 152.0, f"Expected low 152.0, got {record.get('low')}"
    assert record.get("close") == 152.0, f"Expected close 152.0, got {record.get('close')}"
    assert record.get("volume") == 100, f"Expected volume 100, got {record.get('volume')}"

def test_msft_window_2():
    records = get_record("MSFT", "2023-10-01T10:01:00Z")
    assert len(records) == 1, f"Expected 1 record for MSFT at 10:01:00Z, found {len(records)}"
    record = records[0]
    assert record.get("open") == 305.0, f"Expected open 305.0, got {record.get('open')}"
    assert record.get("high") == 305.0, f"Expected high 305.0, got {record.get('high')}"
    assert record.get("low") == 302.0, f"Expected low 302.0, got {record.get('low')}"
    assert record.get("close") == 302.0, f"Expected close 302.0, got {record.get('close')}"
    assert record.get("volume") == 300, f"Expected volume 300, got {record.get('volume')}"
