import os
import subprocess
import pytest

PROJECT_DIR = "/home/user/bytewax_batching"

def get_run_id():
    run_id = os.environ.get("ZEALT_RUN_ID")
    assert run_id, "ZEALT_RUN_ID environment variable is not set."
    return run_id

def test_output_log_exists_and_contains_success_message():
    log_file = os.path.join(PROJECT_DIR, "output.log")
    assert os.path.isfile(log_file), f"Log file {log_file} does not exist."
    
    with open(log_file, "r") as f:
        content = f.read()
    assert "Pipeline finished." in content, "Expected 'Pipeline finished.' in output.log."

def test_sqlite_db_exists():
    run_id = get_run_id()
    db_file = os.path.join(PROJECT_DIR, f"metrics-{run_id}.db")
    assert os.path.isfile(db_file), f"SQLite database {db_file} does not exist."

def test_sqlite_db_total_count():
    run_id = get_run_id()
    db_file = os.path.join(PROJECT_DIR, f"metrics-{run_id}.db")
    
    result = subprocess.run(
        ["sqlite3", db_file, "SELECT COUNT(*) FROM device_metrics;"],
        capture_output=True, text=True
    )
    assert result.returncode == 0, f"sqlite3 query failed: {result.stderr}"
    
    count = int(result.stdout.strip())
    assert count == 20, f"Expected 20 records in device_metrics table, got {count}."

def test_sqlite_db_filtered_data():
    run_id = get_run_id()
    db_file = os.path.join(PROJECT_DIR, f"metrics-{run_id}.db")
    
    result = subprocess.run(
        ["sqlite3", db_file, "SELECT COUNT(*) FROM device_metrics WHERE device_id = 'dev_01' AND metric_value < 0;"],
        capture_output=True, text=True
    )
    assert result.returncode == 0, f"sqlite3 query failed: {result.stderr}"
    
    count = int(result.stdout.strip())
    assert count == 0, f"Expected 0 records with metric_value < 0 for dev_01, got {count}."

def test_source_code_uses_collect_operator():
    dataflow_file = os.path.join(PROJECT_DIR, "dataflow.py")
    assert os.path.isfile(dataflow_file), f"Dataflow script {dataflow_file} does not exist."
    
    result = subprocess.run(
        ["grep", "-q", "op.collect(", dataflow_file]
    )
    assert result.returncode == 0, "The dataflow script must use the 'collect' operator (e.g., 'op.collect(')."

def test_source_code_uses_executemany():
    dataflow_file = os.path.join(PROJECT_DIR, "dataflow.py")
    assert os.path.isfile(dataflow_file), f"Dataflow script {dataflow_file} does not exist."
    
    result = subprocess.run(
        ["grep", "-q", "executemany", dataflow_file]
    )
    assert result.returncode == 0, "The dataflow script must use 'executemany' for bulk inserts in the custom sink."
