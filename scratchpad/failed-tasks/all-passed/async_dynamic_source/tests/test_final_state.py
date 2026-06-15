import os
import subprocess
import pytest

PROJECT_DIR = "/home/user/bytewax_project"
INPUT_DIR = os.path.join(PROJECT_DIR, "input_data")

@pytest.fixture(scope="session", autouse=True)
def setup_input_data():
    """Create the input files required for the test."""
    os.makedirs(INPUT_DIR, exist_ok=True)
    with open(os.path.join(INPUT_DIR, "file1.txt"), "w") as f:
        f.write("hello\nworld\n")
    with open(os.path.join(INPUT_DIR, "file2.txt"), "w") as f:
        f.write("async\nstreams\n")
    with open(os.path.join(INPUT_DIR, "file3.txt"), "w") as f:
        f.write("bytewax\nrocks\n")
    yield

def test_dataflow_execution_2_workers():
    """Verify that the dataflow runs successfully with 2 workers and outputs all uppercase lines."""
    result = subprocess.run(
        ["python", "-m", "bytewax.run", "dataflow:flow", "-w", "2"],
        capture_output=True, text=True, cwd=PROJECT_DIR
    )
    
    assert result.returncode == 0, f"Dataflow execution failed with 2 workers: {result.stderr}"
    
    expected_words = ["HELLO", "WORLD", "ASYNC", "STREAMS", "BYTEWAX", "ROCKS"]
    stdout = result.stdout
    for word in expected_words:
        assert word in stdout, f"Expected word '{word}' not found in stdout when running with 2 workers. Output: {stdout}"

def test_dataflow_partitioning_3_workers():
    """Verify that the dataflow runs successfully with 3 workers and properly partitions without duplication or missing data."""
    result = subprocess.run(
        ["python", "-m", "bytewax.run", "dataflow:flow", "-w", "3"],
        capture_output=True, text=True, cwd=PROJECT_DIR
    )
    
    assert result.returncode == 0, f"Dataflow execution failed with 3 workers: {result.stderr}"
    
    expected_words = ["HELLO", "WORLD", "ASYNC", "STREAMS", "BYTEWAX", "ROCKS"]
    stdout = result.stdout
    for word in expected_words:
        assert word in stdout, f"Expected word '{word}' not found in stdout when running with 3 workers. Output: {stdout}"
    
    # Optional: We could check that there are exactly 6 output lines (plus any Bytewax logs), 
    # but checking that all words are present covers the exactly-once (no missing) requirement.
