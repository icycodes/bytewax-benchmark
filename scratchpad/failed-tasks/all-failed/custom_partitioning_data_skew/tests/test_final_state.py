import os
import subprocess
import pytest

PROJECT_DIR = "/home/user/myproject"

@pytest.fixture(autouse=True)
def setup_environment():
    """Ensure output.jsonl is removed before running the test."""
    output_file = os.path.join(PROJECT_DIR, "output.jsonl")
    if os.path.exists(output_file):
        os.remove(output_file)
    yield

def test_run_dataflow():
    """Run the dataflow and verify it exits with status 0."""
    result = subprocess.run(
        ["python", "-m", "bytewax.run", "dataflow:flow"],
        capture_output=True,
        text=True,
        cwd=PROJECT_DIR
    )
    assert result.returncode == 0, f"Dataflow execution failed:\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"

def test_output_file_line_count():
    """Verify the output file contains all the original lines."""
    input_file = os.path.join(PROJECT_DIR, "input.jsonl")
    output_file = os.path.join(PROJECT_DIR, "output.jsonl")
    
    assert os.path.isfile(output_file), "output.jsonl was not created."
    
    with open(input_file, 'r') as f:
        input_lines = f.readlines()
        
    with open(output_file, 'r') as f:
        output_lines = f.readlines()
        
    assert len(input_lines) == len(output_lines), \
        f"Line count mismatch. Expected {len(input_lines)}, got {len(output_lines)}."

def test_output_file_content_matches():
    """Verify the content of the output file matches the input file."""
    input_file = os.path.join(PROJECT_DIR, "input.jsonl")
    output_file = os.path.join(PROJECT_DIR, "output.jsonl")
    
    assert os.path.isfile(output_file), "output.jsonl was not created."
    
    with open(input_file, 'r') as f:
        input_lines = sorted(f.readlines())
        
    with open(output_file, 'r') as f:
        output_lines = sorted(f.readlines())
        
    assert input_lines == output_lines, "Sorted content of output.jsonl does not match input.jsonl."
