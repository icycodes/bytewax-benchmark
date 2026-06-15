import os
import subprocess
import pytest

PROJECT_DIR = "/home/user/bytewax_project"
INPUT_DIR = os.path.join(PROJECT_DIR, "input_data")
OUTPUT_DIR = os.path.join(PROJECT_DIR, "output_data")
RECOVERY_DIR = os.path.join(PROJECT_DIR, "recovery_dir")

def test_pipeline_execution_and_recovery():
    # Step 1: Create directories
    os.makedirs(INPUT_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    os.makedirs(RECOVERY_DIR, exist_ok=True)
    
    # Step 2: Create initial input files
    file1_input = os.path.join(INPUT_DIR, "file1.txt")
    file2_input = os.path.join(INPUT_DIR, "file2.txt")
    
    with open(file1_input, "w") as f:
        f.write("line1\nline2\nline3\n")
        
    with open(file2_input, "w") as f:
        f.write("hello\nworld\n")
        
    # Step 3: Initialize recovery directory
    result = subprocess.run(
        ["python3", "-m", "bytewax.recovery", RECOVERY_DIR, "1"],
        capture_output=True, text=True, cwd=PROJECT_DIR
    )
    assert result.returncode == 0, f"Failed to initialize recovery directory: {result.stderr}"
    
    # Step 4: Run pipeline
    result = subprocess.run(
        ["python3", "-m", "bytewax.run", "pipeline:flow"],
        capture_output=True, text=True, cwd=PROJECT_DIR
    )
    assert result.returncode == 0, f"Pipeline execution failed: {result.stderr}"
    
    # Step 5: Verify initial output
    file1_output = os.path.join(OUTPUT_DIR, "file1.txt")
    file2_output = os.path.join(OUTPUT_DIR, "file2.txt")
    
    assert os.path.isfile(file1_output), f"Output file {file1_output} was not created."
    with open(file1_output, "r") as f:
        file1_content = f.read().strip().split("\n")
    assert file1_content == ["LINE1", "LINE2", "LINE3"], f"Unexpected content in file1.txt: {file1_content}"
    
    assert os.path.isfile(file2_output), f"Output file {file2_output} was not created."
    with open(file2_output, "r") as f:
        file2_content = f.read().strip().split("\n")
    assert file2_content == ["HELLO", "WORLD"], f"Unexpected content in file2.txt: {file2_content}"
    
    # Step 6: Append new lines to file1.txt
    with open(file1_input, "a") as f:
        f.write("line4\nline5\n")
        
    # Step 7: Run pipeline with recovery enabled
    result = subprocess.run(
        ["python3", "-m", "bytewax.run", "pipeline:flow", "-r", RECOVERY_DIR],
        capture_output=True, text=True, cwd=PROJECT_DIR
    )
    assert result.returncode == 0, f"Pipeline recovery execution failed: {result.stderr}"
    
    # Step 8: Verify recovered output
    with open(file1_output, "r") as f:
        file1_recovered_content = f.read().strip().split("\n")
    
    expected_content = ["LINE1", "LINE2", "LINE3", "LINE4", "LINE5"]
    assert file1_recovered_content == expected_content, \
        f"Unexpected content in file1.txt after recovery. Expected exactly 5 lines without duplication, got: {file1_recovered_content}"
