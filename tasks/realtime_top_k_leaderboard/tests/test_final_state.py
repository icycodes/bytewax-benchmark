import os
import json
import subprocess
import pytest

PROJECT_DIR = "/home/user/bytewax_project"
INPUT_FILE = os.path.join(PROJECT_DIR, "input.jsonl")
OUTPUT_FILE = os.path.join(PROJECT_DIR, "output.json")
SCRIPT_FILE = os.path.join(PROJECT_DIR, "leaderboard.py")

@pytest.fixture(autouse=True)
def setup_environment():
    """Setup the test environment before running verification."""
    # Ensure project directory exists
    os.makedirs(PROJECT_DIR, exist_ok=True)
    
    # Clean up output file if it exists
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)
        
    # Create test input file
    test_data = [
        '{"player_id": "p1", "score": 100}',
        '{"player_id": "p2", "score": 200}',
        '{"player_id": "p1", "score": 150}',
        '{"player_id": "p3", "score": 50}',
        '{"player_id": "p4", "score": 300}',
        '{"player_id": "p2", "score": 180}',
        '{"player_id": "p5", "score": 250}'
    ]
    with open(INPUT_FILE, "w") as f:
        f.write("\n".join(test_data) + "\n")
        
    yield
    
    # Cleanup after test if needed
    if os.path.exists(INPUT_FILE):
        os.remove(INPUT_FILE)
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)

def test_leaderboard_script_exists():
    """Check that the script was created."""
    assert os.path.isfile(SCRIPT_FILE), f"leaderboard.py not found at {SCRIPT_FILE}"

def test_run_leaderboard_dataflow():
    """Run the user script and verify the output."""
    # Run the script
    result = subprocess.run(
        ["python3", "leaderboard.py", "--input", "input.jsonl", "--output", "output.json", "--k", "3"],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True
    )
    assert result.returncode == 0, f"Script execution failed. stdout: {result.stdout}, stderr: {result.stderr}"
    
    # Verify the output file exists
    assert os.path.isfile(OUTPUT_FILE), f"Output file {OUTPUT_FILE} was not created."
    
    # Read and verify output content
    with open(OUTPUT_FILE, "r") as f:
        try:
            output_data = json.load(f)
        except json.JSONDecodeError:
            pytest.fail("Output file does not contain valid JSON.")
            
    assert isinstance(output_data, list), "Output should be a JSON array."
    assert len(output_data) == 3, f"Expected exactly 3 objects in output, got {len(output_data)}."
    
    expected_output = [
        {"player_id": "p4", "score": 300},
        {"player_id": "p5", "score": 250},
        {"player_id": "p2", "score": 200}
    ]
    
    assert output_data == expected_output, f"Output data does not match expected Top-K leaderboard. Expected: {expected_output}, Got: {output_data}"
