import os
import json
import subprocess
import pytest

PROJECT_DIR = "/home/user/project"

@pytest.fixture(scope="session", autouse=True)
def run_pipeline():
    """Setup input files and run the Bytewax pipeline."""
    # Ensure project directory exists
    os.makedirs(PROJECT_DIR, exist_ok=True)
    
    joined_file = os.path.join(PROJECT_DIR, "joined.jsonl")
    if os.path.exists(joined_file):
        os.remove(joined_file)
        
    impressions_file = os.path.join(PROJECT_DIR, "impressions.jsonl")
    clicks_file = os.path.join(PROJECT_DIR, "clicks.jsonl")
    
    with open(impressions_file, "w") as f:
        f.write('{"user_id": "u1", "ad_id": "a1", "timestamp": 100}\n')
        f.write('{"user_id": "u2", "ad_id": "a2", "timestamp": 101}\n')
        
    with open(clicks_file, "w") as f:
        f.write('{"user_id": "u1", "click_id": "c1", "timestamp": 105}\n')
        f.write('{"user_id": "u3", "click_id": "c2", "timestamp": 106}\n')
        
    result = subprocess.run(
        ["python", "-m", "bytewax.run", "flow:flow"],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True
    )
    assert result.returncode == 0, f"Pipeline execution failed: {result.stderr}"

def test_joined_file_created():
    """Verify that joined.jsonl is created."""
    joined_file = os.path.join(PROJECT_DIR, "joined.jsonl")
    assert os.path.isfile(joined_file), f"Output file {joined_file} was not created."

def test_joined_file_contents():
    """Read joined.jsonl and verify records."""
    joined_file = os.path.join(PROJECT_DIR, "joined.jsonl")
    records = []
    with open(joined_file, "r") as f:
        for line in f:
            if line.strip():
                records.append(json.loads(line))
                
    # Check that there is exactly one record for user_id "u1"
    u1_records = [r for r in records if r.get("user_id") == "u1"]
    assert len(u1_records) == 1, f"Expected exactly 1 record for user u1, got {len(u1_records)}"
    
    # Check that it contains data from both streams
    record = u1_records[0]
    # The exact format might vary based on how they serialize the tuple, 
    # but it should contain ad_id and click_id or the original objects.
    record_str = json.dumps(record)
    assert "a1" in record_str, "Impression data (ad_id a1) missing from joined record"
    assert "c1" in record_str, "Click data (click_id c1) missing from joined record"
    
    # Check that there are no records for "u2" or "u3"
    u2_records = [r for r in records if r.get("user_id") == "u2"]
    assert len(u2_records) == 0, f"Expected 0 records for user u2, got {len(u2_records)}"
    
    u3_records = [r for r in records if r.get("user_id") == "u3"]
    assert len(u3_records) == 0, f"Expected 0 records for user u3, got {len(u3_records)}"
