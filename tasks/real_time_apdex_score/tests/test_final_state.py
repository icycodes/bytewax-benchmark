import os
import json
import subprocess
import pytest

PROJECT_DIR = "/home/user/bytewax-apdex"
INPUT_FILE = os.path.join(PROJECT_DIR, "input.jsonl")
OUTPUT_FILE = os.path.join(PROJECT_DIR, "output.jsonl")

def test_apdex_flow():
    # Setup: Create input.jsonl
    input_data = [
        {"timestamp": "2023-10-01T10:00:00Z", "service": "auth", "response_time_ms": 200},
        {"timestamp": "2023-10-01T10:00:02Z", "service": "auth", "response_time_ms": 600},
        {"timestamp": "2023-10-01T10:00:05Z", "service": "auth", "response_time_ms": 2500},
        {"timestamp": "2023-10-01T10:00:08Z", "service": "auth", "response_time_ms": 400},
        {"timestamp": "2023-10-01T10:00:12Z", "service": "auth", "response_time_ms": 100},
        {"timestamp": "2023-10-01T10:00:01Z", "service": "api", "response_time_ms": 1000},
        {"timestamp": "2023-10-01T10:00:04Z", "service": "api", "response_time_ms": 1500}
    ]
    
    os.makedirs(PROJECT_DIR, exist_ok=True)
    with open(INPUT_FILE, "w") as f:
        for item in input_data:
            f.write(json.dumps(item) + "\n")
            
    # Clean up output.jsonl if it exists
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)
        
    # Run the dataflow
    result = subprocess.run(
        ["python", "apdex_flow.py"],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True,
        timeout=60
    )
    
    assert result.returncode == 0, f"apdex_flow.py failed with error: {result.stderr}"
    assert os.path.exists(OUTPUT_FILE), "output.jsonl was not created."
    
    # Read output
    outputs = []
    with open(OUTPUT_FILE, "r") as f:
        for line in f:
            if line.strip():
                outputs.append(json.loads(line))
                
    # Verify outputs
    # Expected Window 1 (auth, 10:00:00Z): apdex 0.62 or 0.63
    auth_w1 = [o for o in outputs if o.get("service") == "auth" and o.get("window_start") == "2023-10-01T10:00:00Z"]
    assert len(auth_w1) > 0, "Missing output for auth service at 2023-10-01T10:00:00Z"
    assert auth_w1[0].get("apdex_score") in [0.62, 0.63], f"Expected apdex_score 0.62 or 0.63 for auth window 1, got {auth_w1[0].get('apdex_score')}"
    
    # Expected Window 2 (auth, 10:00:10Z): apdex 1.0
    auth_w2 = [o for o in outputs if o.get("service") == "auth" and o.get("window_start") == "2023-10-01T10:00:10Z"]
    assert len(auth_w2) > 0, "Missing output for auth service at 2023-10-01T10:00:10Z"
    assert auth_w2[0].get("apdex_score") == 1.0, f"Expected apdex_score 1.0 for auth window 2, got {auth_w2[0].get('apdex_score')}"
    
    # Expected Window 1 (api, 10:00:00Z): apdex 0.5
    api_w1 = [o for o in outputs if o.get("service") == "api" and o.get("window_start") == "2023-10-01T10:00:00Z"]
    assert len(api_w1) > 0, "Missing output for api service at 2023-10-01T10:00:00Z"
    assert api_w1[0].get("apdex_score") == 0.5, f"Expected apdex_score 0.5 for api window 1, got {api_w1[0].get('apdex_score')}"
