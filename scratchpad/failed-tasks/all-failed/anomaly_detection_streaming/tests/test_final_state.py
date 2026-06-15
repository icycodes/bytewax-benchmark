import os
import json
import subprocess
import pytest
import shutil

PROJECT_DIR = "/home/user/anomaly_detection"
DATA_FILE = os.path.join(PROJECT_DIR, "data.jsonl")
ANOMALIES_FILE = os.path.join(PROJECT_DIR, "anomalies.jsonl")
RECOVERY_DIR = os.path.join(PROJECT_DIR, "recovery_dir")

@pytest.fixture(scope="session", autouse=True)
def setup_environment():
    # Setup directories
    os.makedirs(PROJECT_DIR, exist_ok=True)
    
    # Cleanup previous runs
    if os.path.exists(RECOVERY_DIR):
        shutil.rmtree(RECOVERY_DIR)
    if os.path.exists(ANOMALIES_FILE):
        os.remove(ANOMALIES_FILE)
        
    os.makedirs(RECOVERY_DIR, exist_ok=True)
    
    # Initialize Bytewax recovery partitions
    subprocess.run(
        ["python", "-m", "bytewax.recovery", RECOVERY_DIR, "1"],
        check=True
    )
    
    # Create deterministic test data
    # Normal values are 20.0, one anomaly is 100.0
    # The timestamps ensure that the event clock advances enough to emit the windows
    data = [
        {"sensor_id": "sensor_1", "timestamp": "2026-01-01T12:00:00Z", "value": 20.0},
        {"sensor_id": "sensor_1", "timestamp": "2026-01-01T12:00:05Z", "value": 20.0},
        {"sensor_id": "sensor_1", "timestamp": "2026-01-01T12:00:10Z", "value": 20.0},
        {"sensor_id": "sensor_1", "timestamp": "2026-01-01T12:00:15Z", "value": 20.0},
        {"sensor_id": "sensor_1", "timestamp": "2026-01-01T12:00:20Z", "value": 20.0},
        {"sensor_id": "sensor_1", "timestamp": "2026-01-01T12:00:25Z", "value": 20.0},
        {"sensor_id": "sensor_1", "timestamp": "2026-01-01T12:00:30Z", "value": 20.0},
        {"sensor_id": "sensor_1", "timestamp": "2026-01-01T12:00:35Z", "value": 20.0},
        {"sensor_id": "sensor_1", "timestamp": "2026-01-01T12:00:40Z", "value": 20.0},
        {"sensor_id": "sensor_1", "timestamp": "2026-01-01T12:00:45Z", "value": 20.0},
        {"sensor_id": "sensor_1", "timestamp": "2026-01-01T12:00:50Z", "value": 100.0},
        # Event to advance watermark and close the [12:00:00, 12:01:00) window
        {"sensor_id": "sensor_1", "timestamp": "2026-01-01T12:01:05Z", "value": 20.0},
        # Another event to close the subsequent sliding windows
        {"sensor_id": "sensor_1", "timestamp": "2026-01-01T12:02:00Z", "value": 20.0},
    ]
    with open(DATA_FILE, "w") as f:
        for item in data:
            f.write(json.dumps(item) + "\\n")

def test_pipeline_execution():
    """Run the Bytewax pipeline with recovery enabled."""
    result = subprocess.run(
        ["python", "-m", "bytewax.run", "pipeline:flow", "-r", "./recovery_dir", "-s", "1", "-b", "0"],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True
    )
    assert result.returncode == 0, f"Pipeline execution failed. stderr: {result.stderr}"

def test_anomalies_output():
    """Verify that the anomalies were correctly identified and written to the output file."""
    assert os.path.exists(ANOMALIES_FILE), f"Output file {ANOMALIES_FILE} was not created."
    
    anomalies = []
    with open(ANOMALIES_FILE, "r") as f:
        for line in f:
            if line.strip():
                anomalies.append(json.loads(line))
                
    assert len(anomalies) > 0, "No anomalies were detected in the output."
    
    # We expect sensor_1 to have an anomaly with value 100.0
    detected = False
    for a in anomalies:
        if a.get("sensor_id") == "sensor_1" and a.get("value") == 100.0:
            detected = True
            break
            
    assert detected, "The expected anomaly (value 100.0) was not found in the output. Make sure the windowing and std dev logic is correct."
