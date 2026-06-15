import os
import subprocess
import json
import pytest

PROJECT_DIR = "/home/user/bytewax_routing"

@pytest.fixture(scope="session", autouse=True)
def run_dataflow():
    """Setup test data and run the dataflow."""
    # Create sensors.json
    sensors_path = os.path.join(PROJECT_DIR, "sensors.json")
    test_data = [
        '{"sensor_type": "temperature", "value_c": 25.0}\n',
        '{"sensor_type": "humidity", "value": 60.5}\n',
        '{"sensor_type": "temperature", "value_c": 30.0}\n',
        '{"invalid_json": true}\n',
        'this is not json\n'
    ]
    with open(sensors_path, "w") as f:
        f.writelines(test_data)
        
    # Remove existing outputs
    for f in ["temperature.json", "humidity.json", "errors.json"]:
        p = os.path.join(PROJECT_DIR, f)
        if os.path.exists(p):
            os.remove(p)
            
    # Run dataflow
    result = subprocess.run(
        ["python", "dataflow.py"],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True
    )
    assert result.returncode == 0, f"Dataflow execution failed:\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"

def test_temperature_output():
    temp_path = os.path.join(PROJECT_DIR, "temperature.json")
    assert os.path.exists(temp_path), f"{temp_path} was not created."
    
    with open(temp_path, "r") as f:
        lines = [line.strip() for line in f if line.strip()]
        
    assert len(lines) == 2, f"Expected 2 lines in temperature.json, got {len(lines)}"
    
    # Check content
    expected_1 = {"sensor_type": "temperature", "value_f": 77.0}
    expected_2 = {"sensor_type": "temperature", "value_f": 86.0}
    
    parsed_lines = [json.loads(line) for line in lines]
    assert expected_1 in parsed_lines, f"Expected {expected_1} in temperature.json"
    assert expected_2 in parsed_lines, f"Expected {expected_2} in temperature.json"

def test_humidity_output():
    hum_path = os.path.join(PROJECT_DIR, "humidity.json")
    assert os.path.exists(hum_path), f"{hum_path} was not created."
    
    with open(hum_path, "r") as f:
        lines = [line.strip() for line in f if line.strip()]
        
    assert len(lines) == 1, f"Expected 1 line in humidity.json, got {len(lines)}"
    
    expected = {"sensor_type": "humidity", "value": 60.5}
    parsed = json.loads(lines[0])
    assert parsed == expected, f"Expected {expected} in humidity.json, got {parsed}"

def test_errors_output():
    err_path = os.path.join(PROJECT_DIR, "errors.json")
    assert os.path.exists(err_path), f"{err_path} was not created."
    
    with open(err_path, "r") as f:
        lines = [line.strip() for line in f if line.strip()]
        
    assert len(lines) == 2, f"Expected 2 lines in errors.json, got {len(lines)}"
    
    parsed_lines = [json.loads(line) for line in lines]
    
    expected_1 = {"error": "missing sensor_type", "raw": '{"invalid_json": true}'}
    expected_2 = {"error": "invalid json", "raw": "this is not json"}
    
    assert expected_1 in parsed_lines, f"Expected {expected_1} in errors.json"
    assert expected_2 in parsed_lines, f"Expected {expected_2} in errors.json"
