import os
import pytest

PROJECT_DIR = "/home/user/bytewax_routing"

def test_bytewax_available():
    try:
        import bytewax
    except ImportError:
        pytest.fail("bytewax library is not installed or importable.")

def test_project_dir_exists():
    assert os.path.isdir(PROJECT_DIR), f"Project directory {PROJECT_DIR} does not exist."

def test_sensors_json_exists():
    sensors_path = os.path.join(PROJECT_DIR, "sensors.json")
    assert os.path.isfile(sensors_path), f"Input file {sensors_path} does not exist."
