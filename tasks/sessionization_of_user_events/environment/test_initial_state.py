import os
import pytest

PROJECT_DIR = "/home/user/bytewax-sessionization"

def test_bytewax_sdk_available():
    try:
        import bytewax
    except ImportError:
        pytest.fail("Bytewax SDK is not installed or not available in the Python environment.")

def test_project_dir_exists():
    assert os.path.isdir(PROJECT_DIR), f"Project directory {PROJECT_DIR} does not exist."

def test_input_file_exists():
    input_file = os.path.join(PROJECT_DIR, "user_events.jsonl")
    assert os.path.isfile(input_file), f"Input file {input_file} does not exist."
