import os
import pytest

PROJECT_DIR = "/home/user/bytewax_project"
INPUT_FILE = os.path.join(PROJECT_DIR, "input.jsonl")

def test_bytewax_importable():
    try:
        import bytewax
    except ImportError:
        pytest.fail("bytewax is not installed or importable.")

def test_project_dir_exists():
    assert os.path.isdir(PROJECT_DIR), f"Project directory {PROJECT_DIR} does not exist."

def test_input_file_exists():
    assert os.path.isfile(INPUT_FILE), f"Input file {INPUT_FILE} does not exist."

def test_input_file_content():
    with open(INPUT_FILE, "r") as f:
        content = f.read()
    assert '{"sensor_id": "A", "val": 10}' in content, "Input file does not contain expected sensor A data."
    assert '{"sensor_id": "B", "val": 20}' in content, "Input file does not contain expected sensor B data."
