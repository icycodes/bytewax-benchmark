import os
import pytest

PROJECT_DIR = "/home/user/myproject"

def test_bytewax_installed():
    try:
        import bytewax
    except ImportError:
        pytest.fail("bytewax library is not installed.")

def test_project_dir_exists():
    assert os.path.isdir(PROJECT_DIR), f"Project directory {PROJECT_DIR} does not exist."

def test_input_jsonl_exists():
    input_file = os.path.join(PROJECT_DIR, "input.jsonl")
    assert os.path.isfile(input_file), f"Input file {input_file} does not exist."
