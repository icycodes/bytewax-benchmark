import os
import importlib.metadata
import pytest

PROJECT_DIR = "/home/user/anomaly_detection"
DATA_FILE = os.path.join(PROJECT_DIR, "data.jsonl")

def test_bytewax_installed():
    try:
        import bytewax
    except ImportError:
        pytest.fail("Bytewax is not installed or cannot be imported.")

def test_bytewax_version():
    try:
        version = importlib.metadata.version("bytewax")
        assert version == "0.21.1", f"Expected Bytewax version 0.21.1, but found {version}."
    except importlib.metadata.PackageNotFoundError:
        pytest.fail("Bytewax package metadata not found.")

def test_project_dir_exists():
    assert os.path.isdir(PROJECT_DIR), f"Project directory {PROJECT_DIR} does not exist."

def test_data_file_exists():
    assert os.path.isfile(DATA_FILE), f"Input data file {DATA_FILE} does not exist."
