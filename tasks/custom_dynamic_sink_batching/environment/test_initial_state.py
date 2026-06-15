import os
import pytest
import importlib

PROJECT_DIR = "/home/user/bytewax-sink"

def test_bytewax_installed():
    try:
        importlib.import_module("bytewax")
    except ImportError:
        pytest.fail("bytewax package is not installed.")

def test_project_dir_exists():
    assert os.path.isdir(PROJECT_DIR), f"Project directory {PROJECT_DIR} does not exist."
