import os
import importlib
import pytest

PROJECT_DIR = "/home/user/bytewax_recovery"

def test_bytewax_installed():
    try:
        importlib.import_module("bytewax")
    except ImportError:
        pytest.fail("bytewax is not installed or importable in the environment.")

def test_project_dir_exists():
    assert os.path.isdir(PROJECT_DIR), f"Project directory {PROJECT_DIR} does not exist."
