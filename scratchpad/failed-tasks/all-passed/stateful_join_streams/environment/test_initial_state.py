import os
import pytest

PROJECT_DIR = "/home/user/project"

def test_bytewax_installed():
    try:
        import bytewax
    except ImportError:
        pytest.fail("bytewax is not installed or not available in the Python environment.")

def test_project_dir_exists():
    assert os.path.isdir(PROJECT_DIR), f"Project directory {PROJECT_DIR} does not exist."
