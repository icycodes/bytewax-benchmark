import os
import pytest

PROJECT_DIR = "/home/user/bytewax-task"

def test_bytewax_available():
    try:
        import bytewax
    except ImportError:
        pytest.fail("bytewax library is not installed.")

def test_project_directory_exists():
    assert os.path.isdir(PROJECT_DIR), f"Project directory {PROJECT_DIR} does not exist."
