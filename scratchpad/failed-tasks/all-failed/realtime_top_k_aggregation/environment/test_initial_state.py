import os
import pytest

PROJECT_DIR = "/home/user/bytewax_project"

def test_bytewax_importable():
    try:
        import bytewax
    except ImportError:
        pytest.fail("bytewax is not installed or importable.")

def test_project_directory_exists():
    assert os.path.isdir(PROJECT_DIR), f"Project directory {PROJECT_DIR} does not exist."
