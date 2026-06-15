import os
import pytest

PROJECT_DIR = "/home/user/myproject"

def test_bytewax_importable():
    try:
        import bytewax
    except ImportError:
        pytest.fail("bytewax module is not installed or importable.")

def test_project_dir_exists():
    assert os.path.isdir(PROJECT_DIR), f"Project directory {PROJECT_DIR} does not exist."
