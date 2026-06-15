import os
import pytest

PROJECT_DIR = "/home/user/bytewax_project"

def test_bytewax_installed():
    try:
        import bytewax
    except ImportError:
        pytest.fail("bytewax is not installed.")

def test_project_dir_exists():
    assert os.path.isdir(PROJECT_DIR), f"Project directory {PROJECT_DIR} does not exist."
