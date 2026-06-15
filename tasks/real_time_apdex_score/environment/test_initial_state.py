import os
import pytest

PROJECT_DIR = "/home/user/bytewax-apdex"

def test_bytewax_module_importable():
    try:
        import bytewax
    except ImportError:
        pytest.fail("bytewax module not found. Please ensure it is installed.")

def test_project_dir_exists():
    assert os.path.isdir(PROJECT_DIR), f"Project directory {PROJECT_DIR} does not exist."
