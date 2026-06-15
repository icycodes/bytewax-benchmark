import os
import pytest

PROJECT_DIR = "/home/user/bytewax-rate-limiter"

def test_bytewax_is_installed():
    try:
        import bytewax
    except ImportError:
        pytest.fail("bytewax is not installed or importable.")

def test_project_dir_exists():
    assert os.path.isdir(PROJECT_DIR), f"Project directory {PROJECT_DIR} does not exist."
