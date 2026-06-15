import os
import pytest

PROJECT_DIR = "/home/user/project"

def test_bytewax_importable():
    try:
        import bytewax  # type: ignore
    except ImportError:
        pytest.fail("bytewax is not importable.")

def test_project_dir_exists():
    assert os.path.isdir(PROJECT_DIR), f"Project directory {PROJECT_DIR} does not exist."