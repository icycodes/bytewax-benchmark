import os
import pytest

def test_bytewax_importable():
    try:
        import bytewax
    except ImportError:
        pytest.fail("bytewax is not installed or not importable.")

def test_project_dir_exists():
    project_dir = "/home/user/bytewax_project"
    assert os.path.isdir(project_dir), f"Project directory {project_dir} does not exist."