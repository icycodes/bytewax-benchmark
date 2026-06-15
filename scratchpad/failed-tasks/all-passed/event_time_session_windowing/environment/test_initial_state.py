import os
import pytest
import importlib.util

PROJECT_DIR = "/home/user/project"

def test_bytewax_installed():
    """Verify that bytewax is installed."""
    spec = importlib.util.find_spec("bytewax")
    assert spec is not None, "bytewax is not installed."

def test_project_dir_exists():
    """Verify that the project directory exists."""
    assert os.path.isdir(PROJECT_DIR), f"Project directory {PROJECT_DIR} does not exist."
