import os
import importlib.util
import pytest

PROJECT_DIR = "/home/user/bytewax_project"
EVENTS_FILE = os.path.join(PROJECT_DIR, "events.json")

def test_bytewax_installed():
    """Verify that bytewax is installed."""
    assert importlib.util.find_spec("bytewax") is not None, "bytewax is not installed."

def test_project_dir_exists():
    """Verify that the project directory exists."""
    assert os.path.isdir(PROJECT_DIR), f"Project directory {PROJECT_DIR} does not exist."

def test_events_file_exists():
    """Verify that the input events file exists."""
    assert os.path.isfile(EVENTS_FILE), f"Events file {EVENTS_FILE} does not exist."

def test_events_file_contains_data():
    """Verify that the input events file contains the expected test data."""
    with open(EVENTS_FILE, "r") as f:
        content = f.read()
    
    assert "u1" in content, "events.json does not contain user 'u1'."
    assert "2026-01-01T12:00:00Z" in content, "events.json does not contain the expected timestamps."
