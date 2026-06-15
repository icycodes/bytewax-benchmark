import os
import pytest

PROJECT_DIR = "/home/user/ad_pipeline"

def test_bytewax_importable():
    try:
        import bytewax
        import bytewax.run
    except ImportError:
        pytest.fail("bytewax is not installed or not importable.")

def test_project_dir_exists():
    assert os.path.isdir(PROJECT_DIR), f"Project directory {PROJECT_DIR} does not exist."

def test_impressions_file_exists():
    file_path = os.path.join(PROJECT_DIR, "impressions.jsonl")
    assert os.path.isfile(file_path), f"Impressions file {file_path} does not exist."
    with open(file_path, "r") as f:
        content = f.read()
    assert '{"ad_id": 1, "user_id": "u1"}' in content, "Impressions file content is missing expected data."

def test_clicks_file_exists():
    file_path = os.path.join(PROJECT_DIR, "clicks.jsonl")
    assert os.path.isfile(file_path), f"Clicks file {file_path} does not exist."
    with open(file_path, "r") as f:
        content = f.read()
    assert '{"ad_id": "1", "click_time": "2026-06-15T10:00:00Z"}' in content, "Clicks file content is missing expected data."

def test_recovery_dir_exists():
    recovery_dir = os.path.join(PROJECT_DIR, "recovery_dir")
    assert os.path.isdir(recovery_dir), f"Recovery directory {recovery_dir} does not exist."
