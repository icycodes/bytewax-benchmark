import os
import shutil
import pytest

PROJECT_DIR = "/home/user/bytewax_project"

def test_bytewax_available():
    try:
        import bytewax
    except ImportError:
        pytest.fail("bytewax library is not installed or importable.")

def test_project_directory_exists():
    assert os.path.isdir(PROJECT_DIR), f"Project directory {PROJECT_DIR} does not exist."

def test_users_jsonl_exists():
    file_path = os.path.join(PROJECT_DIR, "users.jsonl")
    assert os.path.isfile(file_path), f"File {file_path} does not exist."

def test_emails_jsonl_exists():
    file_path = os.path.join(PROJECT_DIR, "emails.jsonl")
    assert os.path.isfile(file_path), f"File {file_path} does not exist."

def test_activities_jsonl_exists():
    file_path = os.path.join(PROJECT_DIR, "activities.jsonl")
    assert os.path.isfile(file_path), f"File {file_path} does not exist."
