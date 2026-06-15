import os
import shutil

PROJECT_DIR = "/home/user/bytewax_batching"
INPUT_FILE = os.path.join(PROJECT_DIR, "input.jsonl")

def test_bytewax_installed():
    try:
        import bytewax
    except ImportError:
        assert False, "bytewax is not installed."

def test_sqlite3_installed():
    assert shutil.which("sqlite3") is not None, "sqlite3 is not installed."

def test_project_dir_exists():
    assert os.path.isdir(PROJECT_DIR), f"Project directory {PROJECT_DIR} does not exist."

def test_input_file_exists():
    assert os.path.isfile(INPUT_FILE), f"Input file {INPUT_FILE} does not exist."
