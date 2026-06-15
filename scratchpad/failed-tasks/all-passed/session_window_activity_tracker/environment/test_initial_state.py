import os
import pytest

PROJECT_DIR = "/home/user/project"
INPUT_FILE = os.path.join(PROJECT_DIR, "input.jsonl")

def test_bytewax_importable():
    try:
        import bytewax
    except ImportError:
        pytest.fail("bytewax is not importable.")

def test_project_dir_exists():
    assert os.path.isdir(PROJECT_DIR), f"Project directory {PROJECT_DIR} does not exist."

def test_input_file_exists():
    assert os.path.isfile(INPUT_FILE), f"Input file {INPUT_FILE} does not exist."

def test_input_file_content():
    with open(INPUT_FILE, "r") as f:
        lines = f.readlines()
    assert len(lines) == 5, f"Expected 5 lines in input.jsonl, found {len(lines)}."
    assert "u1" in lines[0] and "login" in lines[0], "Line 1 content mismatch."
    assert "u1" in lines[1] and "view_item" in lines[1], "Line 2 content mismatch."
    assert "u1" in lines[2] and "add_to_cart" in lines[2], "Line 3 content mismatch."
    assert "u2" in lines[3] and "login" in lines[3], "Line 4 content mismatch."
    assert "u2" in lines[4] and "checkout" in lines[4], "Line 5 content mismatch."
