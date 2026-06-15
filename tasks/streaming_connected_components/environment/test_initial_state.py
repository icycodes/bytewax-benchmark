import importlib
import os
import shutil

PROJECT_DIR = "/home/user/graph_pipeline"
INPUT_JSONL = os.path.join(PROJECT_DIR, "input_edges.jsonl")
EXPECTED_JSONL_CONTENT = """{"u": "A", "v": "B"}
{"u": "B", "v": "C"}
{"u": "A", "v": "C"}
{"u": "D", "v": "E"}
{"u": "C", "v": "E"}
"""

def test_bytewax_importable():
    try:
        importlib.import_module("bytewax")
    except ImportError as exc:
        raise AssertionError(f"The 'bytewax' package must be importable: {exc}")

def test_project_dir_exists():
    assert os.path.isdir(PROJECT_DIR), f"Project directory {PROJECT_DIR} does not exist."

def test_input_file_exists():
    assert os.path.isfile(INPUT_JSONL), f"Input file {INPUT_JSONL} does not exist."

def test_input_file_contents():
    with open(INPUT_JSONL, "r", encoding="utf-8") as f:
        content = f.read()
    assert content == EXPECTED_JSONL_CONTENT, (
        f"Expected {INPUT_JSONL} to contain the exact fixture, got {content!r}."
    )
