import os
import pytest
import importlib.util

PROJECT_DIR = "/home/user/ml_pipeline"

def test_bytewax_is_installed():
    assert importlib.util.find_spec("bytewax") is not None, "bytewax is not installed."

def test_sentence_transformers_is_installed():
    assert importlib.util.find_spec("sentence_transformers") is not None, "sentence_transformers is not installed."

def test_project_dir_exists():
    assert os.path.isdir(PROJECT_DIR), f"Project directory {PROJECT_DIR} does not exist."

def test_input_jsonl_exists():
    input_path = os.path.join(PROJECT_DIR, "input.jsonl")
    assert os.path.isfile(input_path), f"Input file {input_path} does not exist."
