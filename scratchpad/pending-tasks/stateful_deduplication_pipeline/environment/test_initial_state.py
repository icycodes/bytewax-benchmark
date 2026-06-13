import json
import os

import pytest

PROJECT_DIR = "/home/user/myproject"
DATA_DIR = os.path.join(PROJECT_DIR, "data")
INPUT_FILE = os.path.join(DATA_DIR, "events.jsonl")
OUTPUT_FILE = os.path.join(DATA_DIR, "unique_events.jsonl")


def test_bytewax_importable():
    try:
        import bytewax  # noqa: F401
        import bytewax.operators  # noqa: F401
        from bytewax.dataflow import Dataflow  # noqa: F401
    except ImportError as exc:
        pytest.fail(f"Bytewax is not importable: {exc}")


def test_bytewax_version_is_pinned():
    import importlib.metadata

    installed = importlib.metadata.version("bytewax")
    assert installed == "0.21.1", (
        f"Expected bytewax==0.21.1 but found {installed!r}."
    )


def test_project_directory_exists():
    assert os.path.isdir(PROJECT_DIR), (
        f"Project directory {PROJECT_DIR} does not exist."
    )


def test_data_directory_exists():
    assert os.path.isdir(DATA_DIR), (
        f"Data directory {DATA_DIR} does not exist."
    )


def test_input_events_file_exists():
    assert os.path.isfile(INPUT_FILE), (
        f"Expected input file {INPUT_FILE} to be pre-populated."
    )


def test_input_events_file_has_expected_lines():
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        lines = [line for line in f.read().splitlines() if line.strip()]
    assert len(lines) == 8, (
        f"Expected the input file to contain 8 non-empty JSON lines, found {len(lines)}."
    )
    for idx, line in enumerate(lines):
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as exc:
            pytest.fail(f"Line {idx + 1} of {INPUT_FILE} is not valid JSON: {exc}")
        for key in ("message_id", "user_id", "payload"):
            assert key in obj, (
                f"Line {idx + 1} of {INPUT_FILE} is missing required field {key!r}."
            )


def test_output_file_not_yet_created():
    assert not os.path.exists(OUTPUT_FILE), (
        f"Output file {OUTPUT_FILE} should not exist before the task is executed."
    )
