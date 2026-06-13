import json
import os
import subprocess
import sys

import pytest

PROJECT_DIR = "/home/user/clickstream"
INPUT_PATH = os.path.join(PROJECT_DIR, "input.jsonl")
OUTPUT_PATH = os.path.join(PROJECT_DIR, "output.jsonl")
DATAFLOW_PATH = os.path.join(PROJECT_DIR, "dataflow.py")

INPUT_LINES = [
    '{"user_id": 1, "timestamp": "2026-01-01T00:00:30+00:00", "page": "/home"}',
    '{"user_id": 1, "timestamp": "2026-01-01T00:02:00+00:00", "page": "/about"}',
    '{"user_id": 2, "timestamp": "2026-01-01T00:01:00+00:00", "page": "/home"}',
    '{"user_id": 1, "timestamp": "2026-01-01T00:06:00+00:00", "page": "/contact"}',
    '{"user_id": 2, "timestamp": "2026-01-01T00:07:00+00:00", "page": "/about"}',
    '{"user_id": 1, "timestamp": "2026-01-01T00:08:00+00:00", "page": "/home"}',
    '{"user_id": 3, "timestamp": "2026-01-01T00:12:30+00:00", "page": "/home"}',
]

EXPECTED_TRIPLES = {
    ("1", "2026-01-01T00:00:00+00:00", 2),
    ("2", "2026-01-01T00:00:00+00:00", 1),
    ("1", "2026-01-01T00:05:00+00:00", 2),
    ("2", "2026-01-01T00:05:00+00:00", 1),
    ("3", "2026-01-01T00:10:00+00:00", 1),
}


@pytest.fixture(scope="module", autouse=True)
def setup_and_run_dataflow():
    """Prepare the input file, clean stale output, then run the dataflow once."""
    assert os.path.isfile(DATAFLOW_PATH), (
        f"Expected dataflow.py at {DATAFLOW_PATH}, but it does not exist."
    )

    # Write the canonical input.jsonl required by the verification.
    with open(INPUT_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(INPUT_LINES) + "\n")

    # Remove any stale output produced before verification.
    if os.path.exists(OUTPUT_PATH):
        os.remove(OUTPUT_PATH)

    # Run the dataflow.
    result = subprocess.run(
        [sys.executable, "-m", "bytewax.run", "dataflow:flow"],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True,
        timeout=180,
    )

    yield result


def test_dataflow_exits_zero(setup_and_run_dataflow):
    """The dataflow must exit cleanly with status 0."""
    result = setup_and_run_dataflow
    assert result.returncode == 0, (
        "Dataflow command did not exit with status 0.\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )


def test_output_file_exists_and_non_empty():
    assert os.path.isfile(OUTPUT_PATH), (
        f"Expected output file {OUTPUT_PATH} to exist after running the dataflow."
    )
    assert os.path.getsize(OUTPUT_PATH) > 0, (
        f"Expected output file {OUTPUT_PATH} to be non-empty."
    )


def test_output_lines_are_valid_json_objects():
    with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
        lines = [line for line in (raw.strip() for raw in f) if line]
    assert lines, f"Output file {OUTPUT_PATH} contains no records."

    for idx, line in enumerate(lines):
        try:
            obj = json.loads(line)
        except json.JSONDecodeError as exc:
            pytest.fail(
                f"Line {idx + 1} of {OUTPUT_PATH} is not valid JSON: {line!r} ({exc})"
            )
        assert isinstance(obj, dict), (
            f"Line {idx + 1} of {OUTPUT_PATH} is not a JSON object: {obj!r}"
        )
        for key, expected_type in (
            ("user_id", str),
            ("window_start", str),
            ("count", int),
        ):
            assert key in obj, (
                f"Line {idx + 1} of {OUTPUT_PATH} is missing required key '{key}': {obj!r}"
            )
            # bool is a subclass of int; reject bool counts explicitly.
            if expected_type is int and isinstance(obj[key], bool):
                pytest.fail(
                    f"Line {idx + 1} of {OUTPUT_PATH} has 'count' as bool, expected int."
                )
            assert isinstance(obj[key], expected_type), (
                f"Line {idx + 1} of {OUTPUT_PATH} has '{key}' of type "
                f"{type(obj[key]).__name__}, expected {expected_type.__name__}."
            )


def test_output_matches_expected_aggregates():
    with open(OUTPUT_PATH, "r", encoding="utf-8") as f:
        records = [json.loads(line) for line in f if line.strip()]

    actual = {(r["user_id"], r["window_start"], r["count"]) for r in records}
    missing = EXPECTED_TRIPLES - actual
    unexpected = actual - EXPECTED_TRIPLES
    assert not missing and not unexpected, (
        "Output aggregates do not match expectation.\n"
        f"Missing: {sorted(missing)}\n"
        f"Unexpected: {sorted(unexpected)}\n"
        f"Got: {sorted(actual)}"
    )


def test_flow_is_module_level_dataflow_instance():
    """The dataflow module must expose a `flow` attribute that is a Dataflow."""
    snippet = (
        "import importlib, sys;"
        "sys.path.insert(0, '/home/user/clickstream');"
        "mod = importlib.import_module('dataflow');"
        "from bytewax.dataflow import Dataflow;"
        "assert hasattr(mod, 'flow'), \"module 'dataflow' has no attribute 'flow'\";"
        "assert isinstance(mod.flow, Dataflow), "
        "f\"expected dataflow.flow to be a Dataflow instance, got {type(mod.flow).__name__}\""
    )
    result = subprocess.run(
        [sys.executable, "-c", snippet],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, (
        "Failed to import a Dataflow named `flow` from dataflow.py.\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
