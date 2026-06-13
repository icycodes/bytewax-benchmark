import importlib.metadata
import json
import os
import subprocess

import pytest

PROJECT_DIR = "/home/user/myproject"
DATA_DIR = os.path.join(PROJECT_DIR, "data")
INPUT_FILE = os.path.join(DATA_DIR, "events.jsonl")
OUTPUT_FILE = os.path.join(DATA_DIR, "unique_events.jsonl")

EXPECTED_UNIQUE_PAIRS = {
    ("alice", "m1"): {"action": "login", "ts": 1},
    ("alice", "m2"): {"action": "view", "ts": 2},
    ("alice", "m3"): {"action": "purchase", "ts": 5},
    ("bob", "m1"): {"action": "login", "ts": 4},
    ("bob", "m2"): {"action": "logout", "ts": 8},
}


@pytest.fixture(scope="module")
def pipeline_run():
    """Remove any prior output and run the Bytewax dataflow once."""
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)

    result = subprocess.run(
        ["python", "-m", "bytewax.run", "pipeline:dedup_flow"],
        capture_output=True,
        text=True,
        cwd=PROJECT_DIR,
        timeout=180,
    )
    return result


@pytest.fixture(scope="module")
def output_records(pipeline_run):
    assert os.path.isfile(OUTPUT_FILE), (
        f"Output file {OUTPUT_FILE} does not exist after running the pipeline. "
        f"stdout={pipeline_run.stdout!r} stderr={pipeline_run.stderr!r}"
    )
    records = []
    with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
        for line_no, raw in enumerate(f, start=1):
            line = raw.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError as exc:
                pytest.fail(
                    f"Line {line_no} of {OUTPUT_FILE} is not valid JSON: {exc}; "
                    f"content={line!r}"
                )
            records.append(obj)
    return records


def test_bytewax_version_pinned():
    installed = importlib.metadata.version("bytewax")
    assert installed == "0.21.1", (
        f"Expected bytewax==0.21.1 but found {installed!r}."
    )


def test_pipeline_exits_with_code_zero(pipeline_run):
    assert pipeline_run.returncode == 0, (
        f"Bytewax pipeline exited with non-zero status {pipeline_run.returncode}. "
        f"stdout={pipeline_run.stdout!r} stderr={pipeline_run.stderr!r}"
    )


def test_output_file_non_empty(output_records):
    assert len(output_records) > 0, (
        f"Output file {OUTPUT_FILE} is empty; expected at least one unique event."
    )


def test_output_records_have_required_fields(output_records):
    required = {"message_id", "user_id", "payload"}
    for idx, rec in enumerate(output_records, start=1):
        missing = required - set(rec.keys())
        assert not missing, (
            f"Output record #{idx} is missing required fields {sorted(missing)}: {rec!r}"
        )


def test_output_has_expected_unique_pair_count(output_records):
    assert len(output_records) == len(EXPECTED_UNIQUE_PAIRS), (
        f"Expected exactly {len(EXPECTED_UNIQUE_PAIRS)} unique events in the output, "
        f"got {len(output_records)}: {output_records!r}"
    )


def test_output_unique_pairs_match_expected(output_records):
    seen_pairs = []
    for rec in output_records:
        user_id = rec["user_id"]
        message_id = rec["message_id"]
        seen_pairs.append((user_id, message_id))

    # No duplicate pairs in output.
    assert len(seen_pairs) == len(set(seen_pairs)), (
        f"Duplicate (user_id, message_id) pairs detected in output: {seen_pairs!r}"
    )

    assert set(seen_pairs) == set(EXPECTED_UNIQUE_PAIRS.keys()), (
        f"Output (user_id, message_id) pairs {sorted(set(seen_pairs))!r} "
        f"do not match expected {sorted(EXPECTED_UNIQUE_PAIRS.keys())!r}."
    )


def test_output_payload_preserves_first_occurrence(output_records):
    pair_to_payload = {
        (rec["user_id"], rec["message_id"]): rec["payload"] for rec in output_records
    }
    for pair, expected_payload in EXPECTED_UNIQUE_PAIRS.items():
        assert pair in pair_to_payload, (
            f"Expected pair {pair!r} not present in output."
        )
        assert pair_to_payload[pair] == expected_payload, (
            f"Payload for {pair!r} should preserve the FIRST input occurrence "
            f"{expected_payload!r} but got {pair_to_payload[pair]!r}."
        )
