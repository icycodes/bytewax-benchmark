import json
from pathlib import Path
from bytewax.testing import run_main
from flow import flow

def test_flow_execution():
    # Setup paths
    output_path = Path("joined.jsonl")

    # Ensure output file is removed before run
    if output_path.exists():
        output_path.unlink()

    # Run the flow programmatically
    run_main(flow)

    # Assert output file is created
    assert output_path.exists(), "joined.jsonl was not created!"

    # Parse and check output
    with open(output_path, "r") as f:
        lines = f.readlines()

    assert len(lines) == 2, f"Expected 2 joined records, got {len(lines)}"

    records = [json.loads(line) for line in lines]
    user_ids = {r["user_id"] for r in records}
    assert user_ids == {"user1", "user3"}, f"Unexpected user_ids: {user_ids}"

    # Verify content of user1
    user1_record = next(r for r in records if r["user_id"] == "user1")
    assert user1_record["impression"]["ad_id"] == "ad1"
    assert user1_record["click"]["click_id"] == "click1"

    # Verify content of user3
    user3_record = next(r for r in records if r["user_id"] == "user3")
    assert user3_record["impression"]["ad_id"] == "ad3"
    assert user3_record["click"]["click_id"] == "click2"
