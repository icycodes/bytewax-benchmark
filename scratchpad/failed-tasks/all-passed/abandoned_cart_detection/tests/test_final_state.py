import os
import json
import subprocess
import pytest

PROJECT_DIR = "/home/user/project"
INPUT_FILE = os.path.join(PROJECT_DIR, "test_events.jsonl")
OUTPUT_FILE = os.path.join(PROJECT_DIR, "output.jsonl")

def setup_module():
    os.makedirs(PROJECT_DIR, exist_ok=True)
    events = [
        {"user_id": "u1", "type": "add_to_cart", "item": "shoes"},
        {"user_id": "u1", "type": "add_to_cart", "item": "socks"},
        {"user_id": "u2", "type": "add_to_cart", "item": "laptop"},
        {"user_id": "u2", "type": "checkout"},
        {"user_id": "u3", "type": "add_to_cart", "item": "phone"}
    ]
    with open(INPUT_FILE, "w") as f:
        for event in events:
            f.write(json.dumps(event) + "\n")
    
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)

def test_pipeline_execution():
    env = os.environ.copy()
    env["INPUT_FILE"] = INPUT_FILE
    env["OUTPUT_FILE"] = OUTPUT_FILE
    env["CART_TIMEOUT_SECONDS"] = "1"
    
    result = subprocess.run(
        ["python", "-m", "bytewax.run", "cart_pipeline:flow"],
        cwd=PROJECT_DIR,
        env=env,
        capture_output=True,
        text=True
    )
    assert result.returncode == 0, f"Pipeline execution failed: {result.stderr}"

def test_output_file_exists():
    assert os.path.isfile(OUTPUT_FILE), "Output file was not created by the pipeline."

def test_abandoned_carts_logic():
    with open(OUTPUT_FILE, "r") as f:
        lines = f.readlines()
    
    results = [json.loads(line) for line in lines if line.strip()]
    
    u1_found = False
    u3_found = False
    
    for res in results:
        user_id = res.get("user_id")
        if user_id == "u1":
            u1_found = True
            items = set(res.get("abandoned_items", []))
            assert "shoes" in items and "socks" in items, f"u1 missing expected items: {items}"
        elif user_id == "u3":
            u3_found = True
            items = set(res.get("abandoned_items", []))
            assert "phone" in items, f"u3 missing expected items: {items}"
        elif user_id == "u2":
            pytest.fail("u2 should not be in the output because it checked out.")
            
    assert u1_found, "u1 was not found in the output."
    assert u3_found, "u3 was not found in the output."
