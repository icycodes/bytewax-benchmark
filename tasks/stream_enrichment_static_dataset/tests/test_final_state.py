import os
import json
import subprocess
import pytest

PROJECT_DIR = "/home/user/bytewax_project"

@pytest.fixture(scope="session", autouse=True)
def setup_test_data():
    os.makedirs(PROJECT_DIR, exist_ok=True)
    
    products_data = {
        "p1": {"category": "electronics", "price": 100.0},
        "p2": {"category": "books", "price": 15.0}
    }
    with open(os.path.join(PROJECT_DIR, "products.json"), "w") as f:
        json.dump(products_data, f)
        
    transactions_data = [
        {"transaction_id": "t1", "product_id": "p1", "timestamp": "2026-01-01T12:00:10Z", "quantity": 1},
        {"transaction_id": "t2", "product_id": "p2", "timestamp": "2026-01-01T12:00:20Z", "quantity": 2},
        {"transaction_id": "t3", "product_id": "p1", "timestamp": "2026-01-01T12:00:45Z", "quantity": 2},
        {"transaction_id": "t4", "product_id": "p3", "timestamp": "2026-01-01T12:00:50Z", "quantity": 1},
        {"transaction_id": "t5", "product_id": "p1", "timestamp": "2026-01-01T12:01:05Z", "quantity": 1}
    ]
    with open(os.path.join(PROJECT_DIR, "transactions.jsonl"), "w") as f:
        for t in transactions_data:
            f.write(json.dumps(t) + "\n")

def test_bytewax_pipeline_output():
    result = subprocess.run(
        ["python", "-m", "bytewax.run", "enrichment:flow"],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True
    )
    
    assert result.returncode == 0, f"Bytewax pipeline failed with error: {result.stderr}"
    
    output_lines = result.stdout.strip().split("\n")
    parsed_records = []
    
    for line in output_lines:
        if not line.strip():
            continue
        try:
            record = json.loads(line)
            parsed_records.append(record)
        except json.JSONDecodeError:
            pytest.fail(f"Output line is not valid JSON: {line}")
            
    assert len(parsed_records) == 3, f"Expected exactly 3 records in output, got {len(parsed_records)}: {parsed_records}"
    
    expected_records = [
        {"category": "electronics", "revenue": 300.0},
        {"category": "books", "revenue": 30.0},
        {"category": "electronics", "revenue": 100.0}
    ]
    
    # We check if each expected record matches a parsed record
    # Since window_start can be 2026-01-01T12:00:00+00:00 or 2026-01-01T12:00:00Z, we check it loosely
    found_electronics_300 = False
    found_books_30 = False
    found_electronics_100 = False
    
    for record in parsed_records:
        cat = record.get("category")
        rev = record.get("revenue")
        ws = record.get("window_start", "")
        
        if cat == "electronics" and rev == 300.0 and "12:00:00" in ws:
            found_electronics_300 = True
        elif cat == "books" and rev == 30.0 and "12:00:00" in ws:
            found_books_30 = True
        elif cat == "electronics" and rev == 100.0 and "12:01:00" in ws:
            found_electronics_100 = True
        else:
            pytest.fail(f"Unexpected record found in output: {record}")
            
    assert found_electronics_300, "Missing expected record for electronics with revenue 300.0 at 12:00:00"
    assert found_books_30, "Missing expected record for books with revenue 30.0 at 12:00:00"
    assert found_electronics_100, "Missing expected record for electronics with revenue 100.0 at 12:01:00"
