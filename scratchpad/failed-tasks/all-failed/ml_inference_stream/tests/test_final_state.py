import os
import json
import subprocess
import pytest
import numpy as np

PROJECT_DIR = "/home/user/ml_pipeline"
RUN_ID = os.environ.get("ZEALT_RUN_ID", "test-run-123")
INPUT_FILE = os.path.join(PROJECT_DIR, "input.jsonl")
OUTPUT_FILE = os.path.join(PROJECT_DIR, f"embeddings-{RUN_ID}.jsonl")
RECOVERY_DIR = os.path.join(PROJECT_DIR, "recovery_dir")

TEST_DATA = [
    {"doc_id": "doc1", "text": "This is a test document about stream processing."},
    {"doc_id": "doc2", "text": "Machine learning inference is powerful."}
]

@pytest.fixture(scope="session", autouse=True)
def setup_environment():
    os.makedirs(PROJECT_DIR, exist_ok=True)
    
    with open(INPUT_FILE, "w") as f:
        for item in TEST_DATA:
            f.write(json.dumps(item) + "\n")
            
    if os.path.exists(RECOVERY_DIR):
        import shutil
        shutil.rmtree(RECOVERY_DIR)
    
    subprocess.run(["python", "-m", "bytewax.recovery", RECOVERY_DIR, "1"], check=True)
    
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)
        
    yield

def test_pipeline_execution():
    """Run the pipeline and ensure it exits with 0 and no PicklingError."""
    env = os.environ.copy()
    env["ZEALT_RUN_ID"] = RUN_ID
    
    result = subprocess.run(
        ["python", "-m", "bytewax.run", "pipeline:flow", "-r", "./recovery_dir", "-s", "1"],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True,
        env=env
    )
    
    assert result.returncode == 0, f"Pipeline execution failed. stderr: {result.stderr}\nstdout: {result.stdout}"
    assert "PicklingError" not in result.stderr, "PicklingError detected during execution."

def test_output_file_exists():
    """Check that the output file exists and has correct number of lines."""
    assert os.path.isfile(OUTPUT_FILE), f"Output file {OUTPUT_FILE} was not created."
    
    with open(OUTPUT_FILE, "r") as f:
        lines = f.readlines()
        
    assert len(lines) == len(TEST_DATA), f"Expected {len(TEST_DATA)} lines in output, got {len(lines)}"

def test_output_format_and_embeddings():
    """Verify the contents and semantic correctness of embeddings."""
    with open(OUTPUT_FILE, "r") as f:
        output_data = [json.loads(line) for line in f]
        
    output_docs = {item["doc_id"]: item for item in output_data}
    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer('all-MiniLM-L6-v2')
    
    for expected in TEST_DATA:
        doc_id = expected["doc_id"]
        assert doc_id in output_docs, f"Missing doc_id {doc_id} in output."
        
        embedding = output_docs[doc_id].get("embedding")
        assert embedding is not None, f"Missing 'embedding' field for doc_id {doc_id}."
        assert isinstance(embedding, list), f"Expected 'embedding' to be a list, got {type(embedding)}."
        assert len(embedding) == 384, f"Expected embedding size 384, got {len(embedding)}."
        
        expected_embedding = model.encode(expected["text"]).tolist()
        
        vec1 = np.array(embedding)
        vec2 = np.array(expected_embedding)
        similarity = np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2))
        
        assert similarity > 0.99, f"Embedding for {doc_id} does not match expected output. Similarity: {similarity}"
