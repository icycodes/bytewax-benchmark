import os
import subprocess
import pytest

PROJECT_DIR = "/home/user/bytewax_recovery"
RUN_SCRIPT = os.path.join(PROJECT_DIR, "run.sh")

@pytest.fixture(scope="session")
def run_id():
    return os.environ.get("ZEALT_RUN_ID", "test-run-id")

def test_run_script_exists():
    assert os.path.isfile(RUN_SCRIPT), f"Entrypoint script not found at {RUN_SCRIPT}"

def test_recovery_pipeline(run_id):
    # Setup paths
    input1_path = os.path.join(PROJECT_DIR, "input1.txt")
    output1_path = os.path.join(PROJECT_DIR, "output1.txt")
    input2_path = os.path.join(PROJECT_DIR, "input2.txt")
    output2_path = os.path.join(PROJECT_DIR, "output2.txt")
    recovery_dir = os.path.join(PROJECT_DIR, f"recovery_data_{run_id}")

    # First Run
    with open(input1_path, "w") as f:
        f.write("A,10\nB,5\nA,20\n")

    result1 = subprocess.run(
        ["bash", "run.sh", input1_path, output1_path, recovery_dir],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True
    )
    assert result1.returncode == 0, f"First run failed with error: {result1.stderr}\nStdout: {result1.stdout}"

    # Verify first output
    assert os.path.isfile(output1_path), f"Output file {output1_path} was not created."
    with open(output1_path, "r") as f:
        out1_content = f.read()

    # The output format is key,running_max. For A, it first sees 10, then 20. B sees 5.
    assert "A,10" in out1_content, f"Expected 'A,10' in output1.txt, got: {out1_content}"
    assert "B,5" in out1_content, f"Expected 'B,5' in output1.txt, got: {out1_content}"
    assert "A,20" in out1_content, f"Expected 'A,20' in output1.txt, got: {out1_content}"

    # Verify recovery dir exists
    assert os.path.isdir(recovery_dir), f"Recovery directory {recovery_dir} was not created."
    sqlite_files = [f for f in os.listdir(recovery_dir) if f.endswith(".sqlite3")]
    assert len(sqlite_files) > 0, f"No SQLite recovery databases found in {recovery_dir}"

    # Second Run (Recovery Test)
    with open(input2_path, "w") as f:
        f.write("A,15\nB,25\nC,100\n")

    result2 = subprocess.run(
        ["bash", "run.sh", input2_path, output2_path, recovery_dir],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True
    )
    assert result2.returncode == 0, f"Second run failed with error: {result2.stderr}\nStdout: {result2.stdout}"

    # Verify second output
    assert os.path.isfile(output2_path), f"Output file {output2_path} was not created."
    with open(output2_path, "r") as f:
        out2_content = f.read()

    # In the second run, A sees 15 but max is 20. B sees 25, max is 25. C sees 100.
    assert "A,20" in out2_content, f"Expected 'A,20' in output2.txt (recovered state), got: {out2_content}"
    assert "B,25" in out2_content, f"Expected 'B,25' in output2.txt, got: {out2_content}"
    assert "C,100" in out2_content, f"Expected 'C,100' in output2.txt, got: {out2_content}"
