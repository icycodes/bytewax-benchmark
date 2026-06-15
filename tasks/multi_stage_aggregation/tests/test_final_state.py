import os
import subprocess
import pytest

PROJECT_DIR = "/home/user/bytewax_project"

def test_pipeline_execution_and_output():
    """Run the Bytewax pipeline and verify the final global aggregation output."""
    result = subprocess.run(
        ["python", "-m", "bytewax.run", "pipeline:flow"],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True
    )
    
    assert result.returncode == 0, \
        f"Pipeline execution failed with return code {result.returncode}. Stderr: {result.stderr}"
    
    assert "('global', 75)" in result.stdout or "('global', 75.0)" in result.stdout, \
        f"Expected final global aggregation result ('global', 75) in stdout, but got: {result.stdout}"
