import os
import importlib
import importlib.metadata

PROJECT_DIR = "/home/user/clickstream"


def test_bytewax_importable():
    """The target library Bytewax must be importable in the environment."""
    module = importlib.import_module("bytewax")
    assert module is not None, "Failed to import bytewax."


def test_bytewax_version():
    """Bytewax 0.21.1 must be installed in the environment."""
    version = importlib.metadata.version("bytewax")
    assert version == "0.21.1", (
        f"Expected bytewax==0.21.1 to be installed, but found version {version}."
    )


def test_project_directory_exists():
    """The project directory specified in the task must exist."""
    assert os.path.isdir(PROJECT_DIR), (
        f"Project directory {PROJECT_DIR} does not exist."
    )
