import importlib
import os

import pytest

PROJECT_DIR = "/home/user/myproject"


def test_project_directory_exists():
    assert os.path.isdir(PROJECT_DIR), (
        f"Project directory {PROJECT_DIR} does not exist."
    )


def test_bytewax_is_importable():
    try:
        importlib.import_module("bytewax")
    except ImportError as exc:  # pragma: no cover - tested via assertion
        pytest.fail(f"Bytewax is not installed or not importable: {exc}")


def test_bytewax_version_is_0_21_1():
    from importlib.metadata import PackageNotFoundError, version

    try:
        installed = version("bytewax")
    except PackageNotFoundError:
        pytest.fail("Bytewax package metadata not found; expected version 0.21.1.")
    assert installed == "0.21.1", (
        f"Expected Bytewax version 0.21.1 but found {installed}."
    )


def test_bytewax_run_module_is_available():
    # bytewax.run is the CLI entrypoint used to execute dataflows.
    try:
        importlib.import_module("bytewax.run")
    except ImportError as exc:  # pragma: no cover - tested via assertion
        pytest.fail(f"bytewax.run module is not importable: {exc}")


def test_bytewax_file_connectors_available():
    # FileSource/FileSink live in bytewax.connectors.files and are required by the task.
    try:
        importlib.import_module("bytewax.connectors.files")
    except ImportError as exc:  # pragma: no cover - tested via assertion
        pytest.fail(f"bytewax.connectors.files is not importable: {exc}")
