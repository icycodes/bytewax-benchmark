import os
import importlib

def test_bytewax_is_installed():
    try:
        importlib.import_module("bytewax")
    except ImportError:
        assert False, "bytewax is not installed."

def test_project_dir_exists():
    assert os.path.isdir("/home/user/myproject"), "Project directory /home/user/myproject does not exist."