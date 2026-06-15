import os
import pytest

PROJECT_DIR = "/home/user/bytewax_project"

def test_bytewax_available():
    try:
        import bytewax
    except ImportError:
        pytest.fail("bytewax is not installed or importable.")

def test_project_dir_exists():
    assert os.path.isdir(PROJECT_DIR), f"Project directory {PROJECT_DIR} does not exist."

def test_products_file_exists():
    products_path = os.path.join(PROJECT_DIR, "products.json")
    assert os.path.isfile(products_path), f"Products file {products_path} does not exist."

def test_transactions_file_exists():
    transactions_path = os.path.join(PROJECT_DIR, "transactions.jsonl")
    assert os.path.isfile(transactions_path), f"Transactions file {transactions_path} does not exist."
