import os
import sqlite3
import pytest

PROJECT_DIR = "/home/user/app"
DB_PATH = os.path.join(PROJECT_DIR, "events.db")

def test_bytewax_installed():
    try:
        import bytewax
    except ImportError:
        pytest.fail("bytewax is not installed.")

def test_project_dir_exists():
    assert os.path.isdir(PROJECT_DIR), f"Project directory {PROJECT_DIR} does not exist."

def test_events_db_exists():
    assert os.path.isfile(DB_PATH), f"SQLite database {DB_PATH} does not exist."

def test_events_table_populated():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    # Check if table exists
    c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='events'")
    table_exists = c.fetchone()
    assert table_exists is not None, "Table 'events' does not exist in the database."
    
    # Check row count
    c.execute("SELECT COUNT(*) FROM events")
    count = c.fetchone()[0]
    assert count == 7, f"Expected 7 rows in 'events' table, but found {count}."
    
    conn.close()
