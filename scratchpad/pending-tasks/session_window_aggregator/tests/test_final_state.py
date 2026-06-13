import json
import os
import subprocess
import sys
from typing import Any, Dict, List, Tuple

import pytest

PROJECT_DIR = "/home/user/myproject"
PIPELINE_FILE = os.path.join(PROJECT_DIR, "pipeline.py")
INPUT_FILE = os.path.join(PROJECT_DIR, "input.jsonl")
OUTPUT_FILE = os.path.join(PROJECT_DIR, "output.jsonl")


def _write_input(events: List[Dict[str, Any]]) -> None:
    """Replace the pipeline input file with the provided click events."""
    with open(INPUT_FILE, "w", encoding="utf-8") as fh:
        for event in events:
            fh.write(json.dumps(event) + "\n")


def _reset_output() -> None:
    """Delete any prior output so the pipeline run starts from a clean state."""
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)


def _run_pipeline() -> subprocess.CompletedProcess:
    """Execute the Bytewax pipeline via the official CLI entrypoint."""
    return subprocess.run(
        [sys.executable, "-m", "bytewax.run", "pipeline:flow"],
        cwd=PROJECT_DIR,
        capture_output=True,
        text=True,
        timeout=180,
    )


def _read_sessions() -> List[Dict[str, Any]]:
    assert os.path.isfile(OUTPUT_FILE), (
        f"Pipeline did not create the expected output file at {OUTPUT_FILE}."
    )
    sessions: List[Dict[str, Any]] = []
    with open(OUTPUT_FILE, "r", encoding="utf-8") as fh:
        for line_number, raw in enumerate(fh, start=1):
            line = raw.strip()
            if not line:
                continue
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError as exc:
                pytest.fail(
                    f"Line {line_number} of {OUTPUT_FILE} is not valid JSON: {exc} -- {line!r}"
                )
            assert isinstance(parsed, dict), (
                f"Each output line must be a JSON object; got {type(parsed).__name__} on line {line_number}."
            )
            sessions.append(parsed)
    return sessions


def _normalize_session(session: Dict[str, Any]) -> Tuple[str, int, Tuple[str, ...]]:
    assert "user_id" in session, (
        f"Session is missing required field 'user_id': {session!r}"
    )
    assert "page_count" in session, (
        f"Session is missing required field 'page_count': {session!r}"
    )
    assert "pages" in session, (
        f"Session is missing required field 'pages': {session!r}"
    )
    user_id = session["user_id"]
    assert isinstance(user_id, str), (
        f"Expected 'user_id' to be a string but got {type(user_id).__name__}: {user_id!r}"
    )
    page_count = session["page_count"]
    assert isinstance(page_count, int) and not isinstance(page_count, bool), (
        f"Expected 'page_count' to be an int, got {type(page_count).__name__}: {page_count!r}"
    )
    pages = session["pages"]
    assert isinstance(pages, list) and all(isinstance(p, str) for p in pages), (
        f"Expected 'pages' to be a list[str], got: {pages!r}"
    )
    return user_id, page_count, tuple(pages)


def _run_case(events: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    assert os.path.isfile(PIPELINE_FILE), (
        f"Pipeline module not found at {PIPELINE_FILE}."
    )
    _write_input(events)
    _reset_output()
    proc = _run_pipeline()
    assert proc.returncode == 0, (
        "Bytewax pipeline did not exit cleanly.\n"
        f"stdout:\n{proc.stdout}\nstderr:\n{proc.stderr}"
    )
    return _read_sessions()


def test_two_sessions_for_single_user_with_gap_over_threshold():
    events = [
        {"user_id": "u1", "page": "/home",    "timestamp": "2026-01-01T12:00:00+00:00"},
        {"user_id": "u1", "page": "/about",   "timestamp": "2026-01-01T12:00:05+00:00"},
        {"user_id": "u1", "page": "/contact", "timestamp": "2026-01-01T12:00:08+00:00"},
        {"user_id": "u1", "page": "/shop",    "timestamp": "2026-01-01T12:00:25+00:00"},
        {"user_id": "u1", "page": "/cart",    "timestamp": "2026-01-01T12:00:30+00:00"},
    ]
    sessions = _run_case(events)
    normalized = [_normalize_session(s) for s in sessions]

    assert len(normalized) == 2, (
        f"Expected exactly 2 sessions for u1, got {len(normalized)}: {normalized}"
    )
    for user_id, _count, _pages in normalized:
        assert user_id == "u1", f"Unexpected user_id in output: {user_id!r}"

    sorted_sessions = sorted(normalized, key=lambda x: x[2])
    session_a = sorted_sessions[0]
    session_b = sorted_sessions[1]

    assert session_a[1] == 3, f"Session A page_count expected 3, got {session_a[1]}."
    # Verify by event-time order specifically (ordered windows are the default).
    expected_first_session_pages = ("/home", "/about", "/contact")
    assert session_a[2] == expected_first_session_pages, (
        f"First session pages must be in event-time order {expected_first_session_pages}, got {session_a[2]}"
    )

    assert session_b[1] == 2, f"Session B page_count expected 2, got {session_b[1]}."
    expected_second_session_pages = ("/shop", "/cart")
    assert session_b[2] == expected_second_session_pages, (
        f"Second session pages must be in event-time order {expected_second_session_pages}, got {session_b[2]}"
    )


def test_multiple_users_interleaved_events():
    events = [
        {"user_id": "u1", "page": "/home",     "timestamp": "2026-01-01T12:00:00+00:00"},
        {"user_id": "u2", "page": "/products", "timestamp": "2026-01-01T12:00:02+00:00"},
        {"user_id": "u1", "page": "/about",    "timestamp": "2026-01-01T12:00:05+00:00"},
        {"user_id": "u1", "page": "/shop",     "timestamp": "2026-01-01T12:00:30+00:00"},
        {"user_id": "u2", "page": "/cart",     "timestamp": "2026-01-01T12:00:50+00:00"},
    ]
    sessions = _run_case(events)
    normalized = [_normalize_session(s) for s in sessions]

    by_user: Dict[str, List[Tuple[int, Tuple[str, ...]]]] = {}
    for user_id, count, pages in normalized:
        by_user.setdefault(user_id, []).append((count, pages))

    assert set(by_user.keys()) == {"u1", "u2"}, (
        f"Expected exactly users u1 and u2 in output, got {set(by_user.keys())}."
    )

    u1_sessions = sorted(by_user["u1"], key=lambda x: x[1])
    u2_sessions = sorted(by_user["u2"], key=lambda x: x[1])

    assert len(u1_sessions) == 2, (
        f"Expected 2 sessions for u1, got {len(u1_sessions)}: {u1_sessions}"
    )
    assert len(u2_sessions) == 2, (
        f"Expected 2 sessions for u2, got {len(u2_sessions)}: {u2_sessions}"
    )

    u1_page_sets = {pages for _, pages in u1_sessions}
    assert ("/home", "/about") in u1_page_sets, (
        f"Missing u1 session ('/home', '/about') in {u1_page_sets}."
    )
    assert ("/shop",) in u1_page_sets, (
        f"Missing u1 session ('/shop',) in {u1_page_sets}."
    )

    u2_page_sets = {pages for _, pages in u2_sessions}
    assert ("/products",) in u2_page_sets, (
        f"Missing u2 session ('/products',) in {u2_page_sets}."
    )
    assert ("/cart",) in u2_page_sets, (
        f"Missing u2 session ('/cart',) in {u2_page_sets}."
    )

    for count, pages in u1_sessions + u2_sessions:
        assert count == len(pages), (
            f"page_count {count} does not match number of pages {pages}."
        )


def test_integer_user_id_is_coerced_to_string():
    events = [
        {"user_id": 42, "page": "/home",  "timestamp": "2026-01-01T12:00:00+00:00"},
        {"user_id": 42, "page": "/about", "timestamp": "2026-01-01T12:00:04+00:00"},
    ]
    sessions = _run_case(events)
    normalized = [_normalize_session(s) for s in sessions]

    assert len(normalized) == 1, (
        f"Expected exactly 1 session for user 42, got {len(normalized)}: {normalized}"
    )
    user_id, count, pages = normalized[0]
    assert user_id == "42", (
        f"Expected stringified user_id '42', got {user_id!r} (type {type(user_id).__name__})."
    )
    assert count == 2, f"Expected page_count 2 for the single session, got {count}."
    assert pages == ("/home", "/about"), (
        f"Expected pages ('/home', '/about') in event-time order, got {pages}."
    )


def test_boundary_ten_second_gap_stays_in_same_session():
    events = [
        {"user_id": "u9", "page": "/a", "timestamp": "2026-01-01T12:00:00+00:00"},
        {"user_id": "u9", "page": "/b", "timestamp": "2026-01-01T12:00:10+00:00"},
    ]
    sessions = _run_case(events)
    normalized = [_normalize_session(s) for s in sessions]

    assert len(normalized) == 1, (
        f"A gap of exactly 10 seconds must stay in the same session, got {len(normalized)} sessions: {normalized}"
    )
    user_id, count, pages = normalized[0]
    assert user_id == "u9", f"Unexpected user_id: {user_id!r}"
    assert count == 2, f"Expected page_count 2, got {count}."
    assert pages == ("/a", "/b"), f"Expected pages ('/a', '/b') in order, got {pages}."
