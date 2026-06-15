"""Merge three JSONL streams into unified user profiles using Bytewax."""

import json

from bytewax.connectors.files import FileSource
from bytewax.connectors.stdio import StdOutSink
from bytewax.dataflow import Dataflow
import bytewax.operators as ops


def _parse_users(line: str):
    """Parse a users.jsonl line and return (str_id, name)."""
    record = json.loads(line)
    return (str(record["user_id"]), record["name"])


def _parse_emails(line: str):
    """Parse an emails.jsonl line and return (str_id, email_address)."""
    record = json.loads(line)
    return (str(record["id"]), record["email_address"])


def _parse_activities(line: str):
    """Parse an activities.jsonl line and return (str_id, last_login)."""
    record = json.loads(line)
    return (str(record["uid"]), record["last_login"])


def _build_profile(key_and_values):
    """Convert joined tuple to a JSON string profile."""
    key, (name, email, last_login) = key_and_values
    profile = {
        "id": key,
        "name": name,
        "email": email,
        "last_login": last_login,
    }
    return json.dumps(profile)


flow = Dataflow("merge_profiles")

# --- Input streams ---
users_raw = ops.input("users_input", flow, FileSource("users.jsonl"))
emails_raw = ops.input("emails_input", flow, FileSource("emails.jsonl"))
activities_raw = ops.input("activities_input", flow, FileSource("activities.jsonl"))

# --- Parse and key each stream by the normalised string user ID ---
users_keyed = ops.map("parse_users", users_raw, _parse_users)
emails_keyed = ops.map("parse_emails", emails_raw, _parse_emails)
activities_keyed = ops.map("parse_activities", activities_raw, _parse_activities)

# --- Inner join: only emit when all three sides have data for a key ---
# insert_mode="last"  – keep the latest value per key per side
# emit_mode="complete" – only emit when every side has at least one value (inner join)
joined = ops.join(
    "join_profiles",
    users_keyed,
    emails_keyed,
    activities_keyed,
    insert_mode="last",
    emit_mode="complete",
)

# --- Build the final JSON string ---
profiles = ops.map("build_profile", joined, _build_profile)

# --- Write to stdout ---
ops.output("stdout_output", profiles, StdOutSink())
