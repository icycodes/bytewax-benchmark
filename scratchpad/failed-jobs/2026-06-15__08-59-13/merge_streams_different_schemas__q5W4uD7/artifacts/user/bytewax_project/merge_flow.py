import json
from pathlib import Path
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSource
from bytewax.connectors.stdio import StdOutSink

# Define the dataflow
flow = Dataflow("merge_flow")

# File paths
users_path = Path("users.jsonl")
emails_path = Path("emails.jsonl")
activities_path = Path("activities.jsonl")

# Helper functions to parse and format streams
def parse_user(line: str):
    line = line.strip()
    if not line:
        return None
    try:
        data = json.loads(line)
        user_id = str(data["user_id"])
        name = data["name"]
        return (user_id, name)
    except (json.JSONDecodeError, KeyError):
        return None

def parse_email(line: str):
    line = line.strip()
    if not line:
        return None
    try:
        data = json.loads(line)
        user_id = str(data["id"])
        email = data["email_address"]
        return (user_id, email)
    except (json.JSONDecodeError, KeyError):
        return None

def parse_activity(line: str):
    line = line.strip()
    if not line:
        return None
    try:
        data = json.loads(line)
        user_id = str(data["uid"])
        last_login = data["last_login"]
        return (user_id, last_login)
    except (json.JSONDecodeError, KeyError):
        return None

def format_profile(item):
    # item is (user_id, (name, email, last_login))
    user_id, (name, email, last_login) = item
    output_dict = {
        "id": user_id,
        "name": name,
        "email": email,
        "last_login": last_login
    }
    return json.dumps(output_dict)

# Input streams
users_raw = op.input("users_input", flow, FileSource(users_path))
emails_raw = op.input("emails_input", flow, FileSource(emails_path))
activities_raw = op.input("activities_input", flow, FileSource(activities_path))

# Parse streams to (user_id, value)
users_stream = op.filter_map("parse_users", users_raw, parse_user)
emails_stream = op.filter_map("parse_emails", emails_raw, parse_email)
activities_stream = op.filter_map("parse_activities", activities_raw, parse_activity)

# Inner join of the three streams
# Defaults for op.join: insert_mode="last", emit_mode="complete"
joined = op.join("join_streams", users_stream, emails_stream, activities_stream)

# Format to JSON string
formatted = op.map("format_output", joined, format_profile)

# Output to standard output
op.output("stdout_output", formatted, StdOutSink())
