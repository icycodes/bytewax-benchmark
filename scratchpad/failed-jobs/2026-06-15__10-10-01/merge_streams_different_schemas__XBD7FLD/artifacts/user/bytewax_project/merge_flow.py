import json

from bytewax.dataflow import Dataflow
from bytewax.connectors.files import FileSource
from bytewax.connectors.stdio import StdOutSink
import bytewax.operators as op

flow = Dataflow("merge_flow")


def parse_user(line: str):
    """Parse a line from users.jsonl and emit (key, value) where key is str(user_id)."""
    obj = json.loads(line)
    key = str(obj["user_id"])
    return (key, obj["name"])


def parse_email(line: str):
    """Parse a line from emails.jsonl and emit (key, value) where key is str(id)."""
    obj = json.loads(line)
    key = str(obj["id"])
    return (key, obj["email_address"])


def parse_activity(line: str):
    """Parse a line from activities.jsonl and emit (key, value) where key is str(uid)."""
    obj = json.loads(line)
    key = str(obj["uid"])
    return (key, obj["last_login"])


def format_profile(joined):
    """Format a joined tuple into a unified profile JSON string."""
    key, (name, email, last_login) = joined
    profile = {
        "id": key,
        "name": name,
        "email": email,
        "last_login": last_login,
    }
    return json.dumps(profile)


# Read and key the three input streams
users = op.input("users", flow, FileSource("users.jsonl"))
users_kv = op.map("parse_users", users, parse_user)

emails = op.input("emails", flow, FileSource("emails.jsonl"))
emails_kv = op.map("parse_emails", emails, parse_email)

activities = op.input("activities", flow, FileSource("activities.jsonl"))
activities_kv = op.map("parse_activities", activities, parse_activity)

# Inner join on user ID — only emit when all three sides have a value
joined = op.join("join_profiles", users_kv, emails_kv, activities_kv, emit_mode="complete")

# Format into unified profile JSON strings
profiles = op.map("format_profiles", joined, format_profile)

# Output to stdout
op.output("stdout_sink", profiles, StdOutSink())
