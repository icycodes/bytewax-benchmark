import json

import bytewax.operators as ops
from bytewax.dataflow import Dataflow
from bytewax.connectors.files import FileSource
from bytewax.connectors.stdio import StdOutSink

flow = Dataflow("merge")

# Read and parse users
users = ops.input("users_in", flow, FileSource("users.jsonl"))
users = ops.map("parse_users", users, json.loads)
users = ops.key_on("key_users", users, lambda d: str(d["user_id"]))
users = ops.map("users_name", users, lambda t: (t[0], t[1]["name"]))

# Read and parse emails
emails = ops.input("emails_in", flow, FileSource("emails.jsonl"))
emails = ops.map("parse_emails", emails, json.loads)
emails = ops.key_on("key_emails", emails, lambda d: str(d["id"]))
emails = ops.map("emails_addr", emails, lambda t: (t[0], t[1]["email_address"]))

# Read and parse activities
activities = ops.input("activities_in", flow, FileSource("activities.jsonl"))
activities = ops.map("parse_activities", activities, json.loads)
activities = ops.key_on("key_activities", activities, lambda d: str(d["uid"]))
activities = ops.map("activities_login", activities, lambda t: (t[0], t[1]["last_login"]))

# Join all three streams (inner join by default with emit_mode="complete")
joined = ops.join("join_all", users, emails, activities)


def format_profile(item):
    key, (name, email, last_login) = item
    return json.dumps({
        "id": key,
        "name": name,
        "email": email,
        "last_login": last_login,
    })


result = ops.map("format", joined, format_profile)

# Output to stdout
ops.output("stdout_out", result, StdOutSink())