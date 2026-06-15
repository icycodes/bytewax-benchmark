import json
import bytewax.dataflow as df
import bytewax.operators as op
from bytewax.connectors.files import FileSource
from bytewax.connectors.stdio import StdOutSink

flow = df.Dataflow('merge_flow')

def parse_user(line):
    data = json.loads(line)
    return str(data["user_id"]), data["name"]

def parse_email(line):
    data = json.loads(line)
    return str(data["id"]), data["email_address"]

def parse_activity(line):
    data = json.loads(line)
    return str(data["uid"]), data["last_login"]

users = op.input('users_in', flow, FileSource('users.jsonl'))
users_kv = op.map('users_kv', users, parse_user)

emails = op.input('emails_in', flow, FileSource('emails.jsonl'))
emails_kv = op.map('emails_kv', emails, parse_email)

activities = op.input('activities_in', flow, FileSource('activities.jsonl'))
activities_kv = op.map('activities_kv', activities, parse_activity)

joined = op.join('join', users_kv, emails_kv, activities_kv, emit_mode='complete')

def format_output(item):
    user_id, (name, email, last_login) = item
    return json.dumps({
        "id": user_id,
        "name": name,
        "email": email,
        "last_login": last_login
    })

formatted = op.map('format', joined, format_output)

op.output('out', formatted, StdOutSink())
