import json
from bytewax.dataflow import Dataflow
import bytewax.operators as op
from bytewax.connectors.files import FileSource, FileSink

def parse_impression(line):
    return json.loads(line)

def parse_click(line):
    return json.loads(line)

def key_on_impression(record):
    return str(record['ad_id'])

def key_on_click(record):
    return str(record['ad_id'])

def format_output(key_and_values):
    ad_id, values = key_and_values
    impression, click = values
    return (ad_id, json.dumps({
        'ad_id': impression['ad_id'],
        'user_id': impression['user_id'],
        'click_time': click['click_time']
    }))

flow = Dataflow("ad_pipeline")

impressions_stream = op.input("impressions", flow, FileSource("impressions.jsonl"))
clicks_stream = op.input("clicks", flow, FileSource("clicks.jsonl"))

parsed_impressions = op.map("parse_impressions", impressions_stream, parse_impression)
parsed_clicks = op.map("parse_clicks", clicks_stream, parse_click)

keyed_impressions = op.key_on("key_impressions", parsed_impressions, key_on_impression)
keyed_clicks = op.key_on("key_clicks", parsed_clicks, key_on_click)

joined = op.join("join", keyed_impressions, keyed_clicks)

formatted = op.map("format_output", joined, format_output)

op.output("out", formatted, FileSink("joined.jsonl"))
