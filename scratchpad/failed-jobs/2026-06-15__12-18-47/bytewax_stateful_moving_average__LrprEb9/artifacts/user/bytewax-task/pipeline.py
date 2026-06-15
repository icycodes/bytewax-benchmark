import bytewax.operators as op
from bytewax.dataflow import Dataflow
from bytewax.connectors.files import FileSource, FileSink

class MovingAverageState:
    def __init__(self):
        self.values = []

    def update(self, value):
        new_values = self.values + [value]
        if len(new_values) > 3:
            new_values = new_values[-3:]
        
        avg = sum(new_values) / len(new_values)
        new_state = MovingAverageState()
        new_state.values = new_values
        return new_state, avg

def mapper(state, value):
    if state is None:
        state = MovingAverageState()
    new_state, avg = state.update(value)
    return new_state, avg

def parse_line(line):
    parts = line.strip().split(',')
    if len(parts) == 2:
        try:
            return (parts[0], float(parts[1]))
        except ValueError:
            return None
    return None

def format_output(key_item):
    key, avg = key_item
    return (key, f"{key},{avg:.2f}")

flow = Dataflow("flow")
inp = op.input("input", flow, FileSource("input.csv", batch_size=1))
parsed = op.filter_map("parse", inp, parse_line)
mapped = op.stateful_map("moving_average", parsed, mapper)
formatted = op.map("format", mapped, format_output)
op.output("output", formatted, FileSink("output.csv"))
