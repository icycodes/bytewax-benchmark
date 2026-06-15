from pathlib import Path
from bytewax.connectors.files import CSVSource
source = CSVSource(Path("test.csv"))
parts = source.build_parts()
part = parts[list(parts.keys())[0]]
part.build()
print(part.next_batch())
