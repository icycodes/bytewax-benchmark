import bytewax.connectors
print(dir(bytewax.connectors))
from bytewax.connectors.files import FileSource, FileSink
print("FileSource", dir(FileSource))
