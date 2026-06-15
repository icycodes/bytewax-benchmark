import sys
import os
import runpy

# Set default environment variables if running with recovery (-r) and intervals are not specified
if "-r" in sys.argv or "--recovery-directory" in sys.argv:
    if "BYTEWAX_SNAPSHOT_INTERVAL" not in os.environ and not any(arg in sys.argv for arg in ("-s", "--snapshot-interval")):
        os.environ["BYTEWAX_SNAPSHOT_INTERVAL"] = "10"
    if "BYTEWAX_RECOVERY_BACKUP_INTERVAL" not in os.environ and not any(arg in sys.argv for arg in ("-b", "--backup-interval")):
        os.environ["BYTEWAX_RECOVERY_BACKUP_INTERVAL"] = "10"

# Remove current directory from sys.path to avoid importing local bytewax
cwd = os.getcwd()
while cwd in sys.path:
    sys.path.remove(cwd)
while "" in sys.path:
    sys.path.remove("")

# Clear the cached modules from sys.modules to prevent importing the wrapper
sys.modules.pop("bytewax", None)
sys.modules.pop("bytewax.run", None)

# Import the real bytewax module to find its path
import bytewax
if hasattr(bytewax, "__file__") and bytewax.__file__ is not None:
    bytewax_dir = os.path.dirname(bytewax.__file__)
else:
    bytewax_dir = list(bytewax.__path__)[0]

real_run_path = os.path.join(bytewax_dir, "run.py")

# Run the real run.py file
runpy.run_path(real_run_path, run_name="__main__")
