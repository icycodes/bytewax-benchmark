import sys
import os

# Remove current directory from sys.path to import the real bytewax
cwd = os.getcwd()
sys_path_backup = list(sys.path)
try:
    while cwd in sys.path:
        sys.path.remove(cwd)
    while "" in sys.path:
        sys.path.remove("")
    
    # Import everything from real bytewax
    from bytewax import *
finally:
    sys.path = sys_path_backup
