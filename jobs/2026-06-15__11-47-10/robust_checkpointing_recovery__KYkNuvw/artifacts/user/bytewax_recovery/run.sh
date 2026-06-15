#!/bin/bash

# Exit on any error
set -e

# Check arguments
if [ "$#" -ne 3 ]; then
    echo "Usage: bash run.sh <input_file> <output_file> <recovery_dir>"
    exit 1
fi

INPUT_FILE="$1"
OUTPUT_FILE="$2"
RECOVERY_DIR="$3"

# Export variables for python script
export INPUT_FILE
export OUTPUT_FILE
export RECOVERY_DIR

# Add the script's directory to PYTHONPATH so Bytewax can find pipeline:flow
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export PYTHONPATH="$SCRIPT_DIR:$PYTHONPATH"

# Ensure recovery directory exists
mkdir -p "$RECOVERY_DIR"

# Pre-initialize recovery directory if not already done
if [ ! -f "$RECOVERY_DIR/part-0.sqlite3" ]; then
    python3 -m bytewax.recovery "$RECOVERY_DIR" 1
fi

# Run the dataflow with recovery enabled
python3 -m bytewax.run pipeline:flow -r "$RECOVERY_DIR" -s 10 -b 0
