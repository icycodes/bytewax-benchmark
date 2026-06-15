#!/usr/bin/env bash
set -euo pipefail

# run.sh - Execute the Bytewax running-max dataflow with recovery.
#
# Usage: bash run.sh <input_file> <output_file> <recovery_dir>
#
# Arguments:
#   input_file    - Path to the CSV input file (key,value pairs)
#   output_file   - Path to the output file (key,running_max)
#   recovery_dir  - Directory for SQLite-based state recovery

if [ $# -ne 3 ]; then
    echo "Usage: bash run.sh <input_file> <output_file> <recovery_dir>" >&2
    exit 1
fi

INPUT_FILE="$1"
OUTPUT_FILE="$2"
RECOVERY_DIR="$3"

# Resolve to absolute paths
INPUT_FILE=$(realpath "$INPUT_FILE")
OUTPUT_FILE=$(realpath "$OUTPUT_FILE")
RECOVERY_DIR=$(realpath -m "$RECOVERY_DIR")

# Create the recovery directory if it doesn't exist
mkdir -p "$RECOVERY_DIR"

# Initialize recovery partitions if not already done
# Check if the recovery DB already exists to avoid re-initialization errors
if [ ! -f "$RECOVERY_DIR/part-0.sqlite3" ]; then
    python3 -m bytewax.recovery "$RECOVERY_DIR" 1
fi

# Export paths as environment variables for the dataflow module
export BYTEWAX_INPUT_PATH="$INPUT_FILE"
export BYTEWAX_OUTPUT_PATH="$OUTPUT_FILE"

# Run the dataflow with recovery enabled
# -r: recovery directory for SQLite state persistence
# -s: snapshot interval in seconds (snapshot state every second)
# -b: backup interval in seconds
# The import string points to dataflow.py in the same directory
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

python3 -m bytewax.run \
    -r "$RECOVERY_DIR" \
    -s 1 \
    -b 10 \
    dataflow
