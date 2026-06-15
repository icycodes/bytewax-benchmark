#!/usr/bin/env bash
set -euo pipefail

INPUT_FILE="${1:?Usage: bash run.sh <input_file> <output_file> <recovery_dir>}"
OUTPUT_FILE="${2:?Usage: bash run.sh <input_file> <output_file> <recovery_dir>}"
RECOVERY_DIR="${3:?Usage: bash run.sh <input_file> <output_file> <recovery_dir>}"

# Create recovery directory if it doesn't exist
mkdir -p "$RECOVERY_DIR"

# Initialize recovery partitions only if they don't already exist
if [ ! -f "$RECOVERY_DIR/part-0.sqlite3" ]; then
    python -m bytewax.recovery "$RECOVERY_DIR" 1
fi

# Set environment variables for the dataflow module
export BYTEWAX_INPUT_PATH="$INPUT_FILE"
export BYTEWAX_OUTPUT_PATH="$OUTPUT_FILE"

# Run the dataflow with recovery enabled
# -s sets the snapshot interval in seconds (required for recovery)
# -b sets the backup interval in seconds (required for recovery)
python -m bytewax.run dataflow:get_flow -r "$RECOVERY_DIR" -s 1 -b 0