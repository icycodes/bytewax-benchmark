#!/bin/bash

if [ "$#" -ne 3 ]; then
    echo "Usage: $0 <input_file> <output_file> <recovery_dir>"
    exit 1
fi

INPUT_FILE=$1
OUTPUT_FILE=$2
RECOVERY_DIR=$3

# Create recovery dir if it doesn't exist
mkdir -p "$RECOVERY_DIR"

# Initialize recovery partitions if the directory is empty
if [ -z "$(ls -A "$RECOVERY_DIR")" ]; then
    python3 -m bytewax.recovery "$RECOVERY_DIR" 1
fi

export INPUT_FILE
export OUTPUT_FILE

# Run the dataflow
# -r specifies recovery directory
# -s 1 specifies snapshot interval
# -b 1 specifies backup interval
PYTHONPATH=$(dirname "$0") python3 -m bytewax.run -r "$RECOVERY_DIR" -s 1 -b 1 dataflow:flow
