#!/usr/bin/env bash
# run.sh – Launch the Bytewax running-maximum dataflow with SQLite recovery.
#
# Usage:
#   bash run.sh <input_file> <output_file> <recovery_dir>
#
# Arguments:
#   input_file    Path to the CSV input file (key,value pairs, one per line).
#   output_file   Path where results will be written (key,running_max per line).
#   recovery_dir  Directory used to store / resume SQLite recovery partitions.
#
# On the first run the recovery partitions are created automatically.
# On subsequent runs the existing partitions are reused, so stateful running
# maximums continue from where the previous run left off.

set -euo pipefail

# ── Argument validation ────────────────────────────────────────────────────
if [[ $# -ne 3 ]]; then
    echo "Usage: bash run.sh <input_file> <output_file> <recovery_dir>" >&2
    exit 1
fi

INPUT_FILE="$1"
OUTPUT_FILE="$2"
RECOVERY_DIR="$3"

if [[ ! -f "$INPUT_FILE" ]]; then
    echo "Error: input file not found: $INPUT_FILE" >&2
    exit 1
fi

# ── Locate the dataflow script ─────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Ensure the output file exists (FileSink requires the file to be present) ─
touch "$OUTPUT_FILE"

# ── Ensure the recovery directory exists ──────────────────────────────────
mkdir -p "$RECOVERY_DIR"

# ── Initialise recovery partitions if none exist yet ──────────────────────
# bytewax.recovery expects *.sqlite3 partition files.
# We use 1 partition (single-worker mode). On subsequent runs the existing
# files are reused intact – no re-initialisation is performed.
PARTITION_FILE="$RECOVERY_DIR/part-0.sqlite3"
if [[ ! -f "$PARTITION_FILE" ]]; then
    echo "[run.sh] Initialising recovery partitions in: $RECOVERY_DIR"
    python -m bytewax.recovery "$RECOVERY_DIR" 1
else
    echo "[run.sh] Resuming from existing recovery partitions in: $RECOVERY_DIR"
fi

# ── Run the dataflow ───────────────────────────────────────────────────────
echo "[run.sh] Running dataflow: $INPUT_FILE -> $OUTPUT_FILE"
export INPUT_FILE OUTPUT_FILE

python -m bytewax.run \
    --recovery-directory "$RECOVERY_DIR" \
    --snapshot-interval 10 \
    --backup-interval 0 \
    "dataflow:flow" \
    --workers-per-process 1

echo "[run.sh] Done. Output written to: $OUTPUT_FILE"
