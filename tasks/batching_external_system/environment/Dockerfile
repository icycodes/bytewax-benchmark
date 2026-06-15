FROM ubuntu:24.04

RUN apt-get update -y && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        curl wget ca-certificates git python3 python3-pip python3-pytest bash ripgrep sqlite3 && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip3 install --break-system-packages pytest-json-ctrf bytewax && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /home/user && chmod 0755 /home/user

RUN mkdir -p /home/user/bytewax_batching && \
    cd /home/user/bytewax_batching && \
    cat >input.jsonl <<'EOF'
{"device_id": "dev_01", "timestamp": "2026-06-15T10:00:00Z", "metric_value": 10.5}
{"device_id": "dev_01", "timestamp": "2026-06-15T10:00:01Z", "metric_value": 11.0}
{"device_id": "dev_01", "timestamp": "2026-06-15T10:00:02Z", "metric_value": -5.0}
{"device_id": "dev_01", "timestamp": "2026-06-15T10:00:03Z", "metric_value": 12.5}
{"device_id": "dev_01", "timestamp": "2026-06-15T10:00:04Z", "metric_value": 13.0}
{"device_id": "dev_02", "timestamp": "2026-06-15T10:00:00Z", "metric_value": 20.5}
{"device_id": "dev_02", "timestamp": "2026-06-15T10:00:01Z", "metric_value": 21.0}
{"device_id": "dev_02", "timestamp": "2026-06-15T10:00:02Z", "metric_value": 22.5}
{"device_id": "dev_02", "timestamp": "2026-06-15T10:00:03Z", "metric_value": 23.0}
{"device_id": "dev_02", "timestamp": "2026-06-15T10:00:04Z", "metric_value": 24.5}
{"device_id": "dev_01", "timestamp": "2026-06-15T10:00:05Z", "metric_value": 14.0}
{"device_id": "dev_01", "timestamp": "2026-06-15T10:00:06Z", "metric_value": 15.5}
{"device_id": "dev_01", "timestamp": "2026-06-15T10:00:07Z", "metric_value": 16.0}
{"device_id": "dev_01", "timestamp": "2026-06-15T10:00:08Z", "metric_value": 17.5}
{"device_id": "dev_01", "timestamp": "2026-06-15T10:00:09Z", "metric_value": 18.0}
{"device_id": "dev_03", "timestamp": "2026-06-15T10:00:00Z", "metric_value": 30.5}
{"device_id": "dev_03", "timestamp": "2026-06-15T10:00:01Z", "metric_value": -1.0}
{"device_id": "dev_03", "timestamp": "2026-06-15T10:00:02Z", "metric_value": 32.5}
{"device_id": "dev_03", "timestamp": "2026-06-15T10:00:03Z", "metric_value": 33.0}
{"device_id": "dev_03", "timestamp": "2026-06-15T10:00:04Z", "metric_value": 34.5}
{"device_id": "dev_03", "timestamp": "2026-06-15T10:00:05Z", "metric_value": 35.0}
{"device_id": "dev_03", "timestamp": "2026-06-15T10:00:06Z", "metric_value": 36.5}
{"device_id": "dev_03", "timestamp": "2026-06-15T10:00:07Z", "metric_value": -10.0}
EOF

LABEL Author="generate-harbor-tasks"
LABEL Description="Bytewax batching task environment with sqlite3 and input data."

CMD ["/bin/bash", "-c", "trap : TERM INT; sleep infinity & wait"]

# Container contents:
#   - always required apt packages: curl wget ca-certificates git python3 python3-pip python3-pytest bash ripgrep
#   - additional apt packages: sqlite3
#   - always required pip3 packages: pytest-json-ctrf
#   - target library: bytewax installed via pip3
#   - /home/user/bytewax_batching: task project directory
#   - /home/user/bytewax_batching/input.jsonl: initial input data containing 20 valid and 3 invalid records
