FROM ubuntu:24.04

RUN apt-get update -y && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        curl wget ca-certificates git python3 python3-pip python3-pytest bash ripgrep && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip3 install --break-system-packages pytest-json-ctrf bytewax==0.21.1 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /home/user && chmod 0755 /home/user

RUN mkdir -p /home/user/anomaly_detection && \
    cd /home/user/anomaly_detection && \
    cat >data.jsonl <<'EOF'
{"sensor_id": "sensor_1", "timestamp": "2026-01-01T12:00:00Z", "value": 20.0}
{"sensor_id": "sensor_1", "timestamp": "2026-01-01T12:00:05Z", "value": 20.0}
{"sensor_id": "sensor_1", "timestamp": "2026-01-01T12:00:10Z", "value": 20.0}
{"sensor_id": "sensor_1", "timestamp": "2026-01-01T12:00:15Z", "value": 20.0}
{"sensor_id": "sensor_1", "timestamp": "2026-01-01T12:00:20Z", "value": 20.0}
{"sensor_id": "sensor_1", "timestamp": "2026-01-01T12:00:25Z", "value": 20.0}
{"sensor_id": "sensor_1", "timestamp": "2026-01-01T12:00:30Z", "value": 20.0}
{"sensor_id": "sensor_1", "timestamp": "2026-01-01T12:00:35Z", "value": 20.0}
{"sensor_id": "sensor_1", "timestamp": "2026-01-01T12:00:40Z", "value": 20.0}
{"sensor_id": "sensor_1", "timestamp": "2026-01-01T12:00:45Z", "value": 20.0}
{"sensor_id": "sensor_1", "timestamp": "2026-01-01T12:00:50Z", "value": 100.0}
{"sensor_id": "sensor_1", "timestamp": "2026-01-01T12:01:05Z", "value": 20.0}
{"sensor_id": "sensor_1", "timestamp": "2026-01-01T12:02:00Z", "value": 20.0}
EOF

LABEL Author="generate-harbor-tasks"
LABEL Description="Bytewax environment for anomaly detection streaming task."

CMD ["/bin/bash", "-c", "trap : TERM INT; sleep infinity & wait"]

# Container contents:
#   - always required apt packages: curl wget ca-certificates git python3 python3-pip python3-pytest bash ripgrep
#   - always required pip3 packages: pytest-json-ctrf
#   - bytewax==0.21.1 installed via pip3
#   - /home/user/anomaly_detection: project directory
#   - /home/user/anomaly_detection/data.jsonl: initial sample data
