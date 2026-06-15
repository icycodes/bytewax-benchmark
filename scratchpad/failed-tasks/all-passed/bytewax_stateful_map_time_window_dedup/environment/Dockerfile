FROM ubuntu:24.04

RUN apt-get update -y && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        curl wget ca-certificates git python3 python3-pip python3-pytest bash ripgrep && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip3 install --break-system-packages pytest-json-ctrf bytewax==0.21.1 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /home/user && chmod 0755 /home/user

RUN mkdir -p /home/user/bytewax_project && \
    cd /home/user/bytewax_project && \
    cat >events.json <<'EOF'
{"user_id": "u1", "event_id": "e1", "timestamp": "2026-01-01T12:00:00Z", "payload": "A"}
{"user_id": "u1", "event_id": "e1", "timestamp": "2026-01-01T12:00:05Z", "payload": "A-dup"}
{"user_id": "u1", "event_id": "e2", "timestamp": "2026-01-01T12:00:08Z", "payload": "B"}
{"user_id": "u1", "event_id": "e1", "timestamp": "2026-01-01T12:00:11Z", "payload": "A-dup-after-10s"}
{"user_id": "u2", "event_id": "e1", "timestamp": "2026-01-01T12:00:00Z", "payload": "C"}
EOF

LABEL Author="generate-harbor-tasks"
LABEL Description="Bytewax environment for stateful event deduplication task."

CMD ["/bin/bash", "-c", "trap : TERM INT; sleep infinity & wait"]

# Container contents:
#   - always required apt packages: curl wget ca-certificates git python3 python3-pip python3-pytest bash ripgrep
#   - always required pip3 packages: pytest-json-ctrf
#   - bytewax==0.21.1 installed via pip3
#   - /home/user/bytewax_project: task directory
#   - /home/user/bytewax_project/events.json: initial events data
