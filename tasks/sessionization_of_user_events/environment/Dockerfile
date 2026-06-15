FROM ubuntu:24.04

RUN apt-get update -y && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        curl wget ca-certificates git python3 python3-pip python3-pytest bash ripgrep && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip3 install --break-system-packages pytest-json-ctrf bytewax && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /home/user && chmod 0755 /home/user

RUN mkdir -p /home/user/bytewax-sessionization && \
    cd /home/user/bytewax-sessionization && \
    cat >user_events.jsonl <<'EOF'
{"user_id": "user1", "event_type": "click", "timestamp": "2023-10-01T10:00:00Z"}
{"user_id": "user1", "event_type": "view", "timestamp": "2023-10-01T10:15:00Z"}
{"user_id": "user1", "event_type": "click", "timestamp": "2023-10-01T11:00:00Z"}
{"user_id": "user2", "event_type": "click", "timestamp": "2023-10-01T10:05:00Z"}
{"user_id": "user2", "event_type": "view", "timestamp": "2023-10-01T10:20:00Z"}
{"user_id": "user2", "event_type": "click", "timestamp": "2023-10-01T10:30:00Z"}
EOF

LABEL Author="generate-harbor-tasks"
LABEL Description="Bytewax dataflow environment for sessionization of user events."

CMD ["/bin/bash", "-c", "trap : TERM INT; sleep infinity & wait"]

# Container contents:
#   - always required apt packages: curl wget ca-certificates git python3 python3-pip python3-pytest bash ripgrep
#   - always required pip3 packages: pytest-json-ctrf
#   - bytewax installed via pip3
#   - /home/user/bytewax-sessionization: project directory
#   - /home/user/bytewax-sessionization/user_events.jsonl: initial user events data
