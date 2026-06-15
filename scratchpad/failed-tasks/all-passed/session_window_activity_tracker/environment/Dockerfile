FROM ubuntu:24.04

RUN apt-get update -y && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        curl wget ca-certificates git python3 python3-pip python3-pytest bash ripgrep && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip3 install --break-system-packages pytest-json-ctrf bytewax && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /home/user && chmod 0755 /home/user

RUN mkdir -p /home/user/project && \
    cd /home/user/project && \
    cat >input.jsonl <<'EOF'
{"user_id": "u1", "timestamp": "2023-01-01T10:00:00Z", "event_type": "login"}
{"user_id": "u1", "timestamp": "2023-01-01T10:00:02Z", "event_type": "view_item"}
{"user_id": "u1", "timestamp": "2023-01-01T10:00:08Z", "event_type": "add_to_cart"}
{"user_id": "u2", "timestamp": "2023-01-01T10:00:01Z", "event_type": "login"}
{"user_id": "u2", "timestamp": "2023-01-01T10:00:05Z", "event_type": "checkout"}
EOF

LABEL Author="generate-harbor-tasks"
LABEL Description="Bytewax dataflow task environment with input.jsonl in /home/user/project."

CMD ["/bin/bash", "-c", "trap : TERM INT; sleep infinity & wait"]

# Container contents:
#   - always required apt packages: curl wget ca-certificates git python3 python3-pip python3-pytest bash ripgrep
#   - always required pip3 packages: pytest-json-ctrf
#   - bytewax installed via pip3
#   - /home/user/project: project directory
#   - /home/user/project/input.jsonl: initial input data file
