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
    cat >users.jsonl <<'EOF'
{"user_id": 1, "name": "Alice"}
{"user_id": 2, "name": "Bob"}
{"user_id": 3, "name": "Charlie"}
EOF

RUN cd /home/user/bytewax_project && \
    cat >emails.jsonl <<'EOF'
{"id": "1", "email_address": "alice@example.com"}
{"id": 2, "email_address": "bob@example.com"}
EOF

RUN cd /home/user/bytewax_project && \
    cat >activities.jsonl <<'EOF'
{"uid": 1, "last_login": "2026-01-01"}
{"uid": "2", "last_login": "2026-01-02"}
{"uid": 4, "last_login": "2026-01-04"}
EOF

LABEL Author="generate-harbor-tasks"
LABEL Description="Bytewax environment with pre-existing user data streams to be merged."

CMD ["/bin/bash", "-c", "trap : TERM INT; sleep infinity & wait"]

# Container contents:
#   - always required apt packages: curl wget ca-certificates git python3 python3-pip python3-pytest bash ripgrep
#   - always required pip3 packages: pytest-json-ctrf
#   - bytewax==0.21.1 installed via pip3
#   - /home/user/bytewax_project: project directory
#   - /home/user/bytewax_project/*.jsonl: initial data files
