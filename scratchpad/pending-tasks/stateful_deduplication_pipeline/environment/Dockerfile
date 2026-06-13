FROM ubuntu:24.04

RUN apt-get update -y && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        curl wget ca-certificates git python3 python3-pip python3-pytest bash ripgrep && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    pip3 install --break-system-packages pytest-json-ctrf bytewax==0.21.1 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /home/user && chmod 0755 /home/user

RUN mkdir -p /home/user/myproject/data

RUN cat >/home/user/myproject/data/events.jsonl <<'EOF'
{"message_id": "m1", "user_id": "alice", "payload": {"action": "login", "ts": 1}}
{"message_id": "m2", "user_id": "alice", "payload": {"action": "view", "ts": 2}}
{"message_id": "m1", "user_id": "alice", "payload": {"action": "login_RETRY", "ts": 3}}
{"message_id": "m1", "user_id": "bob", "payload": {"action": "login", "ts": 4}}
{"message_id": "m3", "user_id": "alice", "payload": {"action": "purchase", "ts": 5}}
{"message_id": "m2", "user_id": "alice", "payload": {"action": "view_RETRY", "ts": 6}}
{"message_id": "m1", "user_id": "bob", "payload": {"action": "login_RETRY", "ts": 7}}
{"message_id": "m2", "user_id": "bob", "payload": {"action": "logout", "ts": 8}}
EOF

LABEL Author="generate-harbor-tasks"
LABEL Description="Bytewax 0.21.1 stateful deduplication pipeline task environment with a pre-populated JSON-lines input file."

CMD ["/bin/bash", "-c", "trap : TERM INT; sleep infinity & wait"]

# Container contents:
#   - always required apt packages: curl wget ca-certificates git python3 python3-pip python3-pytest bash ripgrep
#   - always required pip3 packages: pytest-json-ctrf
#   - bytewax==0.21.1 installed via pip3 for the streaming dataflow task
#   - /home/user/myproject: project directory the executor works in
#   - /home/user/myproject/data: data directory pre-populated with events.jsonl
#   - /home/user/myproject/data/events.jsonl: 8-line JSON-lines input with duplicates
